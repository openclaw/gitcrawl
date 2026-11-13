package remotedelivery

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store"
)

const reportOutboxVersion = 2

// InventoryMigrationLimits bounds saved request/receipt bytes and decoded rows.
// The caller must also supply a deadline. No native source is opened.
type InventoryMigrationLimits struct{ MaxRows, MaxBytes int64 }

// ReportOutboxState describes qualified custody, not current source authority.
// ServerCursor and ReceiverSequence remain the qualified baseline checkpoint.
type ReportOutboxState struct {
	Registration                                 SeedRegistration
	BaselineAnchor                               store.NativeReportAnchor
	BaselineNative, BaselineSHA256, ServerCursor string
	InventorySHA256, InventoryLayout             string
	ReceiverSequence                             int64
	ActiveChangeID                               string
}

const inventoryMigrationSchema = `
alter table binding add column baseline_anchor_json blob;
alter table binding add column inventory_sha256 text;
alter table binding add column inventory_layout text;
alter table binding add column receiver_sequence integer;
create table inventory_rows (
 table_name text not null,
 encoded_key blob not null check(typeof(encoded_key)='blob' and length(encoded_key)>0),
 row_sha256 blob not null check(typeof(row_sha256)='blob' and length(row_sha256)=32),
 key_json blob not null,
 repository_id integer not null check(repository_id>0),
 thread_id integer, thread_number integer, node_id text,
 primary key(table_name,encoded_key)
) without rowid;
create index inventory_thread_id on inventory_rows(thread_id,table_name);
create index inventory_thread_number on inventory_rows(thread_number,table_name);
create index inventory_node_id on inventory_rows(node_id,table_name);
`

// MigrateSeedOutboxV2 explicitly upgrades one fully receipted seed. Exact seed
// bodies/receipts and the original v1 token remain untouched. Failure rolls back
// all DDL/index/checkpoint changes; migration never retries or rebinds a file.
func MigrateSeedOutboxV2(ctx context.Context, path string, limits InventoryMigrationLimits) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok || limits.MaxRows <= 0 || limits.MaxBytes <= 0 {
		return errors.New("outbox inventory migration requires a deadline and positive limits")
	}
	o, err := OpenSeedOutbox(ctx, path)
	if err != nil {
		return err
	}
	defer o.Close()
	return o.write(ctx, func(conn *sql.Conn) error {
		// Recheck after acquiring the writer transaction; a prior open is not
		// permission to migrate a concurrently advanced outbox.
		if err := inventoryVersion(ctx, conn, seedOutboxVersion, 3); err != nil {
			return err
		}
		s, err := readSeedState(ctx, conn)
		if err != nil {
			return err
		}
		if s.BaselineState != BaselineQualified || s.NativeRows > limits.MaxRows || s.NativeBytes > limits.MaxBytes {
			return ErrSeedOutbox
		}
		if err := validateSeedRegistration(s.Registration); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, inventoryMigrationSchema); err != nil {
			return err
		}
		codec, err := privateInventoryCodec()
		if err != nil {
			return err
		}
		var plan *remote.GitHubSeedPlan
		var used, seen, nativeBytes int64
		for _, layout := range privateLayouts {
			for _, column := range strings.Fields(layout.columns) {
				nativeBytes += int64(len(column))
			}
		}
		for index := 0; index < s.PartCount; index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			var body, receiptBytes []byte
			var hash string
			var size int64
			if err := conn.QueryRowContext(ctx, `select length(cast(body as blob))+length(cast(receipt_json as blob)) from parts where seed_id=? and part_index=?`, s.SeedID, index).Scan(&size); err != nil {
				return err
			}
			if size <= 0 || size > limits.MaxBytes-used {
				return ErrSeedOutbox
			}
			used += size
			if err := conn.QueryRowContext(ctx, `select body,payload_sha256,receipt_json from parts where seed_id=? and part_index=?`, s.SeedID, index).Scan(&body, &hash, &receiptBytes); err != nil {
				return err
			}
			if privateSeedHash(body) != hash {
				return ErrSeedOutbox
			}
			var receipt remote.SeedReceipt
			if json.Unmarshal(receiptBytes, &receipt) != nil {
				return ErrSeedOutbox
			}
			batch := seedPartBatch(s.SeedID, index, s.PartCount)
			if receipt.App != "gitcrawl" || receipt.Archive != s.Registration.Archive || receipt.BindingID != s.Registration.BindingID || receipt.BatchID != batch || receipt.Cursor != batch || receipt.Sequence != strconv.Itoa(index+1) || receipt.PayloadSHA256 != hash {
				return ErrSeedOutbox
			}
			if index == 0 {
				plan, err = remote.DecodeGitHubSeedManifest(body)
				if err != nil {
					return err
				}
				if err := inventorySeedIdentity(s, plan.Manifest()); err != nil {
					return err
				}
				continue
			}
			if index == s.PartCount-1 {
				if _, err := plan.ValidateComplete(body); err != nil {
					return err
				}
				continue
			}
			chunk, err := plan.ValidateChunk(body)
			if err != nil {
				return err
			}
			if chunk.Index != index-1 || int64(len(chunk.Changes)) > limits.MaxRows-seen {
				return ErrSeedOutbox
			}
			for _, change := range chunk.Changes {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := insertInventoryRow(ctx, conn, codec, s.Anchor, change); err != nil {
					return err
				}
				for _, cell := range change.Row {
					n := int64(1)
					if cell.Kind == "integer" {
						n = 9
					}
					if cell.Kind == "text" {
						n = 9 + int64(len(*cell.Value))
					}
					if n > limits.MaxBytes-nativeBytes {
						return ErrSeedOutbox
					}
					nativeBytes += n
				}
				seen++
			}
		}
		if seen != s.NativeRows || nativeBytes != s.NativeBytes {
			return ErrSeedOutbox
		}
		// Resolve children through the retained native thread ID, never a provider
		// ID. Streaming has finished, so no open part cursor overlaps these writes.
		if _, err := conn.ExecContext(ctx, `update inventory_rows as child set thread_number=(select parent.thread_number from inventory_rows parent where parent.table_name='threads' and parent.thread_id=child.thread_id) where child.table_name in ('comments','thread_child_observation_reservations','thread_child_observation_memberships')`); err != nil {
			return err
		}
		var invalid, repositories int
		if err := conn.QueryRowContext(ctx, `select count(*) from inventory_rows where thread_id is not null and thread_number is null`).Scan(&invalid); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `select count(*) from inventory_rows where table_name='repositories'`).Scan(&repositories); err != nil {
			return err
		}
		if invalid != 0 || repositories != 1 {
			return ErrSeedOutbox
		}
		digest, err := digestInventoryRows(ctx, conn, codec, s.Anchor.ScopeSHA256, limits.MaxRows)
		if err != nil {
			return err
		}
		// Retain the exact original anchor bytes as well as the original token.
		if _, err := conn.ExecContext(ctx, `update binding set baseline_anchor_json=(select anchor_json from captures where seed_id=?),inventory_sha256=?,inventory_layout=?,receiver_sequence=? where singleton=1`, s.SeedID, digest, privateInventoryLayoutID, s.PartCount); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `pragma user_version=2`)
		return err
	})
}

func inventorySeedIdentity(s SeedState, m remote.GitHubSeedManifest) error {
	a, c := s.Anchor, s.Anchor.Cursor
	through := strings.Join([]string{"gitcrawl-native-v1", c.Epoch, strconv.FormatInt(c.Generation, 10), strconv.FormatInt(c.SchemaVersion, 10), strconv.FormatInt(c.Sequence, 10), strconv.FormatInt(c.PrunedThrough, 10), a.ScopeSHA256, s.NativeSHA256}, ":")
	count := 2
	var rows int64
	for _, table := range m.Tables {
		count += len(table.Chunks)
		n, err := strconv.ParseInt(table.Rows, 10, 64)
		if err != nil || n > s.NativeRows-rows {
			return ErrSeedOutbox
		}
		rows += n
	}
	if m.Archive != s.Registration.Archive || m.BindingID != s.Registration.BindingID || m.SeedID != s.SeedID || m.Repository != a.Repository || a.Repository != s.Registration.Repository || a.File != s.Registration.File || m.RepositoryID != strconv.FormatInt(a.RepositoryID, 10) || m.ScopeSHA256 != a.ScopeSHA256 || m.NativeSHA256 != s.NativeSHA256 || m.NativeThrough != s.NativeThrough || through != m.NativeThrough || count != s.PartCount || rows != s.NativeRows {
		return ErrSeedOutbox
	}
	return nil
}

type compactInventoryRow struct {
	Table                          string
	Key, SHA256, KeyJSON           []byte
	RepositoryID                   int64
	ThreadID, ThreadNumber, NodeID any
}

func insertInventoryRow(ctx context.Context, conn *sql.Conn, codec *remote.InventoryCodec, anchor store.NativeReportAnchor, change remote.Change) error {
	row, err := deriveInventoryRow(ctx, conn, codec, anchor, change)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `insert into inventory_rows(table_name,encoded_key,row_sha256,key_json,repository_id,thread_id,thread_number,node_id) values(?,?,?,?,?,?,?,?)`, row.Table, row.Key, row.SHA256, row.KeyJSON, row.RepositoryID, row.ThreadID, row.ThreadNumber, row.NodeID)
	return err
}

func deriveInventoryRow(ctx context.Context, conn *sql.Conn, codec *remote.InventoryCodec, anchor store.NativeReportAnchor, change remote.Change) (compactInventoryRow, error) {
	key, err := codec.EncodeKey(change.Table, change.Key)
	if err != nil {
		return compactInventoryRow{}, err
	}
	hash, err := codec.RowDigest(change.Table, change.Key, change.Row)
	if err != nil {
		return compactInventoryRow{}, err
	}
	cell := func(name string) remote.ChangeValue { return change.Row[deliveryColumn(change.Table, name)] }
	integer := func(name string) (int64, error) { return deliveryInteger(cell(name)) }
	text := func(name string) (string, error) {
		v := cell(name)
		if v.Kind != "text" || v.Value == nil {
			return "", ErrSeedOutbox
		}
		return *v.Value, nil
	}
	var threadID, number any
	var node any
	switch change.Table {
	case "repositories":
		id, e := integer("id")
		name, e2 := text("full_name")
		if e != nil || e2 != nil || id != anchor.RepositoryID || strings.ToLower(name) != anchor.Repository {
			return compactInventoryRow{}, ErrSeedOutbox
		}
	case "threads":
		id, e := integer("repo_id")
		tid, e2 := integer("id")
		n, e3 := integer("number")
		if e != nil || e2 != nil || e3 != nil || id != anchor.RepositoryID {
			return compactInventoryRow{}, ErrSeedOutbox
		}
		threadID, number = tid, n
	case "comments", "thread_child_observation_reservations", "thread_child_observation_memberships":
		tid, e := integer("thread_id")
		if e != nil {
			return compactInventoryRow{}, e
		}
		threadID = tid
		if change.Table == "comments" {
			if cell("raw_json_blob_id").Kind != "null" {
				return compactInventoryRow{}, ErrSeedOutbox
			}
		} else {
			family, e := text("family")
			if e != nil || family != "comments" {
				return compactInventoryRow{}, ErrSeedOutbox
			}
		}
	case "thread_exclusions", "thread_excluded_nodes":
		repo, e := text("repository")
		n, e2 := integer("number")
		if e != nil || e2 != nil || repo != anchor.Repository {
			return compactInventoryRow{}, ErrSeedOutbox
		}
		number = n
		if change.Table == "thread_excluded_nodes" {
			id, e := text("node_id")
			if e != nil {
				return compactInventoryRow{}, e
			}
			node = id
		}
	default:
		return compactInventoryRow{}, ErrSeedOutbox
	}
	if change.Table == "threads" || change.Table == "comments" {
		var raw any
		v := cell("raw_json")
		if v.Value != nil {
			raw = *v.Value
		}
		// Match the native owner's guarded JSON extraction and TEXT affinity,
		// including integers beyond float64 precision, JSON null and malformed JSON.
		var value sql.NullString
		if err := conn.QueryRowContext(ctx, `select cast(json_extract(case when json_valid(?) then ? else '{}' end,'$.node_id') as text)`, raw, raw).Scan(&value); err != nil {
			return compactInventoryRow{}, err
		}
		if value.Valid {
			node = value.String
		}
	}
	keyJSON, err := json.Marshal(change.Key)
	if err != nil {
		return compactInventoryRow{}, err
	}
	return compactInventoryRow{Table: change.Table, Key: key, SHA256: hash[:], KeyJSON: keyJSON, RepositoryID: anchor.RepositoryID, ThreadID: threadID, ThreadNumber: number, NodeID: node}, nil
}

func digestInventoryRows(ctx context.Context, conn *sql.Conn, codec *remote.InventoryCodec, scope string, maxRows int64) (string, error) {
	counts := make([]remote.InventoryCount, 0, len(privateLayouts))
	for _, layout := range privateLayouts {
		var count uint64
		if err := conn.QueryRowContext(ctx, `select count(*) from inventory_rows where table_name=?`, layout.name).Scan(&count); err != nil {
			return "", err
		}
		counts = append(counts, remote.InventoryCount{Table: layout.name, Rows: count})
	}
	rows, err := conn.QueryContext(ctx, `select table_name,encoded_key,row_sha256 from inventory_rows order by table_name collate binary,encoded_key`)
	if err != nil {
		return "", err
	}
	return digestInventoryCursor(ctx, codec, scope, counts, uint64(maxRows), rows)
}

func digestInventoryCursor(ctx context.Context, codec *remote.InventoryCodec, scope string, counts []remote.InventoryCount, maxRows uint64, rows *sql.Rows) (string, error) {
	defer rows.Close()
	// Private profile scope encoding is the 64 lowercase hexadecimal ASCII
	// bytes, not the decoded 32-byte digest. This is part of inventory identity.
	hash, err := codec.Digest(ctx, []byte(scope), counts, remote.InventoryLimits{MaxEntries: maxRows, MaxKeyBytes: remote.MaxChangeBytes}, func(ctx context.Context) (remote.InventoryEntry, error) {
		if err := ctx.Err(); err != nil {
			return remote.InventoryEntry{}, err
		}
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return remote.InventoryEntry{}, err
			}
			return remote.InventoryEntry{}, io.EOF
		}
		var entry remote.InventoryEntry
		var hash []byte
		if err := rows.Scan(&entry.Table, &entry.Key, &hash); err != nil {
			return remote.InventoryEntry{}, err
		}
		if len(hash) != len(entry.RowSHA256) {
			return remote.InventoryEntry{}, ErrSeedOutbox
		}
		copy(entry.RowSHA256[:], hash)
		return entry, nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash[:]), nil
}

func inventoryVersion(ctx context.Context, q seedQuery, version, tables int) error {
	var app, gotVersion, gotTables int
	if err := q.QueryRowContext(ctx, `pragma application_id`).Scan(&app); err != nil {
		return err
	}
	if err := q.QueryRowContext(ctx, `pragma user_version`).Scan(&gotVersion); err != nil {
		return err
	}
	if err := q.QueryRowContext(ctx, `select count(*) from sqlite_schema where type='table' and name not like 'sqlite_%'`).Scan(&gotTables); err != nil {
		return err
	}
	if app != seedOutboxApplicationID || gotVersion != version || gotTables != tables {
		return ErrSeedOutbox
	}
	return nil
}

// ReadReportOutboxState admits schema 2/3 without opening a writer or reading
// native state. It reads the current binding independently of seed history;
// future delta settlement must not reinterpret the old seed as the new baseline.
func ReadReportOutboxState(ctx context.Context, path string) (ReportOutboxState, error) {
	if err := seedOutboxPath(path); err != nil {
		return ReportOutboxState{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return ReportOutboxState{}, err
	}
	if !info.Mode().IsRegular() {
		return ReportOutboxState{}, ErrSeedOutbox
	}
	ro, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return ReportOutboxState{}, err
	}
	var state ReportOutboxState
	err = ro.WithTx(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = readReportState(ctx, tx)
		return err
	})
	closeErr := ro.Close()
	if err != nil {
		return ReportOutboxState{}, err
	}
	if closeErr != nil {
		return ReportOutboxState{}, closeErr
	}
	return state, nil
}

func readReportState(ctx context.Context, q seedQuery) (ReportOutboxState, error) {
	var s ReportOutboxState
	var version int
	if err := q.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return s, err
	}
	tables := 4
	if version == 3 {
		tables = 7
	} else if version != reportOutboxVersion {
		return s, ErrSeedOutbox
	}
	if err := inventoryVersion(ctx, q, version, tables); err != nil {
		return s, err
	}
	var registration, anchor []byte
	var origin, archive, binding, qualified string
	err := q.QueryRowContext(ctx, `select origin,archive,binding_id,registration_json,baseline_state,baseline_anchor_json,baseline_native,baseline_sha256,server_cursor,inventory_sha256,inventory_layout,receiver_sequence from binding where singleton=1`).Scan(&origin, &archive, &binding, &registration, &qualified, &anchor, &s.BaselineNative, &s.BaselineSHA256, &s.ServerCursor, &s.InventorySHA256, &s.InventoryLayout, &s.ReceiverSequence)
	if err != nil {
		return ReportOutboxState{}, err
	}
	if json.Unmarshal(registration, &s.Registration) != nil || json.Unmarshal(anchor, &s.BaselineAnchor) != nil || validateSeedRegistration(s.Registration) != nil || origin != s.Registration.Origin || archive != s.Registration.Archive || binding != s.Registration.BindingID || qualified != BaselineQualified || s.BaselineAnchor.File != s.Registration.File || s.BaselineAnchor.Repository != s.Registration.Repository || s.BaselineAnchor.RepositoryID <= 0 || !digest(s.BaselineSHA256) || !digest(s.InventorySHA256) || s.InventoryLayout != privateInventoryLayoutID || s.ReceiverSequence <= 0 || s.ServerCursor == "" {
		return ReportOutboxState{}, ErrSeedOutbox
	}
	// Token versions preserve their hash semantics: migrated v1 baselines keep
	// the original full inventory hash until explicit v2 receipt settlement.
	a, c := s.BaselineAnchor, s.BaselineAnchor.Cursor
	token, err := remote.ParseGitHubNativeAnchor(s.BaselineNative)
	if err != nil || token.Epoch != c.Epoch || token.Generation != c.Generation || token.SchemaCookie != c.SchemaVersion || token.Sequence != c.Sequence || token.PrunedThrough != c.PrunedThrough || token.ScopeSHA256 != a.ScopeSHA256 || token.InventorySHA256 != s.BaselineSHA256 {
		return ReportOutboxState{}, ErrSeedOutbox
	}
	if token.Version == remote.GitHubNativeV2 && s.BaselineSHA256 != s.InventorySHA256 {
		return ReportOutboxState{}, ErrSeedOutbox
	}
	if version == 3 {
		if err := q.QueryRowContext(ctx, `select coalesce(active_change_id,'') from binding where singleton=1`).Scan(&s.ActiveChangeID); err != nil {
			return ReportOutboxState{}, err
		}
	}
	return s, nil
}

// OpenReportOutbox recognizes schema2/3 before the storage helper can change
// modes/journaling. It does not migrate or permit old seed operations.
func OpenReportOutbox(ctx context.Context, path string) (*Outbox, error) {
	if _, err := ReadReportOutboxState(ctx, path); err != nil {
		return nil, err
	}
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	o := &Outbox{base: base}
	if err := base.WithTx(ctx, func(tx *sql.Tx) error { _, err := readReportState(ctx, tx); return err }); err != nil {
		base.Close()
		return nil, err
	}
	return o, nil
}
