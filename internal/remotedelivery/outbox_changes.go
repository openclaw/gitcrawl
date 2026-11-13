package remotedelivery

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

const changeOutboxVersion = 3

// ChangeStageLimits bounds supplied changes and exact encoded request bodies.
// Staging also streams the complete compact index under the caller's deadline;
// MaxRows is a delta bound, not a bound on the existing qualified inventory.
type ChangeStageLimits struct {
	MaxRows, MaxBytes int64
	MaxParts          int
}
type ChangePartRef struct {
	CaptureID string
	Index     int
}
type PendingChangePart struct {
	Ref       ChangePartRef
	PartCount int
	Body      []byte
	SHA256    string
}

const changeOutboxSchema = `
create table change_captures (
 capture_id text primary key not null,
 predecessor_json blob not null,
 through_anchor_json blob not null,
 native_through text not null,
 inventory_sha256 text not null,
 part_count integer not null check(part_count between 1 and 64),
 completed integer not null default 0 check(completed in (0,1))
);
create table change_parts (
 capture_id text not null references change_captures(capture_id),
 part_index integer not null check(part_index between 0 and 63),
 body blob not null check(length(body) between 1 and 4194304),
 payload_sha256 text not null check(length(payload_sha256)=64),
 receipt_json blob,
 primary key(capture_id,part_index)
);
create table change_mutations (
 capture_id text not null references change_captures(capture_id),
 table_name text not null,
 encoded_key blob not null check(typeof(encoded_key)='blob' and length(encoded_key)>0),
 operation text not null check(operation in ('upsert','remove')),
 row_sha256 blob not null check(typeof(row_sha256)='blob' and length(row_sha256)=32),
 key_json blob not null,
 repository_id integer not null check(repository_id>0),
 thread_id integer,thread_number integer,node_id text,
 primary key(capture_id,table_name,encoded_key)
) without rowid;
create index change_thread_id on change_mutations(capture_id,thread_id,table_name);
alter table binding add column active_change_id text references change_captures(capture_id);
pragma user_version=3;
`

// MigrateReportOutboxV3 is a schema-only explicit upgrade. Seed and compact
// inventory bytes remain unchanged; ordinary opens never migrate a database.
func MigrateReportOutboxV3(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrSeedOutbox
	}
	o, err := OpenReportOutbox(ctx, path)
	if err != nil {
		return err
	}
	defer o.Close()
	return o.write(ctx, func(conn *sql.Conn) error {
		if err := inventoryVersion(ctx, conn, reportOutboxVersion, 4); err != nil {
			return err
		}
		if _, err := readReportState(ctx, conn); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, changeOutboxSchema)
		return err
	})
}

// StageReportChanges consumes one successful source capture after its read
// transactions have closed. It grants no source or outbound authority. All
// bounded upserts are retained, even if only their parent metadata changed.
func (o *Outbox) StageReportChanges(ctx context.Context, expected ReportOutboxState, delta store.NativeReportDelta, limits ChangeStageLimits) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if _, ok := ctx.Deadline(); !ok || limits.MaxRows <= 0 || limits.MaxBytes <= 0 || limits.MaxParts <= 0 || limits.MaxParts > remote.MaxChangeParts {
		return "", ErrSeedOutbox
	}
	count := int64(len(delta.Upserts)) + int64(len(delta.Removals))
	if count > limits.MaxRows || count > int64(limits.MaxParts*remote.MaxChanges) || delta.Rows != count || delta.Bytes < 0 || delta.JournalRecords < 0 || delta.LookupRows < 0 || delta.From != expected.BaselineAnchor || expected.ActiveChangeID != "" {
		return "", ErrSeedOutbox
	}
	identity := delta.Through
	identity.Cursor.Sequence, identity.Cursor.PrunedThrough = delta.From.Cursor.Sequence, delta.From.Cursor.PrunedThrough
	if identity != delta.From {
		return "", ErrSeedOutbox
	}
	changes, err := projectOutboxChanges(ctx, delta, limits.MaxBytes)
	if err != nil {
		return "", err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(random[:])
	err = o.write(ctx, func(conn *sql.Conn) error {
		if err := inventoryVersion(ctx, conn, changeOutboxVersion, 7); err != nil {
			return err
		}
		current, err := readReportState(ctx, conn)
		if err != nil {
			return err
		}
		if current != expected || current.ActiveChangeID != "" {
			return ErrSeedOutbox
		}
		codec, err := privateInventoryCodec()
		if err != nil {
			return err
		}
		predecessor, _ := json.Marshal(expected)
		anchor, _ := json.Marshal(delta.Through)
		// This provisional header is visible only within this writer transaction.
		// Failed staging rolls back the header, mutations and all request parts.
		if _, err := conn.ExecContext(ctx, `insert into change_captures(capture_id,predecessor_json,through_anchor_json,native_through,inventory_sha256,part_count) values(?,?,?,'','',1)`, id, predecessor, anchor); err != nil {
			return err
		}
		for i, change := range changes {
			if err := ctx.Err(); err != nil {
				return err
			}
			var row compactInventoryRow
			if change.Operation == "upsert" {
				row, err = deriveInventoryRow(ctx, conn, codec, delta.Through, change)
			} else {
				row, err = retainedRemoval(ctx, conn, codec, delta.Removals[i-len(delta.Upserts)])
			}
			if err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `insert into change_mutations(capture_id,table_name,encoded_key,operation,row_sha256,key_json,repository_id,thread_id,thread_number,node_id) values(?,?,?,?,?,?,?,?,?,?)`, id, row.Table, row.Key, change.Operation, row.SHA256, row.KeyJSON, row.RepositoryID, row.ThreadID, row.ThreadNumber, row.NodeID); err != nil {
				return err
			}
		}
		// Parent numbers come from the resulting inventory, not the old source
		// or an unchanged child's row hash. Missing dependency expansion fails.
		if _, err := conn.ExecContext(ctx, candidateChildNumbersSQL, id); err != nil {
			return err
		}
		if err := validateCandidateParents(ctx, conn, id, expected.BaselineAnchor.RepositoryID); err != nil {
			return err
		}
		digest, err := candidateInventoryDigest(ctx, conn, codec, id, expected.BaselineAnchor.ScopeSHA256)
		if err != nil {
			return err
		}
		c := delta.Through.Cursor
		through, err := remote.EncodeGitHubNativeAnchor(remote.GitHubNativeAnchor{Version: remote.GitHubNativeV2, Epoch: c.Epoch, Generation: c.Generation, SchemaCookie: c.SchemaVersion, Sequence: c.Sequence, PrunedThrough: c.PrunedThrough, ScopeSHA256: delta.Through.ScopeSHA256, InventorySHA256: digest})
		if err != nil {
			return err
		}
		packed, err := PreparePrivateChanges(ctx, changes, PrivateChangesRequest{Archive: expected.Registration.Archive, BindingID: expected.Registration.BindingID, CaptureID: id, NativeFrom: expected.BaselineNative, NativeThrough: through, ExpectedCursor: expected.ServerCursor, MaxBytes: limits.MaxBytes, MaxParts: limits.MaxParts})
		if err != nil {
			return err
		}
		if expected.ReceiverSequence > math.MaxInt64-int64(len(packed.Parts)) {
			return ErrSeedOutbox
		}
		for i, body := range packed.Parts {
			if _, err := conn.ExecContext(ctx, `insert into change_parts(capture_id,part_index,body,payload_sha256) values(?,?,?,?)`, id, i, body, privateSeedHash(body)); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, `update change_captures set native_through=?,inventory_sha256=?,part_count=? where capture_id=?`, through, digest, len(packed.Parts), id); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `update binding set active_change_id=? where singleton=1`, id)
		return err
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func projectOutboxChanges(ctx context.Context, delta store.NativeReportDelta, maxBytes int64) ([]remote.Change, error) {
	changes := make([]remote.Change, 0, len(delta.Upserts)+len(delta.Removals))
	var used int64
	for _, native := range delta.Upserts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := inventoryNativeKey(native.Key)
		if err != nil {
			return nil, err
		}
		var layout *privateLayout
		for i := range privateLayouts {
			if privateLayouts[i].name == native.Key.Table {
				layout = &privateLayouts[i]
				break
			}
		}
		if layout == nil {
			return nil, ErrSeedOutbox
		}
		columns := strings.Fields(layout.columns)
		if len(native.Values) != len(columns) {
			return nil, ErrSeedOutbox
		}
		change := remote.Change{Table: native.Key.Table, Operation: "upsert", Key: key, Row: make([]remote.ChangeValue, len(columns))}
		for i, v := range native.Values {
			n := int64(9)
			if v.Kind == store.NativeReportText {
				n += int64(len(v.Text))
			}
			if n > maxBytes-used {
				return nil, ErrPrivateChangesLimit
			}
			used += n
			value, err := privateValue(v, strings.Contains(" "+layout.integers+" ", " "+columns[i]+" "))
			if err != nil {
				return nil, err
			}
			change.Row[i] = remote.ChangeValue{Kind: value.Kind, Value: value.Value}
		}
		changes = append(changes, change)
	}
	for _, ref := range delta.Removals {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := inventoryNativeKey(ref.Key)
		if err != nil {
			return nil, err
		}
		for _, v := range key {
			if v.Value == nil {
				return nil, ErrSeedOutbox
			}
			n := int64(9 + len(*v.Value))
			if n > maxBytes-used {
				return nil, ErrPrivateChangesLimit
			}
			used += n
		}
		changes = append(changes, remote.Change{Table: ref.Key.Table, Operation: "remove", Key: key})
	}
	return changes, nil
}

func retainedRemoval(ctx context.Context, conn *sql.Conn, codec *remote.InventoryCodec, ref store.NativeReportRef) (compactInventoryRow, error) {
	var row compactInventoryRow
	row.Table = ref.Key.Table
	wire, err := inventoryNativeKey(ref.Key)
	if err != nil {
		return row, err
	}
	row.Key, err = codec.EncodeKey(row.Table, wire)
	if err != nil {
		return row, err
	}
	var thread, number sql.NullInt64
	var node sql.NullString
	if err := conn.QueryRowContext(ctx, `select row_sha256,key_json,repository_id,thread_id,thread_number,node_id from inventory_rows where table_name=? and encoded_key=?`, row.Table, row.Key).Scan(&row.SHA256, &row.KeyJSON, &row.RepositoryID, &thread, &number, &node); err != nil {
		return row, err
	}
	keyJSON, _ := json.Marshal(wire)
	if !bytes.Equal(keyJSON, row.KeyJSON) || len(row.SHA256) != 32 || row.RepositoryID != ref.RepositoryID || thread.Int64 != ref.ThreadID || number.Int64 != ref.ThreadNumber || node.Valid != (ref.NodeID != nil) || (node.Valid && node.String != *ref.NodeID) {
		return compactInventoryRow{}, ErrSeedOutbox
	}
	if thread.Valid {
		row.ThreadID = thread.Int64
	}
	if number.Valid {
		row.ThreadNumber = number.Int64
	}
	if node.Valid {
		row.NodeID = node.String
	}
	return row, nil
}

// Only compact entries are merged; no baseline body archive or index copy is
// materialized. Each use binds the same capture ID twice.
const mergedInventorySQL = `select table_name,encoded_key,row_sha256,key_json,repository_id,thread_id,thread_number,node_id from inventory_rows i
where not exists(select 1 from change_mutations m where m.capture_id=? and m.table_name=i.table_name and m.encoded_key=i.encoded_key)
union all select table_name,encoded_key,row_sha256,key_json,repository_id,thread_id,thread_number,node_id from change_mutations where capture_id=? and operation='upsert'`

const candidateChildNumbersSQL = `update change_mutations as child set thread_number=coalesce(
 (select candidate_parent.thread_number from change_mutations candidate_parent indexed by change_thread_id
  where candidate_parent.capture_id=child.capture_id and candidate_parent.thread_id=child.thread_id and candidate_parent.table_name='threads' and candidate_parent.operation='upsert'),
 (select base_parent.thread_number from inventory_rows base_parent indexed by inventory_thread_id
  where base_parent.thread_id=child.thread_id and base_parent.table_name='threads'
  and not exists(select 1 from change_mutations m where m.capture_id=child.capture_id and m.table_name=base_parent.table_name and m.encoded_key=base_parent.encoded_key)))
 where child.capture_id=? and child.operation='upsert' and child.table_name in ('comments','thread_child_observation_reservations','thread_child_observation_memberships')`

// Probe parent identity through retained indexes. A correlated CTE reference
// can be flattened by SQLite into a scan of every thread for each child.
const candidateParentCheckSQL = `with candidate as (` + mergedInventorySQL + `)
select (select count(*) from candidate c where c.repository_id<>? or
 (c.table_name in ('comments','thread_child_observation_reservations','thread_child_observation_memberships')
 and not exists(select 1 from change_mutations candidate_parent indexed by change_thread_id
   where candidate_parent.capture_id=? and candidate_parent.thread_id=c.thread_id and candidate_parent.table_name='threads' and candidate_parent.operation='upsert' and candidate_parent.thread_number=c.thread_number)
 and not exists(select 1 from inventory_rows base_parent indexed by inventory_thread_id
   where base_parent.thread_id=c.thread_id and base_parent.table_name='threads' and base_parent.thread_number=c.thread_number
   and not exists(select 1 from change_mutations m where m.capture_id=? and m.table_name=base_parent.table_name and m.encoded_key=base_parent.encoded_key)))),
(select count(*) from candidate where table_name='repositories')`

func validateCandidateParents(ctx context.Context, conn *sql.Conn, id string, repository int64) error {
	var invalid, repositories int
	if err := conn.QueryRowContext(ctx, candidateParentCheckSQL, id, id, repository, id, id).Scan(&invalid, &repositories); err != nil {
		return err
	}
	if invalid != 0 || repositories != 1 {
		return ErrSeedOutbox
	}
	return nil
}

func candidateInventoryDigest(ctx context.Context, conn *sql.Conn, codec *remote.InventoryCodec, id, scope string) (string, error) {
	counts := make([]remote.InventoryCount, len(privateLayouts))
	for i, l := range privateLayouts {
		counts[i].Table = l.name
	}
	rows, err := conn.QueryContext(ctx, `select table_name,count(*) from (`+mergedInventorySQL+`) group by table_name`, id, id)
	if err != nil {
		return "", err
	}
	var total uint64
	for rows.Next() {
		var name string
		var count uint64
		if err := rows.Scan(&name, &count); err != nil {
			rows.Close()
			return "", err
		}
		found := false
		for i := range counts {
			if counts[i].Table == name {
				counts[i].Rows = count
				found = true
				break
			}
		}
		if !found || math.MaxUint64-total < count {
			rows.Close()
			return "", ErrSeedOutbox
		}
		total += count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	entries, err := conn.QueryContext(ctx, `select table_name,encoded_key,row_sha256 from (`+mergedInventorySQL+`) order by table_name collate binary,encoded_key`, id, id)
	if err != nil {
		return "", err
	}
	return digestInventoryCursor(ctx, codec, scope, counts, total, entries)
}

type outboxChangeCapture struct {
	Expected                       ReportOutboxState
	Through                        store.NativeReportAnchor
	NativeThrough, InventorySHA256 string
	PartCount                      int
	Completed                      bool
}

func readChangeCapture(ctx context.Context, q seedQuery, id string) (outboxChangeCapture, error) {
	var c outboxChangeCapture
	var predecessor, anchor []byte
	if err := q.QueryRowContext(ctx, `select predecessor_json,through_anchor_json,native_through,inventory_sha256,part_count,completed from change_captures where capture_id=?`, id).Scan(&predecessor, &anchor, &c.NativeThrough, &c.InventorySHA256, &c.PartCount, &c.Completed); err != nil {
		return c, err
	}
	if json.Unmarshal(predecessor, &c.Expected) != nil || json.Unmarshal(anchor, &c.Through) != nil || c.Expected.ActiveChangeID != "" || c.PartCount < 1 || c.PartCount > remote.MaxChangeParts || c.Expected.ReceiverSequence <= 0 || c.Expected.ReceiverSequence > math.MaxInt64-int64(c.PartCount) {
		return c, ErrSeedOutbox
	}
	from, err := remote.ParseGitHubNativeAnchor(c.Expected.BaselineNative)
	if err != nil {
		return c, err
	}
	through, err := remote.ParseGitHubNativeAnchor(c.NativeThrough)
	if err != nil || remote.ValidateGitHubNativeRange(from, through) != nil || through.InventorySHA256 != c.InventorySHA256 {
		return c, ErrSeedOutbox
	}
	anchorIdentity := c.Through
	anchorIdentity.Cursor = c.Expected.BaselineAnchor.Cursor
	if anchorIdentity != c.Expected.BaselineAnchor || through.Epoch != c.Through.Cursor.Epoch || through.Generation != c.Through.Cursor.Generation || through.SchemaCookie != c.Through.Cursor.SchemaVersion || through.Sequence != c.Through.Cursor.Sequence || through.PrunedThrough != c.Through.Cursor.PrunedThrough || through.ScopeSHA256 != c.Through.ScopeSHA256 {
		return c, ErrSeedOutbox
	}
	var count, first, last, receipts int
	if err := q.QueryRowContext(ctx, `select count(*),min(part_index),max(part_index),count(receipt_json) from change_parts where capture_id=?`, id).Scan(&count, &first, &last, &receipts); err != nil {
		return c, err
	}
	if count != c.PartCount || first != 0 || last != count-1 || (c.Completed && receipts != count) {
		return c, ErrSeedOutbox
	}
	return c, nil
}

func readChangePart(ctx context.Context, q seedQuery, ref ChangePartRef, c outboxChangeCapture) (PendingChangePart, []byte, error) {
	p := PendingChangePart{Ref: ref, PartCount: c.PartCount}
	var recorded []byte
	if ref.Index < 0 || ref.Index >= c.PartCount {
		return p, nil, ErrSeedOutbox
	}
	if err := q.QueryRowContext(ctx, `select body,payload_sha256,receipt_json from change_parts where capture_id=? and part_index=?`, ref.CaptureID, ref.Index).Scan(&p.Body, &p.SHA256, &recorded); err != nil {
		return p, nil, err
	}
	if privateSeedHash(p.Body) != p.SHA256 {
		return p, nil, ErrSeedOutbox
	}
	b, err := remote.DecodeChangeBatch(p.Body)
	if err != nil {
		return p, nil, err
	}
	previous := c.Expected.ServerCursor
	if ref.Index > 0 {
		previous = changePartID(ref.CaptureID, ref.Index-1)
	}
	if b.App != "gitcrawl" || b.Archive != c.Expected.Registration.Archive || b.BindingID != c.Expected.Registration.BindingID || b.CaptureID != ref.CaptureID || b.PartIndex != ref.Index || b.PartCount != c.PartCount || b.NativeFrom != c.Expected.BaselineNative || b.NativeThrough != c.NativeThrough || b.ExpectedCursor != previous {
		return p, nil, ErrSeedOutbox
	}
	return p, recorded, nil
}

func changePartID(id string, index int) string { return "chg1:" + id + ":" + strconv.Itoa(index) }

func checkActiveChange(current ReportOutboxState, id string, c outboxChangeCapture) error {
	if current.ActiveChangeID != id || c.Completed {
		return ErrSeedOutbox
	}
	current.ActiveChangeID = ""
	if current != c.Expected {
		return ErrSeedOutbox
	}
	return nil
}

// NextChangePart releases its read transaction before returning exact bytes.
// Multiple workers may observe the same part; this never authorizes HTTP.
func (o *Outbox) NextChangePart(ctx context.Context) (*PendingChangePart, error) {
	var pending *PendingChangePart
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		if err := inventoryVersion(ctx, tx, changeOutboxVersion, 7); err != nil {
			return err
		}
		s, err := readReportState(ctx, tx)
		if err != nil {
			return err
		}
		if s.ActiveChangeID == "" {
			return nil
		}
		c, err := readChangeCapture(ctx, tx, s.ActiveChangeID)
		if err != nil {
			return err
		}
		if err := checkActiveChange(s, s.ActiveChangeID, c); err != nil {
			return err
		}
		var index int
		if err := tx.QueryRowContext(ctx, `select min(part_index) from change_parts where capture_id=? and receipt_json is null`, s.ActiveChangeID).Scan(&index); err != nil {
			return err
		}
		p, _, err := readChangePart(ctx, tx, ChangePartRef{CaptureID: s.ActiveChangeID, Index: index}, c)
		if err != nil {
			return err
		}
		pending = &p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pending, nil
}

// RecordChangeReceipt settles authenticated receiver evidence supplied by its
// caller. It performs no network/source access. Historical replay cannot move
// a later baseline; only final contiguous settlement promotes this capture.
func (o *Outbox) RecordChangeReceipt(ctx context.Context, ref ChangePartRef, receipt remote.ChangeReceipt) error {
	return o.write(ctx, func(conn *sql.Conn) error {
		if err := inventoryVersion(ctx, conn, changeOutboxVersion, 7); err != nil {
			return err
		}
		c, err := readChangeCapture(ctx, conn, ref.CaptureID)
		if err != nil {
			return err
		}
		p, recorded, err := readChangePart(ctx, conn, ref, c)
		if err != nil {
			return err
		}
		batch := changePartID(ref.CaptureID, ref.Index)
		sequence := c.Expected.ReceiverSequence + int64(ref.Index) + 1
		if receipt.App != "gitcrawl" || receipt.Archive != c.Expected.Registration.Archive || receipt.BindingID != c.Expected.Registration.BindingID || receipt.BatchID != batch || receipt.Cursor != batch || receipt.PayloadSHA256 != p.SHA256 || receipt.Sequence != strconv.FormatInt(sequence, 10) {
			return ErrSeedOutbox
		}
		if recorded != nil {
			var previous remote.ChangeReceipt
			if json.Unmarshal(recorded, &previous) != nil {
				return ErrSeedOutbox
			}
			previous.Replayed = receipt.Replayed
			if previous != receipt {
				return ErrSeedOutbox
			}
			return nil
		}
		s, err := readReportState(ctx, conn)
		if err != nil {
			return err
		}
		if err := checkActiveChange(s, ref.CaptureID, c); err != nil {
			return err
		}
		var next, count int
		if err := conn.QueryRowContext(ctx, `select min(case when receipt_json is null then part_index end),count(receipt_json) from change_parts where capture_id=?`, ref.CaptureID).Scan(&next, &count); err != nil {
			return err
		}
		if next != ref.Index || count != ref.Index {
			return ErrSeedOutbox
		}
		body, _ := json.Marshal(receipt)
		if _, err := conn.ExecContext(ctx, `update change_parts set receipt_json=? where capture_id=? and part_index=?`, body, ref.CaptureID, ref.Index); err != nil {
			return err
		}
		if ref.Index != c.PartCount-1 {
			return nil
		}
		if _, err := conn.ExecContext(ctx, `delete from inventory_rows as i where exists(select 1 from change_mutations m where m.capture_id=? and m.table_name=i.table_name and m.encoded_key=i.encoded_key)`, ref.CaptureID); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `insert into inventory_rows(table_name,encoded_key,row_sha256,key_json,repository_id,thread_id,thread_number,node_id) select table_name,encoded_key,row_sha256,key_json,repository_id,thread_id,thread_number,node_id from change_mutations where capture_id=? and operation='upsert'`, ref.CaptureID); err != nil {
			return err
		}
		anchor, _ := json.Marshal(c.Through)
		if _, err := conn.ExecContext(ctx, `update binding set baseline_anchor_json=?,baseline_native=?,baseline_sha256=?,inventory_sha256=?,server_cursor=?,receiver_sequence=?,active_change_id=null where singleton=1`, anchor, c.NativeThrough, c.InventorySHA256, c.InventorySHA256, receipt.Cursor, sequence); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `update change_captures set completed=1 where capture_id=?`, ref.CaptureID)
		return err
	})
}
