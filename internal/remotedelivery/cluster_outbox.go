package remotedelivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store"
)

var ErrClusterOutbox = errors.New("cluster bundle or staged custody differs; retain the input and outbox for reconciliation")

// ClusterBundle preserves the export's exact decoded request bytes. Neither
// this envelope nor a successful stage is source or publication authority.
type ClusterBundle struct {
	Profile  string   `json:"profile"`
	Manifest []byte   `json:"manifest"`
	Chunks   [][]byte `json:"chunks"`
	Complete []byte   `json:"complete"`
}
type ClusterStageLimits struct{ MaxRows, MaxEncodedBytes int64 }

// The file identity binds the operator-selected source at staging time. The
// exported envelope has no file identity and cannot prove historical custody.
type ClusterState struct {
	Registration                                           SeedRegistration
	RepositoryID                                           int64
	GitHubRepositoryID                                     string
	SeedID, NativeThrough, InventorySHA256, ManifestSHA256 string
	Rows, Bytes                                            int64
	PartCount                                              int
	Receipts                                               int
	BeginSequence                                          int64
}

// A distinct application identity prevents private v1/v2/v3 outbox readers or
// migrations from interpreting cluster snapshots as journal-backed baselines.
const clusterOutboxApplicationID = 0x4743434f // GCCO
const clusterOutboxSchema = `
pragma application_id=1195590479;
pragma user_version=1;
create table binding (
 singleton integer primary key check(singleton=1), registration_json blob not null,
 repository_id integer not null check(repository_id>0), github_repo_id text not null
);
create table captures (
 seed_id text primary key not null, native_through text not null,
 inventory_sha256 text not null check(length(inventory_sha256)=64),
 manifest_sha256 text not null check(length(manifest_sha256)=64),
 rows integer not null check(rows>0), bytes integer not null check(bytes>0),
 part_count integer not null check(part_count between 2 and 8194),
 begin_sequence integer check(begin_sequence>0)
);
create unique index one_seed on captures((1));
create table parts (
 seed_id text not null references captures(seed_id),
 part_index integer not null check(part_index between 0 and 8193),
 body blob not null check(length(body) between 1 and 4194304),
 payload_sha256 text not null check(length(payload_sha256)=64),
 receipt_json blob,
 primary key(seed_id,part_index)
);
`

func clusterStageLimits(l ClusterStageLimits) bool {
	return l.MaxRows > 0 && l.MaxEncodedBytes > 0 && l.MaxEncodedBytes <= int64(remote.MaxChangeBytes)*int64(remote.MaxSeedChunks+2)
}

// ReadClusterBundle bounds the outer envelope before base64 decoding. MaxBytes
// counts decoded wire bytes, not RSS; the envelope allows bounded base64/JSON
// overhead. Unknown, duplicate or missing fields and trailing input are refused.
func ReadClusterBundle(ctx context.Context, input io.Reader, limits ClusterStageLimits) (ClusterBundle, error) {
	var out ClusterBundle
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if !clusterStageLimits(limits) {
		return out, ErrClusterOutbox
	}
	ceiling := ((limits.MaxEncodedBytes+2)/3)*4 + int64(remote.MaxSeedChunks)*16 + 4096
	body, err := io.ReadAll(io.LimitReader(input, ceiling+1))
	if err != nil {
		return out, err
	}
	if int64(len(body)) > ceiling || !utf8.Valid(body) {
		return out, ErrClusterOutbox
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	d := json.NewDecoder(bytes.NewReader(body))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return out, ErrClusterOutbox
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return ClusterBundle{}, ErrClusterOutbox
		}
		seen[key] = true
		switch key {
		case "profile":
			err = d.Decode(&out.Profile)
		case "manifest":
			err = d.Decode(&out.Manifest)
		case "chunks":
			err = d.Decode(&out.Chunks)
		case "complete":
			err = d.Decode(&out.Complete)
		default:
			return ClusterBundle{}, ErrClusterOutbox
		}
		if err != nil {
			return ClusterBundle{}, ErrClusterOutbox
		}
	}
	if last, err := d.Token(); err != nil || last != json.Delim('}') || len(seen) != 4 || d.Decode(new(any)) != io.EOF {
		return ClusterBundle{}, ErrClusterOutbox
	}
	return out, nil
}

func inspectClusterBundle(ctx context.Context, bundle ClusterBundle, limits ClusterStageLimits) (ClusterState, error) {
	var state ClusterState
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if !clusterStageLimits(limits) || bundle.Profile != store.NativeClusterContextProfile || len(bundle.Chunks) > remote.MaxSeedChunks {
		return state, ErrClusterOutbox
	}
	parts := append([][]byte{bundle.Manifest}, bundle.Chunks...)
	parts = append(parts, bundle.Complete)
	for _, body := range parts {
		if len(body) == 0 || len(body) > remote.MaxChangeBytes || int64(len(body)) > limits.MaxEncodedBytes-state.Bytes {
			return ClusterState{}, ErrClusterOutbox
		}
		state.Bytes += int64(len(body))
	}
	plan, err := remote.DecodeGitHubSeedManifest(bundle.Manifest)
	if err != nil {
		return ClusterState{}, ErrClusterOutbox
	}
	m := plan.Manifest()
	if m.Profile != bundle.Profile || m.NativeSchema != 16 {
		return ClusterState{}, ErrClusterOutbox
	}
	scope := []byte(m.Profile + "\n" + store.NativeClusterContextView + "\n" + m.Repository + "\n")
	scopeHash := sha256.Sum256(scope)
	if hex.EncodeToString(scopeHash[:]) != m.ScopeSHA256 {
		return ClusterState{}, ErrClusterOutbox
	}
	state.RepositoryID, err = strconv.ParseInt(m.RepositoryID, 10, 64)
	if err != nil || state.RepositoryID <= 0 {
		return ClusterState{}, ErrClusterOutbox
	}
	var counts []remote.InventoryCount
	chunks := 0
	for _, table := range m.Tables {
		n, err := strconv.ParseInt(table.Rows, 10, 64)
		if err != nil || n < 0 || n > limits.MaxRows-state.Rows {
			return ClusterState{}, ErrClusterOutbox
		}
		if table.Name == "selection" && n != 1 {
			return ClusterState{}, ErrClusterOutbox
		}
		state.Rows += n
		counts = append(counts, remote.InventoryCount{Table: table.Name, Rows: uint64(n)})
		chunks += len(table.Chunks)
	}
	if chunks != len(bundle.Chunks) {
		return ClusterState{}, ErrClusterOutbox
	}
	codec, err := clusterInventoryCodec()
	if err != nil {
		return ClusterState{}, err
	}
	entries := make([]remote.InventoryEntry, 0)
	for index, body := range bundle.Chunks {
		if err := ctx.Err(); err != nil {
			return ClusterState{}, err
		}
		chunk, err := plan.ValidateChunk(body)
		if err != nil || chunk.Index != index {
			return ClusterState{}, ErrClusterOutbox
		}
		for _, change := range chunk.Changes {
			for _, value := range change.Row {
				if value.Kind == "text" && value.Value != nil && len(*value.Value) > clusterMaxScalarBytes {
					return ClusterState{}, ErrClusterOutbox
				}
			}
			key, err := codec.EncodeKey(change.Table, change.Key)
			if err != nil {
				return ClusterState{}, ErrClusterOutbox
			}
			hash, err := codec.RowDigest(change.Table, change.Key, change.Row)
			if err != nil || !clusterRowWithinProfile(change) {
				return ClusterState{}, ErrClusterOutbox
			}
			entries = append(entries, remote.InventoryEntry{Table: change.Table, Key: key, RowSHA256: hash})
			if change.Table == "selection" {
				text := func(i int) string {
					v := change.Row[i]
					if v.Kind != "text" || v.Value == nil {
						return ""
					}
					return *v.Value
				}
				if *change.Row[0].Value != m.RepositoryID || text(1) != m.Repository || text(2) == "" || text(4) != store.NativeClusterContextView || text(6) != "durable_cluster" {
					return ClusterState{}, ErrClusterOutbox
				}
				state.GitHubRepositoryID = text(2)
			}
			if change.Table == "threads" && change.Row[7].Kind == "text" && utf8.RuneCountInString(*change.Row[7].Value) > 800 {
				return ClusterState{}, ErrClusterOutbox
			}
		}
	}
	if int64(len(entries)) != state.Rows || state.GitHubRepositoryID == "" {
		return ClusterState{}, ErrClusterOutbox
	}
	slices.SortFunc(entries, func(a, b remote.InventoryEntry) int {
		if n := strings.Compare(a.Table, b.Table); n != 0 {
			return n
		}
		return bytes.Compare(a.Key, b.Key)
	})
	index := 0
	hash, err := codec.Digest(ctx, scope, counts, remote.InventoryLimits{MaxEntries: uint64(limits.MaxRows), MaxKeyBytes: 1024}, func(context.Context) (remote.InventoryEntry, error) {
		if index == len(entries) {
			return remote.InventoryEntry{}, io.EOF
		}
		entry := entries[index]
		index++
		return entry, nil
	})
	if err != nil || hex.EncodeToString(hash[:]) != m.NativeSHA256 {
		return ClusterState{}, ErrClusterOutbox
	}
	if _, err := plan.ValidateComplete(bundle.Complete); err != nil {
		return ClusterState{}, ErrClusterOutbox
	}
	state.SeedID, state.NativeThrough, state.InventorySHA256, state.ManifestSHA256 = m.SeedID, m.NativeThrough, m.NativeSHA256, plan.ManifestSHA256()
	state.PartCount = len(parts)
	state.Registration.Repository = m.Repository
	state.Registration.Archive = m.Archive
	state.Registration.BindingID = m.BindingID
	return state, nil
}

// StageClusterBundle stores original request bytes, never a re-encoded capture.
// The caller must not mutate bundle during this call. All validation and source
// identity reads precede exclusive creation; failures after creation retain it.
func StageClusterBundle(ctx context.Context, path, source string, r SeedRegistration, bundle ClusterBundle, limits ClusterStageLimits) (ClusterState, error) {
	var empty ClusterState
	if _, ok := ctx.Deadline(); !ok {
		return empty, ErrClusterOutbox
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := seedOutboxPath(path); err != nil {
		return empty, err
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				err = os.ErrExist
			}
			return empty, err
		}
	}
	if err := validateSeedRegistration(r); err != nil {
		return empty, err
	}
	if len(r.Origin) > clusterMaxScalarBytes {
		return empty, ErrClusterOutbox
	}
	state, err := inspectClusterBundle(ctx, bundle, limits)
	if err != nil {
		return empty, err
	}
	if state.Registration.Repository != r.Repository || state.Registration.Archive != r.Archive || state.Registration.BindingID != r.BindingID {
		return empty, ErrClusterOutbox
	}
	if err = store.CheckNativeClusterStageCustody(ctx, source, r.File, r.Repository, state.RepositoryID, state.GitHubRepositoryID); err != nil {
		return empty, err
	}
	state.Registration = r
	registration, _ := json.Marshal(r)
	parts := append([][]byte{bundle.Manifest}, bundle.Chunks...)
	parts = append(parts, bundle.Complete)
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	o, err := createOutboxFile(ctx, path, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, clusterOutboxSchema); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `insert into binding values(1,?,?,?)`, registration, state.RepositoryID, state.GitHubRepositoryID); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `insert into captures(seed_id,native_through,inventory_sha256,manifest_sha256,rows,bytes,part_count) values(?,?,?,?,?,?,?)`, state.SeedID, state.NativeThrough, state.InventorySHA256, state.ManifestSHA256, state.Rows, state.Bytes, state.PartCount); err != nil {
			return err
		}
		return insertSeedParts(ctx, conn, state.SeedID, parts)
	})
	if err != nil {
		return empty, err
	}
	if err = o.Close(); err != nil {
		return empty, err
	}
	return state, nil
}

// ReadClusterOutboxState opens no writer or native source and changes no modes.
// It checks the small manifest and metadata; it is not a full-body audit or a
// publication receipt. SQLite may coordinate normal read-only WAL sidecars.
func ReadClusterOutboxState(ctx context.Context, path string) (ClusterState, error) {
	var state ClusterState
	if _, ok := ctx.Deadline(); !ok {
		return state, ErrClusterOutbox
	}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if err := seedOutboxPath(path); err != nil {
		return state, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return state, err
	}
	if !info.Mode().IsRegular() {
		return state, ErrClusterOutbox
	}
	ro, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return state, err
	}
	defer ro.Close()
	err = ro.WithTx(ctx, func(tx *sql.Tx) error { var err error; state, err = readClusterState(ctx, tx); return err })
	if err != nil {
		return ClusterState{}, err
	}
	return state, nil
}

type clusterQuery interface {
	seedQuery
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readClusterState(ctx context.Context, q clusterQuery) (ClusterState, error) {
	var state ClusterState
	err := func() error {
		var app, version, tables int
		if err := q.QueryRowContext(ctx, `pragma application_id`).Scan(&app); err != nil {
			return err
		}
		if err := q.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
			return err
		}
		if err := q.QueryRowContext(ctx, `select count(*) from sqlite_schema where type='table' and name not like 'sqlite_%'`).Scan(&tables); err != nil {
			return err
		}
		if app != clusterOutboxApplicationID || version != 1 || tables != 3 {
			return ErrClusterOutbox
		}
		var registration []byte
		var begin sql.NullInt64
		if err := q.QueryRowContext(ctx, `select registration_json,repository_id,github_repo_id,seed_id,native_through,inventory_sha256,manifest_sha256,rows,bytes,part_count,begin_sequence from binding cross join captures where singleton=1 and length(registration_json)<=65536 and length(github_repo_id)<=16384 and length(native_through)<=512 and length(seed_id)=32`).Scan(&registration, &state.RepositoryID, &state.GitHubRepositoryID, &state.SeedID, &state.NativeThrough, &state.InventorySHA256, &state.ManifestSHA256, &state.Rows, &state.Bytes, &state.PartCount, &begin); err != nil {
			return err
		}
		if json.Unmarshal(registration, &state.Registration) != nil || validateSeedRegistration(state.Registration) != nil || state.RepositoryID <= 0 || state.GitHubRepositoryID == "" || state.PartCount < 2 || state.PartCount > remote.MaxSeedChunks+2 || state.Rows < 1 || state.Bytes < 1 {
			return ErrClusterOutbox
		}
		var count, first, last, receipts int
		var receiptLast sql.NullInt64
		var total int64
		if err := q.QueryRowContext(ctx, `select count(*),min(part_index),max(part_index),sum(length(body)),count(receipt_json),max(case when receipt_json is not null then part_index end) from parts where seed_id=?`, state.SeedID).Scan(&count, &first, &last, &total, &receipts, &receiptLast); err != nil {
			return err
		}
		if count != state.PartCount || first != 0 || last != count-1 || total != state.Bytes {
			return ErrClusterOutbox
		}
		state.Receipts = receipts
		if receipts == 0 {
			if begin.Valid || receiptLast.Valid {
				return ErrClusterOutbox
			}
		} else {
			if !begin.Valid || begin.Int64 <= 0 || !receiptLast.Valid || receiptLast.Int64 != int64(receipts-1) {
				return ErrClusterOutbox
			}
			state.BeginSequence = begin.Int64
		}
		var body []byte
		var hash string
		if err := q.QueryRowContext(ctx, `select body,payload_sha256 from parts where seed_id=? and part_index=0 and length(body)<=4194304`, state.SeedID).Scan(&body, &hash); err != nil {
			return err
		}
		if privateSeedHash(body) != hash || hash != state.ManifestSHA256 {
			return ErrClusterOutbox
		}
		plan, err := remote.DecodeGitHubSeedManifest(body)
		if err != nil {
			return ErrClusterOutbox
		}
		m := plan.Manifest()
		parts := 2
		var rows int64
		for _, table := range m.Tables {
			n, err := strconv.ParseInt(table.Rows, 10, 64)
			if err != nil || n < 0 || n > state.Rows-rows {
				return ErrClusterOutbox
			}
			rows += n
			parts += len(table.Chunks)
		}
		if m.Profile != store.NativeClusterContextProfile || m.Archive != state.Registration.Archive || m.BindingID != state.Registration.BindingID || m.Repository != state.Registration.Repository || m.RepositoryID != strconv.FormatInt(state.RepositoryID, 10) || m.SeedID != state.SeedID || m.NativeThrough != state.NativeThrough || m.NativeSHA256 != state.InventorySHA256 || rows != state.Rows || parts != state.PartCount {
			return ErrClusterOutbox
		}
		return readClusterReceipts(ctx, q, state)
	}()
	if err != nil {
		return ClusterState{}, err
	}
	return state, nil
}

// Receipt validation is metadata-only and bounded independently of payloads.
// Status and the writer share this owner; a receipt count alone is not proof.
func readClusterReceipts(ctx context.Context, q clusterQuery, state ClusterState) error {
	rows, err := q.QueryContext(ctx, `select part_index,payload_sha256,case when length(receipt_json)<=4096 then receipt_json end from parts where seed_id=? and receipt_json is not null order by part_index`, state.SeedID)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var index int
		var hash string
		var body []byte
		if err := rows.Scan(&index, &hash, &body); err != nil {
			return err
		}
		var receipt remote.SeedReceipt
		if index != seen || json.Unmarshal(body, &receipt) != nil || !clusterReceiptMatches(state, index, hash, receipt, state.BeginSequence) {
			return ErrClusterOutbox
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seen != state.Receipts {
		return ErrClusterOutbox
	}
	return nil
}
