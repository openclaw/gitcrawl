package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const NativeReportProfile = "gitcrawl.native-report/v1"

var (
	ErrNativeReportSource = errors.New("native report requires a matching current native archive and armed change contract")
	ErrNativeReportLimit  = errors.New("native report capture exceeds its bounds")
	ErrNativeReportBlob   = errors.New("native report capture cannot resolve a comment raw blob reference")
)

// NativeReportFile identifies a local physical file, not source authority.
type NativeReportFile struct{ Platform, Device, Inode string }
type NativeReportCursor struct {
	Epoch                                              string
	Generation, SchemaVersion, Sequence, PrunedThrough int64
}
type NativeReportAnchor struct {
	File         NativeReportFile
	Repository   string
	RepositoryID int64
	ScopeSHA256  string
	Cursor       NativeReportCursor
}
type NativeReportLimits struct{ MaxRows, MaxBytes int64 }
type NativeReportRequest struct {
	Repository string
	File       NativeReportFile
	Limits     NativeReportLimits
}

type NativeReportValueKind uint8

const (
	NativeReportNull NativeReportValueKind = iota
	NativeReportText
	NativeReportInteger
)

// NativeReportValue preserves SQL NULL, signed integers and exact text bytes.
// Text is local evidence and may contain NUL or invalid UTF-8. It is not JSON.
type NativeReportValue struct {
	Kind    NativeReportValueKind
	Text    string
	Integer int64
}
type NativeReportTable struct {
	Name                string
	Columns, KeyColumns []string
	Rows                [][]NativeReportValue
	SHA256              string
}

// NativeReportCapture is a complete scoped local snapshot, not a wire profile or
// a qualified delivery baseline. Keys are the indicated columns of each row.
// A future projection must qualify raw evidence and exclude local-only fields.
type NativeReportCapture struct {
	Anchor      NativeReportAnchor
	Tables      []NativeReportTable
	SHA256      string
	Rows, Bytes int64
}

// NativeReportFileIdentity does not open SQLite or authorize access. The owner
// must keep the literal absolute pathname bound to one physical file throughout
// capture. Concurrent SQLite writes are supported; rename/replacement is not.
// Pre/post stat detects persistent replacement, not an A -> B -> A path swap.
func NativeReportFileIdentity(path string) (NativeReportFile, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return NativeReportFile{}, ErrNativeReportSource
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return NativeReportFile{}, ErrNativeReportSource
	}
	identity, ok := nativeReportPhysicalIdentity(info)
	if !ok {
		return NativeReportFile{}, ErrNativeReportSource
	}
	return identity, nil
}

// CaptureNativeReport owns one read transaction and materializes the entire
// selected scope or returns a zero result. It never migrates, enables tracking,
// writes a checkpoint, infers provider deletion, or qualifies a baseline.
// Every call scans scoped content. This snapshot API does not use journal keys
// to skip rows or treat the sequence as a substitute for an inventory digest.
func CaptureNativeReport(ctx context.Context, path string, request NativeReportRequest) (NativeReportCapture, error) {
	if err := ctx.Err(); err != nil {
		return NativeReportCapture{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return NativeReportCapture{}, errors.New("native report capture requires a deadline")
	}
	if request.Limits.MaxRows <= 0 || request.Limits.MaxBytes <= 0 {
		return NativeReportCapture{}, ErrNativeReportLimit
	}
	parts := strings.Split(request.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(request.Repository, " \t\r\n\\\x00") || strings.ToLower(request.Repository) != request.Repository {
		return NativeReportCapture{}, ErrNativeReportSource
	}
	file, err := NativeReportFileIdentity(path)
	if err != nil || file != request.File {
		return NativeReportCapture{}, ErrNativeReportSource
	}
	source, err := OpenReadOnly(ctx, path)
	if err != nil {
		return NativeReportCapture{}, fmt.Errorf("open native report source: %w", err)
	}
	defer source.Close()
	tx, err := source.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return NativeReportCapture{}, err
	}
	defer tx.Rollback()
	capture, err := captureNativeReport(ctx, tx, request)
	if err != nil {
		return NativeReportCapture{}, err
	}
	after, err := NativeReportFileIdentity(path)
	if err != nil || after != file {
		return NativeReportCapture{}, ErrNativeReportSource
	}
	if err := ctx.Err(); err != nil {
		return NativeReportCapture{}, err
	}
	return capture, nil
}

type nativeReportTableSpec struct {
	name, columns, keys, from string
}

// This is a native schema-16 local read set, not the historical GitHub seed or
// a service allowlist. Only the comments observation family is in this scope.
var nativeReportTables = []nativeReportTableSpec{
	{"repositories", "id owner name full_name github_repo_id raw_json updated_at", "id", "repositories x where x.id = ?"},
	{"threads", "id repo_id github_id number kind state title body author_login author_type author_association html_url labels_json assignees_json raw_json content_hash is_draft created_at_gh updated_at_gh closed_at_gh merged_at_gh closed_at_local close_reason_local first_pulled_at last_pulled_at observation_sequence evidence_observation_sequence evidence_source_updated_at updated_at", "id", "threads x where x.id in (" + nativeReportThreads + ")"},
	{"comments", "id thread_id github_id comment_type author_login author_type body is_bot raw_json raw_json_blob_id created_at_gh updated_at_gh deleted_at deletion_reason submitted_at_gh publication_at_gh", "id", "comments x where x.thread_id in (" + nativeReportThreads + ")"},
	{"thread_child_observation_reservations", "thread_id family source_updated_at observation_sequence", "thread_id family", "thread_child_observation_reservations x where x.family = 'comments' and x.thread_id in (" + nativeReportThreads + ")"},
	{"thread_child_observation_memberships", "thread_id family observation_sequence member_ids_json", "thread_id family", "thread_child_observation_memberships x where x.family = 'comments' and x.thread_id in (" + nativeReportThreads + ")"},
	{"thread_exclusions", "repository number kind original_thread_id github_id excluded_at reason plan_id", "repository number", "thread_exclusions x where x.repository = (select lower(full_name) from repositories where id = ?)"},
	{"thread_excluded_nodes", "node_id repository number", "node_id", "thread_excluded_nodes x where x.repository = (select lower(full_name) from repositories where id = ?)"},
}

const nativeReportThreads = `select t.id from threads t join repositories r on r.id=t.repo_id where t.repo_id=? and not exists(select 1 from thread_exclusions e where e.repository=lower(r.full_name) and e.number=t.number)`

func captureNativeReport(ctx context.Context, tx *sql.Tx, request NativeReportRequest) (NativeReportCapture, error) {
	out := NativeReportCapture{Anchor: NativeReportAnchor{File: request.File, Repository: request.Repository}}
	var version int
	if err := tx.QueryRowContext(ctx, "pragma user_version").Scan(&version); err != nil {
		return out, err
	}
	portable, err := changeContractTablePresent(ctx, tx, "portable_metadata")
	if err != nil {
		return out, err
	}
	if version != schemaVersion || portable {
		return out, ErrNativeReportSource
	}
	cursor := &out.Anchor.Cursor
	if err := tx.QueryRowContext(ctx, "pragma schema_version").Scan(&cursor.SchemaVersion); err != nil {
		return out, err
	}
	if err := tx.QueryRowContext(ctx, `select s.contract_version,s.epoch,s.generation,q.value,q.pruned_through from change_contract_state s join change_contract_sequence q on q.id=s.id where s.id=1`).Scan(&version, &cursor.Epoch, &cursor.Generation, &cursor.Sequence, &cursor.PrunedThrough); err != nil {
		return out, ErrNativeReportSource
	}
	if version != ChangeContractVersion || cursor.Epoch == "" || cursor.Generation < 1 || cursor.Sequence < 0 || cursor.PrunedThrough < 0 || cursor.PrunedThrough > cursor.Sequence {
		return out, ErrNativeReportSource
	}
	armed, err := changeContractArmed(ctx, tx)
	if err != nil {
		return out, err
	}
	if !armed {
		return out, ErrNativeReportSource
	}
	var count int
	if err := tx.QueryRowContext(ctx, "select count(*),coalesce(min(id),0) from repositories where lower(full_name)=?", request.Repository).Scan(&count, &out.Anchor.RepositoryID); err != nil {
		return out, err
	}
	if count != 1 || out.Anchor.RepositoryID <= 0 {
		return out, ErrNativeReportSource
	}
	scope := sha256.New()
	nativeReportHashText(scope, NativeReportProfile)
	nativeReportHashText(scope, request.Repository)
	out.Anchor.ScopeSHA256 = hex.EncodeToString(scope.Sum(nil))
	snapshot := sha256.New()
	nativeReportHashText(snapshot, out.Anchor.ScopeSHA256)
	for _, spec := range nativeReportTables {
		table, err := readNativeReportTable(ctx, tx, spec, out.Anchor.RepositoryID, request.Limits, &out.Rows, &out.Bytes)
		if err != nil {
			return NativeReportCapture{}, fmt.Errorf("capture %s: %w", spec.name, err)
		}
		nativeReportHashText(snapshot, table.SHA256)
		out.Tables = append(out.Tables, table)
	}
	out.SHA256 = hex.EncodeToString(snapshot.Sum(nil))
	return out, nil
}

func readNativeReportTable(ctx context.Context, tx *sql.Tx, spec nativeReportTableSpec, repoID int64, limits NativeReportLimits, totalRows, totalBytes *int64) (NativeReportTable, error) {
	table := NativeReportTable{Name: spec.name, Columns: strings.Fields(spec.columns), KeyColumns: strings.Fields(spec.keys)}
	// Refuse missing/extra columns, including empty tables, instead of silently
	// omitting a migration-added fact. Column physical order is not significant.
	rows, err := tx.QueryContext(ctx, "select name from pragma_table_info(?)", spec.name)
	if err != nil {
		return table, err
	}
	var actual []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			break
		}
		actual = append(actual, name)
	}
	readErr := rows.Err()
	rows.Close()
	if err != nil {
		return table, err
	}
	if readErr != nil {
		return table, readErr
	}
	expected := append([]string(nil), table.Columns...)
	slices.Sort(actual)
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		return table, ErrNativeReportSource
	}
	keys, err := changeContractKeyColumns(ctx, tx, spec.name)
	if err != nil {
		return table, err
	}
	if !slices.Equal(keys, table.KeyColumns) {
		return table, ErrNativeReportSource
	}
	var mode string
	if err := tx.QueryRowContext(ctx, `select mode from change_contract_tables where table_name=?`, spec.name).Scan(&mode); err != nil || mode != ChangeContractModeRows {
		return table, ErrNativeReportSource
	}
	armed, err := changeContractTableArmed(ctx, tx, spec.name)
	if err != nil {
		return table, err
	}
	if !armed {
		return table, ErrNativeReportSource
	}
	if spec.name == "threads" || spec.name == "comments" {
		// Match the purge owner's native $.node_id interpretation and global
		// NodeExcluded policy. A retained conflict outside the parent filter
		// holds the snapshot; silently pruning rows would alter membership facts.
		var excluded bool
		if err := tx.QueryRowContext(ctx, "select exists(select 1 from "+spec.from+`
			and exists(select 1 from thread_excluded_nodes e where e.node_id =
			json_extract(case when json_valid(x.raw_json) then x.raw_json else '{}' end,'$.node_id')))
		`, repoID).Scan(&excluded); err != nil {
			return table, err
		}
		if excluded {
			return table, ErrNativeReportSource
		}
	}
	columns, sizes := make([]string, len(table.Columns)), make([]string, len(table.Columns))
	for i, name := range table.Columns {
		columns[i] = "x." + sqliteQuotedIdentifier(name)
		sizes[i] = "case typeof(" + columns[i] + ") when 'null' then 1 when 'integer' then 9 else 9+length(cast(" + columns[i] + " as blob)) end"
		if int64(len(name)) > limits.MaxBytes-*totalBytes {
			return table, ErrNativeReportLimit
		}
		*totalBytes += int64(len(name))
	}
	var n, size int64
	// Preflight scalar sizes without materializing payloads in Go. The deadline
	// bounds SQL scan time; these logical caps are not an RSS guarantee.
	if err := tx.QueryRowContext(ctx, "select count(*),coalesce(sum("+strings.Join(sizes, "+")+"),0) from "+spec.from, repoID).Scan(&n, &size); err != nil {
		return table, err
	}
	if n > limits.MaxRows-*totalRows || size < 0 || size > limits.MaxBytes-*totalBytes {
		return table, ErrNativeReportLimit
	}
	order := make([]string, len(keys))
	for i, k := range keys {
		order[i] = "x." + sqliteQuotedIdentifier(k)
	}
	rows, err = tx.QueryContext(ctx, "select "+strings.Join(columns, ",")+" from "+spec.from+" order by "+strings.Join(order, ","), repoID)
	if err != nil {
		return table, err
	}
	defer rows.Close()
	digest := sha256.New()
	nativeReportHashText(digest, spec.name)
	for _, name := range table.Columns {
		nativeReportHashText(digest, name)
	}
	for rows.Next() {
		if *totalRows >= limits.MaxRows {
			return table, ErrNativeReportLimit
		}
		raw := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return table, err
		}
		values := make([]NativeReportValue, len(raw))
		for i, value := range raw {
			v := NativeReportValue{}
			bytes := int64(1)
			switch x := value.(type) {
			case nil:
			case string:
				v.Kind = NativeReportText
				v.Text = x
				bytes = 9 + int64(len(x))
			case int64:
				v.Kind = NativeReportInteger
				v.Integer = x
				bytes = 9
			default:
				return table, ErrNativeReportSource
			}
			if v.Kind == NativeReportNull && slices.Contains(keys, table.Columns[i]) {
				return table, ErrNativeReportSource
			}
			if spec.name == "comments" && table.Columns[i] == "raw_json_blob_id" && v.Kind != NativeReportNull {
				return table, ErrNativeReportBlob
			}
			if bytes > limits.MaxBytes-*totalBytes {
				return table, ErrNativeReportLimit
			}
			*totalBytes += bytes
			digest.Write([]byte{byte(v.Kind)})
			if v.Kind == NativeReportText {
				nativeReportHashText(digest, v.Text)
			} else if v.Kind == NativeReportInteger {
				nativeReportHashInt(digest, uint64(v.Integer))
			}
			values[i] = v
		}
		table.Rows = append(table.Rows, values)
		*totalRows++
	}
	if err := rows.Err(); err != nil {
		return table, err
	}
	table.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return table, nil
}
func nativeReportHashInt(h hash.Hash, n uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	h.Write(b[:])
}
func nativeReportHashText(h hash.Hash, s string) {
	nativeReportHashInt(h, uint64(len(s)))
	h.Write([]byte(s))
}
