//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

var inventoryTestLimits = InventoryMigrationLimits{MaxRows: 10000, MaxBytes: 32 << 20}

func completeInventorySeed(t *testing.T, ctx context.Context, o *Outbox) {
	t.Helper()
	for {
		part, err := o.NextPart(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if part == nil {
			return
		}
		if err := o.RecordReceipt(ctx, part.Ref, seedReceipt(part)); err != nil {
			t.Fatal(err)
		}
	}
}

// Snapshot exact persisted seed evidence, not a re-encoding of its JSON.
func inventorySeedEvidence(t *testing.T, ctx context.Context, o *Outbox) [][]byte {
	t.Helper()
	var evidence [][]byte
	for _, query := range []string{
		`select cast(seed_id as blob),anchor_json,cast(native_through as blob),cast(native_sha256 as blob) from captures`,
		`select body,cast(payload_sha256 as blob),receipt_json from parts order by part_index`,
	} {
		rows, err := o.base.DB().QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([][]byte, len(columns))
			dest := make([]any, len(values))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			evidence = append(evidence, values...)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	return evidence
}

func independentInventoryDigest(t *testing.T, ctx context.Context, capture store.NativeReportCapture) string {
	t.Helper()
	codec, err := privateInventoryCodec()
	if err != nil {
		t.Fatal(err)
	}
	report, err := ProjectPrivateReport(capture)
	if err != nil {
		t.Fatal(err)
	}
	var entries []remote.InventoryEntry
	var counts []remote.InventoryCount
	for _, table := range report.Tables {
		counts = append(counts, remote.InventoryCount{Table: table.Name, Rows: uint64(len(table.Rows))})
		for _, values := range table.Rows {
			row := make([]remote.ChangeValue, len(values))
			for i, v := range values {
				row[i] = remote.ChangeValue{Kind: v.Kind, Value: v.Value}
			}
			key := make([]remote.ChangeValue, len(table.KeyColumns))
			for i, name := range table.KeyColumns {
				key[i] = row[slices.Index(table.Columns, name)]
			}
			encoded, err := codec.EncodeKey(table.Name, key)
			if err != nil {
				t.Fatal(err)
			}
			hash, err := codec.RowDigest(table.Name, key, row)
			if err != nil {
				t.Fatal(err)
			}
			entries = append(entries, remote.InventoryEntry{Table: table.Name, Key: encoded, RowSHA256: hash})
		}
	}
	slices.SortFunc(entries, func(a, b remote.InventoryEntry) int {
		if d := bytes.Compare([]byte(a.Table), []byte(b.Table)); d != 0 {
			return d
		}
		return bytes.Compare(a.Key, b.Key)
	})
	i := 0
	hash, err := codec.Digest(ctx, []byte(capture.Anchor.ScopeSHA256), counts, remote.InventoryLimits{MaxEntries: uint64(capture.Rows), MaxKeyBytes: remote.MaxChangeBytes}, func(context.Context) (remote.InventoryEntry, error) {
		if i == len(entries) {
			return remote.InventoryEntry{}, io.EOF
		}
		entry := entries[i]
		i++
		return entry, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash[:])
}

func TestOutboxInventoryMigrationExactSeedAndOwnership(t *testing.T) {
	ctx, native := nativeFixture(t)
	rawValues := []string{`{"node_id":9007199254740993}`, `{"node_id":1e30}`, `{"node_id":true}`, `invalid`, `{"node_id":null}`, `{}`, `{"node_id":"nul\u0000snow☃"}`}
	// Cross the real 500-row seed chunk boundary while retaining bounded rows.
	for i := range 501 {
		raw := rawValues[i%len(rawValues)]
		if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: strconv.Itoa(i + 1), CommentType: "issue_comment", Body: "private\x00body☃", RawJSON: raw}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := native.ReserveThreadChildObservation(ctx, 1, store.ThreadChildComments, "2026-01-01", 1); err != nil {
		t.Fatal(err)
	}
	if err := native.ReplaceThreadChildObservationMembers(ctx, 1, store.ThreadChildComments, 1, []int64{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := native.DB().ExecContext(ctx, `insert into thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,plan_id) values('fixture/repo',999,'issue',999,'gone','2026-01-01','owner_requested','fixture'); insert into thread_excluded_nodes(node_id,repository,number) values('absent-node','fixture/repo',999)`); err != nil {
		t.Fatal(err)
	}
	capture, _ := projected(t, ctx, native)
	expectedDigest := independentInventoryDigest(t, ctx, capture)
	o, path := newSeedOutbox(t, ctx, capture)
	completeInventorySeed(t, ctx, o)
	before := inventorySeedEvidence(t, ctx, o)
	state := outboxState(t, ctx, o)
	// Only a disposable fixture is removed. The recorded native path is now
	// absent, proving migration consumes saved bodies without reopening it.
	if err := native.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(native.Path()); err != nil {
		t.Fatal(err)
	}
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, inventorySeedEvidence(t, ctx, o)) {
		t.Fatal("seed request/receipt history changed")
	}
	got, err := ReadReportOutboxState(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Registration != state.Registration || got.BaselineAnchor != capture.Anchor || got.BaselineNative != state.BaselineNative || got.BaselineSHA256 != state.BaselineSHA256 || got.ServerCursor != state.ServerCursor || got.ReceiverSequence != int64(state.PartCount) || got.InventorySHA256 != expectedDigest || got.InventoryLayout != privateInventoryLayoutID {
		t.Fatalf("migrated state differs: %#v", got)
	}
	reopened, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err == nil {
		t.Fatal("second migration accepted")
	}
	if _, err := OpenSeedOutbox(ctx, path); err == nil {
		t.Fatal("schema2 accepted as seed")
	}
	var count int64
	if err := reopened.base.DB().QueryRowContext(ctx, `select count(*) from inventory_rows`).Scan(&count); err != nil || count != capture.Rows {
		t.Fatal("inventory count", count, err)
	}
	for i, raw := range rawValues {
		key, _ := json.Marshal([]remote.ChangeValue{inventoryScalar("integer", strconv.Itoa(i+1))})
		var node, expected sql.NullString
		var repo, thread, number int64
		if err := reopened.base.DB().QueryRowContext(ctx, `select repository_id,thread_id,thread_number,node_id from inventory_rows where table_name='comments' and key_json=?`, key).Scan(&repo, &thread, &number, &node); err != nil {
			t.Fatal(err)
		}
		if err := reopened.base.DB().QueryRowContext(ctx, `select cast(json_extract(case when json_valid(?) then ? else '{}' end,'$.node_id') as text)`, raw, raw).Scan(&expected); err != nil {
			t.Fatal(err)
		}
		if repo != capture.Anchor.RepositoryID || thread != 1 || number != 10 || node != expected {
			t.Fatalf("native ownership/node semantics differ at %d", i)
		}
		if i == 0 && node.String != "9007199254740993" {
			t.Fatal("node integer rounded")
		}
	}
	for _, table := range []string{"thread_child_observation_reservations", "thread_child_observation_memberships"} {
		var thread, number int64
		if err := reopened.base.DB().QueryRowContext(ctx, `select thread_id,thread_number from inventory_rows where table_name=?`, table).Scan(&thread, &number); err != nil || thread != 1 || number != 10 {
			t.Fatal("child ownership", table, err)
		}
	}
}

func TestOutboxInventoryMigrationRefusalRollsBack(t *testing.T) {
	for _, test := range []struct {
		name, mutation string
		limits         InventoryMigrationLimits
		complete       bool
	}{
		{name: "unqualified"},
		{name: "rows", complete: true, limits: InventoryMigrationLimits{MaxRows: 1, MaxBytes: 32 << 20}},
		{name: "bytes", complete: true, limits: InventoryMigrationLimits{MaxRows: 10000, MaxBytes: 7000}},
		{name: "last receipt", complete: true, mutation: `update parts set receipt_json=json_set(receipt_json,'$.sequence','999') where part_index=(select max(part_index) from parts)`},
		{name: "receipt hash", complete: true, mutation: `update parts set receipt_json=json_set(receipt_json,'$.payload_sha256','bad') where part_index=1`},
		{name: "chunk hash", complete: true, mutation: `update parts set body=cast('{}' as blob) where part_index=1`},
		{name: "native count", complete: true, mutation: `update captures set native_rows=native_rows+1`},
		{name: "native bytes", complete: true, mutation: `update captures set native_bytes=native_bytes+1`},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, native := nativeFixture(t)
			capture, _ := projected(t, ctx, native)
			o, path := newSeedOutbox(t, ctx, capture)
			if test.complete {
				completeInventorySeed(t, ctx, o)
			}
			if test.mutation != "" {
				if _, err := o.base.DB().ExecContext(ctx, test.mutation); err != nil {
					t.Fatal(err)
				}
			}
			before := inventorySeedEvidence(t, ctx, o)
			limits := test.limits
			if limits.MaxRows == 0 {
				limits = inventoryTestLimits
			}
			if test.name == "bytes" {
				if err := o.base.DB().QueryRowContext(ctx, `select sum(length(body)+length(receipt_json))-1 from parts`).Scan(&limits.MaxBytes); err != nil {
					t.Fatal(err)
				}
			}
			if err := MigrateSeedOutboxV2(ctx, path, limits); err == nil {
				t.Fatal("invalid migration accepted")
			}
			if !reflect.DeepEqual(before, inventorySeedEvidence(t, ctx, o)) {
				t.Fatal("failure changed seed history")
			}
			var version, tables, columns int
			if err := o.base.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := o.base.DB().QueryRowContext(ctx, `select count(*) from sqlite_schema where type='table' and name not like 'sqlite_%'`).Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if err := o.base.DB().QueryRowContext(ctx, `select count(*) from pragma_table_info('binding') where name='inventory_sha256'`).Scan(&columns); err != nil {
				t.Fatal(err)
			}
			if version != 1 || tables != 3 || columns != 0 {
				t.Fatal("failed migration retained DDL", version, tables, columns)
			}
			if _, err := ReadReportOutboxState(ctx, path); err == nil {
				t.Fatal("v1 accepted as v2")
			}
		})
	}
}

func TestOutboxInventoryScopeEncodingGolden(t *testing.T) {
	codec, err := privateInventoryCodec()
	if err != nil {
		t.Fatal(err)
	}
	var counts []remote.InventoryCount
	for _, layout := range privateLayouts {
		counts = append(counts, remote.InventoryCount{Table: layout.name})
	}
	// Independently length-framed SHA256 vector: all seven declared empty
	// tables and exactly 64 ASCII scope bytes ("0a" repeated), not 32 decoded bytes.
	hash, err := codec.Digest(context.Background(), []byte(strings.Repeat("0a", 32)), counts, remote.InventoryLimits{MaxKeyBytes: 1}, func(context.Context) (remote.InventoryEntry, error) { return remote.InventoryEntry{}, io.EOF })
	if err != nil || hex.EncodeToString(hash[:]) != "86ac9bd2631a4c80426ebeb696213be441d9723cadffaa5c14a7f64a6c07cb06" {
		t.Fatal("private scope encoding changed", hash, err)
	}
}

func TestOutboxInventoryMigrationDeadlineAndBounds(t *testing.T) {
	ctx, native := nativeFixture(t)
	capture, _ := projected(t, ctx, native)
	o, path := newSeedOutbox(t, ctx, capture)
	completeInventorySeed(t, ctx, o)
	before := inventorySeedEvidence(t, ctx, o)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for _, c := range []context.Context{context.Background(), expired} {
		if err := MigrateSeedOutboxV2(c, path, inventoryTestLimits); err == nil {
			t.Fatal("unbounded/expired migration accepted")
		}
	}
	for _, limits := range []InventoryMigrationLimits{{MaxRows: 1}, {MaxBytes: 1}, {MaxRows: -1, MaxBytes: 1}} {
		if err := MigrateSeedOutboxV2(ctx, path, limits); err == nil {
			t.Fatal("nonpositive budget accepted")
		}
	}
	if !reflect.DeepEqual(before, inventorySeedEvidence(t, ctx, o)) {
		t.Fatal("preflight refusal changed history")
	}
	if _, err := ReadSeedOutboxState(ctx, path); err != nil {
		t.Fatal("preflight refusal changed v1", err)
	}
}

func TestOutboxInventoryMigrationMissingParentAndDuplicateKey(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(strconv.FormatBool(duplicate), func(t *testing.T) {
			ctx, native := nativeFixture(t)
			for i := 0; i < 2; i++ {
				if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: strconv.Itoa(i + 1), CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
					t.Fatal(err)
				}
			}
			capture, _ := projected(t, ctx, native)
			if duplicate {
				capture.Tables[2].Rows[1][0].Integer = capture.Tables[2].Rows[0][0].Integer
			} else {
				capture.Tables[2].Rows[0][1].Integer = 999
			}
			o, path := newSeedOutbox(t, ctx, capture)
			completeInventorySeed(t, ctx, o)
			if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err == nil {
				t.Fatal("invalid ownership/key accepted")
			}
			if _, err := ReadSeedOutboxState(ctx, path); err != nil {
				t.Fatal("rollback lost original seed", err)
			}
		})
	}
}

func TestOutboxInventoryReaderRejectsInvalidAnchor(t *testing.T) {
	ctx, native := nativeFixture(t)
	capture, _ := projected(t, ctx, native)
	o, path := newSeedOutbox(t, ctx, capture)
	completeInventorySeed(t, ctx, o)
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	state, err := ReadReportOutboxState(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		field  int
		value  string
		mutate func(*store.NativeReportAnchor)
	}{
		{"epoch", 1, "ABCDEF0123456789abcdef0123456789ab", func(a *store.NativeReportAnchor) { a.Cursor.Epoch = "ABCDEF0123456789abcdef0123456789ab" }},
		{"generation", 2, "0", func(a *store.NativeReportAnchor) { a.Cursor.Generation = 0 }},
		{"cookie", 3, "0", func(a *store.NativeReportAnchor) { a.Cursor.SchemaVersion = 0 }},
		{"sequence", 4, "-1", func(a *store.NativeReportAnchor) { a.Cursor.Sequence = -1 }},
		{"floor", 5, strconv.FormatInt(state.BaselineAnchor.Cursor.Sequence+1, 10), func(a *store.NativeReportAnchor) { a.Cursor.PrunedThrough = a.Cursor.Sequence + 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			anchor := state.BaselineAnchor
			test.mutate(&anchor)
			body, _ := json.Marshal(anchor)
			parts := strings.Split(state.BaselineNative, ":")
			parts[test.field] = test.value
			if _, err := o.base.DB().ExecContext(ctx, `update binding set baseline_anchor_json=?,baseline_native=?`, body, strings.Join(parts, ":")); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadReportOutboxState(ctx, path); err == nil {
				t.Fatal("invalid but matching anchor/token accepted")
			}
		})
	}
}
