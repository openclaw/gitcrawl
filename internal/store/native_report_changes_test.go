//go:build darwin || linux

package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type reportOldIndexFixture struct {
	refs       []NativeReportRef
	beforeKeys func()
	duplicate  int
	err        error
}

func (f *reportOldIndexFixture) walk(ctx context.Context, match func(NativeReportRef) bool, yield func(NativeReportRef) error) error {
	if f.err != nil {
		return f.err
	}
	for _, ref := range f.refs {
		if !match(ref) {
			continue
		}
		for i := 0; i <= f.duplicate; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := yield(ref); err != nil {
				return err
			}
		}
	}
	return nil
}
func (f *reportOldIndexFixture) Keys(ctx context.Context, keys []NativeReportKey, yield func(NativeReportRef) error) error {
	if f.beforeKeys != nil {
		hook := f.beforeKeys
		f.beforeKeys = nil
		hook()
	}
	if len(keys) > 500 {
		panic("unbounded old-index request")
	}
	return f.walk(ctx, func(ref NativeReportRef) bool {
		return slices.ContainsFunc(keys, func(k NativeReportKey) bool { return reflect.DeepEqual(k, ref.Key) })
	}, yield)
}
func (f *reportOldIndexFixture) ThreadSubtrees(ctx context.Context, ids []int64, yield func(NativeReportRef) error) error {
	if len(ids) > 500 {
		panic("unbounded old-index request")
	}
	return f.walk(ctx, func(ref NativeReportRef) bool { return reportRefHasParent(ref) && slices.Contains(ids, ref.ThreadID) }, yield)
}
func (f *reportOldIndexFixture) ThreadNumbers(ctx context.Context, ids []int64, yield func(NativeReportRef) error) error {
	if len(ids) > 500 {
		panic("unbounded old-index request")
	}
	return f.walk(ctx, func(ref NativeReportRef) bool {
		return reportRefHasParent(ref) && slices.Contains(ids, ref.ThreadNumber)
	}, yield)
}
func (f *reportOldIndexFixture) Nodes(ctx context.Context, nodes []string, yield func(NativeReportRef) error) error {
	if len(nodes) > 500 {
		panic("unbounded old-index request")
	}
	return f.walk(ctx, func(ref NativeReportRef) bool { return ref.NodeID != nil && slices.Contains(nodes, *ref.NodeID) }, yield)
}
func reportRefHasParent(ref NativeReportRef) bool {
	return ref.Key.Table == "threads" || ref.Key.Table == "comments" || strings.HasPrefix(ref.Key.Table, "thread_child_observation_")
}

func reportDeltaFixture(t *testing.T) (*Store, context.Context, NativeReportRequest) {
	t.Helper()
	s := changeContractFixture(t, 0)
	ctx, req := nativeReportRequest(t, s)
	if err := s.PrepareNativeReportIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	reportDeltaSQL(t, s, `insert into repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) values(2,'other','repo','other/repo','200','{}','now');
update threads set raw_json='{"node_id":"thread-one"}' where id=1;
insert into comments(id,thread_id,github_id,comment_type,body,raw_json) values
(1,1,'comment-one','issue_comment','first','{"node_id":"comment-one"}'),
(2,2,'comment-two','issue_comment','second','{"node_id":123.0}');`)
	if _, err := s.ReserveThreadChildObservation(ctx, 1, ThreadChildComments, "", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, ThreadChildComments, 1, []int64{1}); err != nil {
		t.Fatal(err)
	}
	return s, ctx, req
}

func reportDeltaSQL(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func reportDeltaRequest(base NativeReportCapture) NativeReportDeltaRequest {
	return NativeReportDeltaRequest{Baseline: base.Anchor, Limits: NativeReportDeltaLimits{MaxJournalRecords: 10000, MaxAffectedKeys: 10000, MaxLookupRows: 100000, MaxRows: 10000, MaxNativeBytes: 8 << 20}}
}

func reportDeltaKey(table NativeReportTable, row []NativeReportValue) NativeReportKey {
	key := NativeReportKey{Table: table.Name}
	for _, name := range table.KeyColumns {
		key.Values = append(key.Values, row[slices.Index(table.Columns, name)])
	}
	return key
}

func reportDeltaIndex(t *testing.T, s *Store, base NativeReportCapture) *reportOldIndexFixture {
	t.Helper()
	f := &reportOldIndexFixture{}
	parents := make(map[int64]int64)
	for _, table := range base.Tables {
		if table.Name == "threads" {
			for _, row := range table.Rows {
				parents[row[0].Integer] = row[slices.Index(table.Columns, "number")].Integer
			}
		}
	}
	for _, table := range base.Tables {
		for _, row := range table.Rows {
			ref := NativeReportRef{Key: reportDeltaKey(table, row), RepositoryID: base.Anchor.RepositoryID}
			if reportRefHasParent(ref) {
				column := "thread_id"
				if table.Name == "threads" {
					column = "id"
				}
				ref.ThreadID = row[slices.Index(table.Columns, column)].Integer
				ref.ThreadNumber = parents[ref.ThreadID]
			} else if table.Name == "thread_exclusions" || table.Name == "thread_excluded_nodes" {
				ref.ThreadNumber = row[slices.Index(table.Columns, "number")].Integer
			}
			if table.Name == "threads" || table.Name == "comments" {
				v := row[slices.Index(table.Columns, "raw_json")]
				var raw any
				if v.Kind == NativeReportText {
					raw = v.Text
				}
				var node sql.NullString
				if err := s.db.QueryRow("select "+nativeReportNodeKeySQL+" from (select ? raw_json)", raw).Scan(&node); err != nil {
					t.Fatal(err)
				}
				if node.Valid {
					ref.NodeID = &node.String
				}
			}
			f.refs = append(f.refs, ref)
		}
	}
	return f
}

func reportRows(c NativeReportCapture) map[string][]NativeReportValue {
	rows := make(map[string][]NativeReportValue)
	for _, table := range c.Tables {
		for _, row := range table.Rows {
			key, _ := nativeReportKeyID(reportDeltaKey(table, row))
			rows[key] = row
		}
	}
	return rows
}

func assertReportDelta(t *testing.T, base, full NativeReportCapture, delta NativeReportDelta) {
	t.Helper()
	got := reportRows(base)
	for _, removed := range delta.Removals {
		id, err := nativeReportKeyID(removed.Key)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got[id]; !ok {
			t.Fatal("removal not qualified by old inventory")
		}
		delete(got, id)
	}
	for _, row := range delta.Upserts {
		id, err := nativeReportKeyID(row.Key)
		if err != nil {
			t.Fatal(err)
		}
		got[id] = row.Values
	}
	if !reflect.DeepEqual(got, reportRows(full)) {
		t.Fatal("applying delta differs from full native capture")
	}
	if delta.From != base.Anchor || delta.Through != full.Anchor {
		t.Fatal("delta mixed snapshot anchors")
	}
	if delta.Rows != int64(len(delta.Upserts)+len(delta.Removals)) {
		t.Fatal("incorrect result row count")
	}
}

func TestNativeReportChangesClosureMatchesSnapshot(t *testing.T) {
	for _, test := range []struct{ name, sql string }{
		{"thread update", `update threads set title='changed' where id=1`},
		{"parent delete", `delete from threads where id=1`},
		{"parent move out", `update threads set repo_id=2 where id=1`},
		{"parent move in", `update threads set repo_id=1 where id=3`},
		{"number change", `update threads set number=12 where id=1`},
		{"comment move", `update comments set thread_id=2 where id=1`},
		{"old and new key", `update comments set id=31 where id=1`},
		{"tombstone", `update comments set deleted_at='2026-01-02',deletion_reason='provider' where id=1`},
		{"membership omission", `update thread_child_observation_memberships set member_ids_json='[]' where thread_id=1`},
		{"exclude retained subtree", `insert into thread_exclusions values('fixture/repo',10,'pull_request',1,'gh10','now','owner_requested','plan')`},
		{"remove exclusion", `delete from thread_exclusions where repository='fixture/repo' and number=10`},
		{"change exclusion number", `update thread_exclusions set number=20 where repository='fixture/repo' and number=10`},
		{"new child", `insert into comments(id,thread_id,github_id,comment_type,body,raw_json) values(50,1,'new','issue_comment','new','{}')`},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, ctx, req := reportDeltaFixture(t)
			if test.name == "parent move in" {
				reportDeltaSQL(t, s, `insert into threads(id,repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) values(3,2,'other',30,'issue','open','other','other','https://example.invalid/3','[]','[]','{}','hash','now'); insert into comments(id,thread_id,github_id,comment_type,body,raw_json) values(3,3,'other','issue_comment','old child','{}')`)
			}
			if test.name == "remove exclusion" || test.name == "change exclusion number" {
				reportDeltaSQL(t, s, `insert into thread_exclusions values('fixture/repo',10,'pull_request',1,'gh10','now','owner_requested','plan')`)
			}
			base := takeNativeReport(t, ctx, s, req)
			old := reportDeltaIndex(t, s, base)
			reportDeltaSQL(t, s, test.sql)
			got, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old)
			if err != nil {
				t.Fatal(err)
			}
			assertReportDelta(t, base, takeNativeReport(t, ctx, s, req), got)
			again, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old)
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatal("delta is not deterministic", err)
			}
			if test.name == "membership omission" && len(got.Removals) != 0 {
				t.Fatal("membership omission inferred comment deletion")
			}
			if test.name == "tombstone" && (len(got.Removals) != 0 || len(got.Upserts) != 1) {
				t.Fatal("native tombstone was not retained as an upsert")
			}
		})
	}
}

func TestNativeReportChangesSparseAndEmptyIntervals(t *testing.T) {
	s, ctx, req := reportDeltaFixture(t)
	base := takeNativeReport(t, ctx, s, req)
	old := reportDeltaIndex(t, s, base)
	for _, mutate := range []bool{false, true} {
		if mutate {
			reportDeltaSQL(t, s, `insert into pull_request_files(thread_id,path,raw_json,fetched_at) values(2,'fixture.go','{}','now')`)
		}
		got, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old)
		if err != nil {
			t.Fatal(err)
		}
		if got.JournalRecords != 0 || got.Rows != 0 || got.LookupRows != 0 {
			t.Fatal("empty journal read content")
		}
		assertReportDelta(t, base, takeNativeReport(t, ctx, s, req), got)
		if mutate && got.Through.Cursor.Sequence <= base.Anchor.Cursor.Sequence {
			t.Fatal("sparse interval did not advance through")
		}
	}
}

func TestNativeReportChangesGlobalNodeHoldAndRemoval(t *testing.T) {
	for _, node := range []string{"thread-one", "comment-one", "123.0"} {
		t.Run(node, func(t *testing.T) {
			s, ctx, req := reportDeltaFixture(t)
			base := takeNativeReport(t, ctx, s, req)
			old := reportDeltaIndex(t, s, base)
			reportDeltaSQL(t, s, `insert into thread_exclusions values('other/repo',90,'issue',90,'90','now','owner_requested','plan'); insert into thread_excluded_nodes values(?,'other/repo',90)`, node)
			got, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old)
			if !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(got, NativeReportDelta{}) {
				t.Fatal("cross-repository node conflict did not hold", err)
			}
			if _, err := CaptureNativeReport(ctx, s.Path(), req); !errors.Is(err, ErrNativeReportSource) {
				t.Fatal("fixture missing full-capture conflict", err)
			}
			// Remove the policy in a later interval while keeping the old qualified
			// baseline. Unchanged descendants remain current, never provider-deleted.
			reportDeltaSQL(t, s, `delete from thread_excluded_nodes where node_id=?`, node)
			got, err = CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old)
			if err != nil {
				t.Fatal(err)
			}
			assertReportDelta(t, base, takeNativeReport(t, ctx, s, req), got)
		})
	}
}

func TestNativeReportChangesPinsPayloadReadsToJournalSnapshot(t *testing.T) {
	s, ctx, req := reportDeltaFixture(t)
	base := takeNativeReport(t, ctx, s, req)
	old := reportDeltaIndex(t, s, base)
	reportDeltaSQL(t, s, `update threads set title='at-H' where id=1; update comments set body='at-H' where id=1`)
	atH := takeNativeReport(t, ctx, s, req)
	old.beforeKeys = func() {
		reportDeltaSQL(t, s, `update threads set title='after-H' where id=1; update comments set body='after-H' where id=1`)
	}
	got, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old)
	if err != nil {
		t.Fatal(err)
	}
	assertReportDelta(t, base, atH, got)
	if takeNativeReport(t, ctx, s, req).Anchor.Cursor.Sequence <= got.Through.Cursor.Sequence {
		t.Fatal("writer did not advance during capture")
	}
}

func TestNativeReportChangesBoundsAndZeroFailure(t *testing.T) {
	s, ctx, req := reportDeltaFixture(t)
	base := takeNativeReport(t, ctx, s, req)
	old := reportDeltaIndex(t, s, base)
	reportDeltaSQL(t, s, `update comments set body=body||' changed'`)
	request := reportDeltaRequest(base)
	got, err := CaptureNativeReportChanges(ctx, s.Path(), request, old)
	if err != nil {
		t.Fatal(err)
	}
	// Every independent budget is required. Exact logical boundaries succeed.
	tight := request
	tight.Limits.MaxJournalRecords = got.JournalRecords
	tight.Limits.MaxLookupRows = got.LookupRows
	tight.Limits.MaxRows = got.Rows
	tight.Limits.MaxNativeBytes = got.Bytes
	tight.Limits.MaxAffectedKeys = 2
	if exact, err := CaptureNativeReportChanges(ctx, s.Path(), tight, old); err != nil || !reflect.DeepEqual(exact, got) {
		t.Fatal("exact bounds rejected", err)
	}
	for _, name := range []string{"journal", "affected", "lookup", "rows", "bytes"} {
		t.Run(name, func(t *testing.T) {
			limited := tight
			switch name {
			case "journal":
				limited.Limits.MaxJournalRecords--
			case "affected":
				limited.Limits.MaxAffectedKeys--
			case "lookup":
				limited.Limits.MaxLookupRows--
			case "rows":
				limited.Limits.MaxRows--
			case "bytes":
				limited.Limits.MaxNativeBytes--
			}
			out, err := CaptureNativeReportChanges(ctx, s.Path(), limited, old)
			if !errors.Is(err, ErrNativeReportLimit) || !reflect.DeepEqual(out, NativeReportDelta{}) {
				t.Fatal("budget returned partial output", err)
			}
			switch name {
			case "journal":
				limited.Limits.MaxJournalRecords = 0
			case "affected":
				limited.Limits.MaxAffectedKeys = 0
			case "lookup":
				limited.Limits.MaxLookupRows = 0
			case "rows":
				limited.Limits.MaxRows = 0
			case "bytes":
				limited.Limits.MaxNativeBytes = 0
			}
			if _, err := CaptureNativeReportChanges(ctx, s.Path(), limited, old); !errors.Is(err, ErrNativeReportLimit) {
				t.Fatal("omitted explicit limit accepted", err)
			}
		})
	}
	old.duplicate = 1
	if out, err := CaptureNativeReportChanges(ctx, s.Path(), tight, old); !errors.Is(err, ErrNativeReportLimit) || !reflect.DeepEqual(out, NativeReportDelta{}) {
		t.Fatal("duplicate lookup yields not charged", err)
	}
	old.duplicate = 0
	canceled, cancel := context.WithCancel(ctx)
	old.beforeKeys = cancel
	if out, err := CaptureNativeReportChanges(canceled, s.Path(), request, old); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(out, NativeReportDelta{}) {
		t.Fatal("cancellation during lookup lost", err)
	}
	if _, err := CaptureNativeReportChanges(context.Background(), s.Path(), request, old); err == nil {
		t.Fatal("missing deadline accepted")
	}
	old.err = errors.New("old index unavailable")
	if out, err := CaptureNativeReportChanges(ctx, s.Path(), request, old); !errors.Is(err, old.err) || !reflect.DeepEqual(out, NativeReportDelta{}) {
		t.Fatal("index failure returned partial output", err)
	}
}

func TestNativeReportChangesPreflightsBytesAndAvoidsUnchangedBodies(t *testing.T) {
	for _, which := range []string{"unchanged child", "changed body", "journal key"} {
		t.Run(which, func(t *testing.T) {
			s, ctx, req := reportDeltaFixture(t)
			if which == "unchanged child" {
				reportDeltaSQL(t, s, `update comments set body=? where id=1`, strings.Repeat("large", 100000))
			}
			base := takeNativeReport(t, ctx, s, req)
			old := reportDeltaIndex(t, s, base)
			request := reportDeltaRequest(base)
			request.Limits.MaxNativeBytes = 4096
			switch which {
			case "unchanged child":
				reportDeltaSQL(t, s, `update threads set title='changed' where id=1`)
			case "changed body":
				reportDeltaSQL(t, s, `update comments set body=? where id=1`, strings.Repeat("large", 100000))
			case "journal key":
				reportDeltaSQL(t, s, `insert into thread_exclusions values('other/repo',90,'issue',90,'90','now','owner_requested','plan'); insert into thread_excluded_nodes values(?,'other/repo',90)`, strings.Repeat("large", 100000))
			}
			lookup := false
			old.beforeKeys = func() { lookup = true }
			got, err := CaptureNativeReportChanges(ctx, s.Path(), request, old)
			if which == "unchanged child" {
				if err != nil {
					t.Fatal("ordinary parent edit scanned historical child bodies", err)
				}
				if len(got.Upserts) != 1 || got.Upserts[0].Key.Table != "threads" {
					t.Fatal("ordinary parent edit expanded unchanged children")
				}
				assertReportDelta(t, base, takeNativeReport(t, ctx, s, req), got)
			} else if !errors.Is(err, ErrNativeReportLimit) || !reflect.DeepEqual(got, NativeReportDelta{}) {
				t.Fatal("oversized native value escaped preflight", err)
			}
			if which == "journal key" && lookup {
				t.Fatal("huge journal key reached dependency expansion")
			}
		})
	}
}

func TestNativeReportChangesInvalidBaselineAndSource(t *testing.T) {
	s, ctx, req := reportDeltaFixture(t)
	base := takeNativeReport(t, ctx, s, req)
	old := reportDeltaIndex(t, s, base)
	for _, name := range []string{"file", "scope", "repository", "epoch", "generation", "cookie", "future", "regressing floor"} {
		t.Run(name, func(t *testing.T) {
			r := reportDeltaRequest(base)
			switch name {
			case "file":
				r.Baseline.File.Inode = "wrong"
			case "scope":
				r.Baseline.ScopeSHA256 = "wrong"
			case "repository":
				r.Baseline.RepositoryID++
			case "epoch":
				r.Baseline.Cursor.Epoch = "wrong"
			case "generation":
				r.Baseline.Cursor.Generation++
			case "cookie":
				r.Baseline.Cursor.SchemaVersion++
			case "future":
				r.Baseline.Cursor.Sequence++
			case "regressing floor":
				r.Baseline.Cursor.PrunedThrough = 1
			}
			if out, err := CaptureNativeReportChanges(ctx, s.Path(), r, old); !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(out, NativeReportDelta{}) {
				t.Fatal("invalid baseline accepted", err)
			}
		})
	}
	reportDeltaSQL(t, s, `update threads set title='after' where id=1; update change_contract_sequence set pruned_through=value`)
	if out, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old); !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(out, NativeReportDelta{}) {
		t.Fatal("expired floor accepted", err)
	}
	for _, test := range []struct {
		name, sql string
		want      error
	}{
		{"blob", `insert into blobs(id,sha256,media_type,size_bytes,storage_kind,created_at) values(1,'hash','application/json',2,'inline','now'); update comments set raw_json_blob_id=1 where id=1`, ErrNativeReportBlob},
		{"null key", `insert into thread_exclusions values('other/repo',90,'issue',90,'90','now','owner_requested','plan'); insert into thread_excluded_nodes values(NULL,'other/repo',90)`, ErrNativeReportSource},
		{"invalid storage", `update comments set body=x'6162' where id=1`, ErrNativeReportSource},
		{"unarmed", `drop trigger change_contract_comments_update`, ErrNativeReportSource},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, ctx, req := reportDeltaFixture(t)
			base := takeNativeReport(t, ctx, s, req)
			old := reportDeltaIndex(t, s, base)
			reportDeltaSQL(t, s, test.sql)
			if out, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old); !errors.Is(err, test.want) || !reflect.DeepEqual(out, NativeReportDelta{}) {
				t.Fatal("unsupported source returned partial result", err)
			}
		})
	}
}

func TestNativeReportChangesRequiresPreparedReverseIndexes(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		s := changeContractFixture(t, 0)
		ctx, req := nativeReportRequest(t, s)
		if wrong {
			reportDeltaSQL(t, s, `create index idx_comments_report_node on comments(thread_id)`)
			if err := s.PrepareNativeReportIndexes(ctx); err != nil {
				t.Fatal(err)
			}
		}
		base := takeNativeReport(t, ctx, s, req)
		if out, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), reportDeltaIndex(t, s, base)); !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(out, NativeReportDelta{}) {
			t.Fatal("missing/mismatched reverse index accepted", err)
		}
	}
}

func TestNativeReportChangesPreservesNativeScalars(t *testing.T) {
	s, ctx, req := reportDeltaFixture(t)
	base := takeNativeReport(t, ctx, s, req)
	old := reportDeltaIndex(t, s, base)
	reportDeltaSQL(t, s, `update comments set body=?,updated_at_gh=NULL,publication_at_gh='2026-01-03T12:34:56.123456789Z' where id=1; update threads set observation_sequence=-9223372036854775807 where id=1`, "raw\x00☃\xff")
	got, err := CaptureNativeReportChanges(ctx, s.Path(), reportDeltaRequest(base), old)
	if err != nil {
		t.Fatal(err)
	}
	assertReportDelta(t, base, takeNativeReport(t, ctx, s, req), got)
}

func TestNativeReportChangesLookupPlans(t *testing.T) {
	s, _, _ := reportDeltaFixture(t)
	queries := []recordedQuery{
		{`select x.id from comments x indexed by idx_comments_report_node join threads t on t.id=x.thread_id where ` + strings.ReplaceAll(nativeReportNodeKeySQL, "raw_json", "x.raw_json") + `=? and t.repo_id=? limit ?`, []any{"comment-one", 1, 500}},
		{`select x.id from threads x indexed by idx_threads_report_node where ` + nativeReportNodeKeySQL + `=? and x.repo_id=? limit ?`, []any{"thread-one", 1, 500}},
		{`select x.id from comments x join threads t on t.id=x.thread_id where x.thread_id=? and t.repo_id=? limit ?`, []any{1, 1, 500}},
		{`select x.body from comments x where exists(` + nativeReportThreads + ` and t.id=x.thread_id) and x.id=?`, []any{1, 1}},
	}
	for i, query := range queries {
		plan := topLevelQueryPlan(t, s, query)
		t.Logf("lookup %d: %s", i, plan)
		if strings.Contains(plan, "SCAN x") || strings.Contains(plan, "SCAN t") || strings.Contains(plan, "SCAN comments") || strings.Contains(plan, "SCAN threads") {
			t.Fatal("key lookup scans native content", plan)
		}
		if i < 2 && (!strings.Contains(plan, "SEARCH x USING") || !strings.Contains(plan, "INDEX idx_")) {
			t.Fatal("reverse lookup not indexed", plan)
		}
		if i == 3 && !strings.Contains(plan, "SEARCH x USING INTEGER PRIMARY KEY") {
			t.Fatal("payload lookup is not keyed", plan)
		}
	}
}
