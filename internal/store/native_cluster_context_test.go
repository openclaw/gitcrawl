//go:build darwin || linux

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func clusterContextSQL(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.DB().Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}
func clusterContextFixture(t *testing.T, groups bool) *Store {
	t.Helper()
	s := changeContractFixture(t, 0)
	clusterContextSQL(t, s, `update repositories set raw_json='{"private":false,"visibility":"public"}'; update threads set kind='issue',updated_at_gh='2026-01-01T00:00:00Z'`)
	if groups {
		clusterContextSQL(t, s, `insert into cluster_groups(id,repo_id,stable_key,stable_slug,status,representative_thread_id,created_at,updated_at) values(1,1,'one','one','active',1,'now','now');
		insert into cluster_memberships(cluster_id,thread_id,role,state,added_by,added_reason_json,created_at,updated_at) values(1,1,'canonical','active','fixture','{}','now','now'),(1,2,'member','active','fixture','{}','now','now')`)
	}
	return s
}
func clusterContextRequest(t *testing.T, s *Store) (context.Context, NativeClusterContextRequest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	f, err := NativeReportFileIdentity(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	return ctx, NativeClusterContextRequest{Repository: "fixture/repo", File: f, Limits: NativeReportLimits{MaxRows: 10000, MaxBytes: 1 << 20}}
}
func takeClusterContext(t *testing.T, s *Store) NativeClusterContext {
	t.Helper()
	ctx, req := clusterContextRequest(t, s)
	got, err := CaptureNativeClusterContext(ctx, s.Path(), req)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func rejectClusterContext(t *testing.T, s *Store, want error) {
	t.Helper()
	ctx, req := clusterContextRequest(t, s)
	got, err := CaptureNativeClusterContext(ctx, s.Path(), req)
	if err == nil || (want != nil && !errors.Is(err, want)) || !reflect.DeepEqual(got, NativeClusterContext{}) {
		t.Fatalf("partial or successful rejected capture: %v", err)
	}
}

func TestNativeClusterContextFidelityAndReadOnly(t *testing.T) {
	s := clusterContextFixture(t, true)
	const large = int64(9007199254740993)
	clusterContextSQL(t, s, `insert into cluster_groups(id,repo_id,stable_key,stable_slug,status,created_at,updated_at) values(?,1,'large','large','active','now','now')`, large)
	clusterContextSQL(t, s, `insert into threads(id,repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) values(?,1,'large',?,'issue','open','large',NULL,'https://example.invalid','[]','[]','{"node_id":9007199254740993}','h','now')`, large, large)
	clusterContextSQL(t, s, `insert into cluster_memberships(cluster_id,thread_id,role,state,added_by,added_reason_json,created_at,updated_at) values(?,?,'member','active','fixture','{}','now','now')`, large, large)
	excerpt := "a\x00" + strings.Repeat("☃", 798)
	clusterContextSQL(t, s, `update threads set body=?,raw_json='{"node_id":"node\u0000one"}' where id=1`, excerpt+strings.Repeat("x", 2<<20)+"\xff")
	clusterContextSQL(t, s, `update threads set body='' where id=2`)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	got := takeClusterContext(t, s)
	if got.Profile != NativeClusterContextProfile || got.View != NativeClusterContextView || got.Selection.ScopeStatus != "legacy_unknown" || got.Selection.Scope != nil || len(got.Groups) != 2 || len(got.Members) != 3 || len(got.Threads) != 3 {
		t.Fatal("wrong compact inventory", got.Selection)
	}
	if got.Threads[0].Body == nil || *got.Threads[0].Body != excerpt || got.Threads[0].NodeID == nil || *got.Threads[0].NodeID != "node\x00one" {
		t.Fatal("NUL/excerpt/node bytes lost")
	}
	if got.Threads[1].Body == nil || *got.Threads[1].Body != "" || got.Threads[2].Body != nil || got.Threads[2].ID != large || got.Threads[2].Number != large || got.Threads[2].NodeID == nil || *got.Threads[2].NodeID != "9007199254740993" {
		t.Fatalf("NULL/empty/identity: emptyNil=%t nullNil=%t id=%d number=%d node=%v", got.Threads[1].Body == nil, got.Threads[2].Body == nil, got.Threads[2].ID, got.Threads[2].Number, got.Threads[2].NodeID)
	}
	if got.Groups[1].ID != large || got.Groups[1].RepresentativeThreadID != nil {
		t.Fatal("NULL representative repaired")
	}
	if again := takeClusterContext(t, s); !reflect.DeepEqual(got, again) {
		t.Fatal("deterministic capture changed")
	}
	after, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("capture wrote native source")
	}
}

func TestNativeClusterContextVisibilityAndRepresentatives(t *testing.T) {
	s := clusterContextFixture(t, true)
	clusterContextSQL(t, s, `insert into cluster_groups(id,repo_id,stable_key,stable_slug,status,representative_thread_id,created_at,updated_at) values(2,1,'two','two','active',2,'now','now'),(3,1,'closed','closed','closed',1,'now','now');
	insert into cluster_memberships(cluster_id,thread_id,role,state,added_by,added_reason_json,created_at,updated_at) values(2,2,'canonical','active','fixture','{}','now','now')`)
	got := takeClusterContext(t, s)
	if len(got.Groups) != 2 || len(got.Members) != 2 || got.Groups[0].MemberCount != 1 || got.Members[1].GroupID != 2 {
		t.Fatal("competing canonical did not hide ordinary membership")
	}
	clusterContextSQL(t, s, `update cluster_memberships set role='representative' where cluster_id=1 and thread_id=2`)
	if got = takeClusterContext(t, s); len(got.Members) != 3 {
		t.Fatal("native competing representative rule changed")
	}
	clusterContextSQL(t, s, `update threads set closed_at_local='2026-01-02' where id=2`)
	got = takeClusterContext(t, s)
	if len(got.Members) != 1 || len(got.Threads) != 2 || got.Groups[1].MemberCount != 0 || got.Groups[1].RepresentativeThreadID == nil || *got.Groups[1].RepresentativeThreadID != 2 {
		t.Fatal("closed representative was repaired or visible as member")
	}
	clusterContextSQL(t, s, `update cluster_groups set closed_at='2026-01-02' where id=2`)
	if got = takeClusterContext(t, s); len(got.Groups) != 1 {
		t.Fatal("closed_at ignored")
	}
}

func TestNativeClusterContextPublicAndExclusionPolicy(t *testing.T) {
	for _, raw := range []string{`{}`, `{"private":true}`, `{"private":"false"}`, `{"private":0}`, `{"private":false,"visibility":"private"}`, `not json`} {
		t.Run(raw, func(t *testing.T) {
			s := clusterContextFixture(t, true)
			clusterContextSQL(t, s, "update repositories set raw_json=?", raw)
			rejectClusterContext(t, s, ErrNativeClusterContext)
		})
	}
	t.Run("number member omitted", func(t *testing.T) {
		s := clusterContextFixture(t, true)
		clusterContextSQL(t, s, `insert into thread_exclusions values('fixture/repo',20,'issue',2,'gh20','now','owner_requested','plan')`)
		got := takeClusterContext(t, s)
		if len(got.Members) != 1 || len(got.Threads) != 1 || got.Groups[0].MemberCount != 1 {
			t.Fatal("excluded member escaped")
		}
	})
	t.Run("representative refused", func(t *testing.T) {
		s := clusterContextFixture(t, true)
		clusterContextSQL(t, s, `insert into thread_exclusions values('fixture/repo',10,'issue',1,'gh10','now','owner_requested','plan')`)
		rejectClusterContext(t, s, ErrNativeClusterContext)
	})
	for _, id := range []int{1, 2} {
		t.Run(string(rune('0'+id))+" global node", func(t *testing.T) {
			s := clusterContextFixture(t, true)
			clusterContextSQL(t, s, `update threads set raw_json='{"node_id":9007199254740993}' where id=?`, id)
			clusterContextSQL(t, s, `insert into thread_exclusions values('other/repo',99,'issue',99,'other','now','owner_requested','plan'); insert into thread_excluded_nodes values('9007199254740993','other/repo',99)`)
			rejectClusterContext(t, s, ErrNativeClusterContext)
		})
	}
	t.Run("cross repository representative", func(t *testing.T) {
		s := clusterContextFixture(t, true)
		clusterContextSQL(t, s, `insert into repositories values(2,'other','repo','other/repo','200','{"private":false}','now'); update threads set repo_id=2 where id=2; update cluster_groups set representative_thread_id=2 where id=1`)
		rejectClusterContext(t, s, ErrNativeClusterContext)
	})
	for _, node := range []string{`{}`, `{"node_id":""}`, `not json`} {
		t.Run("unprovable "+node, func(t *testing.T) {
			s := clusterContextFixture(t, true)
			clusterContextSQL(t, s, `update threads set raw_json='{"node_id":"known"}'; update threads set raw_json=? where id=2`, node)
			// An empty authoritative ledger allows advisory unknown identity.
			takeClusterContext(t, s)
			clusterContextSQL(t, s, `insert into thread_exclusions values('other/repo',99,'issue',99,'other','now','owner_requested','plan'); insert into thread_excluded_nodes values('unrelated','other/repo',99)`)
			rejectClusterContext(t, s, ErrNativeClusterContext)
		})
	}
	t.Run("known nonmatching node bytes", func(t *testing.T) {
		s := clusterContextFixture(t, true)
		clusterContextSQL(t, s, `update threads set raw_json='{"node_id":"\u0000known"}'; insert into thread_exclusions values('other/repo',99,'issue',99,'other','now','owner_requested','plan'); insert into thread_excluded_nodes values('unrelated','other/repo',99)`)
		takeClusterContext(t, s)
	})
}

func TestNativeClusterContextQualificationAndEmpty(t *testing.T) {
	s := clusterContextFixture(t, false)
	legacy := takeClusterContext(t, s)
	if legacy.Selection.Scope != nil || legacy.Selection.ScopeStatus != "legacy_unknown" || len(legacy.Groups) != 0 {
		t.Fatal("legacy empty was qualified")
	}
	scope := ClusterScope{Format: ClusterScopeFormat, Mode: "full-native", RunID: "1", Repository: "fixture/repo", PolicyVersion: 1, PolicyDigest: strings.Repeat("a", 64), SourceAsOf: ClusterScopeClock{Through: "2026-01-01T00:00:00Z", ObservedAt: "2026-01-01T00:00:00Z"}, ProviderComplete: true, FullNativeScopeComplete: true, SelectedInputComplete: true, LastFullRunID: clusterRunKey(1)}
	encoded, err := json.Marshal(map[string]any{"native_producer": map[string]any{"scope": scope}})
	if err != nil {
		t.Fatal(err)
	}
	clusterContextSQL(t, s, `insert into cluster_runs values(1,1,'durable','success','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z',?,NULL)`, string(encoded))
	qualified := takeClusterContext(t, s)
	if qualified.Selection.Scope == nil || !qualified.Selection.Scope.FullNativeScopeComplete || qualified.Selection.ScopeStatus != "native_v1" || len(qualified.Groups) != 0 || qualified.SHA256 == legacy.SHA256 {
		t.Fatal("qualified empty lost provenance")
	}
	clusterContextSQL(t, s, `update cluster_runs set stats_json='{"native_producer":{"scope":{}}}'`)
	rejectClusterContext(t, s, nil)
}

func TestNativeClusterContextRetainedAnalysis(t *testing.T) {
	s := quarantineReaderFixture(t, true, false, false)
	clusterContextSQL(t, s, `update repositories set raw_json='{"private":false}',github_repo_id='100'`)
	got := takeClusterContext(t, s)
	if got.Selection.AnalysisRunID == nil || *got.Selection.AnalysisRunID != 11 || got.Selection.AnalysisStatus != "retained_only" || got.Selection.AnalysisScope == nil || got.Selection.AnalysisScope.IdentityReconciliationComplete == nil || *got.Selection.AnalysisScope.IdentityReconciliationComplete {
		t.Fatal("retained analysis presented as complete")
	}
	quarantined := false
	for _, g := range got.Groups {
		quarantined = quarantined || g.Quarantined
	}
	if !quarantined {
		t.Fatal("quarantine meaning lost")
	}
}

func TestNativeClusterContextBoundsAndEncoding(t *testing.T) {
	s := clusterContextFixture(t, true)
	ctx, req := clusterContextRequest(t, s)
	for _, limits := range []NativeReportLimits{{MaxRows: 1, MaxBytes: 1 << 20}, {MaxRows: 10000, MaxBytes: 1}, {MaxRows: 0, MaxBytes: 100}} {
		req.Limits = limits
		got, err := CaptureNativeClusterContext(ctx, s.Path(), req)
		if !errors.Is(err, ErrNativeReportLimit) || !reflect.DeepEqual(got, NativeClusterContext{}) {
			t.Fatal("limit returned partial success", err)
		}
	}
	req.Limits = NativeReportLimits{MaxRows: 10000, MaxBytes: 1 << 20}
	if _, err := CaptureNativeClusterContext(context.Background(), s.Path(), req); err == nil {
		t.Fatal("missing deadline accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := CaptureNativeClusterContext(canceled, s.Path(), req); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	wrong := req
	wrong.File.Inode = "wrong"
	if _, err := CaptureNativeClusterContext(ctx, s.Path(), wrong); !errors.Is(err, ErrNativeClusterContext) {
		t.Fatal("file mismatch accepted", err)
	}
	clusterContextSQL(t, s, "update threads set title=? where id=1", strings.Repeat("x", 2<<20))
	rejectClusterContext(t, s, ErrNativeReportLimit)
	clusterContextSQL(t, s, "update threads set title='one',body=? where id=1", strings.Repeat("☃", 799)+"\xff")
	rejectClusterContext(t, s, ErrNativeClusterContext)
	// Exactly 800 runes fit the 3200-byte prefix; invalid or incomplete trailing
	// data outside that excerpt is intentionally not part of this profile.
	clusterContextSQL(t, s, "update threads set body=? where id=1", strings.Repeat("😀", 800)+"\xff")
	got := takeClusterContext(t, s)
	if got.Threads[0].Body == nil || *got.Threads[0].Body != strings.Repeat("😀", 800) {
		t.Fatal("four-byte prefix boundary lost")
	}
	clusterContextSQL(t, s, "update threads set body=? where id=1", strings.Repeat("😀", 800)+"another tail")
	if again := takeClusterContext(t, s); again.SHA256 != got.SHA256 {
		t.Fatal("unexported body changed content identity")
	}
	clusterContextSQL(t, s, "insert into cluster_runs(repo_id,scope,status,started_at,stats_json) values(1,'durable','success','2026-01-01',?)", strings.Repeat("x", 2<<20))
	rejectClusterContext(t, s, ErrNativeReportLimit)
}
