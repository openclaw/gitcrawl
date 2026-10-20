//go:build native_cluster_overlay

package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestNativeClusterSnapshotAndPublish(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "native.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	for n := 1; n <= 3; n++ {
		_, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: strconv.Itoa(n), Number: n, Kind: "issue", State: "open", Title: "native", Body: "body", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"})
		if e != nil {
			t.Fatal(e)
		}
	}
	if _, e = s.NativeClusterSnapshot(ctx, policy); e == nil {
		t.Fatal("missing vectors accepted")
	}
	req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 10, policy)
	if e != nil {
		t.Fatal(e)
	}
	for _, task := range req.Tasks {
		v := make([]float64, 1536)
		v[0] = 1
		id, _ := strconv.ParseInt(task.ThreadBinding.ThreadID, 10, 64)
		if e = s.UpsertThreadVector(ctx, ThreadVector{ThreadID: id, Basis: req.Basis, Model: req.Model, Dimensions: 1536, ContentHash: task.NativeContentHash, Vector: v, CreatedAt: "t", UpdatedAt: "t"}); e != nil {
			t.Fatal(e)
		}
	}
	before, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil {
		t.Fatal(e)
	}
	s.DB().Exec("update threads set updated_at='tick',observation_sequence=observation_sequence+1")
	s.DB().Exec("update thread_vectors set updated_at='tick'")
	tick, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil || tick.InputSHA256 != before.InputSHA256 || tick.StateSHA256 != before.StateSHA256 {
		t.Fatal("observation clock churn invalidated semantic signature", e)
	}
	inputs := []DurableClusterInput{{StableKey: "k", StableSlug: "slug", ClusterType: "duplicate_candidate", RepresentativeThreadID: 1, Title: "group", Members: []DurableClusterMemberInput{{ThreadID: 1, Role: "canonical"}, {ThreadID: 2, Role: "related"}, {ThreadID: 3, Role: "related"}}}}
	deadline, done := context.WithTimeout(ctx, 5*time.Second)
	defer done()
	result, state, e := s.NativePublishClusters(deadline, before, policy, inputs)
	if e != nil || result.MemberCount != 3 {
		t.Fatal("same transaction publish failed", e)
	}
	after, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil || after.InputSHA256 != before.InputSHA256 || after.StateSHA256 != state || after.CommittedState != state || after.CommittedInput != after.InputSHA256 {
		t.Fatal("postcommit signature", e)
	}
	// A prior-state replay is rejected, avoiding duplicate run/history churn.
	if _, _, e = s.NativePublishClusters(ctx, before, policy, inputs); e == nil {
		t.Fatal("old state replay accepted")
	}
	var n int
	s.DB().QueryRow("select count(*) from cluster_runs").Scan(&n)
	if n != 1 {
		t.Fatal("duplicate run")
	}
	s.DB().Exec("update threads set body='new reference #42' where id=3")
	if _, _, e = s.NativePublishClusters(ctx, after, policy, inputs); e == nil {
		t.Fatal("stale repaired input accepted")
	}
	s.DB().QueryRow("select count(*) from cluster_runs").Scan(&n)
	if n != 1 {
		t.Fatal("partial run persisted")
	}
	s.DB().Exec("update threads set body='body' where id=3")
	excluded := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":["2"]}`)
	if _, e = s.NativeClusterSnapshot(ctx, excluded); e == nil {
		t.Fatal("consumer excluded input accepted")
	}
	if _, e = s.ExcludeClusterMemberLocally(ctx, repo, 1, 2, "synthetic protected override"); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.NativePublishClusters(ctx, after, policy, inputs); e == nil {
		t.Fatal("changed curation accepted")
	}
	current, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.NativePublishClusters(ctx, current, policy, inputs); e != nil {
		t.Fatal(e)
	}
	var stateText string
	s.DB().QueryRow("select state from cluster_memberships where thread_id=2").Scan(&stateText)
	if stateText != "excluded" {
		t.Fatal("protected native override lost")
	}
}
func TestNativeClusterMeasuredCommit(t *testing.T) {
	if os.Getenv("GITCRAWL_CLUSTER_COMMIT") != "1" {
		t.Skip("explicit bounded synthetic volume gate")
	}
	ctx, done := context.WithTimeout(context.Background(), 90*time.Second)
	defer done()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "synthetic.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	v := make([]float64, 1536)
	for i := range v {
		v[i] = float64(i+1) / 1536
	}
	vr, _ := json.Marshal(v)
	hash := embeddingContentHash("title_original", "text-embedding-3-small", "body")
	tx, e := s.DB().BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	inputs := make([]DurableClusterInput, 0, 9389)
	for i := 1; i <= 9389; i++ {
		_, e = tx.ExecContext(ctx, `insert into threads(repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) values(?,?,?,'issue','open','native','body','https://example.invalid','[]','[]','{}','h','t')`, repo, strconv.Itoa(i), i)
		if e != nil {
			t.Fatal(e)
		}
		_, e = tx.ExecContext(ctx, `insert into thread_vectors(thread_id,basis,model,dimensions,content_hash,vector_json,vector_backend,created_at,updated_at) values(?,'title_original','text-embedding-3-small',1536,?,?,'exact','t','t')`, i, hash, string(vr))
		if e != nil {
			t.Fatal(e)
		}
		inputs = append(inputs, DurableClusterInput{StableKey: strconv.Itoa(i), StableSlug: "singleton-" + strconv.Itoa(i), ClusterType: "singleton_orphan", RepresentativeThreadID: int64(i), Title: "native", Members: []DurableClusterMemberInput{{ThreadID: int64(i), Role: "canonical"}}})
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	snap, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil {
		t.Fatal(e)
	}
	start := time.Now()
	result, state, e := s.NativePublishClusters(ctx, snap, policy, inputs)
	if e != nil {
		t.Fatal(e)
	}
	elapsed := time.Since(start).Seconds()
	after, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil || after.CommittedInput != snap.InputSHA256 || after.CommittedState != state {
		t.Fatal("durable no-op proof", e)
	}
	raw, _ := json.Marshal(map[string]any{"synthetic_only": true, "eligible": 9389, "clusters": result.ClusterCount, "members": result.MemberCount, "write_seconds": elapsed, "signature_and_state_match": true})
	if e = os.WriteFile(os.Getenv("GITCRAWL_CLUSTER_COMMIT_RECEIPT"), raw, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestNativeCombinedAtomicOverlayAndClocks(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "combined.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	for i := 1; i <= 2; i++ {
		_, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: strconv.Itoa(i), Number: i, Kind: "issue", State: "open", Title: "native", Body: "body", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"})
		if e != nil {
			t.Fatal(e)
		}
	}
	v := make([]float64, 1536)
	v[0] = .2
	if e = s.UpsertThreadVector(ctx, ThreadVector{ThreadID: 1, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 1536, ContentHash: "stale", Vector: v, Backend: "legacy-backend", CreatedAt: "t", UpdatedAt: "t"}); e != nil {
		t.Fatal(e)
	}
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	packet := func() ([]byte, []byte) {
		req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 2000, policy)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(req)
		res := NativeEmbeddingResponse{Format: "clawmmunity.native-embeddings.v1", RequestSHA256: NativeSHA256(raw), Model: req.Model, Dimensions: 1536, Tasks: len(req.Tasks), Ready: len(req.Tasks), Status: "complete"}
		for _, x := range req.Tasks {
			vector := make([]float64, 1536)
			vector[0] = .5
			res.Results = append(res.Results, struct {
				TaskID      string    `json:"task_id"`
				InputSHA256 string    `json:"input_sha256"`
				Vector      []float64 `json:"vector"`
			}{x.TaskID, x.InputSHA256, vector})
		}
		response, _ := json.Marshal(res)
		return raw, response
	}
	req, res := packet()
	if _, e = s.NativeClusterSnapshot(ctx, policy); e == nil {
		t.Fatal("separate path must refuse stale/missing scope")
	}
	snap, e := s.NativeCombinedSnapshot(ctx, policy, req, res)
	if e != nil || snap.Eligible != 2 {
		t.Fatal("overlay", e)
	}
	s.DB().Exec("update threads set observation_sequence=observation_sequence+1,updated_at='tick'")
	if _, e = s.NativeCombinedSnapshot(ctx, policy, req, res); e == nil {
		t.Fatal("stale clocks accepted")
	}
	req, res = packet()
	fresh, e := s.NativeCombinedSnapshot(ctx, policy, req, res)
	if e != nil || fresh.InputSHA256 != snap.InputSHA256 {
		t.Fatal("clock-only binding refresh changed graph", e)
	}
	snap = fresh
	inputs := []DurableClusterInput{{StableKey: "pair", StableSlug: "pair", ClusterType: "duplicate_candidate", RepresentativeThreadID: 1, Title: "native", Members: []DurableClusterMemberInput{{ThreadID: 1, Role: "canonical"}, {ThreadID: 2, Role: "related"}}}}
	s.DB().Exec("create trigger fail_combined before insert on cluster_groups begin select raise(abort,'injected cluster failure'); end")
	if _, _, _, e = s.NativeCombinedPublish(ctx, snap, policy, req, res, inputs); e == nil {
		t.Fatal("failure accepted")
	}
	var hash string
	s.DB().QueryRow("select content_hash from thread_vectors where thread_id=1").Scan(&hash)
	var count int
	s.DB().QueryRow("select count(*) from thread_vectors").Scan(&count)
	if hash != "stale" || count != 1 {
		t.Fatal("partial vector import escaped rollback")
	}
	s.DB().QueryRow("select count(*) from cluster_runs").Scan(&count)
	if count != 0 {
		t.Fatal("partial cluster run")
	}
	s.DB().Exec("drop trigger fail_combined")
	imported, clusters, state, e := s.NativeCombinedPublish(ctx, snap, policy, req, res, inputs)
	if e != nil || imported.Imported != 2 || clusters.MemberCount != 2 {
		t.Fatalf("combined %+v %+v %v", imported, clusters, e)
	}
	actual, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil || actual.InputSHA256 != snap.InputSHA256 || actual.StateSHA256 != state {
		t.Fatal("overlay differs from actual post-import native values", e)
	}
	var backend string
	s.DB().QueryRow("select vector_backend from thread_vectors where thread_id=1").Scan(&backend)
	if backend != "exact" {
		t.Fatal("native importer metadata projection changed")
	}
	again, e := s.NativeCombinedSnapshot(ctx, policy, req, res)
	if e != nil {
		t.Fatal(e)
	}
	r, g, _, e := s.NativeCombinedPublish(ctx, again, policy, req, res, inputs)
	if e != nil || r.Imported != 0 || r.Unchanged != 2 || g.RunID != clusters.RunID {
		t.Fatal("combined replay churn", e)
	}
	s.DB().QueryRow("select count(*) from cluster_runs").Scan(&count)
	if count != 1 {
		t.Fatal("duplicate graph history")
	}
	s.DB().Exec("update thread_vectors set vector_backend='legacy-backend' where thread_id=1")
	overlayNoop, e := s.NativeCombinedSnapshot(ctx, policy, req, res)
	if e != nil {
		t.Fatal(e)
	}
	nativeNoop, e := s.NativeClusterSnapshot(ctx, policy)
	if e != nil || overlayNoop.InputSHA256 != nativeNoop.InputSHA256 || overlayNoop.PreviewReceipt.Imported != 0 {
		t.Fatal("no-op overlay metadata differs", e)
	}
	s.DB().Exec("update thread_vectors set vector_backend='exact' where thread_id=1")
	excluded := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":["2"]}`)
	if _, _, _, e = s.NativeCombinedPublish(ctx, again, excluded, req, res, inputs); e == nil {
		t.Fatal("policy exclusion accepted")
	}
	if _, e = s.ExcludeClusterMemberLocally(ctx, repo, 1, 2, "synthetic protected override"); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = s.NativeCombinedPublish(ctx, again, policy, req, res, inputs); e == nil {
		t.Fatal("stale curation accepted")
	}
}
