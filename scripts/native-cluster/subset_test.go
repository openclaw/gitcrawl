//go:build native_cluster_overlay

package store

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestNativeAvailableSubsetAtomicRecoveryNoop(t *testing.T) {
	s, now, unknown := availabilityFixture(t)
	ctx := context.Background()
	repo := int64(1)
	for n := 2; n <= 3; n++ {
		at := now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
		if _, e := s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: "I_" + strconv.Itoa(n), Number: n, Kind: "issue", State: "open", Title: "available " + strconv.Itoa(n), Body: "body", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: at, LastPulledAt: at, UpdatedAtGitHub: at}); e != nil {
			t.Fatal(e)
		}
	}
	old, e := s.SaveCompleteDurableClusters(ctx, repo, []DurableClusterInput{{StableKey: "old-a", StableSlug: "old-a", RepresentativeThreadID: unknown, Title: "retained", Members: []DurableClusterMemberInput{{ThreadID: unknown, Role: "canonical"}, {ThreadID: 2, Role: "related"}}}, {StableKey: "old-b", StableSlug: "old-b", RepresentativeThreadID: 3, Title: "other", Members: []DurableClusterMemberInput{{ThreadID: 3, Role: "canonical"}}}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB().Exec("INSERT INTO cluster_groups(repo_id,stable_key,stable_slug,status,cluster_type,title,created_at,updated_at) VALUES(1,'legacy','legacy','active','similarity','legacy','t','t')"); e != nil {
		t.Fatal(e)
	}
	unavailableAttempt(t, s, 1, now.Add(-time.Minute), isolatedUnavailableFixture)
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	packet := func() ([]byte, []byte, NativeEmbeddingRequest) {
		req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 2000, policy)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(req)
		res := NativeEmbeddingResponse{Format: "clawmmunity.native-embeddings.v1", RequestSHA256: NativeSHA256(raw), Model: req.Model, Dimensions: 1536, Tasks: len(req.Tasks), Ready: len(req.Tasks), Status: "complete"}
		for _, task := range req.Tasks {
			v := make([]float64, 1536)
			id, _ := strconv.Atoi(task.ThreadBinding.ThreadID)
			v[id] = 1
			res.Results = append(res.Results, struct {
				TaskID      string    `json:"task_id"`
				InputSHA256 string    `json:"input_sha256"`
				Vector      []float64 `json:"vector"`
			}{task.TaskID, task.InputSHA256, v})
		}
		b, _ := json.Marshal(res)
		return raw, b, req
	}
	raw, response, req := packet()
	if len(req.Tasks) != 2 {
		t.Fatal("unavailable input exported", len(req.Tasks))
	}
	for _, task := range req.Tasks {
		if task.ThreadBinding.ThreadID == strconv.FormatInt(unknown, 10) {
			t.Fatal("quarantined bytes exported")
		}
	}
	if e = s.ValidateNativeEmbeddingRequest(ctx, raw, policy); e != nil {
		t.Fatal(e)
	}
	snap, e := s.NativeCombinedSnapshot(ctx, policy, raw, response)
	if e != nil {
		t.Fatal(e)
	}
	inputs := []DurableClusterInput{{StableKey: "new-a", StableSlug: "new-a", RepresentativeThreadID: 2, Title: "new", Members: []DurableClusterMemberInput{{ThreadID: 2, Role: "canonical"}}}, {StableKey: "new-b", StableSlug: "new-b", RepresentativeThreadID: 3, Title: "other", Members: []DurableClusterMemberInput{{ThreadID: 3, Role: "canonical"}}}}
	badPlan := []DurableClusterInput{{StableKey: "ambiguous", RepresentativeThreadID: 2, Members: []DurableClusterMemberInput{{ThreadID: 2, Role: "canonical"}, {ThreadID: 3, Role: "related"}}}}
	if e = s.NativeCheckClusterPlan(ctx, snap, badPlan); e == nil {
		t.Fatal("ambiguous identity preview admitted")
	}
	if e = s.NativeCheckClusterPlan(ctx, snap, inputs); e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB().Exec("CREATE TRIGGER refuse_partial BEFORE INSERT ON cluster_runs BEGIN SELECT RAISE(ABORT,'rollback'); END"); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = s.NativeCombinedPublish(ctx, snap, policy, raw, response, inputs); e == nil {
		t.Fatal("injected failure ignored")
	}
	var count int
	s.DB().QueryRow("SELECT count(*) FROM thread_vectors").Scan(&count)
	if count != 0 {
		t.Fatal("vector write escaped rollback")
	}
	s.DB().Exec("DROP TRIGGER refuse_partial")
	imported, published, _, e := s.NativeCombinedPublish(ctx, snap, policy, raw, response, inputs)
	if e != nil || imported.Imported != 2 {
		t.Fatal(imported, published, e)
	}
	scope, e := s.NativePublishedClusterScope(ctx, repo)
	if e != nil || scope.Mode != "available-subset" || scope.ProviderComplete || scope.FullNativeScopeComplete || scope.Population.NativeEligible != 3 || scope.Population.Selected != 2 || scope.RetainedPriorMemberships != 1 {
		t.Fatalf("scope %+v %v", scope, e)
	}
	partialScope := *scope
	var partialStarted, partialFinished string
	s.DB().QueryRow("SELECT started_at,finished_at FROM cluster_runs WHERE id=?", published.RunID).Scan(&partialStarted, &partialFinished)
	s.DB().QueryRow("SELECT count(*) FROM cluster_groups WHERE stable_key='legacy'").Scan(&count)
	if count != 1 {
		t.Fatal("partial legacy cleanup")
	}
	var marker int64
	s.DB().QueryRow("SELECT last_seen_run_id FROM cluster_memberships WHERE thread_id=?", unknown).Scan(&marker)
	if marker != old.RunID {
		t.Fatal("unknown membership relabelled")
	}
	var complete int
	s.DB().QueryRow("SELECT complete FROM analytics_coverage").Scan(&complete)
	if complete != 0 {
		t.Fatal("provider coverage changed")
	}
	s.DB().QueryRow("SELECT count(*) FROM analytics_retries WHERE resolved_at IS NULL").Scan(&count)
	if count != 1 {
		t.Fatal("retry removed")
	}
	detail, e := s.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repo, ClusterID: 1, IncludeClosed: true, MemberLimit: 10})
	if e != nil || detail.Members[0].Thread.ID != unknown || detail.Members[0].GenerationStatus != "retained_prior" {
		t.Fatal("retained representative provenance", e)
	}
	again, e := s.NativeCombinedSnapshot(ctx, policy, raw, response)
	if e != nil {
		t.Fatal(e)
	}
	_, same, _, e := s.NativeCombinedPublish(ctx, again, policy, raw, response, inputs)
	if e != nil || same.RunID != published.RunID {
		t.Fatal("partial replay churn", e)
	}
	unavailableAttempt(t, s, 2, now.Add(-time.Second), isolatedUnavailableFixture)
	if e = s.ValidateNativeEmbeddingRequest(ctx, raw, policy); e == nil {
		t.Fatal("newly unavailable input remained queueable")
	}
	if _, _, _, e = s.NativeCombinedPublish(ctx, again, policy, raw, response, inputs); e == nil {
		t.Fatal("changed availability committed")
	}
	// Only ordinary-success semantics resolve the simulated provider obligations.
	for _, n := range []int{1, 2} {
		at := time.Now().UTC().Format(time.RFC3339Nano)
		if e = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "openclaw/openclaw", Number: n, Operation: "graphql_history", StartedAt: at, FinishedAt: at, Status: "success", Evidence: json.RawMessage(`{}`)}); e != nil {
			t.Fatal(e)
		}
	}
	s.SetAnalyticsCoverageComplete(ctx, "openclaw/openclaw", true)
	raw, response, req = packet()
	if len(req.Tasks) != 1 || req.Tasks[0].ThreadBinding.ThreadID != strconv.FormatInt(unknown, 10) {
		t.Fatal("recovered item not selected")
	}
	snap, e = s.NativeCombinedSnapshot(ctx, policy, raw, response)
	if e != nil {
		t.Fatal(e)
	}
	fullInputs := []DurableClusterInput{{StableKey: "old-a", StableSlug: "old-a", RepresentativeThreadID: unknown, Members: []DurableClusterMemberInput{{ThreadID: unknown, Role: "canonical"}, {ThreadID: 2, Role: "related"}}}, {StableKey: "old-b", StableSlug: "old-b", RepresentativeThreadID: 3, Members: []DurableClusterMemberInput{{ThreadID: 3, Role: "canonical"}}}}
	_, full, _, e := s.NativeCombinedPublish(ctx, snap, policy, raw, response, fullInputs)
	if e != nil {
		t.Fatal(e)
	}
	scope, e = s.NativePublishedClusterScope(ctx, repo)
	if e != nil || scope.Mode != "full-native" || scope.LastFullRunID == nil || *scope.LastFullRunID != strconv.FormatInt(full.RunID, 10) {
		t.Fatal("full recovery scope", e)
	}
	var fullStarted, fullFinished string
	s.DB().QueryRow("SELECT started_at,finished_at FROM cluster_runs WHERE id=?", full.RunID).Scan(&fullStarted, &fullFinished)
	if path := os.Getenv("GITCRAWL_SCOPE_FIXTURE"); path != "" {
		raw, e := json.MarshalIndent(map[string]any{"synthetic_only": true, "partial": partialScope, "partial_started_at": partialStarted, "partial_finished_at": partialFinished, "full": scope, "full_started_at": fullStarted, "full_finished_at": fullFinished}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, append(raw, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
