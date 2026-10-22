//go:build native_cluster_overlay

package store

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func quarantineResponse(t *testing.T, s *Store, policy []byte) ([]byte, []byte) {
	t.Helper()
	r, e := s.ExportNativeEmbeddingRequest(context.Background(), "openclaw/openclaw", 2000, policy)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(r)
	response := NativeEmbeddingResponse{Format: "clawmmunity.native-embeddings.v1", RequestSHA256: NativeSHA256(raw), Model: r.Model, Dimensions: 1536, Tasks: len(r.Tasks), Ready: len(r.Tasks), Status: "complete"}
	for _, task := range r.Tasks {
		v := make([]float64, 1536)
		id, _ := strconv.Atoi(task.ThreadBinding.ThreadID)
		v[id] = 1
		response.Results = append(response.Results, struct {
			TaskID      string    `json:"task_id"`
			InputSHA256 string    `json:"input_sha256"`
			Vector      []float64 `json:"vector"`
		}{task.TaskID, task.InputSHA256, v})
	}
	res, _ := json.Marshal(response)
	return raw, res
}
func quarantineInput(key string, ids ...int64) DurableClusterInput {
	in := DurableClusterInput{StableKey: key, StableSlug: key, ClusterType: "duplicate_candidate", RepresentativeThreadID: ids[0], Title: key}
	for i, id := range ids {
		role := "related"
		if i == 0 {
			role = "canonical"
		}
		in.Members = append(in.Members, DurableClusterMemberInput{ThreadID: id, Role: role})
	}
	return in
}
func TestNativeQuarantineWriterRollbackAnalysisAndRecovery(t *testing.T) {
	s, now, unknown := availabilityFixture(t)
	proofs := map[string]string{}
	ctx := context.Background()
	repo := int64(1)
	for n := 2; n <= 5; n++ {
		at := now.Add(-time.Minute).Format(time.RFC3339Nano)
		if _, e := s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: "I_" + strconv.Itoa(n), Number: n, Kind: "issue", State: "open", Title: "available " + strconv.Itoa(n), Body: "fixture", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "fixture", UpdatedAt: at, LastPulledAt: at, UpdatedAtGitHub: at}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := s.UpsertDocument(ctx, Document{ThreadID: 2, Title: "available 2", Body: "fixture", RawText: "fixture", DedupeText: "available 2 fixture", UpdatedAt: now.Format(time.RFC3339Nano)}); e != nil {
		t.Fatal(e)
	}
	old, e := s.SaveCompleteDurableClusters(ctx, repo, []DurableClusterInput{quarantineInput("a", 2, unknown), quarantineInput("b", 3, 5), quarantineInput("c", 4)})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB().Exec("update cluster_memberships set state='removed',removed_by='cluster',removed_at='fixture' where cluster_id=2 and thread_id=5"); e != nil {
		t.Fatal(e)
	}
	unavailableAttempt(t, s, 1, now.Add(-time.Minute), isolatedUnavailableFixture)
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	raw, res := quarantineResponse(t, s, policy)
	snap, e := s.NativeCombinedSnapshot(ctx, policy, raw, res)
	if e != nil {
		t.Fatal(e)
	}
	inputs := []DurableClusterInput{quarantineInput("merge", 2, 3), quarantineInput("c-new", 4), quarantineInput("new-five", 5)}
	plan, e := s.NativePlanClusters(ctx, snap, inputs)
	if e != nil {
		t.Fatal(e)
	}
	if len(plan.QuarantinedGroups) != 2 || len(plan.Published) != 2 {
		t.Fatalf("plan %+v", plan)
	}
	planRaw, planHash, proofErr := plan.Proof()
	snap.QuarantinePlanSHA256 = planHash
	e = proofErr
	proofs["partial"] = string(planRaw)
	if e != nil {
		t.Fatal(e)
	}
	before, e := s.ClusterQuarantineRowHash(ctx, repo, []int64{1, 2})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB().Exec(`CREATE TRIGGER corrupt_quarantine AFTER INSERT ON cluster_events WHEN NEW.cluster_id=3 BEGIN UPDATE cluster_groups SET title='indirect mutation' WHERE id=1; END`); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = s.NativeCombinedPublish(ctx, snap, policy, raw, res, inputs); e == nil || !strings.Contains(e.Error(), "postimage") {
		t.Fatal("indirect quarantine write not rolled back", e)
	}
	var count int
	s.DB().QueryRow("select count(*) from thread_vectors").Scan(&count)
	if count != 0 {
		t.Fatal("vector escaped rollback")
	}
	s.DB().QueryRow("select count(*) from cluster_runs").Scan(&count)
	if count != int(old.RunID) {
		t.Fatal("run escaped rollback")
	}
	s.DB().Exec("DROP TRIGGER corrupt_quarantine")
	imported, result, _, e := s.NativeCombinedPublish(ctx, snap, policy, raw, res, inputs)
	if e != nil || imported.Imported != 4 {
		t.Fatal(imported, result, e)
	}
	after, e := s.ClusterQuarantineRowHash(ctx, repo, []int64{1, 2})
	if e != nil || after != before {
		t.Fatal("quarantined history changed", e)
	}
	read, e := s.ClusterReadSelection(ctx, repo, ClusterSourceDurable)
	if e != nil {
		t.Fatal(e)
	}
	scope := read.ClusterScope
	if scope.Format != ClusterScopeFormatV2 || *scope.Population.Published != 2 || *scope.Population.IdentityQuarantined != 2 || scope.ProviderComplete || scope.RefreshedMemberships != 2 || scope.RetainedPriorMemberships != 3 {
		t.Fatalf("scope %+v", scope)
	}
	detail, e := s.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repo, ClusterID: 1, IncludeClosed: true, MemberLimit: 10})
	if e != nil || detail.Cluster.IdentityQuarantineStatus != "quarantined" {
		t.Fatal(detail, e)
	}
	for _, m := range detail.Members {
		if m.RetentionReason == nil || *m.RetentionReason != "identity_quarantine" || m.LastSeenRunID == nil || *m.LastSeenRunID != strconv.FormatInt(old.RunID, 10) {
			t.Fatal("member provenance lost", m)
		}
	}
	fixture := map[string]any{"synthetic_only": true}
	save := func(name string, run int64) {
		r, e := s.ClusterReadSelection(ctx, repo, ClusterSourceDurable)
		if e != nil {
			t.Fatal(e)
		}
		var start, end, status string
		s.DB().QueryRow("select started_at,finished_at,status from cluster_runs where id=?", run).Scan(&start, &end, &status)
		if r.Analysis == nil || NativeSHA256([]byte(proofs[name])) != r.Analysis.ClusterScope.Quarantine.ProofSHA256 {
			t.Fatal("emitted proof binding differs", name)
		}
		fixture[name] = map[string]any{"row": map[string]any{"id": run, "started_at": start, "finished_at": end, "status": status}, "read": r, "proof_json": proofs[name]}
		if name == "retained_only" {
			if path := os.Getenv("GITCRAWL_QUARANTINE_DB"); path != "" {
				if _, err := s.DB().Exec("VACUUM INTO ?", path); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	save("partial", result.RunID)
	again, e := s.NativeCombinedSnapshot(ctx, policy, raw, res)
	if e != nil {
		t.Fatal(e)
	}
	_, same, _, e := s.NativeCombinedPublish(ctx, again, policy, raw, res, inputs)
	if e != nil || same.RunID != result.RunID {
		t.Fatal("partial no-op churn", e)
	}
	// Ordinary success semantics resolve only the synthetic provider obligation.
	at := time.Now().UTC().Format(time.RFC3339Nano)
	if e = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "openclaw/openclaw", Number: 1, Operation: "graphql_history", StartedAt: at, FinishedAt: at, Status: "success", Evidence: json.RawMessage(`{}`)}); e != nil {
		t.Fatal(e)
	}
	s.SetAnalyticsCoverageComplete(ctx, "openclaw/openclaw", true)
	raw, res = quarantineResponse(t, s, policy)
	snap, e = s.NativeCombinedSnapshot(ctx, policy, raw, res)
	if e != nil {
		t.Fatal(e)
	}
	inputs = append(inputs, quarantineInput("old-unknown", unknown))
	nextPlan, err := s.NativePlanClusters(ctx, snap, inputs)
	if err != nil {
		t.Fatal(err)
	}
	nextRaw, _, err := nextPlan.Proof()
	if err != nil {
		t.Fatal(err)
	}
	proofs["provider_complete_identity_partial"] = string(nextRaw)
	_, result, _, e = s.NativeCombinedPublish(ctx, snap, policy, raw, res, inputs)
	if e != nil {
		t.Fatal(e)
	}
	read, e = s.ClusterReadSelection(ctx, repo, ClusterSourceDurable)
	if e != nil || !read.ClusterScope.ProviderComplete || read.ClusterScope.FullNativeScopeComplete || *read.ClusterScope.IdentityReconciliationComplete {
		t.Fatal("provider recovery hid identity conflict", e)
	}
	save("provider_complete_identity_partial", result.RunID)
	// All selected nodes in one overlapping candidate: blocked analysis, no success.
	if _, e = s.DB().Exec("update threads set body='fixture #1 #3 #4 #5 changed graph references' where id=2"); e != nil {
		t.Fatal(e)
	}
	snap, e = s.NativeClusterSnapshot(ctx, policy)
	if e != nil {
		t.Fatal(e)
	}
	blockedInputs := []DurableClusterInput{quarantineInput("all", unknown, 2, 3, 4, 5)}
	blockedPlan, err := s.NativePlanClusters(ctx, snap, blockedInputs)
	if err != nil {
		t.Fatal(err)
	}
	blockedRaw, _, err := blockedPlan.Proof()
	if err != nil {
		t.Fatal(err)
	}
	proofs["retained_only"] = string(blockedRaw)
	blocked, _, e := s.NativePublishClusters(ctx, snap, policy, blockedInputs)
	if e != nil {
		t.Fatal(e)
	}
	read, e = s.ClusterReadSelection(ctx, repo, ClusterSourceDurable)
	if e != nil || read.Selection.RunID == nil || *read.Selection.RunID != strconv.FormatInt(result.RunID, 10) || read.Analysis == nil || read.Analysis.Status != "retained_only" || read.Analysis.RunID != strconv.FormatInt(blocked.RunID, 10) {
		t.Fatal("blocked analysis replaced success", read, e)
	}
	save("retained_only", blocked.RunID)
	// v3: the durably blocked run still publishes its complete computed
	// partition as the selected run-scoped generation.
	computed, e := s.SelectedClusterCandidateGeneration(ctx, repo)
	if e != nil || computed.RunID != blocked.RunID || computed.CandidateCount != 1 || computed.MemberCount != 5 ||
		computed.DurableQuarantinedCandidates != 1 || computed.DurablePublishedCandidates != 0 || computed.OverlapCount == 0 {
		t.Fatal("blocked run did not select its computed generation", computed, e)
	}
	generations, e := s.ListClusterCandidateGenerations(ctx, repo, 100)
	if e != nil {
		t.Fatal(e)
	}
	snap, e = s.NativeClusterSnapshot(ctx, policy)
	if e != nil {
		t.Fatal(e)
	}
	same, _, e = s.NativePublishClusters(ctx, snap, policy, blockedInputs)
	if e != nil || same.RunID != blocked.RunID {
		t.Fatal("blocked no-op churn", e)
	}
	if after, e := s.ListClusterCandidateGenerations(ctx, repo, 100); e != nil || len(after) != len(generations) {
		t.Fatal("no-op run recorded another computed generation", e)
	}
	// A different genuine graph with one-to-one identity relations can recover.
	recovery := []DurableClusterInput{quarantineInput("a", 2, unknown), quarantineInput("b", 3), quarantineInput("c", 4), quarantineInput("new-five", 5)}
	// Reflect a changed native graph input, as a real algorithm change in source
	// content would; no-op signatures must not be bypassed in production.
	if _, e = s.DB().Exec("update threads set body='fixture recovered topology' where id=2"); e != nil {
		t.Fatal(e)
	}
	// The title_original document input remains unchanged; the full graph body is
	// intentionally a separate exact input and therefore changes InputSHA256.
	snap, e = s.NativeClusterSnapshot(ctx, policy)
	if e != nil {
		t.Fatal(e)
	}
	fullPlan, err := s.NativePlanClusters(ctx, snap, recovery)
	if err != nil {
		t.Fatal(err)
	}
	fullRaw, _, err := fullPlan.Proof()
	if err != nil {
		t.Fatal(err)
	}
	proofs["full_recovery"] = string(fullRaw)
	full, _, e := s.NativePublishClusters(ctx, snap, policy, recovery)
	if e != nil {
		t.Fatal(e)
	}
	read, e = s.ClusterReadSelection(ctx, repo, ClusterSourceDurable)
	if e != nil || read.ClusterScope.Mode != "full-native" || read.ClusterScope.LastFullRunID == nil || *read.ClusterScope.LastFullRunID != strconv.FormatInt(full.RunID, 10) {
		t.Fatal("full recovery", read, e)
	}
	save("full_recovery", full.RunID)
	if computed, e := s.SelectedClusterCandidateGeneration(ctx, repo); e != nil || computed.RunID != full.RunID || computed.CandidateCount != 4 || computed.DurableQuarantinedCandidates != 0 {
		t.Fatal("full recovery generation", computed, e)
	} else {
		candidates, e := s.ClusterCandidates(ctx, repo, computed.ID, 10, false)
		if e != nil || len(candidates) != 4 {
			t.Fatal("full recovery candidates", candidates, e)
		}
		for _, c := range candidates {
			var key string
			if e = s.DB().QueryRow("select stable_key from cluster_groups where id=?", c.DurableClusterID).Scan(&key); e != nil || c.DurableStatus != "published" {
				t.Fatal("published candidate lacks its durable identity", c, e)
			}
		}
	}
	if rolled, e := s.RollbackClusterCandidateGeneration(ctx, repo); e != nil || rolled.RunID != blocked.RunID {
		t.Fatal("computed generation rollback", rolled, e)
	}
	if path := os.Getenv("GITCRAWL_QUARANTINE_FINAL_DB"); path != "" {
		if _, err := s.DB().Exec("VACUUM INTO ?", path); err != nil {
			t.Fatal(err)
		}
	}
	fixture["quarantined_detail"] = detail
	fixture["original_quarantined_row_sha256"] = before
	if path := os.Getenv("GITCRAWL_QUARANTINE_FIXTURE"); path != "" {
		raw, e := json.MarshalIndent(fixture, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.Write(append(raw, '\n')); e != nil {
			t.Fatal(e)
		}
		f.Close()
	}
}
