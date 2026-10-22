package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func seedCandidateThreads(t *testing.T, ctx context.Context, st *Store, count int) (int64, []int64) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, count)
	for i := 1; i <= count; i++ {
		id, err := st.UpsertThread(ctx, Thread{RepoID: repoID, GitHubID: fmt.Sprint(900 + i), Number: 900 + i, Kind: "issue", State: "open", Title: fmt.Sprintf("candidate thread %d", i),
			HTMLURL: fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", 900+i), LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: fmt.Sprint("h", i), UpdatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return repoID, ids
}

func candidateInput(key string, members ...int64) DurableClusterInput {
	in := DurableClusterInput{StableKey: key, RepresentativeThreadID: members[0]}
	for _, id := range members {
		in.Members = append(in.Members, DurableClusterMemberInput{ThreadID: id, Role: "member"})
	}
	return in
}

// durableHistoryDump captures every row of the six durable history tables.
func durableHistoryDump(t *testing.T, ctx context.Context, st *Store) string {
	t.Helper()
	var out strings.Builder
	for _, table := range []string{"cluster_groups", "cluster_memberships", "cluster_overrides", "cluster_closures", "cluster_aliases", "cluster_events"} {
		if !st.hasTable(ctx, table) {
			fmt.Fprintf(&out, "%s:absent\n", table)
			continue
		}
		rows, err := st.DB().QueryContext(ctx, "SELECT * FROM "+table+" ORDER BY rowid")
		if err != nil {
			t.Fatal(err)
		}
		columns, _ := rows.Columns()
		var dump strings.Builder
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&dump, "%v;", values)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&out, "%s:%s\n", table, dump.String())
	}
	return out.String()
}

func saveGeneration(t *testing.T, ctx context.Context, st *Store, plan ClusterQuarantinePlan, input string) (ClusterCandidateGeneration, error) {
	t.Helper()
	published := map[string]int64{}
	for i, target := range plan.Targets {
		published[target.CandidateKey] = target.ClusterID
		if target.ClusterID == 0 {
			published[target.CandidateKey] = int64(900 + i)
		}
	}
	var generation ClusterCandidateGeneration
	err := st.WithTx(ctx, func(tx *Store) error {
		runID, err := tx.insertClusterRun(ctx, plan.RepositoryID, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		generation, err = tx.SaveClusterCandidateGeneration(ctx, ClusterCandidateGenerationInput{RepoID: plan.RepositoryID, RunID: runID,
			Algorithm: "test-algorithm", InputSHA256: strings.Repeat(input, 64), QuarantinePlanSHA256: strings.Repeat("b", 64), Plan: plan, PublishedClusterIDs: published})
		return err
	})
	return generation, err
}

func TestClusterCandidateGenerationsPublishSelectAndRollBack(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, ids := seedCandidateThreads(t, ctx, st, 6)
	before := durableHistoryDump(t, ctx, st)
	if _, err := st.SelectedClusterCandidateGeneration(ctx, repoID); !errors.Is(err, ErrNoClusterCandidateGeneration) {
		t.Fatalf("empty selection error = %v", err)
	}
	// A two-group merge and a split: both are durable quarantine, but every
	// computed candidate is published in the run-scoped domain.
	plan := ClusterQuarantinePlan{RepositoryID: repoID, SelectedIDs: ids,
		Candidates: []DurableClusterInput{candidateInput("b-merge", ids[0], ids[1], ids[2]), candidateInput("a-split", ids[3]), candidateInput("c-new", ids[4], ids[5])},
		Identities: []ClusterIdentityMetadata{{ID: 11, Active: true, ActiveMembers: []int64{ids[0], ids[1]}}, {ID: 12, Active: true, ActiveMembers: []int64{ids[2], ids[3]}}, {ID: 13, Active: false, ActiveMembers: []int64{ids[4]}}},
		Targets:    []ClusterPublicationTarget{{CandidateKey: "c-new", ClusterID: 0}}}
	first, err := saveGeneration(t, ctx, st, plan, "1")
	if err != nil {
		t.Fatal(err)
	}
	if first.CandidateCount != 3 || first.MemberCount != 6 || first.OverlapCount != 3 || first.DurablePublishedCandidates != 1 || first.DurableQuarantinedCandidates != 2 || !first.Selected || first.Format != ClusterCandidateFormatV3 || len(first.PartitionSHA256) != 64 {
		t.Fatalf("first generation = %+v", first)
	}
	candidates, err := st.ClusterCandidates(ctx, repoID, first.ID, 10, true)
	if err != nil || len(candidates) != 3 {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	merge := candidates[0]
	if merge.ComputedKey != "b-merge" || merge.ID != fmt.Sprintf("%d:2", first.ID) || merge.MemberCount != 3 || merge.DurableStatus != "identity_quarantined" ||
		len(merge.Overlaps) != 2 || merge.Overlaps[0] != (ClusterCandidateOverlap{ClusterID: 11, SharedMembers: 2}) || len(merge.Members) != 3 {
		t.Fatalf("merge candidate = %+v", merge)
	}
	for _, candidate := range candidates {
		if candidate.ComputedKey == "c-new" && (candidate.DurableStatus != "published" || candidate.DurableClusterID != 900) {
			t.Fatalf("newly published identity not recorded: %+v", candidate)
		}
	}
	if after := durableHistoryDump(t, ctx, st); after != before {
		t.Fatalf("generation changed durable history:\n%s\n%s", before, after)
	}
	plan.Candidates = []DurableClusterInput{candidateInput("x", ids[0], ids[1], ids[2], ids[3]), candidateInput("y", ids[4], ids[5])}
	second, err := saveGeneration(t, ctx, st, plan, "2")
	if err != nil {
		t.Fatal(err)
	}
	if selected, err := st.SelectedClusterCandidateGeneration(ctx, repoID); err != nil || selected.ID != second.ID || selected.SelectionReason != "accepted" {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
	rolled, err := st.RollbackClusterCandidateGeneration(ctx, repoID)
	if err != nil || rolled.ID != first.ID || rolled.SelectionReason != "rollback" || !rolled.Selected {
		t.Fatalf("rollback=%+v err=%v", rolled, err)
	}
	if _, err := st.RollbackClusterCandidateGeneration(ctx, repoID); err == nil || !strings.Contains(err.Error(), "no earlier") {
		t.Fatalf("rollback past the first generation = %v", err)
	}
	listed, err := st.ListClusterCandidateGenerations(ctx, repoID, 10)
	if err != nil || len(listed) != 2 || listed[0].ID != second.ID || listed[0].Selected || !listed[1].Selected {
		t.Fatalf("listed=%+v err=%v", listed, err)
	}
	// Generations are immutable history.
	for _, statement := range []string{"UPDATE cluster_candidates SET member_count=99", "DELETE FROM cluster_candidate_overlaps", "UPDATE cluster_candidate_generations SET run_id=0", "DELETE FROM cluster_candidate_members"} {
		if _, err := st.DB().ExecContext(ctx, statement); err == nil || !strings.Contains(err.Error(), "immutable") {
			t.Fatalf("%s error = %v", statement, err)
		}
	}
	// A third accepted run selects its own newer generation again.
	third, err := saveGeneration(t, ctx, st, plan, "3")
	if err != nil {
		t.Fatal(err)
	}
	if selected, _ := st.SelectedClusterCandidateGeneration(ctx, repoID); selected.ID != third.ID {
		t.Fatalf("new run did not select its generation: %+v", selected)
	}
	if after := durableHistoryDump(t, ctx, st); after != before {
		t.Fatal("generations changed durable history")
	}
}

func TestClusterCandidateGenerationRefusesInvalidPartitions(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, ids := seedCandidateThreads(t, ctx, st, 3)
	valid := ClusterQuarantinePlan{RepositoryID: repoID, SelectedIDs: ids, Candidates: []DurableClusterInput{candidateInput("a", ids[0], ids[1]), candidateInput("b", ids[2])}}
	cases := map[string]func(*ClusterQuarantinePlan){
		"duplicate member":   func(p *ClusterQuarantinePlan) { p.Candidates[1] = candidateInput("b", ids[1], ids[2]) },
		"uncovered selected": func(p *ClusterQuarantinePlan) { p.Candidates = p.Candidates[:1] },
		"outside selected":   func(p *ClusterQuarantinePlan) { p.SelectedIDs = ids[:2] },
		"empty candidate":    func(p *ClusterQuarantinePlan) { p.Candidates[1].Members = nil },
		"other repository":   func(p *ClusterQuarantinePlan) { p.RepositoryID = repoID + 1 },
		"outside representative": func(p *ClusterQuarantinePlan) {
			p.Candidates[1].RepresentativeThreadID = ids[0]
		},
		"conflicting published identity": func(p *ClusterQuarantinePlan) {
			p.Targets = []ClusterPublicationTarget{{CandidateKey: "a", ClusterID: -1}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			plan := valid
			plan.Candidates = append([]DurableClusterInput(nil), valid.Candidates...)
			mutate(&plan)
			if _, err := saveGeneration(t, ctx, st, plan, "a"); err == nil {
				t.Fatal("invalid partition accepted")
			}
			if _, err := st.SelectedClusterCandidateGeneration(ctx, repoID); !errors.Is(err, ErrNoClusterCandidateGeneration) {
				t.Fatalf("failed save left a selection: %v", err)
			}
		})
	}
	if _, err := st.SaveClusterCandidateGeneration(ctx, ClusterCandidateGenerationInput{RepoID: repoID, RunID: 1, Algorithm: "a", InputSHA256: strings.Repeat("a", 64), QuarantinePlanSHA256: strings.Repeat("b", 64), Plan: valid}); err == nil || !strings.Contains(err.Error(), "transaction") {
		t.Fatalf("save outside a transaction = %v", err)
	}
	if _, err := st.RollbackClusterCandidateGeneration(ctx, repoID); !errors.Is(err, ErrNoClusterCandidateGeneration) {
		t.Fatalf("rollback without a generation = %v", err)
	}
}

func TestClusterCandidateGenerationsHonorOwnerRemoval(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, ids := seedCandidateThreads(t, ctx, st, 3)
	plan := ClusterQuarantinePlan{RepositoryID: repoID, SelectedIDs: ids, Candidates: []DurableClusterInput{candidateInput("a", ids[0], ids[1]), candidateInput("b", ids[2])}}
	generation, err := saveGeneration(t, ctx, st, plan, "a")
	if err != nil {
		t.Fatal(err)
	}
	preview, err := st.PlanThreadPurge(ctx, "openclaw/openclaw", []int{901})
	if err != nil {
		t.Fatal(err)
	}
	if preview.Counts["cluster_candidate_members"] != 1 {
		t.Fatalf("purge plan did not inventory candidate members: %+v", preview.Counts)
	}
	if _, err := st.PurgeThreads(ctx, "openclaw/openclaw", []int{901}, preview.PlanID); err != nil {
		t.Fatal(err)
	}
	candidates, err := st.ClusterCandidates(ctx, repoID, generation.ID, 10, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range candidates {
		for _, member := range candidate.Members {
			if member.ThreadID == ids[0] {
				t.Fatal("owner-removed thread remains a candidate member")
			}
		}
		if candidate.RepresentativeThreadID == ids[0] || candidate.ComputedKey == "a" && candidate.MemberCount != 1 {
			t.Fatalf("owner-removed thread still counted: %+v", candidate)
		}
	}
	// A later generation containing an owner-excluded thread is refused.
	var stale ClusterQuarantinePlan = plan
	if _, err := saveGeneration(t, ctx, st, stale, "c"); err == nil || !strings.Contains(err.Error(), "owner-excluded") {
		t.Fatalf("stale owner-excluded input = %v", err)
	}
}

func TestPortableRepositoryScopeDropsCandidateGenerations(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, ids := seedCandidateThreads(t, ctx, st, 2)
	plan := ClusterQuarantinePlan{RepositoryID: repoID, SelectedIDs: ids, Candidates: []DurableClusterInput{candidateInput("a", ids[0], ids[1])}}
	if _, err := saveGeneration(t, ctx, st, plan, "a"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "other", FullName: "openclaw/other", RawJSON: "{}", UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RestrictPortableRepository(ctx, "openclaw/other"); err != nil {
		t.Fatalf("restrict a snapshot holding another repository's generations: %v", err)
	}
	var remaining int
	if err := st.DB().QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'cluster_candidate%'").Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("portable snapshot retained %d candidate tables: %v", remaining, err)
	}
}
