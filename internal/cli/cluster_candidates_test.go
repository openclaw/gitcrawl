package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestClusterCandidatesCommandReadsSelectsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	initApp := New()
	initApp.Stdout, initApp.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
	if err := initApp.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath, "--json"}); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for i := 1; i <= 3; i++ {
		id, err := st.UpsertThread(ctx, store.Thread{RepoID: repoID, GitHubID: fmt.Sprint(i), Number: i, Kind: "issue", State: "open", Title: "t", HTMLURL: "https://github.com/openclaw/openclaw/issues/1", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: fmt.Sprint(i), UpdatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for generation := 0; generation < 2; generation++ {
		plan := store.ClusterQuarantinePlan{RepositoryID: repoID, SelectedIDs: ids,
			Candidates: []store.DurableClusterInput{
				{StableKey: "a", RepresentativeThreadID: ids[0], Members: []store.DurableClusterMemberInput{{ThreadID: ids[0]}, {ThreadID: ids[1]}}},
				{StableKey: "b", RepresentativeThreadID: ids[2], Members: []store.DurableClusterMemberInput{{ThreadID: ids[2]}}}},
			Identities: []store.ClusterIdentityMetadata{{ID: 7, Active: true, ActiveMembers: []int64{ids[0]}}},
			Targets:    []store.ClusterPublicationTarget{{CandidateKey: "b", ClusterID: 7}}}
		result, err := st.DB().ExecContext(ctx, "insert into cluster_runs(repo_id, scope, status, started_at) values(?, 'durable', 'success', ?)", repoID, now)
		if err != nil {
			t.Fatal(err)
		}
		runID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if err := st.WithTx(ctx, func(tx *store.Store) error {
			_, err := tx.SaveClusterCandidateGeneration(ctx, store.ClusterCandidateGenerationInput{RepoID: repoID, RunID: runID, Algorithm: "test",
				InputSHA256: strings.Repeat(fmt.Sprint(generation), 64), QuarantinePlanSHA256: strings.Repeat("b", 64), Plan: plan, PublishedClusterIDs: map[string]int64{"b": 7}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	run := func(reader bool, args ...string) (map[string]any, error) {
		app := New()
		var stdout bytes.Buffer
		app.Stdout, app.Stderr = &stdout, &bytes.Buffer{}
		full := append([]string{"--config", configPath, "--json", "cluster-candidates", "openclaw/openclaw"}, args...)
		if reader {
			err = app.RunClusterReader(ctx, full)
		} else {
			err = app.Run(ctx, full)
		}
		if err != nil {
			return nil, err
		}
		var payload map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
			t.Fatalf("decode %s: %v", stdout.String(), err)
		}
		return payload, nil
	}
	current, err := run(true, "--members")
	if err != nil {
		t.Fatal(err)
	}
	generation := current["generation"].(map[string]any)
	candidates := current["candidates"].([]any)
	if generation["generation_id"].(float64) != 2 || current["identity_domain"] != "computed-run-scoped" || current["identity_reconciliation_complete"] != false || len(candidates) != 2 {
		t.Fatalf("selected generation output: %+v", current)
	}
	first := candidates[0].(map[string]any)
	if first["computed_candidate_id"] != "2:1" || first["durable_status"] != "identity_quarantined" || len(first["observed_overlaps"].([]any)) != 1 || len(first["members"].([]any)) != 2 {
		t.Fatalf("candidate output: %+v", first)
	}
	if _, err := run(true, "--rollback"); err == nil {
		t.Fatal("cluster reader allowed rollback")
	}
	rolled, err := run(false, "--rollback")
	if err != nil || rolled["selected_generation"].(map[string]any)["generation_id"].(float64) != 1 {
		t.Fatalf("rollback output=%+v err=%v", rolled, err)
	}
	listed, err := run(false, "--generations")
	if err != nil || len(listed["generations"].([]any)) != 2 || listed["generations"].([]any)[1].(map[string]any)["selected"] != true {
		t.Fatalf("generation list=%+v err=%v", listed, err)
	}
	explicit, err := run(true, "--generation", "2", "--limit", "1")
	if err != nil || len(explicit["candidates"].([]any)) != 1 || explicit["generation"].(map[string]any)["selected"] != false {
		t.Fatalf("explicit generation=%+v err=%v", explicit, err)
	}
	if _, err := run(true, "--generation", "9"); err == nil {
		t.Fatal("unknown generation accepted")
	}
	if _, err := run(false, "--rollback", "--generation", "1"); err == nil {
		t.Fatal("rollback with explicit generation accepted")
	}
}
