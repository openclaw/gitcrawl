package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCloseThreadLocallyHidesThreadAndSurvivesUpsert(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-27T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	thread := Thread{
		RepoID:        repoID,
		GitHubID:      "42",
		Number:        42,
		Kind:          "issue",
		State:         "open",
		Title:         "Noisy duplicate",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/42",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-a",
		UpdatedAt:     "2026-04-27T00:00:00Z",
	}
	threadID, err := st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(1, ?, 'cluster-1', 'cluster-1', 'active', ?, 'Noisy duplicate cluster', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, repoID, threadID); err != nil {
		t.Fatalf("insert cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(1, ?, 'member', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, threadID); err != nil {
		t.Fatalf("insert membership: %v", err)
	}

	if err := st.CloseThreadLocally(ctx, repoID, 42, "TUI manual close"); err != nil {
		t.Fatalf("close thread locally: %v", err)
	}
	openRows, err := st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list open threads: %v", err)
	}
	if len(openRows) != 0 {
		t.Fatalf("locally closed thread should be hidden, got %#v", openRows)
	}
	closedRows, err := st.ListThreads(ctx, repoID, true)
	if err != nil {
		t.Fatalf("list closed threads: %v", err)
	}
	if len(closedRows) != 1 || closedRows[0].ClosedAtLocal == "" || closedRows[0].CloseReasonLocal != "TUI manual close" {
		t.Fatalf("closed thread not marked correctly: %#v", closedRows)
	}
	activeClusters, err := st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list active clusters: %v", err)
	}
	if len(activeClusters) != 0 {
		t.Fatalf("cluster with only locally closed members should be hidden, got %#v", activeClusters)
	}
	allClusters, err := st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list all clusters: %v", err)
	}
	if len(allClusters) != 1 || allClusters[0].MemberCount != 1 {
		t.Fatalf("include closed should retain cluster membership count, got %#v", allClusters)
	}

	thread.Title = "Noisy duplicate updated upstream"
	thread.ContentHash = "hash-b"
	if _, err := st.UpsertThread(ctx, thread); err != nil {
		t.Fatalf("upsert closed thread: %v", err)
	}
	openRows, err = st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list open threads after upsert: %v", err)
	}
	if len(openRows) != 0 {
		t.Fatalf("upsert should preserve local close, got %#v", openRows)
	}

	if err := st.ReopenThreadLocally(ctx, repoID, 42); err != nil {
		t.Fatalf("reopen thread locally: %v", err)
	}
	openRows, err = st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list reopened threads: %v", err)
	}
	if len(openRows) != 1 || openRows[0].ClosedAtLocal != "" || openRows[0].CloseReasonLocal != "" {
		t.Fatalf("reopened thread not visible/cleared: %#v", openRows)
	}
}

func TestCloseThreadLocallyRequiresExistingThread(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.CloseThreadLocally(ctx, 1, 404, "missing"); err == nil {
		t.Fatal("expected missing thread error")
	}
}

func TestReopenThreadLocallyRequiresExistingThread(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.ReopenThreadLocally(ctx, 1, 404); err == nil {
		t.Fatal("expected missing thread error")
	}
}
