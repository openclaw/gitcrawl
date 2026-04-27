package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestListClusterSummaries(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "download stalls", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	_, err = st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(10, ?, 'key', 'slug', 'active', ?, 'Cluster title', '2026-04-26T00:00:00Z', '2026-04-26T00:00:01Z');
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(10, ?, 'member', 'active', 'system', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z');
	`, repoID, threadID, threadID)
	if err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	summaries, err := st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, Sort: "size"})
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}
	if len(summaries) != 1 || summaries[0].StableSlug != "slug" || summaries[0].MemberCount != 1 {
		t.Fatalf("unexpected summaries: %#v", summaries)
	}

	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 10, MemberLimit: 5, BodyChars: 8})
	if err != nil {
		t.Fatalf("cluster detail: %v", err)
	}
	if detail.Cluster.ID != 10 || len(detail.Members) != 1 {
		t.Fatalf("unexpected detail: %#v", detail)
	}
	if detail.Members[0].Thread.Number != 1 {
		t.Fatalf("unexpected member thread: %#v", detail.Members[0].Thread)
	}

	clusterID, err := st.ClusterIDForThreadNumber(ctx, repoID, 1, true)
	if err != nil {
		t.Fatalf("thread cluster id: %v", err)
	}
	if clusterID != 10 {
		t.Fatalf("thread cluster id = %d, want 10", clusterID)
	}
}

func TestCloseAndReopenClusterLocally(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "2", Number: 2, Kind: "issue", State: "open",
		Title: "duplicate cluster", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/2",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-2", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(20, ?, 'key-2', 'slug-2', 'active', ?, 'Cluster title', '2026-04-26T00:00:00Z', '2026-04-26T00:00:01Z');
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(20, ?, 'member', 'active', 'system', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z');
	`, repoID, threadID, threadID); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}

	if err := st.CloseClusterLocally(ctx, repoID, 20, "handled elsewhere"); err != nil {
		t.Fatalf("close cluster: %v", err)
	}
	active, err := st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list active clusters: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("closed cluster should be hidden, got %#v", active)
	}
	all, err := st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list all clusters: %v", err)
	}
	if len(all) != 1 || all[0].Status != "closed" || all[0].ClosedAt == "" {
		t.Fatalf("closed cluster not marked: %#v", all)
	}

	if err := st.ReopenClusterLocally(ctx, repoID, 20); err != nil {
		t.Fatalf("reopen cluster: %v", err)
	}
	active, err = st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list reopened clusters: %v", err)
	}
	if len(active) != 1 || active[0].Status != "active" || active[0].ClosedAt != "" {
		t.Fatalf("reopened cluster not visible/cleared: %#v", active)
	}
}

func TestClusterMemberLocalOverrides(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	firstID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "31", Number: 31, Kind: "issue", State: "open",
		Title: "first member", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/31",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-31", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "32", Number: 32, Kind: "issue", State: "open",
		Title: "second member", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/32",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-32", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("second thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(30, ?, 'key-30', 'slug-30', 'active', ?, 'Cluster title', '2026-04-26T00:00:00Z', '2026-04-26T00:00:01Z')
	`, repoID, firstID); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(30, ?, 'representative', 'active', 'system', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z')
	`, firstID); err != nil {
		t.Fatalf("seed first member: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(30, ?, 'member', 'active', 'system', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z')
	`, secondID); err != nil {
		t.Fatalf("seed second member: %v", err)
	}

	excluded, err := st.ExcludeClusterMemberLocally(ctx, repoID, 30, 31, "not related")
	if err != nil {
		t.Fatalf("exclude member: %v", err)
	}
	if excluded.ThreadID != firstID || excluded.Action != "exclude" {
		t.Fatalf("unexpected exclude result: %#v", excluded)
	}
	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 30, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail after exclude: %v", err)
	}
	if len(detail.Members) != 1 || detail.Members[0].Thread.Number != 32 || detail.Cluster.RepresentativeThreadID != secondID {
		t.Fatalf("excluded member should be hidden and representative refreshed: %#v", detail)
	}

	included, err := st.IncludeClusterMemberLocally(ctx, repoID, 30, 31, "belongs here")
	if err != nil {
		t.Fatalf("include member: %v", err)
	}
	if included.ThreadID != firstID || included.Action != "include" {
		t.Fatalf("unexpected include result: %#v", included)
	}
	detail, err = st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 30, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail after include: %v", err)
	}
	if len(detail.Members) != 2 {
		t.Fatalf("included member should be visible again: %#v", detail)
	}

	canonical, err := st.SetClusterCanonicalLocally(ctx, repoID, 30, 31, "best duplicate")
	if err != nil {
		t.Fatalf("set canonical: %v", err)
	}
	if canonical.ThreadID != firstID || canonical.Action != "canonical" {
		t.Fatalf("unexpected canonical result: %#v", canonical)
	}
	detail, err = st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 30, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail after canonical: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != firstID || detail.Members[0].Thread.Number != 31 || detail.Members[0].Role != "canonical" {
		t.Fatalf("canonical member should become representative and sort first: %#v", detail)
	}
	var excludeOverrides int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from cluster_overrides where cluster_id = 30 and action = 'exclude'`).Scan(&excludeOverrides); err != nil {
		t.Fatalf("count exclude overrides: %v", err)
	}
	if excludeOverrides != 0 {
		t.Fatalf("include/canonical should clear stale exclude overrides, got %d", excludeOverrides)
	}
}
