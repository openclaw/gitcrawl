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
