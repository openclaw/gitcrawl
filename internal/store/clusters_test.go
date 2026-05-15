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

func TestSortClusterSummariesOldest(t *testing.T) {
	clusters := []ClusterSummary{
		{ID: 2, MemberCount: 1, UpdatedAt: "2026-04-27T11:00:00Z"},
		{ID: 1, MemberCount: 5, UpdatedAt: "2026-04-27T10:00:00Z"},
	}

	sortClusterSummaries(clusters, "oldest")

	if clusters[0].ID != 1 || clusters[1].ID != 2 {
		t.Fatalf("oldest sort order = %d,%d; want 1,2", clusters[0].ID, clusters[1].ID)
	}
}

func TestDurableClusterSummariesUsePrimaryOpenMembers(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	canonicalID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "101", Number: 101, Kind: "issue", State: "open",
		Title: "broad canonical", HTMLURL: "https://github.com/openclaw/openclaw/issues/101",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-101", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("canonical thread: %v", err)
	}
	closedID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "102", Number: 102, Kind: "issue", State: "closed",
		Title: "closed stale related", HTMLURL: "https://github.com/openclaw/openclaw/issues/102",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-102", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("closed thread: %v", err)
	}
	specificID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "103", Number: 103, Kind: "issue", State: "open",
		Title: "specific canonical elsewhere", HTMLURL: "https://github.com/openclaw/openclaw/issues/103",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-103", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("specific thread: %v", err)
	}
	relatedOnlyID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "104", Number: 104, Kind: "issue", State: "open",
		Title: "real related member", HTMLURL: "https://github.com/openclaw/openclaw/issues/104",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-104", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("related-only thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(1000, ?, 'broad', 'broad', 'active', ?, 'Broad cluster', '2026-04-26T00:00:00Z', '2026-04-26T00:10:00Z'),
		      (1001, ?, 'specific', 'specific', 'active', ?, 'Specific cluster', '2026-04-26T00:00:00Z', '2026-04-26T00:20:00Z');
	`, repoID, canonicalID, repoID, specificID); err != nil {
		t.Fatalf("seed cluster groups: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(1000, ?, 'canonical', 'active', 'algo', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z'),
		      (1000, ?, 'related', 'active', 'algo', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z'),
		      (1000, ?, 'related', 'active', 'algo', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z'),
		      (1000, ?, 'related', 'active', 'algo', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z'),
		      (1001, ?, 'canonical', 'active', 'algo', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z');
	`, canonicalID, closedID, specificID, relatedOnlyID, specificID); err != nil {
		t.Fatalf("seed cluster memberships: %v", err)
	}

	active, err := st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 10, Sort: "size"})
	if err != nil {
		t.Fatalf("list active clusters: %v", err)
	}
	if len(active) != 2 || active[0].ID != 1000 || active[0].MemberCount != 2 || active[1].ID != 1001 || active[1].MemberCount != 1 {
		t.Fatalf("active summaries should count primary open members, got %#v", active)
	}
	if active[0].Status != "active" {
		t.Fatalf("active summary status should not be derived from hidden historical members, got %#v", active[0])
	}
	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 1000, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("active detail: %v", err)
	}
	if detail.Cluster.Status != "active" {
		t.Fatalf("active detail status should not be derived from hidden historical members, got %#v", detail.Cluster)
	}
	if len(detail.Members) != 2 || detail.Members[0].Thread.Number != 101 || detail.Members[1].Thread.Number != 104 {
		t.Fatalf("active detail should hide closed and secondary related members, got %#v", detail.Members)
	}
	clusterID, err := st.ClusterIDForThreadNumber(ctx, repoID, 103, false)
	if err != nil {
		t.Fatalf("cluster id for specific thread: %v", err)
	}
	if clusterID != 1001 {
		t.Fatalf("specific canonical cluster id = %d, want 1001", clusterID)
	}
	all, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 1000, IncludeClosed: true, MemberLimit: 10})
	if err != nil {
		t.Fatalf("all detail: %v", err)
	}
	if len(all.Members) != 4 {
		t.Fatalf("include closed should preserve all durable memberships, got %#v", all.Members)
	}
}

func TestListDisplayClusterSummariesPrefersLatestRawRun(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	rawOne, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "101", Number: 101, Kind: "issue", State: "open",
		Title: "raw first", HTMLURL: "https://github.com/openclaw/openclaw/issues/101",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "raw-101", UpdatedAt: "2026-04-26T01:00:00Z",
	})
	if err != nil {
		t.Fatalf("raw first thread: %v", err)
	}
	rawTwo, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "102", Number: 102, Kind: "pull_request", State: "open",
		Title: "raw second", HTMLURL: "https://github.com/openclaw/openclaw/pull/102",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "raw-102", UpdatedAt: "2026-04-26T02:00:00Z",
	})
	if err != nil {
		t.Fatalf("raw second thread: %v", err)
	}
	rawClosed, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "103", Number: 103, Kind: "issue", State: "closed",
		Title: "raw closed", HTMLURL: "https://github.com/openclaw/openclaw/issues/103",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "raw-103", UpdatedAt: "2026-04-26T04:00:00Z",
	})
	if err != nil {
		t.Fatalf("raw closed thread: %v", err)
	}
	durableID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "201", Number: 201, Kind: "issue", State: "open",
		Title: "durable member", HTMLURL: "https://github.com/openclaw/openclaw/issues/201",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "durable-201", UpdatedAt: "2026-04-26T03:00:00Z",
	})
	if err != nil {
		t.Fatalf("durable thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_runs(id, repo_id, scope, status, started_at, finished_at, stats_json)
		values(7, ?, 'repo', 'completed', '2026-04-26T00:00:00Z', '2026-04-26T00:01:00Z', '{}');
		insert into clusters(id, repo_id, cluster_run_id, representative_thread_id, member_count, created_at)
		values(70, ?, 7, ?, 3, '2026-04-26T00:01:00Z');
	`, repoID, repoID, rawOne); err != nil {
		t.Fatalf("seed raw cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_members(cluster_id, thread_id, score_to_representative, created_at)
		values(70, ?, 1.0, '2026-04-26T00:01:00Z'),
		      (70, ?, 0.91, '2026-04-26T00:01:00Z'),
		      (70, ?, 0.90, '2026-04-26T00:01:00Z');
	`, rawOne, rawTwo, rawClosed); err != nil {
		t.Fatalf("seed raw members: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(70, ?, 'durable-key', 'durable-slug', 'active', ?, 'Durable title', '2026-04-26T00:00:00Z', '2026-04-26T00:03:00Z')
	`, repoID, durableID); err != nil {
		t.Fatalf("seed durable cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(70, ?, 'member', 'active', 'system', '{}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z')
	`, durableID); err != nil {
		t.Fatalf("seed durable member: %v", err)
	}

	activeDisplay, err := st.ListDisplayClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20, Sort: "size"})
	if err != nil {
		t.Fatalf("list active display clusters: %v", err)
	}
	if len(activeDisplay) != 1 || activeDisplay[0].ID != 70 || activeDisplay[0].Source != ClusterSourceRun || activeDisplay[0].MemberCount != 3 {
		t.Fatalf("active display clusters should prefer latest raw run clusters, got %#v", activeDisplay)
	}
	activeDetail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 70, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("active raw detail: %v", err)
	}
	if len(activeDetail.Members) != 2 || activeDetail.Members[0].Thread.Number != 101 || activeDetail.Members[1].Thread.Number == 103 {
		t.Fatalf("active raw detail should hide closed members, got %#v", activeDetail)
	}
	hiddenByMinSize, err := st.ListDisplayClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 3, Limit: 20, Sort: "size"})
	if err != nil {
		t.Fatalf("list active display clusters with min size: %v", err)
	}
	if len(hiddenByMinSize) != 1 || hiddenByMinSize[0].ID != 70 {
		t.Fatalf("active display min-size should count raw cluster members, got %#v", hiddenByMinSize)
	}

	display, err := st.ListDisplayClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 20, Sort: "size"})
	if err != nil {
		t.Fatalf("list display clusters: %v", err)
	}
	if len(display) != 1 || display[0].ID != 70 || display[0].Source != ClusterSourceRun {
		t.Fatalf("display clusters should prefer raw run groups, got %#v", display)
	}
	durable, err := st.ListClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 20, Sort: "size"})
	if err != nil {
		t.Fatalf("list durable clusters: %v", err)
	}
	if len(durable) != 1 || durable[0].ID != 70 || durable[0].Source != ClusterSourceDurable {
		t.Fatalf("durable clusters should remain available, got %#v", durable)
	}
	durableDetail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 70, Source: ClusterSourceDurable, IncludeClosed: true, MemberLimit: 10})
	if err != nil {
		t.Fatalf("durable detail: %v", err)
	}
	if durableDetail.Cluster.Source != ClusterSourceDurable || len(durableDetail.Members) != 1 || durableDetail.Members[0].Thread.Number != 201 {
		t.Fatalf("source-qualified durable detail should not return raw cluster, got %#v", durableDetail)
	}

	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 70, IncludeClosed: true, MemberLimit: 10})
	if err != nil {
		t.Fatalf("raw detail: %v", err)
	}
	if detail.Cluster.Source != ClusterSourceRun || len(detail.Members) != 3 || detail.Members[0].Thread.Number != 101 {
		t.Fatalf("unexpected raw detail: %#v", detail)
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

func TestSaveDurableClustersAppliesLocalOverrides(t *testing.T) {
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
		RepoID: repoID, GitHubID: "41", Number: 41, Kind: "issue", State: "open",
		Title: "first duplicate", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/41",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-41", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "42", Number: 42, Kind: "issue", State: "open",
		Title: "second duplicate", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/42",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-42", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("second thread: %v", err)
	}
	score := 0.93
	input := DurableClusterInput{
		StableKey:              "members:41,42",
		StableSlug:             "cluster-4142",
		RepresentativeThreadID: firstID,
		Title:                  "first duplicate",
		Members: []DurableClusterMemberInput{
			{ThreadID: firstID, Role: "representative"},
			{ThreadID: secondID, Role: "member", ScoreToRepresentative: &score},
		},
	}
	result, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{input})
	if err != nil {
		t.Fatalf("save durable clusters: %v", err)
	}
	if result.ClusterCount != 1 || result.MemberCount != 2 || result.RunID == 0 {
		t.Fatalf("unexpected save result: %#v", result)
	}
	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 1, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail after save: %v", err)
	}
	if detail.Cluster.StableSlug != "cluster-4142" || len(detail.Members) != 2 {
		t.Fatalf("unexpected saved cluster detail: %#v", detail)
	}

	if _, err := st.ExcludeClusterMemberLocally(ctx, repoID, detail.Cluster.ID, 41, "not related"); err != nil {
		t.Fatalf("exclude member: %v", err)
	}
	if _, err := st.SetClusterCanonicalLocally(ctx, repoID, detail.Cluster.ID, 42, "best issue"); err != nil {
		t.Fatalf("set canonical: %v", err)
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{input}); err != nil {
		t.Fatalf("resave durable clusters: %v", err)
	}
	detail, err = st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: detail.Cluster.ID, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail after overrides: %v", err)
	}
	if len(detail.Members) != 1 || detail.Members[0].Thread.ID != secondID || detail.Members[0].Role != "canonical" || detail.Cluster.RepresentativeThreadID != secondID {
		t.Fatalf("saved cluster should replay local overrides: %#v", detail)
	}
	all, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: detail.Cluster.ID, IncludeClosed: true, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail including excluded: %v", err)
	}
	if len(all.Members) != 2 || all.Members[1].State != "excluded" {
		t.Fatalf("excluded member should remain visible with include closed: %#v", all)
	}
}

func TestSaveDurableClustersChoosesVisibleRepresentative(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-05-15T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	closedID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "51", Number: 51, Kind: "issue", State: "closed",
		Title: "closed representative", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/51",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-51", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("closed thread: %v", err)
	}
	openID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "52", Number: 52, Kind: "issue", State: "open",
		Title: "open replacement", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/52",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-52", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("open thread: %v", err)
	}
	score := 0.75
	input := DurableClusterInput{
		StableKey:              "members:51,52",
		StableSlug:             "cluster-5152",
		RepresentativeThreadID: closedID,
		Title:                  "closed representative",
		Members: []DurableClusterMemberInput{
			{ThreadID: closedID, Role: "representative"},
			{ThreadID: openID, Role: "member", ScoreToRepresentative: &score},
		},
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{input}); err != nil {
		t.Fatalf("save durable clusters: %v", err)
	}
	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 1, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != openID || detail.Cluster.RepresentativeNumber != 52 {
		t.Fatalf("active cluster should choose visible representative, got %#v", detail.Cluster)
	}
	if len(detail.Members) != 1 || detail.Members[0].Thread.ID != openID {
		t.Fatalf("active detail should hide closed representative, got %#v", detail.Members)
	}
}

func TestSaveCompleteDurableClustersRefreshesRepresentativeAfterRetiringStaleClusters(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-05-15T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	closedID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "61", Number: 61, Kind: "issue", State: "closed",
		Title: "closed representative", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/61",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-61", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("closed thread: %v", err)
	}
	openID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "62", Number: 62, Kind: "issue", State: "open",
		Title: "open replacement", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/62",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-62", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("open thread: %v", err)
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{{
		StableKey:              "old:62",
		StableSlug:             "old-62",
		RepresentativeThreadID: openID,
		Title:                  "old open cluster",
		Members:                []DurableClusterMemberInput{{ThreadID: openID, Role: "canonical"}},
	}}); err != nil {
		t.Fatalf("seed stale durable cluster: %v", err)
	}
	score := 0.75
	if _, err := st.SaveCompleteDurableClusters(ctx, repoID, []DurableClusterInput{{
		StableKey:              "new:61,62",
		StableSlug:             "new-6162",
		RepresentativeThreadID: closedID,
		Title:                  "new cluster",
		Members: []DurableClusterMemberInput{
			{ThreadID: closedID, Role: "representative"},
			{ThreadID: openID, Role: "member", ScoreToRepresentative: &score},
		},
	}}); err != nil {
		t.Fatalf("save complete durable clusters: %v", err)
	}
	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 2, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != openID || detail.Cluster.RepresentativeNumber != 62 {
		t.Fatalf("complete refresh should choose visible representative after retiring stale clusters, got %#v", detail.Cluster)
	}
}

func TestSaveDurableClustersRefreshesRepresentativeAfterDeletingLegacyClusters(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-05-15T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	closedID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "81", Number: 81, Kind: "issue", State: "closed",
		Title: "closed representative", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/81",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-81", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("closed thread: %v", err)
	}
	openID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "82", Number: 82, Kind: "issue", State: "open",
		Title: "open replacement", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/82",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-82", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("open thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, cluster_type, representative_thread_id, title, created_at, updated_at)
		values(99, ?, 'legacy:82', 'legacy-82', 'active', 'similarity', ?, 'legacy open cluster', ?, ?)
	`, repoID, openID, "2026-05-15T00:00:00Z", "2026-05-15T00:00:00Z"); err != nil {
		t.Fatalf("seed legacy cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(99, ?, 'canonical', 'active', 'system', '{}', ?, ?)
	`, openID, "2026-05-15T00:00:00Z", "2026-05-15T00:00:00Z"); err != nil {
		t.Fatalf("seed legacy member: %v", err)
	}

	score := 0.75
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{{
		StableKey:              "new:81,82",
		StableSlug:             "new-8182",
		RepresentativeThreadID: closedID,
		Title:                  "new cluster",
		Members: []DurableClusterMemberInput{
			{ThreadID: closedID, Role: "representative"},
			{ThreadID: openID, Role: "member", ScoreToRepresentative: &score},
		},
	}}); err != nil {
		t.Fatalf("save durable clusters: %v", err)
	}
	var legacyCount int
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from cluster_groups
		where repo_id = ? and cluster_type = 'similarity'
	`, repoID).Scan(&legacyCount); err != nil {
		t.Fatalf("count legacy clusters: %v", err)
	}
	if legacyCount != 0 {
		t.Fatalf("legacy similarity cluster should be deleted, got %d", legacyCount)
	}
	var clusterID int64
	if err := st.DB().QueryRowContext(ctx, `
		select id
		from cluster_groups
		where repo_id = ? and stable_key = 'new:81,82'
	`, repoID).Scan(&clusterID); err != nil {
		t.Fatalf("find durable cluster: %v", err)
	}
	detail, err := st.ClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: clusterID, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != openID || detail.Cluster.RepresentativeNumber != 82 {
		t.Fatalf("legacy cleanup should refresh visible representative, got %#v", detail.Cluster)
	}
}

func TestSaveDurableClustersPreservesLocallyClosedRepresentative(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-05-15T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	closedID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "71", Number: 71, Kind: "issue", State: "closed",
		Title: "closed canonical", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/71",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-71", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("closed thread: %v", err)
	}
	openID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "72", Number: 72, Kind: "issue", State: "open",
		Title: "open related", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/72",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-72", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("open thread: %v", err)
	}
	score := 0.5
	input := DurableClusterInput{
		StableKey:              "members:71,72",
		StableSlug:             "cluster-7172",
		RepresentativeThreadID: closedID,
		Title:                  "closed canonical",
		Members: []DurableClusterMemberInput{
			{ThreadID: closedID, Role: "canonical"},
			{ThreadID: openID, Role: "member", ScoreToRepresentative: &score},
		},
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{input}); err != nil {
		t.Fatalf("save durable cluster: %v", err)
	}
	if _, err := st.SetClusterCanonicalLocally(ctx, repoID, 1, 71, "historical canonical"); err != nil {
		t.Fatalf("set historical canonical: %v", err)
	}
	if err := st.CloseClusterLocally(ctx, repoID, 1, "done"); err != nil {
		t.Fatalf("close cluster: %v", err)
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{input}); err != nil {
		t.Fatalf("resave locally closed durable cluster: %v", err)
	}
	detail, err := st.DurableClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: 1, IncludeClosed: true, MemberLimit: 10})
	if err != nil {
		t.Fatalf("closed cluster detail: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != closedID || detail.Cluster.RepresentativeNumber != 71 {
		t.Fatalf("locally closed cluster should preserve historical representative, got %#v", detail.Cluster)
	}
}

func TestSaveDurableClustersRetiresMissingClusters(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	first := DurableClusterInput{
		StableKey:              "members:301",
		StableSlug:             "cluster-301",
		RepresentativeThreadID: threadIDs[0],
		Members:                []DurableClusterMemberInput{{ThreadID: threadIDs[0], Role: "canonical"}},
	}
	second := DurableClusterInput{
		StableKey:              "members:302",
		StableSlug:             "cluster-302",
		RepresentativeThreadID: threadIDs[1],
		Members:                []DurableClusterMemberInput{{ThreadID: threadIDs[1], Role: "canonical"}},
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{first, second}); err != nil {
		t.Fatalf("seed durable clusters: %v", err)
	}
	secondID, err := st.ClusterIDForThreadNumber(ctx, repoID, 302, false)
	if err != nil {
		t.Fatalf("second cluster id: %v", err)
	}

	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{first}); err != nil {
		t.Fatalf("partial resave without second cluster: %v", err)
	}
	if _, err := st.ClusterIDForThreadNumber(ctx, repoID, 302, false); err != nil {
		t.Fatalf("partial save should not retire missing cluster: %v", err)
	}

	if _, err := st.SaveCompleteDurableClusters(ctx, repoID, []DurableClusterInput{first}); err != nil {
		t.Fatalf("complete resave without second cluster: %v", err)
	}
	if _, err := st.ClusterIDForThreadNumber(ctx, repoID, 302, false); err == nil {
		t.Fatal("cluster missing from complete save should not remain active")
	}
	retired, err := st.DurableClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: secondID, IncludeClosed: true, MemberLimit: 5})
	if err != nil {
		t.Fatalf("retired cluster detail: %v", err)
	}
	if retired.Cluster.Status != "closed" || retired.Cluster.ClosedAt == "" {
		t.Fatalf("retired cluster = %+v", retired.Cluster)
	}
	var retiredEvents int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from cluster_events where cluster_id = ? and event_type = 'retired'`, secondID).Scan(&retiredEvents); err != nil {
		t.Fatalf("count retired events: %v", err)
	}
	if retiredEvents != 1 {
		t.Fatalf("retired events = %d, want 1", retiredEvents)
	}

	if _, err := st.SaveCompleteDurableClusters(ctx, repoID, []DurableClusterInput{second}); err != nil {
		t.Fatalf("resave with second cluster: %v", err)
	}
	reactivated, err := st.DurableClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: secondID, IncludeClosed: false, MemberLimit: 5})
	if err != nil {
		t.Fatalf("reactivated cluster detail: %v", err)
	}
	if reactivated.Cluster.Status != "active" || reactivated.Cluster.ClosedAt != "" {
		t.Fatalf("reactivated cluster = %+v", reactivated.Cluster)
	}

	if err := st.CloseClusterLocally(ctx, repoID, secondID, "not actionable"); err != nil {
		t.Fatalf("close second cluster: %v", err)
	}
	if _, err := st.SaveCompleteDurableClusters(ctx, repoID, []DurableClusterInput{second}); err != nil {
		t.Fatalf("resave locally closed cluster: %v", err)
	}
	if _, err := st.DurableClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: secondID, IncludeClosed: false, MemberLimit: 5}); err == nil {
		t.Fatal("locally closed cluster should stay hidden after reappearing")
	}
}

func TestSaveDurableClustersRejectsEmptyMembers(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	input := DurableClusterInput{
		StableKey:              "members-empty",
		StableSlug:             "cluster-empty",
		RepresentativeThreadID: threadIDs[0],
		Members:                []DurableClusterMemberInput{{ThreadID: threadIDs[0], Role: "canonical"}},
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{input}); err != nil {
		t.Fatalf("seed durable cluster: %v", err)
	}
	clusterID, err := st.ClusterIDForThreadNumber(ctx, repoID, 301, false)
	if err != nil {
		t.Fatalf("cluster id: %v", err)
	}

	input.Members = nil
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{input}); err == nil {
		t.Fatal("empty member cluster should fail")
	}

	var activeMembers int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from cluster_memberships where cluster_id = ? and state = 'active'`, clusterID).Scan(&activeMembers); err != nil {
		t.Fatalf("count active members: %v", err)
	}
	if activeMembers != 1 {
		t.Fatalf("active members = %d, want 1", activeMembers)
	}
	if _, err := st.ClusterIDForThreadNumber(ctx, repoID, 301, false); err != nil {
		t.Fatalf("cluster should remain available after rejected save: %v", err)
	}
}
