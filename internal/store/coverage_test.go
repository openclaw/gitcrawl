package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRepositoryListAndStoreUtilityBranches(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if st.Path() == "" || st.DB() == nil {
		t.Fatalf("store accessors returned empty values")
	}
	first, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "one", FullName: "openclaw/one", GitHubRepoID: "1", RawJSON: `{"id":1}`, UpdatedAt: "2026-04-30T01:00:00Z"})
	if err != nil {
		t.Fatalf("first repo: %v", err)
	}
	second, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "two", FullName: "openclaw/two", UpdatedAt: "2026-04-30T02:00:00Z"})
	if err != nil {
		t.Fatalf("second repo: %v", err)
	}
	repos, err := st.ListRepositories(ctx)
	if err != nil {
		t.Fatalf("list repos: %v", err)
	}
	if len(repos) != 2 || repos[0].ID != second || repos[1].ID != first {
		t.Fatalf("repo order = %+v", repos)
	}
	if err := st.WithTx(ctx, func(tx *Store) error {
		if _, err := tx.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "three", FullName: "openclaw/three", UpdatedAt: "2026-04-30T03:00:00Z"}); err != nil {
			return err
		}
		return sql.ErrTxDone
	}); err == nil {
		t.Fatal("expected transaction rollback error")
	}
	repos, err = st.ListRepositories(ctx)
	if err != nil {
		t.Fatalf("list repos after rollback: %v", err)
	}
	if len(repos) != 2 {
		t.Fatalf("rollback should keep repo count at 2, got %d", len(repos))
	}
	updatedID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "one", FullName: "openclaw/one", GitHubRepoID: "updated", RawJSON: `{"id":"updated"}`, UpdatedAt: "2026-04-30T04:00:00Z"})
	if err != nil {
		t.Fatalf("update repo: %v", err)
	}
	if updatedID != first {
		t.Fatalf("updated repo id = %d, want %d", updatedID, first)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/one")
	if err != nil {
		t.Fatalf("repo by full name: %v", err)
	}
	if repo.GitHubRepoID != "updated" || repo.RawJSON == "" {
		t.Fatalf("updated repo = %+v", repo)
	}
}

func TestVectorsThreadsByIDsAndDecodeBranches(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := st.UpsertThreadVector(ctx, ThreadVector{ThreadID: threadIDs[0], Basis: "title_original", Model: "test", Dimensions: 3, ContentHash: "h1", Vector: []float64{1, 0, 0}, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert vector 1: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, ThreadVector{ThreadID: threadIDs[1], Basis: "title_original", Model: "test", Dimensions: 3, ContentHash: "h2", Vector: []float64{0.9, 0.1, 0}, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert vector 2: %v", err)
	}
	vectors, err := st.ListThreadVectors(ctx, repoID)
	if err != nil {
		t.Fatalf("list vectors: %v", err)
	}
	if len(vectors) != 2 || vectors[0].Backend != "exact" {
		t.Fatalf("vectors = %+v", vectors)
	}
	thread, vector, err := st.ThreadVectorByNumber(ctx, ThreadVectorQuery{RepoID: repoID, Model: "test", Basis: "title_original", Dimensions: 3}, 301)
	if err != nil {
		t.Fatalf("thread vector by number: %v", err)
	}
	if thread.Number != 301 || len(vector.Vector) != 3 {
		t.Fatalf("thread/vector = %+v %+v", thread, vector)
	}
	threads, err := st.ThreadsByIDs(ctx, repoID, []int64{threadIDs[1], threadIDs[0], 999999})
	if err != nil {
		t.Fatalf("threads by ids: %v", err)
	}
	if len(threads) != 2 || threads[threadIDs[0]].Number != 301 {
		t.Fatalf("threads by ids = %+v", threads)
	}
	if _, err := decodeStoredVector([]byte{1}); err == nil {
		t.Fatal("expected invalid binary vector error")
	}
}

func TestEmbeddingTaskBasisBranches(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at)
		values(?, '2026-04-30T00:00:00Z', 'content', 'title', 'body', 'labels', '2026-04-30T00:00:00Z')
	`, threadIDs[0]); err != nil {
		t.Fatalf("seed revision: %v", err)
	}
	var revisionID int64
	if err := st.DB().QueryRowContext(ctx, `select id from thread_revisions where thread_id = ?`, threadIDs[0]).Scan(&revisionID); err != nil {
		t.Fatalf("revision id: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(?, 'llm_key_summary', 'test', 'test', 'test', 'input', 'output', 'key summary text', '2026-04-30T00:01:00Z')
	`, revisionID); err != nil {
		t.Fatalf("seed key summary: %v", err)
	}
	cases := []struct {
		basis string
		want  string
	}{
		{basis: "title_original", want: "First vector thread"},
		{basis: "dedupe_text", want: "first vector thread"},
		{basis: "llm_key_summary", want: "key_summary"},
	}
	for _, tc := range cases {
		tasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{RepoID: repoID, Basis: tc.basis, Model: "test", IncludeClosed: true})
		if err != nil {
			t.Fatalf("list tasks %s: %v", tc.basis, err)
		}
		joined := ""
		for _, task := range tasks {
			joined += "\n" + task.Text
		}
		if len(tasks) == 0 || !strings.Contains(joined, tc.want) {
			t.Fatalf("tasks %s = %+v, want text containing %q", tc.basis, tasks, tc.want)
		}
	}
	if _, err := embeddingTextForBasis("missing", "", "", "", "", ""); err == nil {
		t.Fatal("unsupported basis should fail")
	}
}

func TestEmbeddingTasksSkipExistingHashAndForce(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	text := "First vector thread\n\nalpha body"
	hash := embeddingContentHash("title_original", "test", text)
	if err := st.UpsertThreadVector(ctx, ThreadVector{ThreadID: threadIDs[0], Basis: "title_original", Model: "test", Dimensions: 2, ContentHash: hash, Vector: []float64{1, 0}, CreatedAt: "2026-04-30T00:00:00Z", UpdatedAt: "2026-04-30T00:00:00Z"}); err != nil {
		t.Fatalf("upsert existing vector: %v", err)
	}
	tasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{RepoID: repoID, Basis: "title_original", Model: "test", Number: 301})
	if err != nil {
		t.Fatalf("list skipped tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("matching existing hash should skip task: %+v", tasks)
	}
	tasks, err = st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{RepoID: repoID, Basis: "title_original", Model: "test", Number: 301, Force: true, Limit: 1})
	if err != nil {
		t.Fatalf("list forced tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Number != 301 {
		t.Fatalf("forced tasks = %+v", tasks)
	}
	if _, err := embeddingTextForBasis("title_original", "title", "", "raw fallback", "", ""); err != nil {
		t.Fatalf("raw text fallback: %v", err)
	}
	if text, err := embeddingTextForBasis("llm_key_summary", "title", "", "", "", ""); err != nil || text != "" {
		t.Fatalf("empty key summary text=%q err=%v", text, err)
	}
}

func TestPortablePruneCanonicalizesSchemaAndMetadata(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	if _, err := st.UpsertDocument(ctx, Document{ThreadID: threadIDs[0], Title: "doc", RawText: "doc text", DedupeText: "doc text", UpdatedAt: "2026-04-30T00:00:00Z"}); err != nil {
		t.Fatalf("upsert document: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, ThreadVector{ThreadID: threadIDs[0], Basis: "title_original", Model: "test", Dimensions: 2, ContentHash: "hash", Vector: []float64{1, 0}, CreatedAt: "2026-04-30T00:00:00Z", UpdatedAt: "2026-04-30T00:00:00Z"}); err != nil {
		t.Fatalf("upsert vector: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `insert into sync_runs(repo_id, scope, status, started_at, finished_at, stats_json) values(?, 'open', 'success', '2026-04-30T00:00:00Z', '2026-04-30T00:01:00Z', '{}')`, repoID); err != nil {
		t.Fatalf("seed sync run: %v", err)
	}
	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 5, Vacuum: false})
	if err != nil {
		t.Fatalf("prune portable: %v", err)
	}
	if stats.BodyChars != 5 || stats.DocumentsDeleted == 0 || len(stats.DroppedTables) == 0 || len(stats.DroppedColumns) == 0 {
		t.Fatalf("portable stats = %+v", stats)
	}
	if !st.tableExists(ctx, "portable_metadata") || st.hasColumn(ctx, "threads", "body") {
		t.Fatalf("portable schema was not canonicalized")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	ro, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open readonly portable: %v", err)
	}
	defer ro.Close()
	status, err := ro.Status(ctx)
	if err != nil {
		t.Fatalf("portable status: %v", err)
	}
	if status.LastSyncAt.IsZero() {
		t.Fatalf("portable metadata should provide last sync time: %+v", status)
	}
}

func TestClusterHelperBranches(t *testing.T) {
	summaries := []ClusterSummary{
		{ID: 1, MemberCount: 1, UpdatedAt: "2026-04-30T01:00:00Z"},
		{ID: 2, MemberCount: 3, UpdatedAt: "2026-04-30T00:00:00Z"},
	}
	sortClusterSummaries(summaries, "size")
	if summaries[0].ID != 2 {
		t.Fatalf("size sort = %+v", summaries)
	}
	sortClusterSummaries(summaries, "recent")
	if summaries[0].ID != 1 {
		t.Fatalf("recent sort = %+v", summaries)
	}
	if ids := parseIDSet(`1, 2, 0, bad, 3`); len(ids) != 3 || !ids[2] {
		t.Fatalf("parse id set = %+v", ids)
	}
	if got := idSetOverlapRatio(map[int64]bool{1: true, 2: true}, map[int64]bool{2: true, 3: true}); got != 0.5 {
		t.Fatalf("overlap = %v", got)
	}
	if got := snippetRunes("abcdef", 3); got != "abc" {
		t.Fatalf("snippet = %q", got)
	}
	if func() (panicked bool) {
		defer func() { panicked = recover() != nil }()
		_ = sqliteIdentifier(`bad"name`)
		return false
	}() != true {
		t.Fatal("unsafe sqlite identifier should panic")
	}
}

func TestThreadGitHubCloseAndRunBranches(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if err := (*Store)(nil).Close(); err != nil {
		t.Fatalf("nil close: %v", err)
	}
	repoID, _ := seedVectorThreads(t, ctx, st)
	if _, err := st.MarkOpenThreadClosedFromGitHub(ctx, Thread{}); err == nil {
		t.Fatal("missing repo id should fail")
	}
	if _, err := st.MarkOpenThreadClosedFromGitHub(ctx, Thread{RepoID: repoID}); err == nil {
		t.Fatal("missing number should fail")
	}
	if _, err := st.MarkOpenThreadClosedFromGitHub(ctx, Thread{RepoID: repoID, Number: 301}); err == nil {
		t.Fatal("missing kind should fail")
	}
	closed, err := st.MarkOpenThreadClosedFromGitHub(ctx, Thread{
		RepoID:          repoID,
		GitHubID:        "301",
		Number:          301,
		Kind:            "issue",
		Title:           "closed upstream",
		HTMLURL:         "https://github.com/openclaw/openclaw/issues/301",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "closed-hash",
		ClosedAtGitHub:  "2026-04-30T02:00:00Z",
		LastPulledAt:    "2026-04-30T02:00:00Z",
		UpdatedAt:       "2026-04-30T02:00:00Z",
		UpdatedAtGitHub: "2026-04-30T02:00:00Z",
	})
	if err != nil {
		t.Fatalf("mark closed: %v", err)
	}
	if !closed {
		t.Fatal("expected open thread to be marked closed")
	}
	closed, err = st.MarkOpenThreadClosedFromGitHub(ctx, Thread{
		RepoID:        repoID,
		GitHubID:      "301",
		Number:        301,
		Kind:          "issue",
		State:         "closed",
		Title:         "already closed",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/301",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "closed-hash",
		UpdatedAt:     "2026-04-30T02:01:00Z",
	})
	if err != nil {
		t.Fatalf("mark already closed: %v", err)
	}
	if closed {
		t.Fatal("already closed thread should not be updated")
	}

	for _, kind := range []string{"sync", "summary", "embedding", "cluster"} {
		id, err := st.RecordRun(ctx, RunRecord{
			RepoID:     repoID,
			Kind:       kind,
			Scope:      "test",
			Status:     "success",
			StartedAt:  "2026-04-30T03:00:00Z",
			FinishedAt: "2026-04-30T03:00:01Z",
			StatsJSON:  "{}",
		})
		if err != nil {
			t.Fatalf("record %s run: %v", kind, err)
		}
		runs, err := st.ListRuns(ctx, repoID, kind, 0)
		if err != nil {
			t.Fatalf("list %s runs: %v", kind, err)
		}
		if len(runs) == 0 || runs[0].ID != id || runs[0].Kind != kind {
			t.Fatalf("%s runs = %+v", kind, runs)
		}
	}
	if _, err := st.RecordRun(ctx, RunRecord{RepoID: repoID, Kind: "bad"}); err == nil {
		t.Fatal("bad run kind should fail")
	}
	if _, err := st.ListRuns(ctx, repoID, "bad", 1); err == nil {
		t.Fatal("bad list run kind should fail")
	}
	last, err := st.LastSuccessfulSyncAt(ctx, repoID)
	if err != nil {
		t.Fatalf("last sync: %v", err)
	}
	if last.IsZero() {
		t.Fatal("expected last successful sync")
	}
	if _, err := st.DB().ExecContext(ctx, `insert into sync_runs(repo_id, scope, status, started_at, finished_at) values(?, 'bad', 'success', '2026-04-30T04:00:00Z', 'not-a-time')`, repoID); err != nil {
		t.Fatalf("seed bad sync: %v", err)
	}
	if _, err := st.LastSuccessfulSyncAt(ctx, repoID); err == nil {
		t.Fatal("bad sync timestamp should fail")
	}
	if err := st.CloseThreadLocally(ctx, 0, 301, ""); err == nil {
		t.Fatal("close thread bad repo should fail")
	}
	if err := st.CloseThreadLocally(ctx, repoID, 0, ""); err == nil {
		t.Fatal("close thread bad number should fail")
	}
	if err := st.CloseThreadLocally(ctx, repoID, 999, ""); err == nil {
		t.Fatal("close missing thread should fail")
	}
	if err := st.ReopenThreadLocally(ctx, 0, 301); err == nil {
		t.Fatal("reopen thread bad repo should fail")
	}
	if err := st.ReopenThreadLocally(ctx, repoID, 0); err == nil {
		t.Fatal("reopen thread bad number should fail")
	}
	if err := st.ReopenThreadLocally(ctx, repoID, 999); err == nil {
		t.Fatal("reopen missing thread should fail")
	}
}

func TestDurableClusterLifecycleAndClosedSummaries(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	thirdID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "303", Number: 303, Kind: "issue", State: "closed",
		Title: "Third closed vector thread", Body: "gamma body", HTMLURL: "https://github.com/openclaw/openclaw/issues/303",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h303", UpdatedAt: "2026-04-30T00:03:00Z", UpdatedAtGitHub: "2026-04-30T00:03:00Z",
	})
	if err != nil {
		t.Fatalf("third thread: %v", err)
	}
	if _, err := st.SaveDurableClusters(ctx, 0, nil); err == nil {
		t.Fatal("missing repo id should fail")
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{{Members: []DurableClusterMemberInput{{ThreadID: threadIDs[0]}}}}); err == nil {
		t.Fatal("missing stable key should fail")
	}
	if _, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{{StableKey: "bad", Members: []DurableClusterMemberInput{{ThreadID: 0}}}}); err == nil {
		t.Fatal("invalid member id should fail")
	}
	score := 0.91
	result, err := st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{{
		StableKey:              "cluster-key",
		StableSlug:             "cluster-slug",
		Title:                  "Cluster title",
		RepresentativeThreadID: threadIDs[0],
		Members: []DurableClusterMemberInput{
			{ThreadID: threadIDs[0], Role: "canonical", ScoreToRepresentative: &score},
			{ThreadID: threadIDs[1], Role: "member"},
			{ThreadID: thirdID, Role: ""},
		},
	}})
	if err != nil {
		t.Fatalf("save durable clusters: %v", err)
	}
	if result.ClusterCount != 1 || result.MemberCount != 3 || result.RunID == 0 {
		t.Fatalf("save result = %+v", result)
	}
	clusterID, err := st.ClusterIDForThreadNumber(ctx, repoID, 301, false)
	if err != nil {
		t.Fatalf("cluster id: %v", err)
	}
	if clusterID == 0 {
		t.Fatal("expected durable cluster id")
	}
	if _, err := st.ClusterIDForThreadNumber(ctx, repoID, 303, false); err == nil {
		t.Fatal("closed member should be hidden from active lookup")
	}
	if _, err := st.ExcludeClusterMemberLocally(ctx, 0, clusterID, 302, ""); err == nil {
		t.Fatal("exclude with bad repo should fail")
	}
	excluded, err := st.ExcludeClusterMemberLocally(ctx, repoID, clusterID, 302, "")
	if err != nil {
		t.Fatalf("exclude member: %v", err)
	}
	if excluded.Action != "exclude" || excluded.Reason != "local exclude" {
		t.Fatalf("exclude override = %+v", excluded)
	}
	if _, err := st.SetClusterCanonicalLocally(ctx, repoID, clusterID, 302, ""); err == nil {
		t.Fatal("excluded member should not be canonical while inactive")
	}
	included, err := st.IncludeClusterMemberLocally(ctx, repoID, clusterID, 302, "")
	if err != nil {
		t.Fatalf("include member: %v", err)
	}
	if included.Action != "include" || included.Reason != "local include" {
		t.Fatalf("include override = %+v", included)
	}
	canonical, err := st.SetClusterCanonicalLocally(ctx, repoID, clusterID, 302, "")
	if err != nil {
		t.Fatalf("set canonical: %v", err)
	}
	if canonical.Action != "canonical" || canonical.Reason != "local canonical" {
		t.Fatalf("canonical override = %+v", canonical)
	}
	if err := st.CloseClusterLocally(ctx, repoID, clusterID, "done"); err != nil {
		t.Fatalf("close cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_runs(id, repo_id, scope, status, started_at, finished_at, stats_json)
		values(901, ?, 'raw', 'completed', '2026-04-30T05:00:00Z', '2026-04-30T05:00:01Z', '{}');
		insert into clusters(id, repo_id, cluster_run_id, representative_thread_id, member_count, created_at)
		values(902, ?, 901, ?, 1, '2026-04-30T05:00:01Z');
		insert into cluster_members(cluster_id, thread_id, score_to_representative, created_at)
		values(902, ?, 1.0, '2026-04-30T05:00:01Z');
	`, repoID, repoID, threadIDs[0], threadIDs[0]); err != nil {
		t.Fatalf("seed raw display cluster: %v", err)
	}
	closed, err := st.ListDisplayClusterSummaries(ctx, ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 10, Sort: "size"})
	if err != nil {
		t.Fatalf("closed display summaries: %v", err)
	}
	foundClosed := false
	for _, summary := range closed {
		if summary.ID == clusterID && summary.Status == "closed" && summary.ClosedAt != "" {
			foundClosed = true
		}
	}
	if !foundClosed {
		t.Fatalf("closed summaries = %+v", closed)
	}
	if err := st.ReopenClusterLocally(ctx, repoID, clusterID); err != nil {
		t.Fatalf("reopen cluster: %v", err)
	}
	reopened, err := st.DurableClusterDetail(ctx, ClusterDetailOptions{RepoID: repoID, ClusterID: clusterID, IncludeClosed: true, MemberLimit: 10})
	if err != nil {
		t.Fatalf("reopened detail: %v", err)
	}
	if reopened.Cluster.Status != "active" || reopened.Cluster.ClosedAt != "" {
		t.Fatalf("reopened cluster = %+v", reopened.Cluster)
	}
	result, err = st.SaveDurableClusters(ctx, repoID, []DurableClusterInput{{
		StableKey:              "cluster-key",
		RepresentativeThreadID: threadIDs[0],
		Members:                []DurableClusterMemberInput{{ThreadID: threadIDs[0], Role: "canonical"}, {ThreadID: threadIDs[1], Role: "member"}},
	}})
	if err != nil {
		t.Fatalf("resave durable clusters: %v", err)
	}
	if result.MemberCount != 2 {
		t.Fatalf("resave result = %+v", result)
	}
}

func TestSearchAndStoreErrorBranches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if _, err := OpenReadOnly(ctx, filepath.Join(dir, "missing.db")); err == nil {
		t.Fatal("missing readonly db should fail")
	}
	dbPath := filepath.Join(dir, "newer.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open newer db: %v", err)
	}
	if _, err := db.ExecContext(ctx, `pragma user_version = 99`); err != nil {
		t.Fatalf("set newer schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close newer db: %v", err)
	}
	if _, err := OpenReadOnly(ctx, dbPath); err == nil {
		t.Fatal("newer readonly db should fail")
	}

	st, err := Open(ctx, filepath.Join(dir, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, _ := seedVectorThreads(t, ctx, st)
	if hits, err := st.SearchDocuments(ctx, repoID, "", 0); err != nil || len(hits) != 0 {
		t.Fatalf("empty document search hits=%+v err=%v", hits, err)
	}
	hits, err := st.SearchDocuments(ctx, repoID, "alpha", 1)
	if err != nil {
		t.Fatalf("document search: %v", err)
	}
	if len(hits) != 1 || hits[0].Number != 301 {
		t.Fatalf("document search hits = %+v", hits)
	}
	threads, err := st.SearchThreads(ctx, ThreadSearchOptions{RepoID: repoID, Query: "beta", Kind: "pull_request", State: "open", Limit: 1})
	if err != nil {
		t.Fatalf("thread search: %v", err)
	}
	if len(threads) != 1 || threads[0].Number != 302 {
		t.Fatalf("thread search = %+v", threads)
	}
	if got := threadSearchOrder(true); !strings.Contains(got, "bm25") {
		t.Fatalf("fts order = %q", got)
	}
	if got := escapeLike(`a%b_c\`); got != `a\%b\_c\\` {
		t.Fatalf("escape = %q", got)
	}
}

func TestOpenBackfillsLegacyPortableColumns(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		create table repositories (
			id integer primary key,
			owner text not null,
			name text not null,
			full_name text not null,
			github_repo_id text,
			updated_at text not null
		);
		create table threads (
			id integer primary key,
			repo_id integer not null,
			github_id text not null,
			number integer not null,
			kind text not null,
			state text not null,
			title text not null,
			body_excerpt text,
			author_login text,
			author_type text,
			html_url text not null,
			labels_json text not null,
			assignees_json text not null,
			content_hash text not null,
			is_draft integer not null default 0,
			created_at_gh text,
			updated_at_gh text,
			closed_at_gh text,
			merged_at_gh text,
			first_pulled_at text,
			last_pulled_at text,
			updated_at text not null,
			closed_at_local text,
			close_reason_local text
		);
		insert into repositories(id, owner, name, full_name, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '2026-04-30T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body_excerpt, html_url, labels_json, assignees_json, content_hash, updated_at)
		values(1, 1, '1', 1, 'issue', 'open', 'legacy', 'legacy excerpt', 'https://github.com/openclaw/openclaw/issues/1', '[]', '[]', 'hash', '2026-04-30T00:00:00Z');
	`); err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open migrated legacy db: %v", err)
	}
	defer st.Close()
	if !st.hasColumn(ctx, "repositories", "raw_json") || !st.hasColumn(ctx, "threads", "body") || !st.hasColumn(ctx, "threads", "raw_json") {
		t.Fatal("legacy columns were not added")
	}
	threads, err := st.ListThreads(ctx, 1, true)
	if err != nil {
		t.Fatalf("list migrated threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Body != "legacy excerpt" {
		t.Fatalf("migrated thread = %+v", threads)
	}
}

func TestClusterErrorBranchesAndSummaryScanning(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	if err := st.CloseClusterLocally(ctx, 0, 1, ""); err == nil {
		t.Fatal("close cluster bad repo should fail")
	}
	if err := st.CloseClusterLocally(ctx, repoID, 0, ""); err == nil {
		t.Fatal("close cluster bad id should fail")
	}
	if err := st.CloseClusterLocally(ctx, repoID, 999, ""); err == nil {
		t.Fatal("close missing cluster should fail")
	}
	if err := st.ReopenClusterLocally(ctx, 0, 1); err == nil {
		t.Fatal("reopen cluster bad repo should fail")
	}
	if err := st.ReopenClusterLocally(ctx, repoID, 0); err == nil {
		t.Fatal("reopen cluster bad id should fail")
	}
	if err := st.ReopenClusterLocally(ctx, repoID, 999); err == nil {
		t.Fatal("reopen missing cluster should fail")
	}
	if _, err := st.IncludeClusterMemberLocally(ctx, repoID, 999, 301, ""); err == nil {
		t.Fatal("include missing cluster member should fail")
	}
	if _, err := st.IncludeClusterMemberLocally(ctx, repoID, 1, 0, ""); err == nil {
		t.Fatal("include bad number should fail")
	}
	if _, err := st.SetClusterCanonicalLocally(ctx, repoID, 999, 301, ""); err == nil {
		t.Fatal("canonical missing cluster member should fail")
	}

	if _, err := st.DB().ExecContext(ctx, `
		insert into document_summaries(thread_id, summary_kind, provider, model, prompt_version, content_hash, summary_text, created_at, updated_at)
		values(?, 'problem_summary', 'test', 'm1', 'v1', 'h1', 'document problem', '2026-04-30T00:00:00Z', '2026-04-30T00:01:00Z'),
		      (?, 'problem_summary', 'test', 'm2', 'v1', 'h2', 'older duplicate ignored', '2026-04-30T00:00:00Z', '2026-04-30T00:00:30Z')
	`, threadIDs[0], threadIDs[0]); err != nil {
		t.Fatalf("seed summaries: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at)
		values(?, '2026-04-30T00:00:00Z', 'content', 'title', 'body', 'labels', '2026-04-30T00:00:00Z')
	`, threadIDs[1]); err != nil {
		t.Fatalf("seed revision: %v", err)
	}
	var revisionID int64
	if err := st.DB().QueryRowContext(ctx, `select id from thread_revisions where thread_id = ?`, threadIDs[1]).Scan(&revisionID); err != nil {
		t.Fatalf("revision id: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(?, 'key_summary', 'v1', 'test', 'm1', 'input', 'output', 'thread key text', '2026-04-30T00:02:00Z')
	`, revisionID); err != nil {
		t.Fatalf("seed key summary: %v", err)
	}
	summaries, err := st.summariesByThreadIDs(ctx, []int64{threadIDs[0], threadIDs[1]})
	if err != nil {
		t.Fatalf("summaries by thread ids: %v", err)
	}
	if summaries[threadIDs[0]]["problem_summary"] != "document problem" || summaries[threadIDs[1]]["key_summary"] != "thread key text" {
		t.Fatalf("summaries = %+v", summaries)
	}
	empty, err := st.summariesByThreadIDs(ctx, nil)
	if err != nil {
		t.Fatalf("empty summaries: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty summaries = %+v", empty)
	}
}

func TestPortableVacuumAndVectorQueryBranches(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	if err := st.UpsertThreadVector(ctx, ThreadVector{ThreadID: threadIDs[0], Basis: "title_original", Model: "test", Dimensions: 2, ContentHash: "hash", Vector: []float64{0.5, 0.5}, CreatedAt: "2026-04-30T00:00:00Z", UpdatedAt: "2026-04-30T00:00:00Z"}); err != nil {
		t.Fatalf("upsert vector: %v", err)
	}
	if _, _, err := st.ThreadVectorByNumber(ctx, ThreadVectorQuery{RepoID: repoID, Model: "missing"}, 301); err == nil {
		t.Fatal("missing vector query should fail")
	}
	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 3, Vacuum: true})
	if err != nil {
		t.Fatalf("portable prune with vacuum: %v", err)
	}
	if stats.BodyChars != 3 {
		t.Fatalf("prune stats = %+v", stats)
	}
}

func seedVectorThreads(t *testing.T, ctx context.Context, st *Store) (int64, []int64) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads := []Thread{
		{RepoID: repoID, GitHubID: "301", Number: 301, Kind: "issue", State: "open", Title: "First vector thread", Body: "alpha body", HTMLURL: "https://github.com/openclaw/openclaw/issues/301", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h301", UpdatedAt: now},
		{RepoID: repoID, GitHubID: "302", Number: 302, Kind: "pull_request", State: "open", Title: "Second vector thread", Body: "beta body", HTMLURL: "https://github.com/openclaw/openclaw/pull/302", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h302", UpdatedAt: now},
	}
	ids := make([]int64, 0, len(threads))
	for _, thread := range threads {
		id, err := st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("thread %d: %v", thread.Number, err)
		}
		ids = append(ids, id)
		if _, err := st.UpsertDocument(ctx, Document{ThreadID: id, Title: thread.Title, RawText: thread.Title + "\n" + thread.Body, DedupeText: strings.ToLower(thread.Title + " " + thread.Body), UpdatedAt: now}); err != nil {
			t.Fatalf("document %d: %v", thread.Number, err)
		}
	}
	return repoID, ids
}
