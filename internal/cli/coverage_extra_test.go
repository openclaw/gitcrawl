package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestCLIAppCommandCoveragePaths(t *testing.T) {
	ctx := context.Background()
	configPath := seedCLICommandArchive(t, ctx)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreadsFiltered(ctx, store.ThreadListOptions{RepoID: repo.ID, IncludeClosed: true, Numbers: []int{10, 12}})
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 2 {
		t.Fatalf("seed threads = %+v", threads)
	}
	result, err := st.SaveCompleteDurableClusters(ctx, repo.ID, []store.DurableClusterInput{{
		StableKey:              "cli:10,12",
		StableSlug:             "cli-10-12",
		RepresentativeThreadID: threads[0].ID,
		Title:                  "CLI command cluster",
		Members: []store.DurableClusterMemberInput{
			{ThreadID: threads[0].ID, Role: "canonical"},
			{ThreadID: threads[1].ID, Role: "member"},
		},
	}})
	if err != nil {
		t.Fatalf("save cluster: %v", err)
	}
	if _, err := st.RecordRun(ctx, store.RunRecord{RepoID: repo.ID, Kind: "sync", Scope: "open", Status: "success", StartedAt: "2026-05-08T01:00:00Z", FinishedAt: "2026-05-08T01:00:01Z", StatsJSON: "{}"}); err != nil {
		t.Fatalf("record run: %v", err)
	}
	clusterID, err := st.ClusterIDForThreadNumber(ctx, repo.ID, 10, true)
	if err != nil {
		t.Fatalf("cluster id: %v", err)
	}
	if result.RunID == 0 {
		t.Fatal("cluster run id should be non-zero")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	commands := [][]string{
		{"--config", configPath, "--json", "configure", "--summary-model", "gpt-test", "--embed-model", "embed-test", "--embedding-basis", "title_original"},
		{"--config", configPath, "--json", "metadata"},
		{"--config", configPath, "--json", "status"},
		{"--config", configPath, "--json", "threads", "openclaw/openclaw", "--numbers", "https://github.com/openclaw/openclaw/issues/10,https://github.com/openclaw/openclaw/pull/12", "--include-closed", "--limit", "2"},
		{"--config", configPath, "--json", "runs", "openclaw/openclaw", "--kind", "sync", "--limit", "1"},
		{"--config", configPath, "--json", "clusters", "openclaw/openclaw", "--include-closed", "--sort", "oldest", "--min-size", "1", "--limit", "5"},
		{"--config", configPath, "--json", "durable-clusters", "openclaw/openclaw", "--include-closed", "--sort", "size", "--min-size", "1", "--limit", "5"},
		{"--config", configPath, "--json", "cluster-detail", "openclaw/openclaw", "--id", strconv.FormatInt(clusterID, 10), "--member-limit", "2", "--body-chars", "10", "--include-closed"},
		{"--config", configPath, "--json", "close-thread", "openclaw/openclaw", "--number", "https://github.com/openclaw/openclaw/issues/10", "--reason", "covered"},
		{"--config", configPath, "--json", "reopen-thread", "openclaw/openclaw", "--number", "10"},
		{"--config", configPath, "--json", "close-cluster", "openclaw/openclaw", "--id", strconv.FormatInt(clusterID, 10), "--reason", "covered"},
		{"--config", configPath, "--json", "reopen-cluster", "openclaw/openclaw", "--id", strconv.FormatInt(clusterID, 10)},
		{"--config", configPath, "--json", "exclude-cluster-member", "openclaw/openclaw", "--id", strconv.FormatInt(clusterID, 10), "--number", "12", "--reason", "covered"},
		{"--config", configPath, "--json", "include-cluster-member", "openclaw/openclaw", "--id", strconv.FormatInt(clusterID, 10), "--number", "12", "--reason", "covered"},
		{"--config", configPath, "--json", "set-cluster-canonical", "openclaw/openclaw", "--id", strconv.FormatInt(clusterID, 10), "--number", "12", "--reason", "covered"},
	}
	for _, args := range commands {
		app := New()
		var stdout, stderr bytes.Buffer
		app.Stdout = &stdout
		app.Stderr = &stderr
		if err := app.Run(ctx, args); err != nil {
			t.Fatalf("%v failed: %v\nstdout=%s\nstderr=%s", args, err, stdout.String(), stderr.String())
		}
		if stdout.Len() == 0 {
			t.Fatalf("%v produced no output", args)
		}
	}
	if clusterID <= 0 {
		t.Fatalf("cluster id = %d", clusterID)
	}
}

func TestCLIAppControlOutputFormats(t *testing.T) {
	ctx := context.Background()
	configPath := seedCLICommandArchive(t, ctx)
	for _, tc := range []struct {
		command string
		want    map[string]any
	}{
		{command: "metadata", want: map[string]any{"id": "gitcrawl"}},
		{command: "status", want: map[string]any{"app_id": "gitcrawl", "summary": "2 threads across 1 repositories"}},
		{command: "doctor", want: map[string]any{"repository_count": float64(1), "thread_count": float64(2)}},
	} {
		t.Run(tc.command, func(t *testing.T) {
			app := New()
			var stdout bytes.Buffer
			app.Stdout = &stdout
			if err := app.Run(ctx, []string{"--config", configPath, tc.command}); err != nil {
				t.Fatal(err)
			}
			prefix := tc.command + "\n"
			if !strings.HasPrefix(stdout.String(), prefix) {
				t.Fatalf("output = %q, want %q prefix", stdout.String(), prefix)
			}
			var payload map[string]any
			if err := json.Unmarshal(stdout.Bytes()[len(prefix):], &payload); err != nil {
				t.Fatalf("decode output: %v", err)
			}
			for key, want := range tc.want {
				if got := payload[key]; got != want {
					t.Errorf("%s = %v, want %v", key, got, want)
				}
			}
		})
	}

	t.Run("doctor log", func(t *testing.T) {
		app := New()
		var stdout bytes.Buffer
		app.Stdout = &stdout
		if err := app.Run(ctx, []string{"--config", configPath, "--format", "log", "doctor"}); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(stdout.String(), "doctor=") || !strings.Contains(stdout.String(), " thread_count:2 ") || !strings.Contains(stdout.String(), " repository_count:1 ") {
			t.Fatalf("doctor log output = %q, want seeded archive counts", stdout.String())
		}
	})
}

func TestCLIAppVectorFallbackCoveragePaths(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	repoID, firstID, secondID := seedCommandFlowStore(t, dbPath)
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, vector := range []store.ThreadVector{
		{ThreadID: firstID, Basis: "other_basis", Model: "other-model", Dimensions: 2, ContentHash: "v1", Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: now},
		{ThreadID: secondID, Basis: "other_basis", Model: "other-model", Dimensions: 2, ContentHash: "v2", Vector: []float64{0.95, 0.05}, CreatedAt: now, UpdatedAt: now},
	} {
		if err := st.UpsertThreadVector(ctx, vector); err != nil {
			t.Fatalf("upsert vector: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	configure := New()
	if err := configure.Run(ctx, []string{"--config", configPath, "configure", "--embed-model", "missing-model", "--embedding-basis", "missing-basis"}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	for _, args := range [][]string{
		{"--config", configPath, "--json", "neighbors", "openclaw/openclaw", "--number", "101", "--limit", "1", "--threshold", "0.99"},
		{"--config", configPath, "--json", "search", "openclaw/openclaw", "--query", "gateway", "--mode", ""},
	} {
		run := New()
		var stdout bytes.Buffer
		run.Stdout = &stdout
		if err := run.Run(ctx, args); err != nil {
			t.Fatalf("%v failed: %v\n%s", args, err, stdout.String())
		}
	}
	for _, args := range [][]string{
		{"--config", configPath, "--json", "cluster", "openclaw/openclaw", "--threshold", "0.5", "--min-size", "2", "--limit", "2"},
		{"--config", configPath, "--json", "refresh", "openclaw/openclaw", "--no-sync", "--no-embed", "--threshold", "0.5", "--min-size", "2"},
	} {
		run := New()
		var stdout bytes.Buffer
		run.Stdout = &stdout
		if err := run.Run(ctx, args); err != nil {
			t.Fatalf("%v compatible fallback failed: %v", args, err)
		}
		if !strings.Contains(stdout.String(), `"fallback": true`) {
			t.Fatalf("%v fallback output = %q", args, stdout.String())
		}
	}
	for _, args := range [][]string{
		{"--config", configPath, "--json", "cluster", "openclaw/openclaw", "--threshold", "0.5", "--min-size", "2", "--limit", "2", "--strict-vectors"},
		{"--config", configPath, "--json", "refresh", "openclaw/openclaw", "--no-sync", "--no-embed", "--threshold", "0.5", "--min-size", "2", "--strict-vectors"},
	} {
		err := New().Run(ctx, args)
		if err == nil || !strings.Contains(err.Error(), "vector coverage cannot be verified") {
			t.Fatalf("%v error = %v", args, err)
		}
	}
	if repoID == 0 {
		t.Fatal("seed repo id should be non-zero")
	}
}

func seedCLICommandArchive(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(dir, "cache"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(dir, "state"))
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-27T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	issueID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "10",
		Number:          10,
		Kind:            "issue",
		State:           "open",
		Title:           "Hot loop burns CPU",
		Body:            "the runtime has a hot loop",
		AuthorLogin:     "alice",
		AuthorType:      "User",
		HTMLURL:         "https://github.com/openclaw/openclaw/issues/10",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "issue-10",
		UpdatedAtGitHub: "2026-04-27T01:00:00Z",
		UpdatedAt:       "2026-04-27T01:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{ThreadID: issueID, Title: "Hot loop burns CPU", RawText: "runtime hot loop burns CPU", DedupeText: "runtime hot loop burns cpu", UpdatedAt: "2026-04-27T01:00:00Z"}); err != nil {
		t.Fatalf("seed issue document: %v", err)
	}
	prID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "12",
		Number:          12,
		Kind:            "pull_request",
		State:           "open",
		Title:           "Manifest cache update",
		AuthorLogin:     "bob",
		AuthorType:      "User",
		HTMLURL:         "https://github.com/openclaw/openclaw/pull/12",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         `{"head":{"sha":"abc123"}}`,
		ContentHash:     "pr-12",
		IsDraft:         true,
		UpdatedAtGitHub: "2026-04-27T02:00:00Z",
		UpdatedAt:       "2026-04-27T02:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed pr: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{ThreadID: prID, Title: "Manifest cache update", RawText: "manifest cache refresh", DedupeText: "manifest cache refresh", UpdatedAt: "2026-04-27T02:00:00Z"}); err != nil {
		t.Fatalf("seed pr document: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return configPath
}

func TestDedupeThreadVectorsByThreadPrefersNewest(t *testing.T) {
	vectors := dedupeThreadVectorsByThread([]store.ThreadVector{
		{ThreadID: 1, Basis: "z_basis", Model: "model", UpdatedAt: "2026-05-15T00:00:00.12Z", Vector: []float64{1, 0}},
		{ThreadID: 1, Basis: "a_basis", Model: "model", UpdatedAt: "2026-05-15T00:00:00.123Z", Vector: []float64{0, 1}},
		{ThreadID: 2, Basis: "z_basis", Model: "model", UpdatedAt: "2026-05-15T00:00:00Z", Vector: []float64{0.5, 0.5}},
	})
	if len(vectors) != 2 {
		t.Fatalf("vectors = %#v, want one row per thread", vectors)
	}
	if vectors[0].ThreadID != 1 || vectors[0].Basis != "a_basis" {
		t.Fatalf("did not keep newest duplicate vector: %#v", vectors)
	}
}
