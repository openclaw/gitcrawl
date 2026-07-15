package cli

import (
	"bytes"
	"context"
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
	configPath := seedGHShimRepo(t, ctx)
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
	result, err := st.SaveDurableClusters(ctx, repo.ID, []store.DurableClusterInput{{
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

func TestCLIAppHumanAndLogOutputE2E(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	textCommands := [][]string{
		{"--config", configPath, "version"},
		{"--config", configPath, "metadata"},
		{"--config", configPath, "status"},
		{"--config", configPath, "doctor"},
		{"--config", configPath, "help", "portable"},
		{"--config", configPath, "help", "tui"},
	}
	for _, args := range textCommands {
		app := New()
		var stdout bytes.Buffer
		app.Stdout = &stdout
		if err := app.Run(ctx, args); err != nil {
			t.Fatalf("%v failed: %v", args, err)
		}
		if strings.TrimSpace(stdout.String()) == "" {
			t.Fatalf("%v produced no text output", args)
		}
	}

	logCommands := [][]string{
		{"--config", configPath, "--format", "log", "configure", "--summary-model", "gpt-log"},
		{"--config", configPath, "--format", "log", "doctor"},
	}
	for _, args := range logCommands {
		app := New()
		var stdout bytes.Buffer
		app.Stdout = &stdout
		if err := app.Run(ctx, args); err != nil {
			t.Fatalf("%v failed: %v", args, err)
		}
		if !strings.Contains(stdout.String(), "=") {
			t.Fatalf("%v log output = %q", args, stdout.String())
		}
	}

	jsonVersion := New()
	var jsonOut bytes.Buffer
	jsonVersion.Stdout = &jsonOut
	if err := jsonVersion.Run(ctx, []string{"--config", configPath, "--format", "json", "version"}); err != nil {
		t.Fatalf("json version: %v", err)
	}
	if !strings.Contains(jsonOut.String(), `"version"`) {
		t.Fatalf("json version output = %q", jsonOut.String())
	}
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

func seedGHShimRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.CacheDir = filepath.Join(dir, "cache")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
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

func TestCLIAppUsageBranches(t *testing.T) {
	ctx := context.Background()
	configPath := filepath.Join(t.TempDir(), "config.toml")
	cases := [][]string{
		{"--format", "yaml", "status"},
		{"serve"},
		{"unknown"},
		{"configure", "--bad"},
		{"metadata", "extra"},
		{"status", "extra"},
		{"portable"},
		{"portable", "unknown"},
		{"portable", "prune", "extra"},
		{"portable", "prune", "--body-chars", "bad"},
		{"threads"},
		{"threads", "bad-repo"},
		{"threads", "openclaw/openclaw", "--numbers", "bad"},
		{"threads", "openclaw/openclaw", "--limit", "bad"},
		{"runs"},
		{"runs", "openclaw/openclaw", "--limit", "bad"},
		{"cluster-detail", "openclaw/openclaw", "--id", "bad"},
		{"close-thread", "openclaw/openclaw"},
		{"reopen-thread", "openclaw/openclaw", "--number", "bad"},
		{"close-cluster", "openclaw/openclaw"},
		{"reopen-cluster", "openclaw/openclaw", "--id", "bad"},
		{"exclude-cluster-member", "openclaw/openclaw", "--id", "1"},
		{"include-cluster-member", "openclaw/openclaw", "--id", "bad", "--number", "1"},
		{"set-cluster-canonical", "openclaw/openclaw", "--id", "1", "--number", "bad"},
		{"sync", "openclaw/openclaw", "--with", "bad"},
		{"refresh"},
		{"refresh", "openclaw/openclaw", "--no-sync", "--no-embed", "--no-cluster"},
		{"refresh", "bad-repo"},
		{"refresh", "openclaw/openclaw", "--limit", "bad"},
		{"refresh", "openclaw/openclaw", "--threshold", "bad"},
		{"refresh", "openclaw/openclaw", "--threshold", "2"},
		{"refresh", "openclaw/openclaw", "--min-size", "bad"},
		{"refresh", "openclaw/openclaw", "--k", "bad"},
		{"search"},
		{"search", "openclaw/openclaw"},
		{"search", "bad-repo", "--query", "x"},
		{"search", "openclaw/openclaw", "--query", "x", "--limit", "bad"},
		{"search", "openclaw/openclaw", "--query", "x", "--mode", "bad"},
		{"neighbors"},
		{"neighbors", "bad-repo"},
		{"neighbors", "openclaw/openclaw"},
		{"neighbors", "openclaw/openclaw", "--number", "bad"},
		{"neighbors", "openclaw/openclaw", "--number", "1", "--limit", "bad"},
		{"neighbors", "openclaw/openclaw", "--number", "1", "--threshold", "bad"},
		{"cluster"},
		{"cluster", "bad-repo"},
		{"cluster", "openclaw/openclaw", "--threshold", "bad"},
		{"cluster", "openclaw/openclaw", "--threshold", "2"},
		{"cluster", "openclaw/openclaw", "--min-size", "bad"},
		{"cluster", "openclaw/openclaw", "--max-cluster-size", "bad"},
		{"cluster", "openclaw/openclaw", "--limit", "bad"},
		{"embed"},
		{"embed", "bad-repo"},
		{"embed", "openclaw/openclaw", "--number", "bad"},
		{"embed", "openclaw/openclaw", "--limit", "bad"},
		{"summarize"},
		{"summarize", "bad-repo"},
		{"summarize", "openclaw/openclaw", "--number", "bad"},
		{"summarize", "openclaw/openclaw", "--limit", "bad"},
		{"tui", "one", "two"},
		{"tui", "--sort", "bad"},
	}
	for _, args := range cases {
		app := New()
		app.Stdout = &bytes.Buffer{}
		app.Stderr = &bytes.Buffer{}
		full := append([]string{"--config", configPath}, args...)
		if err := app.Run(ctx, full); err == nil {
			t.Fatalf("%v succeeded, want error", args)
		}
	}
}
