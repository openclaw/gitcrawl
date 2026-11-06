package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

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

func TestCLIAppUnsupportedBasisFallback(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	_, firstID, secondID := seedCommandFlowStore(t, dbPath)
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
	neighbors := New()
	var stdout bytes.Buffer
	neighbors.Stdout = &stdout
	if err := neighbors.Run(ctx, []string{"--config", configPath, "--json", "neighbors", "openclaw/openclaw", "--number", "101", "--limit", "1", "--threshold", "0.99"}); err != nil {
		t.Fatalf("fallback neighbors: %v", err)
	}
	var payload struct {
		Thread    struct{ Number int }
		Neighbors []struct {
			Number int
			Score  float64
		}
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode fallback neighbors: %v", err)
	}
	if payload.Thread.Number != 101 || len(payload.Neighbors) != 1 || payload.Neighbors[0].Number != 102 ||
		payload.Neighbors[0].Score < 0.99 || payload.Neighbors[0].Score > 1 {
		t.Fatalf("fallback neighbors = %+v, want source 101 and neighbor 102 above threshold", payload)
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
