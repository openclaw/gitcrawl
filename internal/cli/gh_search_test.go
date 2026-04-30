package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestParseGHSearchDuration(t *testing.T) {
	tests := []struct {
		value string
		want  time.Duration
	}{
		{value: "", want: 0},
		{value: "60", want: time.Minute},
		{value: "2m", want: 2 * time.Minute},
		{value: "1h30m", want: 90 * time.Minute},
	}
	for _, tt := range tests {
		got, err := parseGHSearchDuration(tt.value)
		if err != nil {
			t.Fatalf("parseGHSearchDuration(%q): %v", tt.value, err)
		}
		if got != tt.want {
			t.Fatalf("parseGHSearchDuration(%q) = %s, want %s", tt.value, got, tt.want)
		}
	}
	if _, err := parseGHSearchDuration("-1s"); err == nil {
		t.Fatal("expected negative duration to fail")
	}
	if _, err := parseGHSearchDuration("nope"); err == nil {
		t.Fatal("expected invalid duration to fail")
	}
}

func TestGHSearchCacheStaleUsesRepoSyncRuns(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
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
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	finishedAt := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339Nano)
	if _, err := st.RecordRun(ctx, store.RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "open",
		Status:     "success",
		StartedAt:  finishedAt,
		FinishedAt: finishedAt,
	}); err != nil {
		t.Fatalf("record sync: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	run.configPath = configPath
	stale, lastSync, err := run.ghSearchCacheStale(ctx, "openclaw", "openclaw", 2*time.Hour)
	if err != nil {
		t.Fatalf("freshness check: %v", err)
	}
	if stale || lastSync.IsZero() {
		t.Fatalf("expected cache to be fresh, stale=%v lastSync=%s", stale, lastSync)
	}
	stale, _, err = run.ghSearchCacheStale(ctx, "openclaw", "openclaw", 30*time.Minute)
	if err != nil {
		t.Fatalf("stale freshness check: %v", err)
	}
	if !stale {
		t.Fatal("expected cache to be stale")
	}
}

func TestGHSearchCacheStaleWhenRepoMissing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}

	run := New()
	run.configPath = configPath
	stale, lastSync, err := run.ghSearchCacheStale(ctx, "openclaw", "missing", time.Minute)
	if err != nil {
		t.Fatalf("freshness check: %v", err)
	}
	if !stale || !lastSync.IsZero() {
		t.Fatalf("expected missing repo to be stale, stale=%v lastSync=%s", stale, lastSync)
	}
}

func TestGHSearchSyncIfStaleHydratesCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/openclaw/openclaw":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 101, "full_name": "openclaw/openclaw"})
		case "/repos/openclaw/openclaw/issues":
			if got := r.URL.Query().Get("state"); got != "open" {
				t.Fatalf("state query = %q", got)
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"id":         501,
				"number":     501,
				"state":      "open",
				"title":      "sync stale cache",
				"body":       "hydrate before search",
				"html_url":   "https://github.com/openclaw/openclaw/issues/501",
				"created_at": "2026-04-30T00:00:00Z",
				"updated_at": "2026-04-30T00:00:00Z",
				"labels":     []string{"bug", "cache"},
				"assignees":  []map[string]any{},
				"user":       map[string]any{"login": "alice", "type": "User"},
			}})
		default:
			t.Fatalf("unexpected path: %s", r.URL.String())
		}
	}))
	defer server.Close()
	t.Setenv("GITHUB_TOKEN", "test-token")
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "search", "issues", "stale", "-R", "openclaw/openclaw", "--sync-if-stale", "1s", "--json", "number,title,labels"}); err != nil {
		t.Fatalf("search with stale sync: %v", err)
	}
	if !strings.Contains(stderr.String(), "syncing before search") {
		t.Fatalf("stderr missing stale sync note: %q", stderr.String())
	}
	if !strings.Contains(stdout.String(), `"number": 501`) || !strings.Contains(stdout.String(), `"cache"`) {
		t.Fatalf("search output = %q", stdout.String())
	}
}

func TestGHSearchJSONHelpersCoverFieldsAndFallbackLabels(t *testing.T) {
	thread := store.Thread{
		Number:          10,
		Title:           "title",
		State:           "closed",
		HTMLURL:         "https://github.test/10",
		UpdatedAtGitHub: "",
		UpdatedAt:       "2026-04-30T00:00:00Z",
		CreatedAtGitHub: "2026-04-29T00:00:00Z",
		ClosedAtGitHub:  "2026-04-30T01:00:00Z",
		MergedAtGitHub:  "2026-04-30T02:00:00Z",
		LabelsJSON:      `["bug",""," triage "]`,
		IsDraft:         true,
		AuthorLogin:     "alice",
		AuthorType:      "User",
		Body:            "body text",
	}
	rows, err := ghSearchJSONRows([]store.Thread{thread}, "number,title,state,url,updatedAt,createdAt,closedAt,mergedAt,labels,isDraft,author,body")
	if err != nil {
		t.Fatalf("json rows: %v", err)
	}
	if rows[0]["updatedAt"] != thread.UpdatedAt || rows[0]["body"] != "body text" {
		t.Fatalf("rows = %#v", rows)
	}
	labels := rows[0]["labels"].([]ghLabel)
	if len(labels) != 2 || labels[1].Name != "triage" {
		t.Fatalf("labels = %#v", labels)
	}
	if labels := ghLabelsFromJSON(`not-json`); labels != nil {
		t.Fatalf("bad labels = %#v", labels)
	}
	if _, err := ghSearchJSONRows([]store.Thread{thread}, "number,missing"); err == nil {
		t.Fatal("unsupported json field should fail")
	}
	if _, err := parseGHSearchLimit("5", "6"); err == nil {
		t.Fatal("disagreeing limits should fail")
	}
	if err := validateGHSearchState("bogus"); err == nil {
		t.Fatal("bad state should fail")
	}
	query, repo, state := parseGHSearchQuery("repo:openclaw/openclaw is:pr is:closed hello world")
	if query != "hello world" || repo != "openclaw/openclaw" || state != "closed" {
		t.Fatalf("query=%q repo=%q state=%q", query, repo, state)
	}
}
