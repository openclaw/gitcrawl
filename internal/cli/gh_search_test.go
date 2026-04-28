package cli

import (
	"context"
	"path/filepath"
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
