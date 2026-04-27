package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestInitWritesConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout

	err := app.Run(context.Background(), []string{"--config", configPath, "--json", "init", "--db", dbPath})
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if !strings.Contains(stdout.String(), `"config_path"`) {
		t.Fatalf("expected json init output, got %q", stdout.String())
	}
}

func TestInitDefaultOutputIsHumanReadable(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout

	err := app.Run(context.Background(), []string{"--config", configPath, "init", "--db", dbPath})
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	out := stdout.String()
	if !strings.Contains(out, "gitcrawl init") {
		t.Fatalf("expected human init output, got %q", out)
	}
	if strings.Contains(out, `"config_path"`) || strings.Contains(out, "{") {
		t.Fatalf("default init output should not be json, got %q", out)
	}
}

func TestInitRejectsDBAndPortableStore(t *testing.T) {
	dir := t.TempDir()
	app := New()
	err := app.Run(context.Background(), []string{
		"--config", filepath.Join(dir, "config.toml"),
		"init",
		"--db", filepath.Join(dir, "gitcrawl.db"),
		"--portable-store", "https://github.com/openclaw/gitcrawl-store.git",
	})
	if err == nil {
		t.Fatal("expected init to reject conflicting database options")
	}
	if ExitCode(err) != 2 {
		t.Fatalf("exit code: got %d want 2", ExitCode(err))
	}
}

func TestDefaultPortableStoreDir(t *testing.T) {
	got := defaultPortableStoreDir("/tmp/gitcrawl/config.toml", "https://github.com/openclaw/gitcrawl-store.git")
	want := filepath.Join("/tmp/gitcrawl", "stores", "gitcrawl-store")
	if got != want {
		t.Fatalf("store dir: got %q want %q", got, want)
	}
}

func TestPortablePruneCommand(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(context.Background(), []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seed := New()
	if err := seed.Run(context.Background(), []string{"--config", configPath, "portable", "prune", "--body-chars", "8", "--no-vacuum", "--json"}); err != nil {
		t.Fatalf("portable prune: %v", err)
	}
}

func TestMainHelpListsNeighbors(t *testing.T) {
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout

	if err := app.Run(context.Background(), nil); err != nil {
		t.Fatalf("help: %v", err)
	}
	out := stdout.String()
	if !strings.Contains(out, "neighbors") {
		t.Fatalf("main help should list neighbors command, got %q", out)
	}
}

func TestTUIInfersRepository(t *testing.T) {
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
	if _, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before tui: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "tui", "--json"}); err != nil {
		t.Fatalf("tui: %v", err)
	}
	out := stdout.String()
	if !strings.Contains(out, `"repository": "openclaw/openclaw"`) {
		t.Fatalf("expected inferred repository, got %q", out)
	}
	if !strings.Contains(out, `"inferred_repository": true`) {
		t.Fatalf("expected inferred flag, got %q", out)
	}
	if !strings.Contains(out, `"min_size": 5`) {
		t.Fatalf("expected default tui min size, got %q", out)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after tui: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("tui mutated database bytes")
	}
}

func TestTUIRequiresInteractiveTerminalByDefault(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	var initOut bytes.Buffer
	app.Stdout = &initOut
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err = run.Run(ctx, []string{"--config", configPath, "tui"})
	if err == nil {
		t.Fatal("expected tui to require a tty")
	}
	if ExitCode(err) != 2 {
		t.Fatalf("exit code: got %d want 2", ExitCode(err))
	}
	if stdout.Len() != 0 {
		t.Fatalf("tui should not dump json by default, got %q", stdout.String())
	}
}

func TestCloseThreadCommandLocallyClosesThread(t *testing.T) {
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
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	if _, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "42",
		Number:        42,
		Kind:          "issue",
		State:         "open",
		Title:         "Close me",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/42",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed thread: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "close-thread", "openclaw/openclaw", "--number", "42", "--reason", "test close", "--json"}); err != nil {
		t.Fatalf("close-thread: %v", err)
	}
	if !strings.Contains(stdout.String(), `"closed": true`) {
		t.Fatalf("close-thread output = %q", stdout.String())
	}

	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	rows, err := st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list open threads: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("closed thread should be hidden, got %#v", rows)
	}

	reopen := New()
	stdout.Reset()
	reopen.Stdout = &stdout
	if err := reopen.Run(ctx, []string{"--config", configPath, "reopen-thread", "openclaw/openclaw", "--number", "42", "--json"}); err != nil {
		t.Fatalf("reopen-thread: %v", err)
	}
	if !strings.Contains(stdout.String(), `"reopened": true`) {
		t.Fatalf("reopen-thread output = %q", stdout.String())
	}
	rows, err = st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list reopened threads: %v", err)
	}
	if len(rows) != 1 || rows[0].ClosedAtLocal != "" {
		t.Fatalf("reopened thread should be visible, got %#v", rows)
	}
}

func TestCloseClusterCommandLocallyClosesCluster(t *testing.T) {
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
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "77",
		Number:        77,
		Kind:          "issue",
		State:         "open",
		Title:         "Cluster member",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/77",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(77, ?, 'cluster-77', 'cluster-77', 'active', ?, 'Cluster 77', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z');
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(77, ?, 'member', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z');
	`, repoID, threadID, threadID); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "close-cluster", "openclaw/openclaw", "--id", "77", "--reason", "handled", "--json"}); err != nil {
		t.Fatalf("close-cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"closed": true`) {
		t.Fatalf("close-cluster output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	active, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list active clusters: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("closed cluster should be hidden, got %#v", active)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after close check: %v", err)
	}

	reopen := New()
	stdout.Reset()
	reopen.Stdout = &stdout
	if err := reopen.Run(ctx, []string{"--config", configPath, "reopen-cluster", "openclaw/openclaw", "--id", "77", "--json"}); err != nil {
		t.Fatalf("reopen-cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"reopened": true`) {
		t.Fatalf("reopen-cluster output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store after cluster reopen: %v", err)
	}
	defer st.Close()
	active, err = st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list reopened clusters: %v", err)
	}
	if len(active) != 1 || active[0].ClosedAt != "" {
		t.Fatalf("reopened cluster should be visible, got %#v", active)
	}
}

func TestTUIHelp(t *testing.T) {
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(context.Background(), []string{"help", "tui"}); err != nil {
		t.Fatalf("help tui: %v", err)
	}
	out := stdout.String()
	if !strings.Contains(out, "gitcrawl tui [owner/repo]") {
		t.Fatalf("expected tui usage, got %q", out)
	}
	if !strings.Contains(out, "right-click for actions") {
		t.Fatalf("tui help should mention mouse actions, got %q", out)
	}
	if !strings.Contains(out, "Press a to open") {
		t.Fatalf("tui help should mention keyboard action menu, got %q", out)
	}
	if !strings.Contains(out, "Press # to jump") {
		t.Fatalf("tui help should mention number jump, got %q", out)
	}
	if !strings.Contains(out, "Press p to switch") {
		t.Fatalf("tui help should mention repository switching, got %q", out)
	}
	if !strings.Contains(out, "Press n to load neighbors") {
		t.Fatalf("tui help should mention neighbor loading, got %q", out)
	}
	if strings.Contains(strings.ToLower(out), "future tui") {
		t.Fatalf("tui help still implies future-only support: %q", out)
	}
}

func TestServeIsUnsupported(t *testing.T) {
	app := New()
	err := app.Run(context.Background(), []string{"serve"})
	if err == nil {
		t.Fatal("expected serve to fail")
	}
	if ExitCode(err) != 2 {
		t.Fatalf("exit code: got %d want 2", ExitCode(err))
	}
}
