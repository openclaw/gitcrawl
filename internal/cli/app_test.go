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

func TestSyncPortableStoreResetsDirtyCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	dbPath := filepath.Join(remoteDir, "data", "openclaw__openclaw.sync.db")
	if err := os.WriteFile(dbPath, []byte("remote-v1"), 0o644); err != nil {
		t.Fatalf("write remote db: %v", err)
	}
	if err := runGit(ctx, remoteDir, "add", "data/openclaw__openclaw.sync.db"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	action, err := syncPortableStore(ctx, remoteDir, checkoutDir)
	if err != nil {
		t.Fatalf("initial portable sync: %v", err)
	}
	if action != "cloned" {
		t.Fatalf("initial action = %q, want cloned", action)
	}
	if err := os.WriteFile(filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db"), []byte("local-cache-edit"), 0o644); err != nil {
		t.Fatalf("dirty checkout db: %v", err)
	}
	if err := os.WriteFile(dbPath, []byte("remote-v2"), 0o644); err != nil {
		t.Fatalf("write updated remote db: %v", err)
	}
	if err := runGit(ctx, remoteDir, "add", "data/openclaw__openclaw.sync.db"); err != nil {
		t.Fatalf("git add update: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "update store"); err != nil {
		t.Fatalf("git commit update: %v", err)
	}

	action, err = syncPortableStore(ctx, remoteDir, checkoutDir)
	if err != nil {
		t.Fatalf("dirty portable sync: %v", err)
	}
	if action != "reset-pulled" {
		t.Fatalf("dirty action = %q, want reset-pulled", action)
	}
	got, err := os.ReadFile(filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db"))
	if err != nil {
		t.Fatalf("read checkout db: %v", err)
	}
	if string(got) != "remote-v2" {
		t.Fatalf("checkout db = %q, want remote-v2", string(got))
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

func TestClusterMemberOverrideCommands(t *testing.T) {
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
	firstID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "81",
		Number:        81,
		Kind:          "issue",
		State:         "open",
		Title:         "First member",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/81",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-81",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "82",
		Number:        82,
		Kind:          "issue",
		State:         "open",
		Title:         "Second member",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/82",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-82",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed second thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(81, ?, 'cluster-81', 'cluster-81', 'active', ?, 'Cluster 81', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, repoID, firstID); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(81, ?, 'representative', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, firstID); err != nil {
		t.Fatalf("seed first member: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(81, ?, 'member', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, secondID); err != nil {
		t.Fatalf("seed second member: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "exclude-cluster-member", "openclaw/openclaw", "--id", "81", "--number", "81", "--reason", "bad match", "--json"}); err != nil {
		t.Fatalf("exclude-cluster-member: %v", err)
	}
	if !strings.Contains(stdout.String(), `"excluded": true`) {
		t.Fatalf("exclude-cluster-member output = %q", stdout.String())
	}
	stdout.Reset()
	run = New()
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "include-cluster-member", "openclaw/openclaw", "--id", "81", "--number", "81", "--json"}); err != nil {
		t.Fatalf("include-cluster-member: %v", err)
	}
	if !strings.Contains(stdout.String(), `"included": true`) {
		t.Fatalf("include-cluster-member output = %q", stdout.String())
	}
	stdout.Reset()
	run = New()
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "set-cluster-canonical", "openclaw/openclaw", "--id", "81", "--number", "82", "--json"}); err != nil {
		t.Fatalf("set-cluster-canonical: %v", err)
	}
	if !strings.Contains(stdout.String(), `"canonical": true`) {
		t.Fatalf("set-cluster-canonical output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	detail, err := st.ClusterDetail(ctx, store.ClusterDetailOptions{RepoID: repoID, ClusterID: 81, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("cluster detail: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != secondID || detail.Members[0].Thread.Number != 82 || detail.Members[0].Role != "canonical" {
		t.Fatalf("canonical command did not update cluster detail: %#v", detail)
	}
}

func TestClusterCommandPersistsDurableClusters(t *testing.T) {
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
	firstID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "91",
		Number:        91,
		Kind:          "issue",
		State:         "open",
		Title:         "First duplicate",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/91",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-91",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "92",
		Number:        92,
		Kind:          "issue",
		State:         "open",
		Title:         "Second duplicate",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/92",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-92",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed second thread: %v", err)
	}
	thirdID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "93",
		Number:        93,
		Kind:          "issue",
		State:         "open",
		Title:         "Unrelated issue",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/93",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-93",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed third thread: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, vector := range []store.ThreadVector{
		{ThreadID: firstID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 2, ContentHash: "hash-91", Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: now},
		{ThreadID: secondID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 2, ContentHash: "hash-92", Vector: []float64{0.95, 0.05}, CreatedAt: now, UpdatedAt: now},
		{ThreadID: thirdID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 2, ContentHash: "hash-93", Vector: []float64{0, 1}, CreatedAt: now, UpdatedAt: now},
	} {
		if err := st.UpsertThreadVector(ctx, vector); err != nil {
			t.Fatalf("upsert vector: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.90", "--json"}); err != nil {
		t.Fatalf("cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"cluster_count": 1`) {
		t.Fatalf("cluster output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}
	if len(clusters) != 1 || clusters[0].MemberCount != 2 {
		t.Fatalf("expected one durable cluster, got %#v", clusters)
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
