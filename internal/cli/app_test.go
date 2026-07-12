package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/control"
	crawlremote "github.com/openclaw/crawlkit/remote"
	clusterer "github.com/openclaw/gitcrawl/internal/cluster"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/zalando/go-keyring"
)

func TestInitWritesConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "custom", "gitcrawl.db")
	wantVectorDir := filepath.Join(filepath.Dir(dbPath), "vectors")
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
	var result initResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode init output: %v\n%s", err, stdout.String())
	}
	if result.VectorDir != wantVectorDir {
		t.Fatalf("vector dir = %q, want %q", result.VectorDir, wantVectorDir)
	}
	if _, err := os.Stat(wantVectorDir); err != nil {
		t.Fatalf("stat vector dir: %v", err)
	}
}

func TestInitRelativeDBPathIsAbsoluteAndWritable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	workDir := filepath.Join(dir, "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir work dir: %v", err)
	}
	t.Chdir(workDir)
	configPath := filepath.Join(dir, "config.toml")

	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", "archive.db"}); err != nil {
		t.Fatalf("init relative db: %v", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	wantDBPath := filepath.Join(workDir, "archive.db")
	if cfg.DBPath != wantDBPath {
		t.Fatalf("db path = %q, want %q", cfg.DBPath, wantDBPath)
	}
	if cfg.VectorDir != filepath.Join(workDir, "vectors") {
		t.Fatalf("vector dir = %q, want sibling of resolved db", cfg.VectorDir)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open resolved db: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close resolved db: %v", err)
	}
	if info, err := os.Stat(wantDBPath); err != nil || info.Size() == 0 {
		t.Fatalf("resolved db was not initialized: info=%v err=%v", info, err)
	}
}

func TestInitRelativeRuntimeDirIsAbsolute(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	configPath := filepath.Join(dir, "config.toml")

	if err := New().Run(context.Background(), []string{"--config", configPath, "init", "--runtime-dir", "runtime"}); err != nil {
		t.Fatalf("init relative runtime: %v", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	wantRoot := filepath.Join(dir, "runtime")
	if cfg.DBPath != filepath.Join(wantRoot, "gitcrawl.db") || cfg.CacheDir != filepath.Join(wantRoot, "cache") || cfg.VectorDir != filepath.Join(wantRoot, "vectors") || cfg.LogDir != filepath.Join(wantRoot, "logs") {
		t.Fatalf("relative runtime paths not anchored under %q: %+v", wantRoot, cfg)
	}
}

func TestInitHomeRelativePathsExpandBeforeAnchoring(t *testing.T) {
	homeDir := t.TempDir()
	workDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Chdir(workDir)

	dbConfigPath := filepath.Join(workDir, "db.toml")
	if err := New().Run(context.Background(), []string{"--config", dbConfigPath, "init", "--db", "~/archive.db"}); err != nil {
		t.Fatalf("init home-relative db: %v", err)
	}
	dbConfig, err := config.Load(dbConfigPath)
	if err != nil {
		t.Fatalf("load db config: %v", err)
	}
	if want := filepath.Join(homeDir, "archive.db"); dbConfig.DBPath != want {
		t.Fatalf("home-relative db path = %q, want %q", dbConfig.DBPath, want)
	}

	runtimeConfigPath := filepath.Join(workDir, "runtime.toml")
	if err := New().Run(context.Background(), []string{"--config", runtimeConfigPath, "init", "--runtime-dir", "~/runtime"}); err != nil {
		t.Fatalf("init home-relative runtime: %v", err)
	}
	runtimeConfig, err := config.Load(runtimeConfigPath)
	if err != nil {
		t.Fatalf("load runtime config: %v", err)
	}
	wantRuntimeRoot := filepath.Join(homeDir, "runtime")
	if runtimeConfig.DBPath != filepath.Join(wantRuntimeRoot, "gitcrawl.db") || runtimeConfig.CacheDir != filepath.Join(wantRuntimeRoot, "cache") || runtimeConfig.VectorDir != filepath.Join(wantRuntimeRoot, "vectors") || runtimeConfig.LogDir != filepath.Join(wantRuntimeRoot, "logs") {
		t.Fatalf("home-relative runtime paths not anchored under %q: %+v", wantRuntimeRoot, runtimeConfig)
	}
}

func TestInitResolvesWorkingDirectoryOnlyForRelativePaths(t *testing.T) {
	app := New()
	app.getWorkingDirectory = func() (string, error) {
		return "", errors.New("working directory unavailable")
	}

	relativeCases := []struct {
		name string
		args []string
		flag string
	}{
		{name: "database", args: []string{"init", "--db", "archive.db"}, flag: "--db"},
		{name: "runtime", args: []string{"init", "--runtime-dir", "runtime"}, flag: "--runtime-dir"},
		{name: "portable store", args: []string{"init", "--portable-store", "https://example.com/store.git", "--store-dir", "store"}, flag: "--store-dir"},
	}
	for _, tc := range relativeCases {
		err := app.Run(context.Background(), tc.args)
		if err == nil || !strings.Contains(err.Error(), "resolve "+tc.flag+" path") {
			t.Errorf("%s error = %v, want %s resolution failure", tc.name, err, tc.flag)
		}
	}

	dir := t.TempDir()
	absoluteConfig := filepath.Join(dir, "absolute.toml")
	if err := app.Run(context.Background(), []string{"--config", absoluteConfig, "init", "--db", filepath.Join(dir, "archive.db")}); err != nil {
		t.Fatalf("absolute db init consulted working directory: %v", err)
	}
	remoteConfig := filepath.Join(dir, "remote.toml")
	if err := app.Run(context.Background(), []string{"--config", remoteConfig, "init", "--remote", "https://archive.example.com", "--archive", "example"}); err != nil {
		t.Fatalf("remote init consulted working directory: %v", err)
	}
}

func TestInitRejectsNonFilesystemDBValues(t *testing.T) {
	for _, dbPath := range []string{":memory:", "file:archive.db", "file:/tmp/archive.db?mode=rwc"} {
		err := New().Run(context.Background(), []string{"init", "--db", dbPath})
		if err == nil || !strings.Contains(err.Error(), "--db requires a filesystem path") {
			t.Errorf("init --db %q error = %v", dbPath, err)
		}
	}
}

func TestInitRejectsHomeRelativePathsWithoutHomeDirectory(t *testing.T) {
	t.Setenv("HOME", "")
	tests := []struct {
		name string
		args []string
	}{
		{name: "database", args: []string{"init", "--db", "~/archive.db"}},
		{name: "runtime", args: []string{"init", "--runtime-dir", "~/runtime"}},
		{name: "portable store", args: []string{"init", "--portable-store", "https://example.com/store.git", "--store-dir", "~/store"}},
	}
	for _, tc := range tests {
		err := New().Run(context.Background(), tc.args)
		if err == nil || !strings.Contains(err.Error(), "home directory unavailable") {
			t.Errorf("%s error = %v, want unavailable-home failure", tc.name, err)
		}
	}
}

func TestInitRuntimeDirIsolatesRuntimePaths(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	runtimeDir := filepath.Join(dir, "runtime")
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout

	if err := app.Run(context.Background(), []string{"--config", configPath, "--json", "init", "--runtime-dir", runtimeDir}); err != nil {
		t.Fatalf("run isolated init: %v", err)
	}
	var result initResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode init output: %v\n%s", err, stdout.String())
	}
	if result.RuntimeDir != runtimeDir {
		t.Fatalf("runtime dir = %q, want %q", result.RuntimeDir, runtimeDir)
	}
	wantPaths := map[string]string{
		"db":      filepath.Join(runtimeDir, "gitcrawl.db"),
		"cache":   filepath.Join(runtimeDir, "cache"),
		"vectors": filepath.Join(runtimeDir, "vectors"),
		"logs":    filepath.Join(runtimeDir, "logs"),
	}
	gotPaths := map[string]string{
		"db":      result.DBPath,
		"cache":   result.CacheDir,
		"vectors": result.VectorDir,
		"logs":    result.LogDir,
	}
	for name, want := range wantPaths {
		if gotPaths[name] != want {
			t.Errorf("%s path = %q, want %q", name, gotPaths[name], want)
		}
	}
	for _, path := range []string{filepath.Dir(wantPaths["db"]), wantPaths["cache"], wantPaths["vectors"], wantPaths["logs"]} {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Errorf("runtime directory %q: info=%v err=%v", path, info, err)
		}
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load isolated config: %v", err)
	}
	if loaded.DBPath != wantPaths["db"] || loaded.CacheDir != wantPaths["cache"] || loaded.VectorDir != wantPaths["vectors"] || loaded.LogDir != wantPaths["logs"] {
		t.Fatalf("isolated config paths = %+v", loaded)
	}
}

func TestInitRuntimeDirRejectsOtherArchiveModes(t *testing.T) {
	dir := t.TempDir()
	app := New()
	err := app.Run(context.Background(), []string{"--config", filepath.Join(dir, "config.toml"), "init", "--runtime-dir", filepath.Join(dir, "runtime"), "--db", filepath.Join(dir, "other.db")})
	if err == nil || !strings.Contains(err.Error(), "use only one of") {
		t.Fatalf("runtime/db conflict error = %v", err)
	}
}

func TestGitcrawlThreadBodyExpr(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		schema string
		want   string
	}{
		{name: "body and excerpt", schema: "create table threads(id text primary key, body text, body_excerpt text)", want: "coalesce(body, body_excerpt, '')"},
		{name: "body only", schema: "create table threads(id text primary key, body text)", want: "coalesce(body, '')"},
		{name: "excerpt only", schema: "create table threads(id text primary key, body_excerpt text)", want: "coalesce(body_excerpt, '')"},
		{name: "neither", schema: "create table threads(id text primary key)", want: "''"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "threads.db"))
			if err != nil {
				t.Fatalf("open sqlite: %v", err)
			}
			defer db.Close()
			if _, err := db.ExecContext(ctx, tc.schema); err != nil {
				t.Fatalf("create schema: %v", err)
			}
			got, err := gitcrawlThreadBodyExpr(ctx, db)
			if err != nil {
				t.Fatalf("body expr: %v", err)
			}
			if got != tc.want {
				t.Fatalf("body expr = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCloudSQLiteSnapshotFallbacks(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}
	fallback := filepath.Join(t.TempDir(), "fallback.db")
	got, cleanup, err := sqliteSnapshotPath(ctx, db, fallback)
	if err != nil {
		t.Fatalf("snapshot fallback: %v", err)
	}
	defer cleanup()
	if got != fallback {
		t.Fatalf("snapshot fallback path = %q, want %q", got, fallback)
	}
	if _, cleanup, err := sqliteSnapshotPath(ctx, db, ""); err == nil {
		cleanup()
		t.Fatal("snapshot without fallback unexpectedly succeeded")
	}
}

func TestCloudSQLiteSnapshotVacuum(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "create table repositories(id text primary key)"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, "insert into repositories(id) values('r1')"); err != nil {
		t.Fatalf("seed schema: %v", err)
	}
	snapshot, cleanup, err := sqliteSnapshotPath(ctx, db, "")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer cleanup()
	snapshotDB, err := sql.Open("sqlite", snapshot)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snapshotDB.Close()
	var count int
	if err := snapshotDB.QueryRowContext(ctx, "select count(*) from repositories").Scan(&count); err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if count != 1 {
		t.Fatalf("snapshot count = %d, want 1", count)
	}
}

func TestCloudSQLiteSnapshotDropsLocalCodeCorpus(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "source.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.ReplaceCodeSnapshot(ctx, store.CodeSnapshot{
		RepoID: repoID, SourceRoot: "/private/repo", GitSHA: "abc",
		FileCount: 1, ByteCount: 6, IndexedAt: "2026-06-06T00:00:00Z",
	}, []store.CodeDocument{{Path: "secret.txt", Language: "txt", ContentHash: "h", Text: "secret", ByteSize: 6, UpdatedAt: "2026-06-06T00:00:00Z"}}); err != nil {
		t.Fatalf("replace code snapshot: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into threads(
			id, repo_id, github_id, number, kind, state, title, body, html_url,
			labels_json, assignees_json, raw_json, content_hash, updated_at
		) values(
			1, ?, 'pr-1', 1, 'pull_request', 'open', 'PR', 'body',
			'https://github.com/openclaw/gitcrawl/pull/1', '[]', '[]',
			'{"secret":"thread"}', 'hash', '2026-06-06T00:00:00Z'
		);
		insert into pull_request_details(
			thread_id, repo_id, number, raw_json, fetched_at, updated_at
		) values(
			1, ?, 1, '{"secret":"detail"}',
			'2026-06-06T00:00:00Z', '2026-06-06T00:00:00Z'
		);
			insert into pull_request_files(
				thread_id, position, path, patch, raw_json, fetched_at
			) values(
				1, 0, 'secret.go', '@@ private source', '{"secret":"file"}',
				'2026-06-06T00:00:00Z'
			);
			insert into pull_request_review_threads(
				thread_id, review_thread_id, path, comments_json, raw_json, fetched_at
			) values(
				1, 'review-thread-1', 'secret.go',
				'[{"body":"review body","diffHunk":"@@ private review source"}]',
				'{"secret":"review-thread"}', '2026-06-06T00:00:00Z'
			);
			insert into blobs(
				sha256, media_type, compression, size_bytes, storage_kind, inline_text, created_at
			) values(
			'blob-hash', 'application/json', 'none', 6, 'inline', 'secret',
			'2026-06-06T00:00:00Z'
		);
			insert into sync_runs(
				repo_id, scope, status, started_at, finished_at, stats_json, error_text
			) values(
				?, 'all', 'failed', '2026-06-06T00:00:00Z',
				'2026-06-06T00:01:00Z',
				'{"message":"/private/host/token","threads":1}', '/private/host/token'
			);
			insert into summary_runs(
				repo_id, scope, status, started_at, finished_at, stats_json, error_text
			) values(
				?, 'all', 'failed', '2026-06-06T00:00:00Z',
				'2026-06-06T00:01:00Z',
				'{"failures":[{"message":"provider secret"}]}', 'provider secret'
			);
			insert into embedding_runs(
				repo_id, scope, status, started_at, finished_at, stats_json, error_text
			) values(
				?, 'all', 'failed', '2026-06-06T00:00:00Z',
				'2026-06-06T00:01:00Z',
				'{"failures":[{"message":"/private/embed"}]}', '/private/embed'
			);
			insert into cluster_runs(
				repo_id, scope, status, started_at, finished_at, stats_json, error_text
			) values(
				?, 'all', 'failed', '2026-06-06T00:00:00Z',
				'2026-06-06T00:01:00Z',
				'{"message":"/private/cluster"}', '/private/cluster'
			);
			create table portable_metadata(key text primary key, value text not null);
			insert into portable_metadata(key, value) values('source_path', '/private/archive.db')
		`, repoID, repoID, repoID, repoID, repoID, repoID); err != nil {
		t.Fatalf("seed private cloud payloads: %v", err)
	}
	snapshotPath, cleanup, err := cloudSQLiteSnapshotPath(ctx, st.DB(), dbPath)
	if err != nil {
		t.Fatalf("cloud snapshot: %v", err)
	}
	defer cleanup()
	snapshotDB, err := sql.Open("sqlite", snapshotPath)
	if err != nil {
		t.Fatalf("open cloud snapshot: %v", err)
	}
	defer snapshotDB.Close()
	for _, table := range []string{
		"code_snapshots",
		"code_documents",
		"code_documents_fts",
		"thread_code_snapshots",
		"thread_changed_files",
		"thread_hunk_signatures",
	} {
		var count int
		if err := snapshotDB.QueryRowContext(ctx, `select count(*) from sqlite_schema where name = ?`, table).Scan(&count); err != nil {
			t.Fatalf("inspect %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("cloud snapshot retained %s", table)
		}
	}
	var repositoryRawJSON, threadRawJSON, detailRawJSON, filePatch, fileRawJSON string
	if err := snapshotDB.QueryRowContext(ctx, `
		select repository.raw_json, thread.raw_json, detail.raw_json, file.patch, file.raw_json
		from repositories repository
		join threads thread on thread.repo_id = repository.id
		join pull_request_details detail on detail.thread_id = thread.id
		join pull_request_files file on file.thread_id = thread.id
	`).Scan(&repositoryRawJSON, &threadRawJSON, &detailRawJSON, &filePatch, &fileRawJSON); err != nil {
		t.Fatalf("inspect scrubbed cloud payloads: %v", err)
	}
	if repositoryRawJSON != "" || threadRawJSON != "" || detailRawJSON != "" || filePatch != "" || fileRawJSON != "" {
		t.Fatalf(
			"cloud snapshot retained private payloads: repository=%q thread=%q detail=%q patch=%q file=%q",
			repositoryRawJSON,
			threadRawJSON,
			detailRawJSON,
			filePatch,
			fileRawJSON,
		)
	}
	var blobCount int
	if err := snapshotDB.QueryRowContext(ctx, `select count(*) from blobs`).Scan(&blobCount); err != nil {
		t.Fatalf("inspect cloud blobs: %v", err)
	}
	if blobCount != 0 {
		t.Fatalf("cloud snapshot retained %d blobs", blobCount)
	}
	var reviewComments string
	if err := snapshotDB.QueryRowContext(ctx, `
			select comments_json from pull_request_review_threads limit 1
		`).Scan(&reviewComments); err != nil {
		t.Fatalf("inspect scrubbed review comments: %v", err)
	}
	if reviewComments != "" {
		t.Fatalf("cloud snapshot retained review diff payloads: %q", reviewComments)
	}
	for _, table := range []string{"sync_runs", "summary_runs", "embedding_runs", "cluster_runs"} {
		var statsJSON, errorText string
		if err := snapshotDB.QueryRowContext(ctx, `
				select coalesce(stats_json, ''), coalesce(error_text, '') from `+table+` limit 1
			`).Scan(&statsJSON, &errorText); err != nil {
			t.Fatalf("inspect scrubbed cloud %s payload: %v", table, err)
		}
		if statsJSON != "" || errorText != "" {
			t.Fatalf(
				"cloud snapshot retained %s failure payloads: stats=%q error=%q",
				table,
				statsJSON,
				errorText,
			)
		}
	}
	var sourcePath string
	if err := snapshotDB.QueryRowContext(ctx, `select value from portable_metadata where key = 'source_path'`).Scan(&sourcePath); err != nil {
		t.Fatalf("inspect scrubbed cloud source path: %v", err)
	}
	if sourcePath != "" {
		t.Fatalf("cloud snapshot retained private source path: %q", sourcePath)
	}
	var sourceCount int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from code_documents`).Scan(&sourceCount); err != nil {
		t.Fatalf("source code documents: %v", err)
	}
	if sourceCount != 1 {
		t.Fatalf("cloud snapshot mutated source corpus: count=%d", sourceCount)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

func TestCloudFileSHA256(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload.txt")
	if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	got, err := cloudFileSHA256(path)
	if err != nil {
		t.Fatalf("hash payload: %v", err)
	}
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
	if _, err := cloudFileSHA256(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing payload unexpectedly hashed")
	}
}

func TestSendIngestRowsBatchesAndAcceptsEmpty(t *testing.T) {
	ctx := context.Background()
	var cursors []string
	var finals []bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.EscapedPath() != "/v1/apps/gitcrawl/archives/gitcrawl%2Fopenclaw/ingest" {
			http.NotFound(w, req)
			return
		}
		var body crawlremote.IngestRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cursors = append(cursors, body.Cursor)
		finals = append(finals, body.Final)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{Table: body.Table, RowsAccepted: int64(len(body.Rows)), Complete: body.Final})
	}))
	defer server.Close()
	client, err := crawlremote.NewClient(crawlremote.Options{
		Endpoint:      server.URL,
		TokenProvider: crawlremote.StaticToken("token"),
	})
	if err != nil {
		t.Fatalf("remote client: %v", err)
	}
	manifest := crawlremote.IngestManifest{App: "gitcrawl", Archive: "gitcrawl/openclaw"}
	empty, err := sendIngestRows(ctx, client, "gitcrawl", "gitcrawl/openclaw", manifest, "repositories", gitcrawlRepositoryColumns, nil, false)
	if err != nil {
		t.Fatalf("empty ingest: %v", err)
	}
	if empty != 0 {
		t.Fatalf("empty rows accepted = %d, want 0", empty)
	}
	rows := make([][]any, gitcrawlCloudBatchSize+1)
	for i := range rows {
		rows[i] = []any{fmt.Sprintf("r%d", i), "openclaw/repo", "openclaw", "repo", "", "", ""}
	}
	total, err := sendIngestRows(ctx, client, "gitcrawl", "gitcrawl/openclaw", manifest, "repositories", gitcrawlRepositoryColumns, rows, true)
	if err != nil {
		t.Fatalf("batched ingest: %v", err)
	}
	if total != int64(len(rows)) {
		t.Fatalf("rows accepted = %d, want %d", total, len(rows))
	}
	if got, want := strings.Join(cursors, ","), ",,250"; got != want {
		t.Fatalf("cursors = %q, want %q", got, want)
	}
	if got, want := fmt.Sprint(finals), "[false false true]"; got != want {
		t.Fatalf("finals = %s, want %s", got, want)
	}
}

func TestRemoteValueCoercionHelpers(t *testing.T) {
	value := map[string]any{
		"bool_string":  "true",
		"bool_number":  json.Number("1"),
		"int_string":   "42",
		"int_number":   json.Number("43"),
		"int64_string": "42000000000",
		"float_string": "4.25",
		"float_number": json.Number("5.5"),
	}
	if !boolValue(value, "bool_string") || !boolValue(value, "bool_number") || boolValue(value, "missing") {
		t.Fatalf("bool coercion failed")
	}
	if got := intValue(value, "int_string"); got != 42 {
		t.Fatalf("int string = %d, want 42", got)
	}
	if got := intValue(value, "int_number"); got != 43 {
		t.Fatalf("int number = %d, want 43", got)
	}
	if got := int64Value(value, "int64_string"); got != 42000000000 {
		t.Fatalf("int64 string = %d, want 42000000000", got)
	}
	if got := floatValue(value, "float_string"); got != 4.25 {
		t.Fatalf("float string = %v, want 4.25", got)
	}
	if got := floatValue(value, "float_number"); got != 5.5 {
		t.Fatalf("float number = %v, want 5.5", got)
	}
	ref := threadReference{Owner: "openclaw", Repo: "gitcrawl"}
	if got := ref.FullName(); got != "openclaw/gitcrawl" {
		t.Fatalf("full name = %q, want openclaw/gitcrawl", got)
	}
	if got := (threadReference{Owner: "openclaw"}).FullName(); got != "" {
		t.Fatalf("partial full name = %q, want empty", got)
	}
}

func TestUploadSQLiteArchiveReturnsRemoteError(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "create table repositories(id text primary key)"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer server.Close()
	client, err := crawlremote.NewClient(crawlremote.Options{
		Endpoint:      server.URL,
		TokenProvider: crawlremote.StaticToken("token"),
	})
	if err != nil {
		t.Fatalf("remote client: %v", err)
	}
	_, err = uploadSQLiteArchive(ctx, client, "gitcrawl", "gitcrawl/openclaw", db, "", crawlremote.IngestManifest{SchemaName: "gitcrawl-cloud-v1"}, nil)
	if err == nil {
		t.Fatal("upload unexpectedly succeeded")
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

func TestRemoteCloudModeDoesNotCreateLocalDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("CRAWL_REMOTE_TOKEN", "test-token")

	var queryArgSets [][]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("authorization"); got != "Bearer test-token" {
			http.Error(w, "missing bearer", http.StatusUnauthorized)
			return
		}
		w.Header().Set("content-type", "application/json")
		switch r.URL.EscapedPath() {
		case "/v1/apps/gitcrawl/archives/gitcrawl%2Fopenclaw__openclaw/status":
			_ = json.NewEncoder(w).Encode(crawlremote.Status{
				App:          "gitcrawl",
				Archive:      "gitcrawl/openclaw__openclaw",
				Mode:         "cloud",
				LastIngestAt: "2026-05-27T12:00:00Z",
				Counts: []control.Count{
					control.NewCount("repositories", "Repositories", 1),
					control.NewCount("threads", "Threads", 2),
				},
			})
		case "/v1/apps/gitcrawl/archives/gitcrawl%2Fopenclaw__openclaw/query":
			var req crawlremote.QueryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.Name != "gitcrawl.threads.search" || req.Args["owner"] != "openclaw" || req.Args["repo"] != "openclaw" {
				http.Error(w, "unexpected query", http.StatusBadRequest)
				return
			}
			argNames := make([]string, 0, len(req.Args))
			for name := range req.Args {
				argNames = append(argNames, name)
			}
			sort.Strings(argNames)
			queryArgSets = append(queryArgSets, argNames)
			_ = json.NewEncoder(w).Encode(crawlremote.QueryResult{
				Values: []map[string]any{{
					"thread_id":    10,
					"github_id":    "I_kwD",
					"number":       42,
					"kind":         "issue",
					"state":        "open",
					"title":        "remote search",
					"html_url":     "https://github.com/openclaw/openclaw/issues/42",
					"author_login": "alice",
					"snippet":      "remote search",
					"score":        0.9,
				}},
			})
		case "/v1/archives":
			_ = json.NewEncoder(w).Encode(map[string]any{"archives": []crawlremote.Archive{{ID: "gitcrawl/openclaw__openclaw", App: "gitcrawl", Slug: "openclaw__openclaw"}}})
		case "/v1/whoami":
			_ = json.NewEncoder(w).Encode(crawlremote.Identity{Owner: "openclaw", Org: "openclaw", Login: "alice"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	configPath := filepath.Join(dir, "config.toml")
	initApp := New()
	var initOut bytes.Buffer
	initApp.Stdout = &initOut
	if err := initApp.Run(ctx, []string{"--config", configPath, "--json", "init", "--remote", server.URL, "--archive", "gitcrawl/openclaw__openclaw"}); err != nil {
		t.Fatalf("remote init: %v", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load remote config: %v", err)
	}
	if cfg.Remote.Mode != "cloud" || cfg.Remote.Endpoint != server.URL || cfg.Remote.Archive != "gitcrawl/openclaw__openclaw" {
		t.Fatalf("remote config = %#v", cfg.Remote)
	}
	if _, err := os.Stat(filepath.Join(dir, ".config", "gitcrawl", "gitcrawl.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remote init should not create local db, stat err=%v", err)
	}

	statusApp := New()
	var statusOut bytes.Buffer
	statusApp.Stdout = &statusOut
	if err := statusApp.Run(ctx, []string{"--config", configPath, "--json", "status"}); err != nil {
		t.Fatalf("remote status: %v", err)
	}
	var status map[string]any
	if err := json.Unmarshal(statusOut.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v\n%s", err, statusOut.String())
	}
	if status["database_path"] != nil {
		t.Fatalf("remote status should not expose local database path: %#v", status)
	}
	remoteStatus, ok := status["remote"].(map[string]any)
	if !ok || remoteStatus["archive"] != "gitcrawl/openclaw__openclaw" {
		t.Fatalf("remote status missing archive: %#v", status)
	}

	searchApp := New()
	var searchOut bytes.Buffer
	searchApp.Stdout = &searchOut
	if err := searchApp.Run(ctx, []string{"--config", configPath, "--json", "search", "openclaw/openclaw", "--query", "remote"}); err != nil {
		t.Fatalf("remote search: %v", err)
	}
	if len(queryArgSets) != 1 ||
		!slices.Equal(queryArgSets[0], []string{"limit", "mode", "owner", "query", "repo"}) ||
		!strings.Contains(searchOut.String(), `"remote search"`) {
		t.Fatalf("remote search query args=%v out=%s", queryArgSets, searchOut.String())
	}
	ghSearchApp := New()
	var ghSearchOut bytes.Buffer
	ghSearchApp.Stdout = &ghSearchOut
	if err := ghSearchApp.Run(ctx, []string{"--config", configPath, "search", "issues", "remote", "-R", "openclaw/openclaw", "--json", "number,title,url"}); err != nil {
		t.Fatalf("remote gh search: %v", err)
	}
	if !strings.Contains(ghSearchOut.String(), `"number": 42`) || !strings.Contains(ghSearchOut.String(), `"url": "https://github.com/openclaw/openclaw/issues/42"`) {
		t.Fatalf("remote gh search output = %s", ghSearchOut.String())
	}
	if len(queryArgSets) != 2 ||
		!slices.Equal(queryArgSets[1], []string{"kind", "limit", "owner", "query", "repo", "state"}) {
		t.Fatalf("remote gh search query args=%v", queryArgSets)
	}
	doctorApp := New()
	var doctorOut bytes.Buffer
	doctorApp.Stdout = &doctorOut
	if err := doctorApp.Run(ctx, []string{"--config", configPath, "--json", "doctor"}); err != nil {
		t.Fatalf("remote doctor: %v", err)
	}
	if !strings.Contains(doctorOut.String(), `"health": "ok"`) || strings.Contains(doctorOut.String(), `"db_path"`) {
		t.Fatalf("remote doctor output = %s", doctorOut.String())
	}
	for _, args := range [][]string{
		{"--config", configPath, "--json", "remote", "status"},
		{"--config", configPath, "--json", "remote", "archives"},
		{"--config", configPath, "--json", "remote", "whoami"},
		{"--config", configPath, "--json", "whoami"},
	} {
		app := New()
		var out bytes.Buffer
		app.Stdout = &out
		if err := app.Run(ctx, args); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
}

func testSnapshotPublishContract() crawlremote.Contract {
	contract := crawlremote.BaseContract()
	contract.Apps = []crawlremote.AppSpec{{
		App: "gitcrawl",
		Queries: []crawlremote.QuerySpec{
			{Name: "gitcrawl.threads.search", Args: []string{"limit", "mode", "state", "kind", "query", "repo", "owner"}},
			{Name: "gitcrawl.clusters.related", Args: []string{"owner", "repo", "number"}},
			{Name: "gitcrawl.clusters.list", Args: []string{"owner", "repo", "status", "min_size"}},
			{Name: "gitcrawl.clusters.members", Args: []string{"owner", "repo", "cluster_id"}},
			{Name: "gitcrawl.pull_requests.review_context", Args: []string{"owner", "repo", "number"}},
			{Name: "gitcrawl.coverage", Args: []string{"dataset"}},
		},
		IngestTables: []crawlremote.IngestTableSpec{
			{Name: "repositories", Columns: gitcrawlRepositoryColumns},
			{Name: "threads", Columns: gitcrawlThreadColumns},
			{Name: "thread_revisions", Columns: []string{
				"id", "thread_id", "source_updated_at", "content_hash", "title_hash",
				"body_hash", "labels_hash", "created_at",
			}},
			{Name: "thread_fingerprints", Columns: []string{
				"id", "thread_revision_id", "algorithm_version", "fingerprint_hash",
				"fingerprint_slug", "body_token_hash", "file_set_hash", "simhash64", "created_at",
			}},
			{Name: "thread_key_summaries", Columns: []string{
				"id", "thread_revision_id", "summary_kind", "prompt_version", "provider",
				"model", "input_hash", "output_hash", "key_text", "created_at",
			}},
			{Name: "cluster_groups", Columns: []string{
				"id", "repo_id", "stable_key", "stable_slug", "status", "cluster_type",
				"representative_thread_id", "title", "member_count", "created_at", "updated_at",
				"closed_at",
			}},
			{Name: "cluster_memberships", Columns: []string{
				"cluster_id", "thread_id", "role", "state", "score_to_representative",
				"created_at", "updated_at", "removed_at",
			}},
			{Name: "pull_request_details", Columns: []string{
				"thread_id", "repo_id", "number", "base_sha", "head_sha", "head_ref",
				"head_repo_full_name", "mergeable_state", "additions", "deletions",
				"changed_files", "fetched_at", "updated_at",
			}},
			{Name: "pull_request_files", Columns: []string{
				"thread_id", "position", "path", "status", "additions", "deletions",
				"changes", "previous_path", "fetched_at",
			}},
			{Name: "dataset_coverage", Columns: gitcrawlCloudCoverageColumns},
		},
		Capabilities: []string{
			gitcrawlSnapshotAtomicCapability,
			gitcrawlSnapshotCutoverCapability,
			gitcrawlSnapshotProvenanceCapability,
			sqliteBundleGzipUploadCapability,
		},
	}}
	return contract
}

func TestGitcrawlPublisherStatusMatchesExactMetadata(t *testing.T) {
	snapshotID := strings.Repeat("a", 64)
	manifest := crawlremote.IngestManifest{
		App:           "gitcrawl",
		Archive:       "gitcrawl/openclaw__openclaw",
		SchemaName:    gitcrawlCloudSchemaName,
		SchemaVersion: gitcrawlCloudSchemaVersion,
		SchemaHash:    gitcrawlCloudSchemaHash,
		SourceSyncAt:  "2026-07-12T12:00:00Z",
		SnapshotID:    snapshotID,
		SourceSHA256:  snapshotID,
	}
	publicationCapabilities := gitcrawlCloudPublicationCapabilities(manifest.Capabilities)
	status := crawlremote.PublisherStatus{
		App:              "gitcrawl",
		Archive:          manifest.Archive,
		ActiveSnapshotID: snapshotID,
		CoverageComplete: true,
		Snapshot: &crawlremote.ArchiveSnapshot{
			ID:                 snapshotID,
			SourceSHA256:       snapshotID,
			SourceSyncAt:       manifest.SourceSyncAt,
			DatasetGeneratedAt: "2026-07-12T12:01:00Z",
			SchemaName:         gitcrawlCloudSchemaName,
			SchemaVersion:      gitcrawlCloudSchemaVersion,
			SchemaHash:         gitcrawlCloudSchemaHash,
			Capabilities:       publicationCapabilities,
			CoverageComplete:   false,
		},
	}
	if !gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) {
		t.Fatal("exact staged snapshot did not match")
	}

	activeMismatch := status
	activeMismatch.ActiveSnapshotID = strings.Repeat("f", 64)
	if gitcrawlPublisherStatusMatches(activeMismatch, manifest, publicationCapabilities) {
		t.Fatal("non-active staged snapshot was reused")
	}

	sourceMismatch := status
	sourceMismatch.Snapshot = &crawlremote.ArchiveSnapshot{}
	*sourceMismatch.Snapshot = *status.Snapshot
	sourceMismatch.Snapshot.SourceSHA256 = strings.Repeat("b", 64)
	if gitcrawlPublisherStatusMatches(sourceMismatch, manifest, publicationCapabilities) {
		t.Fatal("source digest mismatch was reused")
	}

	sourceSyncMismatch := status
	sourceSyncMismatch.Snapshot = &crawlremote.ArchiveSnapshot{}
	*sourceSyncMismatch.Snapshot = *status.Snapshot
	sourceSyncMismatch.Snapshot.SourceSyncAt = "2026-07-12T12:00:01Z"
	if gitcrawlPublisherStatusMatches(sourceSyncMismatch, manifest, publicationCapabilities) {
		t.Fatal("source sync mismatch was reused")
	}

	schemaMismatch := status
	schemaMismatch.Snapshot = &crawlremote.ArchiveSnapshot{}
	*schemaMismatch.Snapshot = *status.Snapshot
	schemaMismatch.Snapshot.SchemaVersion++
	if gitcrawlPublisherStatusMatches(schemaMismatch, manifest, publicationCapabilities) {
		t.Fatal("schema mismatch was reused")
	}

	missingGeneratedAt := status
	missingGeneratedAt.Snapshot = &crawlremote.ArchiveSnapshot{}
	*missingGeneratedAt.Snapshot = *status.Snapshot
	missingGeneratedAt.Snapshot.DatasetGeneratedAt = ""
	if gitcrawlPublisherStatusMatches(missingGeneratedAt, manifest, publicationCapabilities) {
		t.Fatal("snapshot without its staged generation timestamp was reused")
	}

	status.CoverageComplete = false
	if gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) {
		t.Fatal("incomplete publisher status was reused")
	}
	status.CoverageComplete = true

	manifest.Capabilities = []string{gitcrawlObservationOrderCapability}
	publicationCapabilities = gitcrawlCloudPublicationCapabilities(manifest.Capabilities)
	if gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) {
		t.Fatal("staged snapshot reused without snapshot capability proof")
	}
	status.Snapshot.Capabilities = slices.Clone(publicationCapabilities)
	if !gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) {
		t.Fatal("staged snapshot with requested capability did not match")
	}
	status.Snapshot.Capabilities = append(
		slices.Clone(publicationCapabilities),
		"gitcrawl.unrequested-profile.v1",
	)
	if gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) {
		t.Fatal("snapshot with a broader publication profile was reused")
	}
	status.Snapshot.Capabilities = append(
		slices.Clone(publicationCapabilities),
		gitcrawlObservationOrderCapability,
	)
	if gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) {
		t.Fatal("snapshot with duplicate publication capabilities was reused")
	}
}

func TestCloudPublishSendsLocalRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	cfg := config.Default()
	cfg.DBPath = dbPath
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	tokenEnv := "GITCRAWL_TEST_PUBLISH_TOKEN"
	t.Setenv(tokenEnv, "publish-token")
	seenTables := map[string]crawlremote.IngestRequest{}
	var sawSQLitePart bool
	var sawSQLiteManifest bool
	var sawCutover bool
	var snapshotID string
	mutationCounter := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.EscapedPath() == "/v1/contract" {
			w.Header().Set("content-type", "application/json")
			_ = json.NewEncoder(w).Encode(testSnapshotPublishContract())
			return
		}
		if got := r.Header.Get("authorization"); got != "Bearer publish-token" {
			http.Error(w, "missing bearer", http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodPut && r.URL.EscapedPath() == "/v1/apps/gitcrawl/archives/gitcrawl%2Fopenclaw__openclaw/sqlite" {
			uploadKind := r.Header.Get("x-crawl-sqlite-upload")
			payload, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("content-type", "application/json")
			switch uploadKind {
			case "bundle-part":
				sawSQLitePart = true
				if got := r.Header.Get("x-crawl-snapshot-id"); len(got) != 64 {
					http.Error(w, "missing snapshot id", http.StatusBadRequest)
					return
				} else {
					snapshotID = got
				}
				partSHA := r.Header.Get("x-crawl-content-sha256")
				partIndex, err := strconv.Atoi(r.Header.Get("x-crawl-bundle-part-index"))
				if r.Header.Get("content-type") != "application/gzip" ||
					partSHA == "" ||
					err != nil {
					http.Error(w, "bad bundle part headers", http.StatusBadRequest)
					return
				}
				if !bytes.HasPrefix(payload, []byte{0x1f, 0x8b}) {
					http.Error(w, "not gzip", http.StatusBadRequest)
					return
				}
				reader, err := gzip.NewReader(bytes.NewReader(payload))
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				decompressed, err := io.ReadAll(reader)
				if closeErr := reader.Close(); err == nil {
					err = closeErr
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if !bytes.HasPrefix(decompressed, []byte("SQLite format 3")) {
					http.Error(w, "bundle did not contain sqlite", http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(crawlremote.SQLiteUploadResult{
					App:      "gitcrawl",
					Archive:  "gitcrawl/openclaw__openclaw",
					Complete: false,
					Object: &crawlremote.SQLiteObject{
						Key: crawlremote.SQLiteSnapshotBundlePartKey(
							"gitcrawl",
							"gitcrawl/openclaw__openclaw",
							snapshotID,
							partSHA,
							partIndex,
						),
						Size: int64(len(payload)),
					},
				})
			case "bundle-manifest":
				sawSQLiteManifest = true
				var manifest crawlremote.SQLiteBundleManifest
				if err := json.Unmarshal(payload, &manifest); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if manifest.Format != crawlremote.SQLiteGzipChunkedBundleFormat || manifest.Compression.Algorithm != crawlremote.SQLiteGzipCompression || manifest.CompressedObject.Size == 0 {
					http.Error(w, "bad manifest", http.StatusBadRequest)
					return
				}
				if manifest.Privacy["includes_private_messages"] != true {
					http.Error(w, "bundle privacy should disclose message bodies", http.StatusBadRequest)
					return
				}
				if manifest.SnapshotID != snapshotID ||
					manifest.Object.Key != crawlremote.SQLiteSnapshotObjectKey("gitcrawl", "gitcrawl/openclaw__openclaw", snapshotID) {
					http.Error(w, "bundle snapshot mismatch", http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(crawlremote.SQLiteBundleUploadResult{
					App:      "gitcrawl",
					Archive:  "gitcrawl/openclaw__openclaw",
					Complete: true,
					Bundle: &crawlremote.SQLiteBundle{
						Key:      crawlremote.SQLiteSnapshotBundleManifestKey("gitcrawl", "gitcrawl/openclaw__openclaw", snapshotID),
						Manifest: &manifest,
					},
				})
			default:
				http.Error(w, "missing upload kind", http.StatusBadRequest)
			}
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/publish-status") {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.EscapedPath(), "/cutover") {
			var request struct {
				SnapshotID string `json:"snapshot_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.SnapshotID != snapshotID {
				http.Error(w, "cutover snapshot mismatch", http.StatusBadRequest)
				return
			}
			sawCutover = true
			_ = json.NewEncoder(w).Encode(crawlremote.CutoverResult{
				Archive:      "gitcrawl/openclaw__openclaw",
				SnapshotID:   snapshotID,
				SnapshotMode: "snapshot",
				CutoverAt:    "2026-07-12T09:00:00Z",
			})
			return
		}
		if r.Method != http.MethodPost || r.URL.EscapedPath() != "/v1/apps/gitcrawl/archives/gitcrawl%2Fopenclaw__openclaw/ingest" {
			http.NotFound(w, r)
			return
		}
		var body crawlremote.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body.Manifest.App != "gitcrawl" ||
			body.Manifest.Archive != "gitcrawl/openclaw__openclaw" ||
			body.Manifest.SnapshotID != snapshotID ||
			body.Manifest.SourceSHA256 != snapshotID {
			http.Error(w, "manifest mismatch", http.StatusBadRequest)
			return
		}
		if body.Table == "dataset_coverage" {
			if body.MutationToken == "" || !body.Final || len(body.Rows) != 9 {
				http.Error(w, "coverage activation mismatch", http.StatusBadRequest)
				return
			}
			for _, row := range body.Rows {
				if row[len(row)-1] != body.MutationToken {
					http.Error(w, "coverage token mismatch", http.StatusBadRequest)
					return
				}
			}
		}
		seenTables[body.Table] = body
		mutationCounter++
		token := fmt.Sprintf("mutation-%d", mutationCounter)
		if body.Table == "dataset_coverage" {
			token = body.MutationToken
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{
			Table:         body.Table,
			SnapshotID:    snapshotID,
			MutationToken: token,
			RowsAccepted:  int64(len(body.Rows)),
			Complete:      body.Final,
		})
	}))
	defer server.Close()

	app := New()
	var out bytes.Buffer
	app.Stdout = &out
	if err := app.Run(ctx, []string{
		"--config", cfgPath,
		"cloud", "publish",
		"--remote", server.URL,
		"--archive", "gitcrawl/openclaw__openclaw",
		"--token-env", tokenEnv,
		"--allow-incomplete",
		"--json",
	}); err != nil {
		t.Fatalf("cloud publish: %v", err)
	}

	if len(seenTables) != 10 {
		t.Fatalf("tables = %v, want all cloud-v2 datasets and coverage", seenTables)
	}
	if got := len(seenTables["repositories"].Rows); got != 1 {
		t.Fatalf("repositories rows = %d, want 1", got)
	}
	if got := len(seenTables["threads"].Rows); got != 3 {
		t.Fatalf("threads rows = %d, want 3", got)
	}
	if seenTables["threads"].Final {
		t.Fatalf("threads batch should remain staged")
	}
	if !seenTables["dataset_coverage"].Final {
		t.Fatalf("coverage batch should activate the snapshot")
	}
	if !sawSQLitePart || !sawSQLiteManifest {
		t.Fatalf("cloud publish did not upload compressed sqlite bundle: part=%v manifest=%v", sawSQLitePart, sawSQLiteManifest)
	}
	if !sawCutover {
		t.Fatal("cloud publish did not cut over the activated snapshot")
	}
	var payload map[string]any
	if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
		t.Fatalf("decode output: %v\n%s", err, out.String())
	}
	datasets, ok := payload["datasets"].(map[string]any)
	if !ok || datasets["repositories"] != float64(1) || datasets["threads"] != float64(3) {
		t.Fatalf("unexpected output: %#v", payload)
	}
	if payload["snapshot_id"] != snapshotID || payload["source_sha256"] != snapshotID {
		t.Fatalf("missing snapshot provenance: %#v", payload)
	}
	if payload["sqlite_bundle"] == nil {
		t.Fatalf("missing sqlite bundle output: %#v", payload)
	}
	privacy, ok := payload["sqlite_bundle_privacy"].(map[string]any)
	if !ok || privacy["includes_private_messages"] != true {
		t.Fatalf("missing sqlite bundle privacy output: %#v", payload)
	}
}

func TestCloudPublishRejectsMissingSnapshotCapabilityBeforeUpload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	cfg := config.Default()
	cfg.DBPath = dbPath
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	const tokenEnv = "GITCRAWL_TEST_CONTRACT_TOKEN"
	t.Setenv(tokenEnv, "publish-token")
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.EscapedPath() == "/v1/contract" {
			contract := testSnapshotPublishContract()
			contract.Apps[0].Capabilities = []string{
				gitcrawlSnapshotAtomicCapability,
				sqliteBundleGzipUploadCapability,
			}
			w.Header().Set("content-type", "application/json")
			_ = json.NewEncoder(w).Encode(contract)
			return
		}
		mutations++
		http.Error(w, "unexpected mutation", http.StatusInternalServerError)
	}))
	defer server.Close()

	err := New().Run(ctx, []string{
		"--config", configPath,
		"cloud", "publish",
		"--remote", server.URL,
		"--archive", "gitcrawl/openclaw__openclaw",
		"--token-env", tokenEnv,
		"--allow-incomplete",
		"--json",
	})
	if err == nil || !strings.Contains(err.Error(), gitcrawlSnapshotProvenanceCapability) {
		t.Fatalf("cloud publish error = %v", err)
	}
	if mutations != 0 {
		t.Fatalf("remote mutations = %d, want 0", mutations)
	}
}

func TestCloudPublishRejectsMissingRequestedCapabilityBeforeUpload(t *testing.T) {
	tests := []struct {
		name              string
		args              []string
		missingCapability string
	}{
		{
			name:              "observation order",
			args:              []string{"--observation-order", "--stage-only"},
			missingCapability: gitcrawlObservationOrderCapability,
		},
		{
			name:              "cutover",
			missingCapability: gitcrawlSnapshotCutoverCapability,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.toml")
			dbPath := filepath.Join(dir, "gitcrawl.db")
			cfg := config.Default()
			cfg.DBPath = dbPath
			if err := config.Save(configPath, cfg); err != nil {
				t.Fatalf("save config: %v", err)
			}
			seedCommandFlowStore(t, dbPath)

			const tokenEnv = "GITCRAWL_TEST_OPTION_CONTRACT_TOKEN"
			t.Setenv(tokenEnv, "publish-token")
			mutations := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.EscapedPath() == "/v1/contract" {
					contract := testSnapshotPublishContract()
					capabilities := make([]string, 0, len(contract.Apps[0].Capabilities))
					for _, capability := range contract.Apps[0].Capabilities {
						if capability != test.missingCapability {
							capabilities = append(capabilities, capability)
						}
					}
					contract.Apps[0].Capabilities = capabilities
					w.Header().Set("content-type", "application/json")
					_ = json.NewEncoder(w).Encode(contract)
					return
				}
				mutations++
				http.Error(w, "unexpected mutation", http.StatusInternalServerError)
			}))
			defer server.Close()

			args := []string{
				"--config", configPath,
				"cloud", "publish",
				"--remote", server.URL,
				"--archive", "gitcrawl/openclaw__openclaw",
				"--token-env", tokenEnv,
				"--allow-incomplete",
				"--json",
			}
			args = append(args, test.args...)
			err := New().Run(ctx, args)
			if err == nil || !strings.Contains(err.Error(), test.missingCapability) {
				t.Fatalf("cloud publish error = %v", err)
			}
			if mutations != 0 {
				t.Fatalf("remote mutations = %d, want 0", mutations)
			}
		})
	}
}

func TestCloudPublishRejectsIncompleteRemoteSurfaceBeforeUpload(t *testing.T) {
	type remoteSurfaceTest struct {
		name      string
		want      string
		mutate    func(*crawlremote.Contract)
		extraArgs []string
	}
	tests := []remoteSurfaceTest{
		{
			name: "missing ingest column",
			want: "pull_request_files is missing required column position",
			mutate: func(contract *crawlremote.Contract) {
				for tableIndex := range contract.Apps[0].IngestTables {
					table := &contract.Apps[0].IngestTables[tableIndex]
					if table.Name == "pull_request_files" {
						table.Columns = slices.DeleteFunc(
							table.Columns,
							func(column string) bool { return column == "position" },
						)
					}
				}
			},
		},
		{
			name: "missing cutover route",
			want: "POST /v1/apps/:app/archives/:archive/cutover",
			mutate: func(contract *crawlremote.Contract) {
				contract.Routes = slices.DeleteFunc(
					contract.Routes,
					func(route crawlremote.RouteSpec) bool {
						return route.Method == http.MethodPost &&
							route.Path == "/v1/apps/:app/archives/:archive/cutover"
					},
				)
			},
		},
		{
			name: "missing publisher status route",
			want: "GET /v1/apps/:app/archives/:archive/publish-status",
			mutate: func(contract *crawlremote.Contract) {
				contract.Routes = slices.DeleteFunc(
					contract.Routes,
					func(route crawlremote.RouteSpec) bool {
						return route.Method == http.MethodGet &&
							route.Path == "/v1/apps/:app/archives/:archive/publish-status"
					},
				)
			},
		},
		{
			name: "publisher status route with reader auth",
			want: "GET /v1/apps/:app/archives/:archive/publish-status",
			mutate: func(contract *crawlremote.Contract) {
				for index := range contract.Routes {
					route := &contract.Routes[index]
					if route.Method == http.MethodGet &&
						route.Path == "/v1/apps/:app/archives/:archive/publish-status" {
						route.Auth = crawlremote.AuthReader
					}
				}
			},
		},
		{
			name: "missing reader query route",
			want: "POST /v1/apps/:app/archives/:archive/query",
			mutate: func(contract *crawlremote.Contract) {
				contract.Routes = slices.DeleteFunc(
					contract.Routes,
					func(route crawlremote.RouteSpec) bool {
						return route.Method == http.MethodPost &&
							route.Path == "/v1/apps/:app/archives/:archive/query"
					},
				)
			},
		},
		{
			name: "zero reader queries",
			want: "required reader query gitcrawl.threads.search",
			mutate: func(contract *crawlremote.Contract) {
				contract.Apps[0].Queries = nil
			},
		},
		{
			name: "thread search missing mode and limit",
			want: "reader query gitcrawl.threads.search has arguments",
			mutate: func(contract *crawlremote.Contract) {
				contract.Apps[0].Queries[0].Args = []string{
					"owner", "repo", "query", "kind", "state",
				}
			},
		},
		{
			name: "thread search duplicate argument",
			want: "reader query gitcrawl.threads.search has arguments",
			mutate: func(contract *crawlremote.Contract) {
				contract.Apps[0].Queries[0].Args = []string{
					"owner", "repo", "query", "kind", "state", "mode", "mode",
				}
			},
		},
		{
			name: "duplicate reader query",
			want: "required reader query gitcrawl.threads.search more than once",
			mutate: func(contract *crawlremote.Contract) {
				contract.Apps[0].Queries = append(
					contract.Apps[0].Queries,
					contract.Apps[0].Queries[0],
				)
			},
		},
	}
	for _, required := range gitcrawlCloudReaderQuerySpecs() {
		tests = append(tests, remoteSurfaceTest{
			name: "missing reader query " + required.Name,
			want: "required reader query " + required.Name,
			mutate: func(contract *crawlremote.Contract) {
				contract.Apps[0].Queries = slices.DeleteFunc(
					contract.Apps[0].Queries,
					func(query crawlremote.QuerySpec) bool {
						return query.Name == required.Name
					},
				)
			},
		})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.toml")
			dbPath := filepath.Join(dir, "gitcrawl.db")
			cfg := config.Default()
			cfg.DBPath = dbPath
			if err := config.Save(configPath, cfg); err != nil {
				t.Fatalf("save config: %v", err)
			}
			seedCommandFlowStore(t, dbPath)

			const tokenEnv = "GITCRAWL_TEST_SURFACE_CONTRACT_TOKEN"
			t.Setenv(tokenEnv, "publish-token")
			mutations := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.EscapedPath() == "/v1/contract" {
					contract := testSnapshotPublishContract()
					test.mutate(&contract)
					w.Header().Set("content-type", "application/json")
					_ = json.NewEncoder(w).Encode(contract)
					return
				}
				mutations++
				http.Error(w, "unexpected mutation", http.StatusInternalServerError)
			}))
			defer server.Close()

			args := []string{
				"--config", configPath,
				"cloud", "publish",
				"--remote", server.URL,
				"--archive", "gitcrawl/openclaw__openclaw",
				"--token-env", tokenEnv,
				"--allow-incomplete",
				"--json",
			}
			args = append(args, test.extraArgs...)
			err := New().Run(ctx, args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("cloud publish error = %v", err)
			}
			if mutations != 0 {
				t.Fatalf("remote mutations = %d, want 0", mutations)
			}
		})
	}
}

func TestCloudPublishStageOnlyThenResumesDefaultCutover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	cfg := config.Default()
	cfg.DBPath = dbPath
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	const tokenEnv = "GITCRAWL_TEST_RESUME_TOKEN"
	t.Setenv(tokenEnv, "publish-token")
	var snapshotID string
	var stagedSnapshot *crawlremote.ArchiveSnapshot
	servingSnapshotID := strings.Repeat("f", 64)
	ingestRequests := 0
	publisherStatusRequests := 0
	readerStatusRequests := 0
	cutovers := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.EscapedPath() == "/v1/contract" {
			contract := testSnapshotPublishContract()
			contract.Apps[0].Capabilities = append(
				contract.Apps[0].Capabilities,
				gitcrawlObservationOrderCapability,
			)
			for index := range contract.Apps[0].IngestTables {
				table := &contract.Apps[0].IngestTables[index]
				if table.Name == "threads" || table.Name == "thread_revisions" {
					table.Columns = append(table.Columns, "observation_sequence")
				}
			}
			w.Header().Set("content-type", "application/json")
			_ = json.NewEncoder(w).Encode(contract)
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/status") {
			readerStatusRequests++
			if r.Header.Get("authorization") != "Bearer reader-token" {
				http.Error(w, "publisher token has no reader role", http.StatusForbidden)
				return
			}
			_ = json.NewEncoder(w).Encode(crawlremote.Status{
				App:              "gitcrawl",
				Archive:          "gitcrawl/openclaw__openclaw",
				ActiveSnapshotID: servingSnapshotID,
				CoverageComplete: true,
			})
			return
		}
		if got := r.Header.Get("authorization"); got != "Bearer publish-token" {
			http.Error(w, "missing bearer", http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/publish-status") {
			publisherStatusRequests++
			activeSnapshotID := ""
			if stagedSnapshot != nil {
				activeSnapshotID = stagedSnapshot.ID
			}
			_ = json.NewEncoder(w).Encode(crawlremote.PublisherStatus{
				App:              "gitcrawl",
				Archive:          "gitcrawl/openclaw__openclaw",
				ActiveSnapshotID: activeSnapshotID,
				CoverageComplete: stagedSnapshot != nil,
				Snapshot:         stagedSnapshot,
			})
			return
		}
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.EscapedPath(), "/sqlite") {
			w.Header().Set("content-type", "application/json")
			switch r.Header.Get("x-crawl-sqlite-upload") {
			case "bundle-part":
				snapshotID = r.Header.Get("x-crawl-snapshot-id")
				partSHA := r.Header.Get("x-crawl-content-sha256")
				partIndex, err := strconv.Atoi(r.Header.Get("x-crawl-bundle-part-index"))
				if len(snapshotID) != 64 || len(partSHA) != 64 || err != nil {
					http.Error(w, "invalid bundle part", http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(crawlremote.SQLiteUploadResult{
					App:      "gitcrawl",
					Archive:  "gitcrawl/openclaw__openclaw",
					Complete: false,
					Object: &crawlremote.SQLiteObject{
						Key: crawlremote.SQLiteSnapshotBundlePartKey(
							"gitcrawl",
							"gitcrawl/openclaw__openclaw",
							snapshotID,
							partSHA,
							partIndex,
						),
					},
				})
			case "bundle-manifest":
				var manifest crawlremote.SQLiteBundleManifest
				if err := json.NewDecoder(r.Body).Decode(&manifest); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if manifest.SnapshotID != snapshotID {
					http.Error(w, "snapshot mismatch", http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(crawlremote.SQLiteBundleUploadResult{
					App:      "gitcrawl",
					Archive:  "gitcrawl/openclaw__openclaw",
					Complete: true,
					Bundle: &crawlremote.SQLiteBundle{
						Key:      crawlremote.SQLiteSnapshotBundleManifestKey("gitcrawl", "gitcrawl/openclaw__openclaw", snapshotID),
						Manifest: &manifest,
					},
				})
			default:
				http.Error(w, "missing upload kind", http.StatusBadRequest)
			}
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.EscapedPath(), "/ingest") {
			ingestRequests++
			if stagedSnapshot != nil {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":   "snapshot_active",
					"message": "activated Gitcrawl snapshots are immutable",
				})
				return
			}
			var body crawlremote.IngestRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if body.Manifest.SnapshotID != snapshotID ||
				body.Manifest.SourceSHA256 != snapshotID {
				http.Error(w, "ingest provenance mismatch", http.StatusBadRequest)
				return
			}
			mutationToken := fmt.Sprintf("resume-mutation-%d", ingestRequests)
			if body.Final {
				if body.Table != "dataset_coverage" || body.MutationToken == "" {
					http.Error(w, "invalid snapshot activation", http.StatusBadRequest)
					return
				}
				mutationToken = body.MutationToken
				stagedSnapshot = &crawlremote.ArchiveSnapshot{
					ID:                 body.Manifest.SnapshotID,
					SourceSHA256:       body.Manifest.SourceSHA256,
					SourceSyncAt:       body.Manifest.SourceSyncAt,
					DatasetGeneratedAt: fmt.Sprint(body.Rows[0][5]),
					SchemaName:         body.Manifest.SchemaName,
					SchemaVersion:      body.Manifest.SchemaVersion,
					SchemaHash:         body.Manifest.SchemaHash,
					Capabilities: gitcrawlCloudPublicationCapabilities(
						body.Manifest.Capabilities,
					),
					CoverageComplete: true,
				}
			}
			_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{
				Table:         body.Table,
				SnapshotID:    body.Manifest.SnapshotID,
				MutationToken: mutationToken,
				RowsAccepted:  int64(len(body.Rows)),
				Complete:      body.Final,
			})
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.EscapedPath(), "/cutover") {
			var request struct {
				SnapshotID string `json:"snapshot_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil ||
				request.SnapshotID != snapshotID {
				http.Error(w, "cutover snapshot mismatch", http.StatusBadRequest)
				return
			}
			if stagedSnapshot == nil || stagedSnapshot.ID != request.SnapshotID {
				http.Error(w, "snapshot is not staged", http.StatusConflict)
				return
			}
			cutovers++
			servingSnapshotID = request.SnapshotID
			_ = json.NewEncoder(w).Encode(crawlremote.CutoverResult{
				Archive:      "gitcrawl/openclaw__openclaw",
				SnapshotID:   snapshotID,
				SnapshotMode: "snapshot",
				CutoverAt:    "2026-07-12T11:00:00Z",
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	baseArgs := []string{
		"--config", configPath,
		"cloud", "publish",
		"--remote", server.URL,
		"--archive", "gitcrawl/openclaw__openclaw",
		"--token-env", tokenEnv,
		"--allow-incomplete",
		"--observation-order",
		"--json",
	}
	stageIngestRequests := 0
	originalServingSnapshotID := servingSnapshotID
	var stagedDatasetGeneratedAt string
	for index, extra := range [][]string{{"--stage-only"}, nil} {
		app := New()
		var output bytes.Buffer
		app.Stdout = &output
		args := append(append([]string(nil), baseArgs...), extra...)
		if err := app.Run(ctx, args); err != nil {
			t.Fatalf("cloud publish run %d: %v", index+1, err)
		}
		var result struct {
			DatasetGeneratedAt string                     `json:"dataset_generated_at"`
			AlreadyStaged      bool                       `json:"already_staged"`
			MutationToken      string                     `json:"mutation_token"`
			Cutover            *crawlremote.CutoverResult `json:"cutover"`
		}
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatalf("decode run %d output: %v\n%s", index+1, err, output.String())
		}
		if index == 0 && result.Cutover != nil {
			t.Fatalf("stage-only publish unexpectedly cut over: %#v", result.Cutover)
		}
		if index == 0 && result.AlreadyStaged {
			t.Fatal("initial stage-only publish unexpectedly reused a candidate")
		}
		if index == 0 {
			stageIngestRequests = ingestRequests
			if stageIngestRequests == 0 || stagedSnapshot == nil {
				t.Fatal("stage-only publish did not complete the candidate snapshot")
			}
			if servingSnapshotID != originalServingSnapshotID {
				t.Fatalf("stage-only changed serving snapshot to %q", servingSnapshotID)
			}
			stagedDatasetGeneratedAt = result.DatasetGeneratedAt
			if stagedDatasetGeneratedAt == "" || result.MutationToken == "" {
				t.Fatalf("stage-only did not bind its generation: %#v", result)
			}
			time.Sleep(2 * time.Millisecond)
		}
		if index == 1 {
			if !result.AlreadyStaged || result.Cutover == nil {
				t.Fatalf("default publish did not resume the staged candidate: %s", output.String())
			}
			if result.DatasetGeneratedAt != stagedDatasetGeneratedAt {
				t.Fatalf(
					"resumed generation = %q, want staged generation %q",
					result.DatasetGeneratedAt,
					stagedDatasetGeneratedAt,
				)
			}
			if result.MutationToken != "" {
				t.Fatalf("resumed publish minted mutation token %q", result.MutationToken)
			}
		}
	}
	if ingestRequests != stageIngestRequests {
		t.Fatalf(
			"ingest requests = %d after complete resume, want stage-only count %d",
			ingestRequests,
			stageIngestRequests,
		)
	}
	if cutovers != 1 {
		t.Fatalf("cutovers = %d, want 1", cutovers)
	}
	if servingSnapshotID != snapshotID {
		t.Fatalf("serving snapshot = %q, want staged snapshot %q", servingSnapshotID, snapshotID)
	}
	if publisherStatusRequests != 2 {
		t.Fatalf("publisher status requests = %d, want 2", publisherStatusRequests)
	}
	if readerStatusRequests != 0 {
		t.Fatalf("reader status requests = %d, publisher-only flow must not require reader role", readerStatusRequests)
	}
}

func TestCloudPublishRejectsIncompleteHydrationBeforeRemoteMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	cfg := config.Default()
	cfg.DBPath = dbPath
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	tokenEnv := "GITCRAWL_TEST_INCOMPLETE_PUBLISH_TOKEN"
	t.Setenv(tokenEnv, "publish-token")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	err := New().Run(ctx, []string{
		"--config", cfgPath,
		"cloud", "publish",
		"--remote", server.URL,
		"--archive", "gitcrawl/openclaw__openclaw",
		"--token-env", tokenEnv,
		"--json",
	})
	if err == nil || !strings.Contains(err.Error(), "cloud snapshot enrichment is incomplete") {
		t.Fatalf("cloud publish error = %v", err)
	}
	if requests != 0 {
		t.Fatalf("remote requests = %d, want 0", requests)
	}
}

func TestRemoteLoginStoresKeyringToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	keyring.MockInit()

	var pollSecretHash string
	var pollSecret string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		switch r.URL.Path {
		case "/v1/auth/github/start":
			var req crawlremote.LoginStartRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			pollSecretHash = req.PollSecretHash
			_ = json.NewEncoder(w).Encode(crawlremote.LoginStartResult{LoginID: "login-1", URL: server.URL + "/authorize"})
		case "/v1/auth/github/poll":
			var req crawlremote.LoginPollRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.LoginID != "login-1" {
				http.Error(w, "wrong login", http.StatusBadRequest)
				return
			}
			pollSecret = req.PollSecret
			_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "complete", Token: "session-token", Org: "openclaw", Login: "alice"})
		case "/v1/whoami":
			if r.Header.Get("authorization") != "Bearer session-token" {
				http.Error(w, "missing session token", http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(crawlremote.Identity{Owner: "openclaw", Org: "openclaw", Login: "alice", Auth: "github"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	configPath := filepath.Join(dir, "config.toml")
	app := New()
	var out bytes.Buffer
	app.Stdout = &out
	if err := app.Run(ctx, []string{
		"--config", configPath,
		"--json",
		"remote", "login",
		"--endpoint", server.URL,
		"--no-browser",
		"--timeout", "1s",
		"--poll-interval", "1ms",
	}); err != nil {
		t.Fatalf("remote login: %v", err)
	}
	if pollSecretHash == "" || pollSecret == "" || crawlremote.LoginPollSecretHash(pollSecret) != pollSecretHash {
		t.Fatalf("poll secret/hash not linked: secret=%q hash=%q", pollSecret, pollSecretHash)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Remote.Auth.TokenSource != "keyring" || cfg.Remote.Auth.KeyringService == "" || cfg.Remote.Auth.KeyringAccount == "" {
		t.Fatalf("remote auth = %#v", cfg.Remote.Auth)
	}
	stored, err := keyring.Get(cfg.Remote.Auth.KeyringService, cfg.Remote.Auth.KeyringAccount)
	if err != nil {
		t.Fatalf("keyring get: %v", err)
	}
	if stored != "session-token" {
		t.Fatalf("stored token = %q", stored)
	}

	whoami := New()
	var whoamiOut bytes.Buffer
	whoami.Stdout = &whoamiOut
	if err := whoami.Run(ctx, []string{"--config", configPath, "--json", "whoami"}); err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if !strings.Contains(whoamiOut.String(), `"login": "alice"`) {
		t.Fatalf("whoami output = %s", whoamiOut.String())
	}
}

func TestRemoteLoginWithGitHubTokenEnvStoresKeyringToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	keyring.MockInit()
	t.Setenv("GITCRAWL_TEST_GITHUB_TOKEN", "github-token")

	var sawToken string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		switch r.URL.Path {
		case "/v1/auth/github/token":
			var req crawlremote.GitHubTokenLoginRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			sawToken = req.Token
			_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "complete", Token: "session-token", Org: "openclaw", Login: "alice"})
		case "/v1/whoami":
			if r.Header.Get("authorization") != "Bearer session-token" {
				http.Error(w, "missing session token", http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(crawlremote.Identity{Owner: "openclaw", Org: "openclaw", Login: "alice", Auth: "github"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	configPath := filepath.Join(dir, "config.toml")
	app := New()
	var out bytes.Buffer
	app.Stdout = &out
	if err := app.Run(ctx, []string{
		"--config", configPath,
		"--json",
		"remote", "login",
		"--endpoint", server.URL,
		"--github-token-env", "GITCRAWL_TEST_GITHUB_TOKEN",
	}); err != nil {
		t.Fatalf("remote login: %v", err)
	}
	if sawToken != "github-token" {
		t.Fatalf("github token = %q", sawToken)
	}
	if !strings.Contains(out.String(), `"login_method": "github-token"`) {
		t.Fatalf("login output = %s", out.String())
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	stored, err := keyring.Get(cfg.Remote.Auth.KeyringService, cfg.Remote.Auth.KeyringAccount)
	if err != nil {
		t.Fatalf("keyring get: %v", err)
	}
	if stored != "session-token" {
		t.Fatalf("stored token = %q", stored)
	}

	whoami := New()
	var whoamiOut bytes.Buffer
	whoami.Stdout = &whoamiOut
	if err := whoami.Run(ctx, []string{"--config", configPath, "--json", "whoami"}); err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if !strings.Contains(whoamiOut.String(), `"login": "alice"`) {
		t.Fatalf("whoami output = %s", whoamiOut.String())
	}
}

func TestMetadataStatusAndControlStatusJSON(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := os.WriteFile(dbPath+"-wal", []byte("wal"), 0o600); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "metadata", args: []string{"--config", configPath, "metadata", "--json"}, want: "commands"},
		{name: "status", args: []string{"--config", configPath, "status", "--json"}, want: "databases"},
		{name: "status missing config", args: []string{"--config", filepath.Join(dir, "missing.toml"), "status", "--json"}, want: "counts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := New()
			var stdout bytes.Buffer
			app.Stdout = &stdout
			if err := app.Run(ctx, tc.args); err != nil {
				t.Fatalf("run %s: %v", tc.name, err)
			}
			var payload map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
				t.Fatalf("decode %s output %q: %v", tc.name, stdout.String(), err)
			}
			if payload["app_id"] != "gitcrawl" && payload["id"] != "gitcrawl" {
				t.Fatalf("expected gitcrawl payload, got %#v", payload)
			}
			if _, ok := payload[tc.want]; !ok {
				t.Fatalf("expected %s in %#v", tc.want, payload)
			}
		})
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	sizePath := filepath.Join(dir, "sized.db")
	if err := os.WriteFile(sizePath, []byte("db"), 0o600); err != nil {
		t.Fatalf("write sized db: %v", err)
	}
	if err := os.WriteFile(sizePath+"-wal", []byte("wal"), 0o600); err != nil {
		t.Fatalf("write sized wal: %v", err)
	}
	lastSync := time.Unix(100, 0)
	out := controlStatus(configPath, cfg, store.Status{
		DBPath:          sizePath,
		RepositoryCount: 2,
		ThreadCount:     3,
		OpenThreadCount: 1,
		ClusterCount:    4,
		LastSyncAt:      lastSync,
	})
	if out.DatabaseBytes == 0 {
		t.Fatalf("database bytes should be populated: %#v", out)
	}
	if out.WALBytes != 3 {
		t.Fatalf("wal bytes = %d, want 3", out.WALBytes)
	}
	if out.LastSyncAt != lastSync.UTC().Format(time.RFC3339) {
		t.Fatalf("last sync = %q", out.LastSyncAt)
	}
	if len(out.Databases) != 1 || out.Databases[0].Path != sizePath || !out.Databases[0].IsPrimary {
		t.Fatalf("database metadata = %#v", out.Databases)
	}
	if got := fileSize(filepath.Join(dir, "missing.db")); got != 0 {
		t.Fatalf("missing file size = %d, want 0", got)
	}

	var helpOut bytes.Buffer
	help := New()
	help.Stdout = &helpOut
	if err := help.printCommandUsage("portable"); err != nil {
		t.Fatalf("portable help: %v", err)
	}
	if !strings.Contains(helpOut.String(), "portable") {
		t.Fatalf("portable help output = %q", helpOut.String())
	}
	helpOut.Reset()
	if err := help.printCommandUsage("tui"); err != nil {
		t.Fatalf("tui help: %v", err)
	}
	if !strings.Contains(helpOut.String(), "cluster browser") {
		t.Fatalf("tui help output = %q", helpOut.String())
	}
	for _, topic := range []string{"metadata", "status", "remote", "whoami", "init", "configure", "doctor", "sync", "refresh", "summarize", "embed", "threads", "search", "code", "cluster", "clusters", "clusters-report", "durable-clusters", "cluster-detail", "cluster-explain", "neighbors", "runs", "close-thread", "reopen-thread", "close-cluster", "reopen-cluster", "exclude-cluster-member", "include-cluster-member", "set-cluster-canonical", "gh"} {
		helpOut.Reset()
		if err := help.printCommandUsage(topic); err != nil {
			t.Fatalf("%s help: %v", topic, err)
		}
		if !strings.Contains(helpOut.String(), "Usage:") {
			t.Fatalf("%s help output = %q", topic, helpOut.String())
		}
	}
	helpOut.Reset()
	if err := help.printCommandUsage("refresh"); err != nil {
		t.Fatalf("refresh help: %v", err)
	}
	if strings.Contains(helpOut.String(), "--sync-if-stale") {
		t.Fatalf("refresh help should not advertise search-only --sync-if-stale: %q", helpOut.String())
	}
	if err := New().Run(ctx, []string{"--config", configPath, "status", "extra"}); err == nil {
		t.Fatal("status extra arg should fail")
	}
}

func TestControlRepositoryAndClusterHelperBranches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "gitcrawl.db")
	payload := emptyClusterBrowserPayload(ctx, cfg, "", "recent", "focus", 2, 50, true)
	if payload.DBSource != "local" || payload.DBLocation != "gitcrawl.db" {
		t.Fatalf("empty payload source = %s/%s", payload.DBSource, payload.DBLocation)
	}
	if payload.Sort != "recent" || payload.Layout != "focus" || payload.MinSize != 2 || payload.Limit != 50 || !payload.HideClosed {
		t.Fatalf("empty payload options = %#v", payload)
	}

	rt := localRuntime{Config: cfg}
	if got := remoteRefreshSource(rt); got != "" {
		t.Fatalf("local refresh source = %q", got)
	}
	if got := remoteRuntimePath(rt); got != "" {
		t.Fatalf("local runtime path = %q", got)
	}
	rt.RemoteSource = true
	rt.SourceDBPath = filepath.Join(dir, "store", "data", "archive.db")
	if got := remoteRefreshSource(rt); got != rt.SourceDBPath {
		t.Fatalf("remote refresh source = %q", got)
	}
	if got := remoteRuntimePath(rt); got != cfg.DBPath {
		t.Fatalf("remote runtime path = %q", got)
	}

	if got := githubRepoFromRemote("git@github.com:openclaw/gitcrawl-store.git"); got != "openclaw/gitcrawl-store" {
		t.Fatalf("ssh remote repo = %q", got)
	}
	if got := githubRepoFromRemote("https://github.com/openclaw/gitcrawl-store.git"); got != "openclaw/gitcrawl-store" {
		t.Fatalf("https remote repo = %q", got)
	}
	if got := githubRepoFromRemote("ssh://git@github.com/openclaw/gitcrawl-store.git"); got != "openclaw/gitcrawl-store" {
		t.Fatalf("ssh url remote repo = %q", got)
	}
	if got := githubRepoFromRemote("https://example.com/openclaw/gitcrawl-store.git"); got != "" {
		t.Fatalf("non-github remote repo = %q", got)
	}
	if got := githubRepoFromRemote("https://github.com/openclaw"); got != "" {
		t.Fatalf("short github remote repo = %q", got)
	}

	with, err := parseSyncWith(" pr-details, ")
	if err != nil || !with["pr-details"] {
		t.Fatalf("parse sync with = %#v, %v", with, err)
	}
	if _, err := parseSyncWith("reviews"); err == nil {
		t.Fatal("unsupported sync --with value should fail")
	}
	maxSize, fanout, crossKind, err := parseClusterShapeOptions("cluster", "", "", "")
	if err != nil {
		t.Fatalf("default cluster shape: %v", err)
	}
	if maxSize != defaultClusterMaxSize || fanout != defaultClusterFanout || crossKind != defaultCrossKindMinScore {
		t.Fatalf("default cluster shape = %d/%d/%f", maxSize, fanout, crossKind)
	}
	if _, _, _, err := parseClusterShapeOptions("cluster", "2", "3", "1.5"); err == nil {
		t.Fatal("out-of-range cross-kind threshold should fail")
	}
	if !stateIncludesClosed("all") || !stateIncludesClosed(" closed ") || stateIncludesClosed("open") {
		t.Fatal("state closed helper mismatch")
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

func TestDatabaseSourceLocationLocal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	if got := databaseSourceKind(dbPath); got != "local" {
		t.Fatalf("source kind = %q, want local", got)
	}
	if got := databaseSourceLocation(context.Background(), dbPath); got != "gitcrawl.db" {
		t.Fatalf("source location = %q, want db filename", got)
	}
}

func TestDatabaseSourceLocationRemoteGitHubStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "gitcrawl-store")
	dbPath := filepath.Join(storeDir, "data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir store data: %v", err)
	}
	if err := runGit(ctx, storeDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := runGit(ctx, storeDir, "remote", "add", "origin", "https://github.com/openclaw/gitcrawl-store.git"); err != nil {
		t.Fatalf("git remote add: %v", err)
	}

	if got := databaseSourceKind(dbPath); got != "remote" {
		t.Fatalf("source kind = %q, want remote", got)
	}
	want := "openclaw/gitcrawl-store:openclaw__openclaw.sync.db"
	if got := databaseSourceLocation(ctx, dbPath); got != want {
		t.Fatalf("source location = %q, want %q", got, want)
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
	if err := os.WriteFile(filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db-wal"), []byte("stale wal"), 0o644); err != nil {
		t.Fatalf("write stale wal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db-shm"), []byte("stale shm"), 0o644); err != nil {
		t.Fatalf("write stale shm: %v", err)
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
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db"+suffix)); !os.IsNotExist(err) {
			t.Fatalf("stale sqlite sidecar %s was not removed: %v", suffix, err)
		}
	}
}

func TestSyncPortableStoreIgnoresBrokenPullRebaseConfig(t *testing.T) {
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
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("initial portable sync: %v", err)
	}
	if err := runGit(ctx, "", "-C", checkoutDir, "config", "pull.rebase", "true"); err != nil {
		t.Fatalf("set pull rebase: %v", err)
	}
	if err := runGit(ctx, "", "-C", checkoutDir, "config", "--add", "branch.main.merge", "refs/heads/backup"); err != nil {
		t.Fatalf("add second merge branch: %v", err)
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

	action, err := syncPortableStore(ctx, remoteDir, checkoutDir)
	if err != nil {
		t.Fatalf("portable sync with broken pull config: %v", err)
	}
	if action != "pulled" {
		t.Fatalf("action = %q, want pulled", action)
	}
	got, err := os.ReadFile(filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db"))
	if err != nil {
		t.Fatalf("read checkout db: %v", err)
	}
	if string(got) != "remote-v2" {
		t.Fatalf("checkout db = %q, want remote-v2", string(got))
	}
}

func TestSyncPortableStoreRejectsDifferentExistingCheckout(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	otherRemoteDir := filepath.Join(dir, "other-remote")
	checkoutDir := filepath.Join(dir, "checkout")
	for _, repoDir := range []string{remoteDir, otherRemoteDir} {
		if err := os.MkdirAll(filepath.Join(repoDir, "data"), 0o755); err != nil {
			t.Fatalf("mkdir repo: %v", err)
		}
		if err := runGit(ctx, repoDir, "init", "-b", "main"); err != nil {
			t.Fatalf("git init: %v", err)
		}
		if err := os.WriteFile(filepath.Join(repoDir, "data", "openclaw__openclaw.sync.db"), []byte(filepath.Base(repoDir)), 0o644); err != nil {
			t.Fatalf("write db: %v", err)
		}
		if err := runGit(ctx, repoDir, "add", "data/openclaw__openclaw.sync.db"); err != nil {
			t.Fatalf("git add: %v", err)
		}
		if err := runGit(ctx, repoDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
			t.Fatalf("git commit seed: %v", err)
		}
	}
	if _, err := syncPortableStore(ctx, otherRemoteDir, checkoutDir); err != nil {
		t.Fatalf("initial portable sync: %v", err)
	}
	dirtyPath := filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db")
	if err := os.WriteFile(dirtyPath, []byte("dirty local data"), 0o644); err != nil {
		t.Fatalf("dirty checkout: %v", err)
	}

	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err == nil {
		t.Fatal("mismatched existing checkout should fail")
	}
	got, err := os.ReadFile(dirtyPath)
	if err != nil {
		t.Fatalf("read dirty checkout: %v", err)
	}
	if string(got) != "dirty local data" {
		t.Fatalf("mismatched checkout was modified: %q", string(got))
	}
}

func TestSyncPortableStoreHonorsBranchRemote(t *testing.T) {
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
		t.Fatalf("write db: %v", err)
	}
	if err := runGit(ctx, remoteDir, "add", "data/openclaw__openclaw.sync.db"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("initial portable sync: %v", err)
	}
	if err := runGit(ctx, "", "-C", checkoutDir, "remote", "rename", "origin", "store"); err != nil {
		t.Fatalf("rename remote: %v", err)
	}
	if got := portableStoreRemoteURL(ctx, checkoutDir); got != remoteDir {
		t.Fatalf("portable store remote URL = %q, want %q", got, remoteDir)
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

	action, err := syncPortableStore(ctx, remoteDir, checkoutDir)
	if err != nil {
		t.Fatalf("portable sync with renamed remote: %v", err)
	}
	if action != "pulled" {
		t.Fatalf("action = %q, want pulled", action)
	}
	got, err := os.ReadFile(filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db"))
	if err != nil {
		t.Fatalf("read checkout db: %v", err)
	}
	if string(got) != "remote-v2" {
		t.Fatalf("checkout db = %q, want remote-v2", string(got))
	}
}

func TestSameGitRemoteKeepsLocalDotGitDistinct(t *testing.T) {
	dir := t.TempDir()
	repoDir := filepath.Join(dir, "store")
	bareRepoDir := filepath.Join(dir, "store.git")

	if sameGitRemote(repoDir, bareRepoDir) {
		t.Fatalf("local paths %q and %q must not compare equal", repoDir, bareRepoDir)
	}
}

func TestSameGitRemotePreservesSCPPathCase(t *testing.T) {
	if !sameGitRemote("git@Example.com:Team/Store.git", "git@example.com:Team/Store") {
		t.Fatal("scp-style host case and .git suffix should normalize")
	}
	if sameGitRemote("git@example.com:Team/Store.git", "git@example.com:team/store.git") {
		t.Fatal("scp-style repository path case must stay distinct")
	}
}

func TestSameGitRemoteIgnoresHTTPSCredentials(t *testing.T) {
	if !sameGitRemote("https://user:old-token@example.com/org/store.git", "https://user:new-token@example.com/org/store") {
		t.Fatal("https credential changes should not change remote identity")
	}
}

func TestGitRemoteForMessageRedactsURLCredentials(t *testing.T) {
	got := gitRemoteForMessage("https://user:secret@example.com/org/store.git")
	if strings.Contains(got, "user") || strings.Contains(got, "secret") {
		t.Fatalf("remote message leaked credentials: %q", got)
	}
	if got != "https://example.com/org/store.git" {
		t.Fatalf("remote message = %q, want sanitized URL", got)
	}
}

func TestInitWithPortableStoreCloneAndPull(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Chdir(dir)
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := "checkout"
	wantCheckoutDir := filepath.Join(dir, checkoutDir)
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 7, "portable init issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--portable-store", remoteDir, "--store-dir", checkoutDir, "--portable-db", filepath.ToSlash(dbRel)}); err != nil {
		t.Fatalf("portable init: %v", err)
	}
	if !strings.Contains(stdout.String(), "Portable store") || !strings.Contains(stdout.String(), "cloned") {
		t.Fatalf("portable init output = %q", stdout.String())
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load portable config: %v", err)
	}
	if loaded.DBPath != filepath.Join(wantCheckoutDir, dbRel) {
		t.Fatalf("portable db path = %q, want %q", loaded.DBPath, filepath.Join(wantCheckoutDir, dbRel))
	}
	action, err := syncPortableStore(ctx, remoteDir, checkoutDir)
	if err != nil {
		t.Fatalf("portable pull: %v", err)
	}
	if action != "pulled" {
		t.Fatalf("portable action = %q, want pulled", action)
	}
	invalid := New()
	if err := invalid.Run(ctx, []string{"--config", filepath.Join(dir, "bad.toml"), "init", "--portable-store", remoteDir, "--store-dir", filepath.Join(dir, "bad-checkout"), "--portable-db", "../bad.db"}); err == nil {
		t.Fatal("invalid portable db should fail")
	}
	if _, err := syncPortableStore(ctx, "", checkoutDir); err == nil {
		t.Fatal("missing portable URL should fail")
	}
	if _, err := syncPortableStore(ctx, remoteDir, ""); err == nil {
		t.Fatal("missing portable dir should fail")
	}
	nonGitDir := filepath.Join(dir, "not-git")
	if err := os.MkdirAll(nonGitDir, 0o755); err != nil {
		t.Fatalf("mkdir non-git: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nonGitDir, "file"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write non-git file: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, nonGitDir); err == nil {
		t.Fatal("non-git existing dir should fail")
	}
}

func TestReadCommandRefreshesPortableStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "initial issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", filepath.Join(checkoutDir, dbRel)}); err != nil {
		t.Fatalf("init config: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 2, "refreshed issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add update: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "update store"); err != nil {
		t.Fatalf("git commit update: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "threads", "openclaw/openclaw", "--numbers", "2", "--json"}); err != nil {
		t.Fatalf("threads: %v", err)
	}
	if !strings.Contains(stdout.String(), "refreshed issue") {
		t.Fatalf("read command did not refresh portable store, got %q", stdout.String())
	}
	if !gitWorktreeClean(ctx, checkoutDir) {
		t.Fatal("portable checkout should stay clean after read-only command")
	}
	mirrorPath, err := run.portableRuntimeDBPath(filepath.Join(checkoutDir, dbRel))
	if err != nil {
		t.Fatalf("runtime db path: %v", err)
	}
	if _, err := os.Stat(mirrorPath); err != nil {
		t.Fatalf("runtime mirror db was not created: %v", err)
	}

	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 3, "too soon issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add second update: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "second update"); err != nil {
		t.Fatalf("git commit second update: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "threads", "openclaw/openclaw", "--numbers", "3", "--json"}); err != nil {
		t.Fatalf("threads within refresh ttl: %v", err)
	}
	if strings.Contains(stdout.String(), "too soon issue") {
		t.Fatalf("read command should not refresh portable store again within ttl, got %q", stdout.String())
	}
}

func TestReadCommandUsesCachedPortableStoreWhenRefreshFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "cached issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}
	if err := runGit(ctx, "", "-C", checkoutDir, "remote", "set-url", "origin", filepath.Join(dir, "missing-remote")); err != nil {
		t.Fatalf("break portable remote: %v", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", filepath.Join(checkoutDir, dbRel)}); err != nil {
		t.Fatalf("init config: %v", err)
	}
	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "threads", "openclaw/openclaw", "--numbers", "1", "--json"}); err != nil {
		t.Fatalf("threads should use cached portable store after refresh failure: %v", err)
	}
	if !strings.Contains(stdout.String(), "cached issue") {
		t.Fatalf("cached portable store was not queried, got %q", stdout.String())
	}
}

func TestReadCommandRepairsMalformedDirtyPortableStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "initial issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", filepath.Join(checkoutDir, dbRel)}); err != nil {
		t.Fatalf("init config: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 2, "repaired issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add update: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "update store"); err != nil {
		t.Fatalf("git commit update: %v", err)
	}

	checkoutDB := filepath.Join(checkoutDir, dbRel)
	if err := os.Truncate(checkoutDB, 128); err != nil {
		t.Fatalf("truncate checkout db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutDir, "README.md"), []byte("dirty generated dashboard\n"), 0o644); err != nil {
		t.Fatalf("dirty readme: %v", err)
	}
	run := New()
	run.configPath = configPath
	mirrorPath, err := run.portableRuntimeDBPath(checkoutDB)
	if err != nil {
		t.Fatalf("runtime db path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		t.Fatalf("mkdir mirror: %v", err)
	}
	if err := os.WriteFile(mirrorPath, []byte("malformed mirror"), 0o600); err != nil {
		t.Fatalf("write malformed mirror: %v", err)
	}

	var stdout bytes.Buffer
	run.Stdout = &stdout
	t.Setenv("GITCRAWL_PORTABLE_REFRESH_TTL", "0")
	if err := run.Run(ctx, []string{"--config", configPath, "threads", "openclaw/openclaw", "--numbers", "2", "--json"}); err != nil {
		t.Fatalf("threads should repair malformed portable store: %v", err)
	}
	if !strings.Contains(stdout.String(), "repaired issue") {
		t.Fatalf("repaired portable store was not queried, got %q", stdout.String())
	}
	if !gitWorktreeClean(ctx, checkoutDir) {
		t.Fatal("portable checkout should be clean after repair")
	}
	if err := sqliteStoreHealth(ctx, checkoutDB); err != nil {
		t.Fatalf("checkout db should be healthy after repair: %v", err)
	}
	if err := sqliteStoreHealth(ctx, mirrorPath); err != nil {
		t.Fatalf("runtime mirror should be healthy after repair: %v", err)
	}
}

func TestRecloneMalformedPortableStorePreservesBranch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "main issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add main seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed main store"); err != nil {
		t.Fatalf("git commit main seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "checkout", "-b", "portable"); err != nil {
		t.Fatalf("checkout portable branch: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 2, "portable branch issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add branch seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed branch store"); err != nil {
		t.Fatalf("git commit branch seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "checkout", "main"); err != nil {
		t.Fatalf("restore remote main branch: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}
	if err := runGit(ctx, "", "-C", checkoutDir, "fetch", "origin", "portable:refs/remotes/origin/portable"); err != nil {
		t.Fatalf("fetch portable branch: %v", err)
	}
	if err := runGit(ctx, "", "-C", checkoutDir, "checkout", "-b", "portable", "origin/portable"); err != nil {
		t.Fatalf("checkout portable branch: %v", err)
	}
	checkoutDB := filepath.Join(checkoutDir, dbRel)
	if err := os.Truncate(checkoutDB, 128); err != nil {
		t.Fatalf("truncate checkout db: %v", err)
	}
	if _, err := recloneMalformedPortableStoreForDB(ctx, checkoutDB, filepath.Join(dir, "config.toml")); err != nil {
		t.Fatalf("reclone malformed portable store: %v", err)
	}
	if got := currentGitBranch(ctx, checkoutDir); got != "portable" {
		t.Fatalf("recloned branch = %q, want portable", got)
	}
	st, err := store.OpenReadOnly(ctx, checkoutDB)
	if err != nil {
		t.Fatalf("open recloned db: %v", err)
	}
	defer st.Close()
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("read repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(threads) != 2 {
		t.Fatalf("recloned db threads = %d, want branch contents with 2 threads", len(threads))
	}
}

func TestPortableRuntimePropagatesNonCorruptionSourceErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "healthy issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}

	checkoutDB := filepath.Join(checkoutDir, dbRel)
	run := New()
	run.configPath = filepath.Join(dir, "config.toml")
	mirrorPath, err := run.portableRuntimeDBPath(checkoutDB)
	if err != nil {
		t.Fatalf("runtime db path: %v", err)
	}
	if err := copyFileAtomic(checkoutDB, mirrorPath); err != nil {
		t.Fatalf("seed healthy mirror: %v", err)
	}
	st, err := store.Open(ctx, checkoutDB)
	if err != nil {
		t.Fatalf("open checkout db: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma user_version = 99`); err != nil {
		_ = st.Close()
		t.Fatalf("set newer schema: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close checkout db: %v", err)
	}

	changed, err := refreshPortableRuntimeDB(ctx, checkoutDB, mirrorPath, false, run.configPath)
	if err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("newer source schema should be reported, changed=%v err=%v", changed, err)
	}
	if changed {
		t.Fatal("newer source schema should not refresh the mirror")
	}
	if err := sqliteStoreHealth(ctx, mirrorPath); err != nil {
		t.Fatalf("healthy mirror should remain usable: %v", err)
	}
}

func TestPortableRuntimeRejectsManifestMismatchBeforeReplacingMirror(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	checkoutDB := filepath.Join(checkoutDir, dbRel)
	mirrorPath := filepath.Join(dir, "runtime", dbRel)
	if err := os.MkdirAll(filepath.Dir(checkoutDB), 0o755); err != nil {
		t.Fatalf("mkdir checkout db: %v", err)
	}
	if err := runGit(ctx, checkoutDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, checkoutDB, 1, "source issue")
	if err := copyFileAtomic(checkoutDB, mirrorPath); err != nil {
		t.Fatalf("seed mirror: %v", err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(mirrorPath, past, past); err != nil {
		t.Fatalf("age mirror db: %v", err)
	}
	before, err := fileSHA256(mirrorPath)
	if err != nil {
		t.Fatalf("hash mirror before: %v", err)
	}
	seedPortableThread(t, checkoutDB, 2, "new source issue")
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(checkoutDB, future, future); err != nil {
		t.Fatalf("age source db: %v", err)
	}
	badManifest := portableDBManifest{OutputBytes: 1, SHA256: strings.Repeat("0", 64)}
	data, err := json.Marshal(badManifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(portableDBManifestPath(checkoutDB), data, 0o644); err != nil {
		t.Fatalf("write bad manifest: %v", err)
	}
	if err := copySQLiteFileAtomicVerified(ctx, checkoutDB, mirrorPath); err == nil || !strings.Contains(err.Error(), "portable manifest mismatch") {
		t.Fatalf("manifest mismatch should fail without copy, err=%v", err)
	}
	after, err := fileSHA256(mirrorPath)
	if err != nil {
		t.Fatalf("hash mirror after: %v", err)
	}
	if after != before {
		t.Fatal("runtime mirror should not be replaced by source with bad manifest")
	}
	if err := os.WriteFile(portableDBManifestPath(checkoutDB), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write incomplete manifest: %v", err)
	}
	if err := copySQLiteFileAtomicVerified(ctx, checkoutDB, mirrorPath); err == nil || !strings.Contains(err.Error(), "schema missing") {
		t.Fatalf("incomplete manifest should fail without copy, err=%v", err)
	}
	info, err := os.Stat(checkoutDB)
	if err != nil {
		t.Fatalf("stat checkout db: %v", err)
	}
	sum, err := fileSHA256(checkoutDB)
	if err != nil {
		t.Fatalf("hash checkout db: %v", err)
	}
	legacyManifest := portableDBManifest{
		Schema:      "ghcrawl-portable-sync-v1",
		OutputBytes: info.Size(),
		SHA256:      fmt.Sprintf("%x", sum),
	}
	data, err = json.Marshal(legacyManifest)
	if err != nil {
		t.Fatalf("marshal legacy manifest: %v", err)
	}
	if err := os.WriteFile(portableDBManifestPath(checkoutDB), data, 0o644); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}
	if err := validatePortableSQLiteFile(ctx, checkoutDB, checkoutDB); err != nil {
		t.Fatalf("legacy manifest without quickCheck should remain valid: %v", err)
	}
	validManifest := portableDBManifest{
		Schema:      "gitcrawl-portable-sync-v2",
		OutputBytes: info.Size(),
		SHA256:      fmt.Sprintf("%x", sum),
		QuickCheck:  "ok",
	}
	data, err = json.Marshal(validManifest)
	if err != nil {
		t.Fatalf("marshal valid manifest: %v", err)
	}
	if err := os.WriteFile(portableDBManifestPath(checkoutDB), data, 0o644); err != nil {
		t.Fatalf("write valid manifest: %v", err)
	}
	futureMirror := time.Now().Add(time.Hour)
	if err := os.Chtimes(mirrorPath, futureMirror, futureMirror); err != nil {
		t.Fatalf("age stale mirror db: %v", err)
	}
	changed, err := refreshPortableRuntimeDB(ctx, checkoutDB, mirrorPath, false, filepath.Join(dir, "config.toml"))
	if err != nil || !changed {
		t.Fatalf("runtime mirror manifest mismatch should recopy, changed=%v err=%v", changed, err)
	}
	after, err = fileSHA256(mirrorPath)
	if err != nil {
		t.Fatalf("hash mirror after recopy: %v", err)
	}
	if after != sum {
		t.Fatal("runtime mirror should match manifest source after recopy")
	}
	if err := os.WriteFile(portableDBManifestPath(checkoutDB), []byte("{"), 0o644); err != nil {
		t.Fatalf("write malformed manifest: %v", err)
	}
	err = validatePortableSQLiteFile(ctx, checkoutDB, checkoutDB)
	if err == nil || !isPortableSourceRepairableHealthError(err) {
		t.Fatalf("malformed manifest should be a repairable health error, got %v", err)
	}
}

func TestWritableRuntimeUsesPortableMirror(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "portable issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", filepath.Join(checkoutDir, dbRel)}); err != nil {
		t.Fatalf("init config: %v", err)
	}

	run := New()
	run.configPath = configPath
	rt, err := run.openLocalRuntime(ctx)
	if err != nil {
		t.Fatalf("open writable runtime: %v", err)
	}
	repo, err := rt.repository(ctx, "openclaw", "openclaw")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	threads, err := rt.Store.ListThreadsFiltered(ctx, store.ThreadListOptions{RepoID: repo.ID, Numbers: []int{1}})
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(threads) != 1 {
		t.Fatalf("threads = %d, want 1", len(threads))
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := rt.Store.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID:    threads[0].ID,
		Basis:       "title_original",
		Model:       "text-embedding-3-small",
		Dimensions:  3,
		ContentHash: "hash-vector",
		Vector:      []float64{0.1, 0.2, 0.3},
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("upsert runtime vector: %v", err)
	}
	if err := rt.Store.Close(); err != nil {
		t.Fatalf("close writable runtime: %v", err)
	}
	if rt.SourceDBPath == rt.Config.DBPath {
		t.Fatalf("runtime db path should differ from portable source: %s", rt.Config.DBPath)
	}
	if !gitWorktreeClean(ctx, checkoutDir) {
		t.Fatal("portable checkout should stay clean after writable runtime command")
	}

	read := New()
	read.configPath = configPath
	readRT, err := read.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		t.Fatalf("open read-only runtime: %v", err)
	}
	defer readRT.Store.Close()
	if _, _, err := readRT.Store.ThreadVectorByNumber(ctx, store.ThreadVectorQuery{
		RepoID:     repo.ID,
		Model:      "text-embedding-3-small",
		Basis:      "title_original",
		Dimensions: 3,
	}, 1); err != nil {
		t.Fatalf("read runtime vector: %v", err)
	}
}

func TestDoctorRefreshesPortableStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "initial issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", filepath.Join(checkoutDir, dbRel)}); err != nil {
		t.Fatalf("init config: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 2, "refreshed issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add update: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "update store"); err != nil {
		t.Fatalf("git commit update: %v", err)
	}

	doctor := New()
	var stdout bytes.Buffer
	doctor.Stdout = &stdout
	if err := doctor.Run(ctx, []string{"--config", configPath, "doctor", "--json"}); err != nil {
		t.Fatalf("doctor: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("parse doctor json: %v\n%s", err, stdout.String())
	}
	if got := payload["thread_count"]; got != float64(2) {
		t.Fatalf("doctor thread_count = %#v, want 2; payload=%s", got, stdout.String())
	}
}

func TestDoctorJSONReportsCurrentSchemaDiagnosticsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gitcrawl.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	configPath := writeDoctorTestConfig(t, dir, dbPath)
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	payload := runDoctorJSON(t, ctx, configPath)
	schema := doctorMap(t, payload, "db_schema")
	if got := schema["state"]; got != "current" {
		t.Fatalf("db_schema.state = %#v, payload=%#v", got, schema)
	}
	if got := schema["current_version"]; got != float64(10) {
		t.Fatalf("db_schema.current_version = %#v, payload=%#v", got, schema)
	}
	if got := schema["supported_version"]; got != float64(10) {
		t.Fatalf("db_schema.supported_version = %#v, payload=%#v", got, schema)
	}
	if got := schema["child_observation_reservations"]; got != true {
		t.Fatalf("db_schema.child_observation_reservations = %#v, payload=%#v", got, schema)
	}
	prDetails := doctorMap(t, schema, "pr_details")
	if got := prDetails["duplicate_path_files_supported"]; got != true {
		t.Fatalf("pr_details.duplicate_path_files_supported = %#v, payload=%#v", got, prDetails)
	}
	runtime := doctorMap(t, payload, "runtime")
	if runtime["version"] != version || strings.TrimSpace(fmt.Sprint(runtime["executable_path"])) == "" {
		t.Fatalf("runtime identity = %#v", runtime)
	}
	if strings.TrimSpace(fmt.Sprint(payload["executable_path"])) == "" {
		t.Fatalf("top-level executable_path missing: %#v", payload)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("doctor schema diagnostics mutated current database bytes")
	}
}

func TestDoctorJSONReportsMissingSchemaWithoutCreatingDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "missing", "gitcrawl.db")
	configPath := writeDoctorTestConfig(t, dir, dbPath)

	payload := runDoctorJSON(t, ctx, configPath)
	schema := doctorMap(t, payload, "db_schema")
	if got := schema["state"]; got != "missing" {
		t.Fatalf("db_schema.state = %#v, payload=%#v", got, schema)
	}
	if got := schema["exists"]; got != false {
		t.Fatalf("db_schema.exists = %#v, payload=%#v", got, schema)
	}
	if len(doctorStringList(t, schema, "next_steps")) == 0 {
		t.Fatalf("db_schema.next_steps empty: %#v", schema)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("doctor created missing db: %v", err)
	}
}

func TestDoctorJSONReportsNewerSchemaWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gitcrawl.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma user_version = 99`); err != nil {
		_ = st.Close()
		t.Fatalf("set newer schema: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	configPath := writeDoctorTestConfig(t, dir, dbPath)
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	doctor := New()
	var stdout bytes.Buffer
	doctor.Stdout = &stdout
	err = doctor.Run(ctx, []string{"--config", configPath, "doctor", "--json"})
	if err == nil {
		t.Fatalf("newer schema doctor unexpectedly succeeded: %s", stdout.String())
	}
	var payload map[string]any
	if jsonErr := json.Unmarshal(stdout.Bytes(), &payload); jsonErr != nil {
		t.Fatalf("parse doctor json: %v\n%s", jsonErr, stdout.String())
	}
	schema := doctorMap(t, payload, "db_schema")
	if got := schema["state"]; got != "newer" {
		t.Fatalf("db_schema.state = %#v, payload=%#v", got, schema)
	}
	if got := schema["newer"]; got != true {
		t.Fatalf("db_schema.newer = %#v, payload=%#v", got, schema)
	}
	if got := schema["current_version"]; got != float64(99) {
		t.Fatalf("db_schema.current_version = %#v, payload=%#v", got, schema)
	}
	if strings.TrimSpace(fmt.Sprint(payload["runtime_open_error"])) == "" {
		t.Fatalf("runtime_open_error missing: %#v", payload)
	}
	if len(doctorStringList(t, schema, "next_steps")) == 0 {
		t.Fatalf("db_schema.next_steps empty: %#v", schema)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("doctor schema diagnostics mutated newer database bytes")
	}
	if got := doctorUserVersion(t, ctx, dbPath); got != 99 {
		t.Fatalf("user_version = %d, want 99", got)
	}
}

func TestDoctorJSONReportsRuntimeEnvSchemaFailureWithoutConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gitcrawl.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma user_version = 99`); err != nil {
		_ = st.Close()
		t.Fatalf("set newer schema: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	t.Setenv("GITCRAWL_DB_PATH", dbPath)

	doctor := New()
	var stdout bytes.Buffer
	doctor.Stdout = &stdout
	err = doctor.Run(ctx, []string{"--config", filepath.Join(dir, "missing.toml"), "doctor", "--json"})
	if err == nil {
		t.Fatalf("newer runtime-env schema unexpectedly succeeded: %s", stdout.String())
	}
	var payload map[string]any
	if jsonErr := json.Unmarshal(stdout.Bytes(), &payload); jsonErr != nil {
		t.Fatalf("parse doctor json: %v\n%s", jsonErr, stdout.String())
	}
	if strings.TrimSpace(fmt.Sprint(payload["runtime_open_error"])) == "" {
		t.Fatalf("runtime_open_error missing: %#v", payload)
	}
	schema := doctorMap(t, payload, "db_schema")
	if got := schema["state"]; got != "newer" {
		t.Fatalf("db_schema.state = %#v, payload=%#v", got, schema)
	}
}

func TestDoctorJSONReportsStatusFailureWithSchemaDrift(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gitcrawl.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		create table repositories (id integer primary key);
		create table threads (id integer primary key, state text);
		pragma user_version = 4;
	`)
	if err != nil {
		_ = db.Close()
		t.Fatalf("seed schema drift: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}
	configPath := writeDoctorTestConfig(t, dir, dbPath)
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	doctor := New()
	var stdout bytes.Buffer
	doctor.Stdout = &stdout
	err = doctor.Run(ctx, []string{"--config", configPath, "doctor", "--json"})
	if err == nil {
		t.Fatalf("schema-drift status unexpectedly succeeded: %s", stdout.String())
	}
	var payload map[string]any
	if jsonErr := json.Unmarshal(stdout.Bytes(), &payload); jsonErr != nil {
		t.Fatalf("parse doctor json: %v\n%s", jsonErr, stdout.String())
	}
	if strings.TrimSpace(fmt.Sprint(payload["runtime_status_error"])) == "" {
		t.Fatalf("runtime_status_error missing: %#v", payload)
	}
	schema := doctorMap(t, payload, "db_schema")
	if got := schema["state"]; got != "pending_migration" {
		t.Fatalf("db_schema.state = %#v, payload=%#v", got, schema)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("doctor mutated schema-drift database")
	}
}

func TestDoctorJSONRuntimeOpenErrorReturnsFailureWithPayload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := os.WriteFile(dbPath, []byte("not a sqlite database"), 0o644); err != nil {
		t.Fatalf("write corrupt db: %v", err)
	}
	configPath := writeDoctorTestConfig(t, dir, dbPath)

	doctor := New()
	var stdout bytes.Buffer
	doctor.Stdout = &stdout
	err := doctor.Run(ctx, []string{"--config", configPath, "doctor", "--json"})
	if err == nil {
		t.Fatalf("doctor unexpectedly succeeded: %s", stdout.String())
	}

	var payload map[string]any
	if jsonErr := json.Unmarshal(stdout.Bytes(), &payload); jsonErr != nil {
		t.Fatalf("parse doctor json: %v\n%s", jsonErr, stdout.String())
	}
	if strings.TrimSpace(fmt.Sprint(payload["runtime_open_error"])) == "" {
		t.Fatalf("runtime_open_error missing: %#v", payload)
	}
	schema := doctorMap(t, payload, "db_schema")
	if got := schema["state"]; got != "error" {
		t.Fatalf("db_schema.state = %#v, payload=%#v", got, schema)
	}
}

func TestDoctorJSONReportsLegacyPendingSchemaWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gitcrawl.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	seedDoctorLegacyPullRequestFilesSchema(t, ctx, dbPath)
	configPath := writeDoctorTestConfig(t, dir, dbPath)
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	payload := runDoctorJSON(t, ctx, configPath)
	schema := doctorMap(t, payload, "db_schema")
	if got := schema["state"]; got != "pending_migration" {
		t.Fatalf("db_schema.state = %#v, payload=%#v", got, schema)
	}
	if got := schema["pending_migration"]; got != true {
		t.Fatalf("db_schema.pending_migration = %#v, payload=%#v", got, schema)
	}
	prDetails := doctorMap(t, schema, "pr_details")
	if got := prDetails["files_position_key"]; got != false {
		t.Fatalf("pr_details.files_position_key = %#v, payload=%#v", got, prDetails)
	}
	if got := prDetails["duplicate_path_files_supported"]; got != false {
		t.Fatalf("pr_details.duplicate_path_files_supported = %#v, payload=%#v", got, prDetails)
	}
	pending := doctorStringList(t, schema, "pending_migrations")
	if !doctorListContains(pending, "schema_version_3_to_10") ||
		!doctorListContains(pending, "pull_request_files_position_key") ||
		!doctorListContains(pending, "thread_child_observation_reservations_table") {
		t.Fatalf("pending_migrations = %#v", pending)
	}
	if len(doctorStringList(t, schema, "next_steps")) == 0 {
		t.Fatalf("db_schema.next_steps empty: %#v", schema)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("doctor schema diagnostics mutated legacy database bytes")
	}
	if got := doctorUserVersion(t, ctx, dbPath); got != 3 {
		t.Fatalf("user_version = %d, want 3", got)
	}
	if doctorPullRequestFilesPositionKey(t, ctx, dbPath) {
		t.Fatal("doctor migrated pull_request_files primary key")
	}
}

func seedPortableThread(t *testing.T, dbPath string, number int, title string) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open portable db: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("upsert repository: %v", err)
	}
	if _, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      strconv.Itoa(number),
		Number:        number,
		Kind:          "issue",
		State:         "open",
		Title:         title,
		Body:          title,
		HTMLURL:       fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", number),
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   fmt.Sprintf("hash-%d", number),
		UpdatedAt:     now,
	}); err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint portable db: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable db: %v", err)
	}
}

func writeDoctorTestConfig(t *testing.T, dir, dbPath string) string {
	t.Helper()
	configPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.VectorDir = filepath.Join(dir, "vectors")
	cfg.LogDir = filepath.Join(dir, "logs")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	return configPath
}

func runDoctorJSON(t *testing.T, ctx context.Context, configPath string) map[string]any {
	t.Helper()
	doctor := New()
	var stdout bytes.Buffer
	doctor.Stdout = &stdout
	if err := doctor.Run(ctx, []string{"--config", configPath, "doctor", "--json"}); err != nil {
		t.Fatalf("doctor: %v\n%s", err, stdout.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("parse doctor json: %v\n%s", err, stdout.String())
	}
	return payload
}

func doctorMap(t *testing.T, payload map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := payload[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want object", key, payload[key])
	}
	return value
}

func doctorStringList(t *testing.T, payload map[string]any, key string) []string {
	t.Helper()
	raw, ok := payload[key].([]any)
	if !ok {
		t.Fatalf("%s = %#v, want array", key, payload[key])
	}
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		out = append(out, fmt.Sprint(value))
	}
	return out
}

func doctorListContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func seedDoctorLegacyPullRequestFilesSchema(t *testing.T, ctx context.Context, dbPath string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `
		drop index if exists idx_pull_request_files_path;
		drop index if exists idx_pull_request_files_thread_path;
		alter table pull_request_files rename to pull_request_files_current;
		create table pull_request_files (
			thread_id integer not null references threads(id) on delete cascade,
			path text not null,
			status text,
			additions integer not null default 0,
			deletions integer not null default 0,
			changes integer not null default 0,
			previous_path text,
			patch text,
			raw_json text not null,
			fetched_at text not null,
			primary key(thread_id, path)
		);
		insert into pull_request_files(thread_id, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at)
			select thread_id, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at
			from pull_request_files_current;
		drop table pull_request_files_current;
		create index if not exists idx_pull_request_files_path on pull_request_files(path);
		drop table thread_child_observation_reservations;
		pragma user_version = 3;
	`)
	if err != nil {
		t.Fatalf("seed legacy pull_request_files table: %v", err)
	}
}

func doctorUserVersion(t *testing.T, ctx context.Context, dbPath string) int {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	return version
}

func doctorPullRequestFilesPositionKey(t *testing.T, ctx context.Context, dbPath string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `pragma table_info(pull_request_files)`)
	if err != nil {
		t.Fatalf("read pull_request_files schema: %v", err)
	}
	defer rows.Close()
	pk := map[string]int{}
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan pull_request_files schema: %v", err)
		}
		if primaryKey > 0 {
			pk[name] = primaryKey
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pull_request_files schema: %v", err)
	}
	return pk["thread_id"] == 1 && pk["position"] == 2
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
	var stdout bytes.Buffer
	seed.Stdout = &stdout
	if err := seed.Run(context.Background(), []string{"--config", configPath, "portable", "prune", "--body-chars", "8", "--no-vacuum", "--json"}); err != nil {
		t.Fatalf("portable prune: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("parse prune json: %v\n%s", err, stdout.String())
	}
	manifestPath, _ := payload["manifest_path"].(string)
	if manifestPath == "" {
		t.Fatalf("manifest_path missing from prune output: %s", stdout.String())
	}
	if err := validatePortableSQLiteFile(context.Background(), dbPath, dbPath); err != nil {
		t.Fatalf("portable prune should leave valid db + manifest: %v", err)
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

func TestAppOutputModesAndUsageBranches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "version flag text", args: []string{"--version"}, want: "dev"},
		{name: "version command json", args: []string{"--json", "version"}, want: `"version"`},
		{name: "version command log fallback", args: []string{"--format", "log", "version"}, want: "dev"},
		{name: "help tui", args: []string{"help", "tui"}, want: "cluster browser"},
		{name: "configure creates config", args: []string{"--config", filepath.Join(dir, "configure.toml"), "--format", "log", "configure"}, want: "configure="},
		{name: "doctor default json", args: []string{"--config", filepath.Join(dir, "missing.toml"), "--json", "doctor"}, want: `"config_exists": false`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := New()
			var stdout bytes.Buffer
			app.Stdout = &stdout
			if err := app.Run(ctx, tc.args); err != nil {
				t.Fatalf("run: %v", err)
			}
			if !strings.Contains(stdout.String(), tc.want) {
				t.Fatalf("output = %q, want %q", stdout.String(), tc.want)
			}
		})
	}

	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCommandFlowStore(t, dbPath)
	errorCases := [][]string{
		{"--format", "xml", "version"},
		{"help", "unknown"},
		{"serve"},
		{"summarize"},
		{"configure", "--unknown"},
		{"refresh", "--unknown"},
		{"refresh", "openclaw/openclaw", "--no-sync", "--no-embed", "--no-cluster"},
		{"refresh", "badrepo"},
		{"search", "--unknown"},
		{"search", "openclaw/openclaw"},
		{"search", "openclaw/openclaw", "--query", "x", "--mode", "bogus"},
		{"neighbors", "--unknown"},
		{"neighbors", "openclaw/openclaw"},
		{"cluster", "--unknown"},
		{"cluster", "openclaw/openclaw", "--threshold", "2"},
		{"cluster", "openclaw/openclaw", "--cross-kind-threshold", "2"},
		{"embed", "--unknown"},
		{"embed", "openclaw/openclaw", "--number", "bad"},
		{"clusters", "--unknown"},
		{"clusters", "openclaw/openclaw", "--sort", "bogus"},
		{"clusters-report", "--unknown"},
		{"clusters-report", "openclaw/openclaw", "--sort", "bogus"},
		{"clusters-report", "openclaw/openclaw", "--limit", "bad"},
		{"tui", "--unknown"},
		{"tui", "openclaw/openclaw", "extra"},
		{"tui", "openclaw/openclaw", "--min-size", "bad"},
		{"tui", "openclaw/openclaw", "--limit", "bad"},
		{"tui", "openclaw/openclaw", "--sort", "bad", "--json"},
		{"cluster-detail", "--unknown"},
		{"cluster-detail", "openclaw/openclaw"},
		{"runs", "--unknown"},
		{"runs", "openclaw/openclaw", "--limit", "nope"},
		{"threads", "--unknown"},
		{"threads", "openclaw/openclaw", "--numbers", "1,nope"},
		{"sync", "--unknown"},
		{"close-thread", "--unknown"},
		{"close-thread", "openclaw/openclaw"},
		{"reopen-thread", "--unknown"},
		{"reopen-thread", "openclaw/openclaw"},
		{"close-cluster", "--unknown"},
		{"close-cluster", "openclaw/openclaw"},
		{"reopen-cluster", "--unknown"},
		{"reopen-cluster", "openclaw/openclaw"},
		{"exclude-cluster-member", "--unknown"},
		{"exclude-cluster-member", "openclaw/openclaw", "--id", "1"},
		{"include-cluster-member", "--unknown"},
		{"include-cluster-member", "openclaw/openclaw", "--number", "1"},
		{"set-cluster-canonical", "--unknown"},
		{"set-cluster-canonical", "openclaw/openclaw", "--id", "bad", "--number", "1"},
		{"portable"},
		{"portable", "unknown"},
		{"portable", "prune", "--unknown"},
		{"portable", "prune", "extra"},
	}
	for _, args := range errorCases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			app := New()
			err := app.Run(ctx, append([]string{"--config", configPath}, args...))
			if err == nil {
				t.Fatalf("expected error for %v", args)
			}
			if ExitCode(err) == 0 {
				t.Fatalf("expected nonzero exit for %v", args)
			}
			if err.Error() == "" {
				t.Fatalf("empty error for %v", args)
			}
		})
	}

	emptyConfig := filepath.Join(dir, "empty.toml")
	emptyDB := filepath.Join(dir, "empty.db")
	if err := New().Run(ctx, []string{"--config", emptyConfig, "init", "--db", emptyDB}); err != nil {
		t.Fatalf("empty init: %v", err)
	}
	runtimeErrorCases := [][]string{
		{"search", "openclaw/openclaw", "--query", "x"},
		{"neighbors", "openclaw/openclaw", "--number", "1"},
		{"cluster", "openclaw/openclaw"},
		{"clusters", "openclaw/openclaw"},
		{"clusters-report", "openclaw/openclaw"},
		{"cluster-detail", "openclaw/openclaw", "--id", "1"},
		{"runs", "openclaw/openclaw"},
		{"threads", "openclaw/openclaw"},
		{"close-thread", "openclaw/openclaw", "--number", "1"},
		{"reopen-thread", "openclaw/openclaw", "--number", "1"},
	}
	for _, args := range runtimeErrorCases {
		t.Run("empty "+strings.Join(args, " "), func(t *testing.T) {
			err := New().Run(ctx, append([]string{"--config", emptyConfig}, args...))
			if err == nil {
				t.Fatalf("expected runtime error for %v", args)
			}
		})
	}

	if _, err := resolveOutputFormat("bad", false); err == nil {
		t.Fatal("bad output format should fail")
	}
	if _, _, err := parseOwnerRepo("bad"); err == nil {
		t.Fatal("bad owner/repo should fail")
	}
	if _, err := parseOptionalPositiveInt("0"); err == nil {
		t.Fatal("zero int should fail")
	}
	if _, err := parseOptionalPositiveIntList("1, 0"); err == nil {
		t.Fatal("bad int list should fail")
	}
	if owner, repo, err := parseOwnerRepo("https://github.com/openclaw/openclaw/issues/78601"); err != nil || owner != "openclaw" || repo != "openclaw" {
		t.Fatalf("full issue URL owner/repo = %q/%q err=%v", owner, repo, err)
	}
	if got, err := parseOptionalThreadNumber("https://github.com/openclaw/openclaw/issues/78601"); err != nil || got != 78601 {
		t.Fatalf("full issue URL number = %d err=%v", got, err)
	}
	if got, err := parseOptionalThreadNumber("https://github.com/openclaw/openclaw/pull/78602#issuecomment-1"); err != nil || got != 78602 {
		t.Fatalf("full pull URL number = %d err=%v", got, err)
	}
	if got, err := parseOptionalThreadNumberList("https://github.com/openclaw/openclaw/issues/78601, openclaw/openclaw#78602, pull/78603, #78604"); err != nil || len(got) != 4 || got[0] != 78601 || got[1] != 78602 || got[2] != 78603 || got[3] != 78604 {
		t.Fatalf("thread ref list = %#v err=%v", got, err)
	}
	if _, _, _, err := parseClusterShapeOptions("test", "bad", "1", "0.5"); err == nil {
		t.Fatal("bad cluster shape should fail")
	}
	if !isDirtyPortablePullError(fmt.Errorf("Your local changes would be overwritten by merge")) {
		t.Fatal("dirty portable pull error not detected")
	}
	t.Setenv("OPENAI_BASE_URL", "https://openai.example/v1")
	if got := openAIBaseURL(); got != "https://openai.example/v1" {
		t.Fatalf("openAIBaseURL fallback = %q", got)
	}
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", "https://gitcrawl-openai.example/v1")
	if got := openAIBaseURL(); got != "https://gitcrawl-openai.example/v1" {
		t.Fatalf("openAIBaseURL override = %q", got)
	}
	t.Setenv("GITHUB_BASE_URL", "https://github.example")
	if got := githubBaseURL(); got != "https://github.example" {
		t.Fatalf("githubBaseURL fallback = %q", got)
	}
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", "https://gitcrawl-github.example")
	if got := githubBaseURL(); got != "https://gitcrawl-github.example" {
		t.Fatalf("githubBaseURL override = %q", got)
	}
	if got := formatOptionalTime(time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)); got == "" {
		t.Fatal("non-zero optional time should be formatted")
	}
}

func TestGlobalCommandBranches(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		args      []string
		wantErr   bool
		wantOut   string
		exitCode  int
		jsonShape bool
	}{
		{args: []string{"--help"}, wantOut: "Usage:"},
		{args: []string{"help"}, wantOut: "Usage:"},
		{args: []string{"help", "sync"}, wantOut: "gitcrawl sync"},
		{args: []string{"--version"}, wantOut: "dev"},
		{args: []string{"version"}, wantOut: "dev"},
		{args: []string{"--json", "version"}, wantOut: `"version"`},
		{args: []string{"--bad"}, wantErr: true, exitCode: 2},
		{args: []string{"--format", "bad", "version"}, wantErr: true, exitCode: 2},
		{args: []string{"serve"}, wantErr: true, exitCode: 2},
		{args: []string{"completion"}, wantErr: true, exitCode: 1},
		{args: []string{"unknown"}, wantErr: true, exitCode: 2},
		{args: []string{"cluster-explain"}, wantErr: true, exitCode: 2},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			app := New()
			var stdout bytes.Buffer
			app.Stdout = &stdout
			err := app.Run(ctx, tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				if tc.exitCode != 0 && ExitCode(err) != tc.exitCode {
					t.Fatalf("exit code = %d, want %d: %v", ExitCode(err), tc.exitCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if tc.wantOut != "" && !strings.Contains(stdout.String(), tc.wantOut) {
				t.Fatalf("output missing %q: %q", tc.wantOut, stdout.String())
			}
		})
	}
}

func TestGHSearchSyntaxUsesLocalCache(t *testing.T) {
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
		LabelsJSON:      `[{"name":"bug","color":"d73a4a"}]`,
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
	closedID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "11",
		Number:        11,
		Kind:          "issue",
		State:         "closed",
		Title:         "Hot loop old report",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/11",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "issue-11",
		UpdatedAt:     "2026-04-27T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed closed issue: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{ThreadID: closedID, Title: "Hot loop old report", RawText: "old hot loop", DedupeText: "old hot loop", UpdatedAt: "2026-04-27T00:00:00Z"}); err != nil {
		t.Fatalf("seed closed document: %v", err)
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
		RawJSON:         "{}",
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

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "search", "issues", "hot loop", "-R", "openclaw/openclaw", "--state", "open", "--json", "number,title,state,url,updatedAt,labels", "--limit", "30"}); err != nil {
		t.Fatalf("gh issue search: %v", err)
	}
	var issues []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &issues); err != nil {
		t.Fatalf("decode issue search: %v\n%s", err, stdout.String())
	}
	if len(issues) != 1 || int(issues[0]["number"].(float64)) != 10 {
		t.Fatalf("issue search should return only open cached issue, got %#v", issues)
	}
	labels := issues[0]["labels"].([]any)
	if len(labels) != 1 || labels[0].(map[string]any)["name"] != "bug" {
		t.Fatalf("issue labels = %#v", labels)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "search", "prs", "manifest cache", "-R", "openclaw/openclaw", "--state", "open", "--json", "number,title,state,url,updatedAt,isDraft,author", "--limit", "20"}); err != nil {
		t.Fatalf("gh pr search: %v", err)
	}
	var prs []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &prs); err != nil {
		t.Fatalf("decode pr search: %v\n%s", err, stdout.String())
	}
	if len(prs) != 1 || int(prs[0]["number"].(float64)) != 12 || prs[0]["isDraft"] != true {
		t.Fatalf("pr search should return cached draft PR, got %#v", prs)
	}
	author := prs[0]["author"].(map[string]any)
	if author["login"] != "bob" {
		t.Fatalf("author = %#v", author)
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

func TestTUIJSONUsesDefaultsWhenConfigMissing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "missing.toml")
	t.Setenv("GITCRAWL_DB_PATH", filepath.Join(dir, "missing.db"))

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "tui", "--json", "--layout", "focus"}); err != nil {
		t.Fatalf("tui: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode tui payload: %v\n%s", err, stdout.String())
	}
	if payload["mode"] != "cluster-browser" {
		t.Fatalf("mode = %#v", payload["mode"])
	}
	if payload["layout"] != "focus" {
		t.Fatalf("layout = %#v", payload["layout"])
	}
	clusters, ok := payload["clusters"].([]any)
	if !ok || len(clusters) != 0 {
		t.Fatalf("clusters = %#v", payload["clusters"])
	}
	if _, err := os.Stat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("config file should not be created, stat err=%v", err)
	}

	t.Setenv("GITCRAWL_TUI_LAYOUT", "right-stack")
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "tui", "--json"}); err != nil {
		t.Fatalf("tui env layout: %v", err)
	}
	payload = map[string]any{}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode env layout payload: %v\n%s", err, stdout.String())
	}
	if payload["layout"] != "right-stack" {
		t.Fatalf("env layout = %#v", payload["layout"])
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "tui", "--json", "--layout", "bogus"}); err == nil || !strings.Contains(err.Error(), `unsupported layout "bogus"`) {
		t.Fatalf("bogus layout err = %v", err)
	}
}

func TestTUIJSONHandlesEmptyStoreWithoutRepository(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "tui", "--json", "--layout", "focus"}); err != nil {
		t.Fatalf("tui: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode tui payload: %v\n%s", err, stdout.String())
	}
	if payload["layout"] != "focus" {
		t.Fatalf("layout = %#v", payload["layout"])
	}
	clusters, ok := payload["clusters"].([]any)
	if !ok || len(clusters) != 0 {
		t.Fatalf("clusters = %#v", payload["clusters"])
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "tui", "--json", "--layout", "bogus"}); err == nil || !strings.Contains(err.Error(), `unsupported layout "bogus"`) {
		t.Fatalf("bogus layout err = %v", err)
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

func TestResolveOptionalRepositoryBranches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gitcrawl.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	app := New()
	rt := localRuntime{Store: st}
	if _, _, err := app.resolveOptionalRepository(ctx, rt, nil); err == nil {
		t.Fatal("empty store should not infer repository")
	}
	first, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "one", FullName: "openclaw/one", UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatalf("first repo: %v", err)
	}
	if _, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "two", FullName: "openclaw/two", UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("second repo: %v", err)
	}
	if repo, inferred, err := app.resolveOptionalRepository(ctx, rt, nil); err != nil || !inferred || repo.FullName == "" {
		t.Fatalf("multi repo inference repo=%+v inferred=%v err=%v", repo, inferred, err)
	}
	repo, inferred, err := app.resolveOptionalRepository(ctx, rt, []string{"openclaw/one"})
	if err != nil {
		t.Fatalf("explicit repo: %v", err)
	}
	if inferred || repo.ID != first {
		t.Fatalf("explicit repo=%+v inferred=%v", repo, inferred)
	}
}

func TestCommandFlowCoversSearchEmbedNeighborsClusterDetailRunsAndRefresh(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}

	configure := New()
	var stdout bytes.Buffer
	configure.Stdout = &stdout
	if err := configure.Run(ctx, []string{
		"--config", configPath,
		"configure",
		"--summary-model", "summary-test",
		"--embed-model", "embed-test",
		"--embedding-basis", "title_original",
		"--json",
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if !strings.Contains(stdout.String(), `"updated": true`) {
		t.Fatalf("configure output = %q", stdout.String())
	}

	repoID, firstID, secondID := seedCommandFlowStore(t, dbPath)

	search := New()
	stdout.Reset()
	search.Stdout = &stdout
	if err := search.Run(ctx, []string{"--config", configPath, "search", "openclaw/openclaw", "--query", "gateway websocket", "--mode", "hybrid", "--limit", "5", "--json"}); err != nil {
		t.Fatalf("search: %v", err)
	}
	if !strings.Contains(stdout.String(), "Gateway websocket stalls") {
		t.Fatalf("search output missing seeded hit: %s", stdout.String())
	}

	openAIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Fatalf("unexpected OpenAI path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-openai-key" {
			t.Fatalf("authorization = %q", got)
		}
		var payload struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode embeddings request: %v", err)
		}
		data := make([]map[string]any, 0, len(payload.Input))
		for index, text := range payload.Input {
			vector := []float64{0, 1}
			if strings.Contains(text, "Gateway") || strings.Contains(text, "websocket") {
				vector = []float64{1, 0}
			}
			if strings.Contains(text, "typing") {
				vector = []float64{0.92, 0.08}
			}
			data = append(data, map[string]any{"index": index, "embedding": vector})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer openAIServer.Close()
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", openAIServer.URL)

	embed := New()
	stdout.Reset()
	embed.Stdout = &stdout
	if err := embed.Run(ctx, []string{"--config", configPath, "embed", "openclaw/openclaw", "--limit", "3", "--json"}); err != nil {
		t.Fatalf("embed: %v", err)
	}
	if !strings.Contains(stdout.String(), `"embedded": 3`) {
		t.Fatalf("embed output = %q", stdout.String())
	}

	semanticSearch := New()
	stdout.Reset()
	semanticSearch.Stdout = &stdout
	if err := semanticSearch.Run(ctx, []string{"--config", configPath, "search", "openclaw/openclaw", "--query", "semantic-only", "--mode", "semantic", "--limit", "2", "--json"}); err != nil {
		t.Fatalf("semantic search: %v", err)
	}
	if !strings.Contains(stdout.String(), `"mode": "semantic"`) || !strings.Contains(stdout.String(), `"number": 103`) {
		t.Fatalf("semantic search output = %q", stdout.String())
	}

	hybridSearch := New()
	stdout.Reset()
	hybridSearch.Stdout = &stdout
	if err := hybridSearch.Run(ctx, []string{"--config", configPath, "search", "openclaw/openclaw", "--query", "semantic-only", "--mode", "hybrid", "--limit", "2", "--json"}); err != nil {
		t.Fatalf("hybrid search: %v", err)
	}
	if !strings.Contains(stdout.String(), `"mode": "hybrid"`) || !strings.Contains(stdout.String(), `"number": 103`) {
		t.Fatalf("hybrid search output = %q", stdout.String())
	}

	neighbors := New()
	stdout.Reset()
	neighbors.Stdout = &stdout
	if err := neighbors.Run(ctx, []string{"--config", configPath, "neighbors", "openclaw/openclaw", "--number", "101", "--limit", "2", "--threshold", "0.1", "--json"}); err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if !strings.Contains(stdout.String(), `"number": 102`) {
		t.Fatalf("neighbors output = %q", stdout.String())
	}

	cluster := New()
	stdout.Reset()
	cluster.Stdout = &stdout
	if err := cluster.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.7", "--min-size", "2", "--k", "2", "--json"}); err != nil {
		t.Fatalf("cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"cluster_count": 1`) {
		t.Fatalf("cluster output = %q", stdout.String())
	}

	clusters := New()
	stdout.Reset()
	clusters.Stdout = &stdout
	if err := clusters.Run(ctx, []string{"--config", configPath, "clusters", "openclaw/openclaw", "--sort", "recent", "--min-size", "1", "--limit", "5", "--json"}); err != nil {
		t.Fatalf("clusters: %v", err)
	}
	if !strings.Contains(stdout.String(), `"clusters"`) {
		t.Fatalf("clusters output = %q", stdout.String())
	}

	report := New()
	stdout.Reset()
	report.Stdout = &stdout
	if err := report.Run(ctx, []string{"--config", configPath, "clusters-report", "openclaw/openclaw", "--sort", "oldest", "--min-size", "1", "--limit", "3", "--member-limit", "5"}); err != nil {
		t.Fatalf("clusters-report: %v", err)
	}
	if out := stdout.String(); !strings.Contains(out, "# Cluster Report: openclaw/openclaw") || !strings.Contains(out, "sorted by oldest") || !strings.Contains(out, "total members") || !strings.Contains(out, "## At a Glance") || !strings.Contains(out, "Gateway websocket stalls") {
		t.Fatalf("clusters-report output = %q", out)
	}

	reportJSON := New()
	stdout.Reset()
	reportJSON.Stdout = &stdout
	if err := reportJSON.Run(ctx, []string{"--config", configPath, "clusters-report", "openclaw/openclaw", "--sort", "size", "--min-size", "1", "--limit", "3", "--member-limit", "5", "--json"}); err != nil {
		t.Fatalf("clusters-report json: %v", err)
	}
	if out := stdout.String(); !strings.Contains(out, `"repository": "openclaw/openclaw"`) || !strings.Contains(out, `"clusters"`) {
		t.Fatalf("clusters-report json output = %q", out)
	}

	durable := New()
	stdout.Reset()
	durable.Stdout = &stdout
	if err := durable.Run(ctx, []string{"--config", configPath, "durable-clusters", "openclaw/openclaw", "--include-closed", "--min-size", "1", "--json"}); err != nil {
		t.Fatalf("durable-clusters: %v", err)
	}
	var durablePayload struct {
		Clusters []store.ClusterSummary `json:"clusters"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &durablePayload); err != nil {
		t.Fatalf("decode durable clusters: %v\n%s", err, stdout.String())
	}
	if len(durablePayload.Clusters) == 0 {
		t.Fatalf("durable clusters output = %q", stdout.String())
	}

	detail := New()
	stdout.Reset()
	detail.Stdout = &stdout
	if err := detail.Run(ctx, []string{"--config", configPath, "cluster-detail", "openclaw/openclaw", "--id", strconv.FormatInt(durablePayload.Clusters[0].ID, 10), "--member-limit", "5", "--body-chars", "12", "--json"}); err != nil {
		t.Fatalf("cluster-detail: %v", err)
	}
	if !strings.Contains(stdout.String(), "Gateway") {
		t.Fatalf("cluster-detail output = %q", stdout.String())
	}

	runs := New()
	stdout.Reset()
	runs.Stdout = &stdout
	if err := runs.Run(ctx, []string{"--config", configPath, "runs", "openclaw/openclaw", "--kind", "embedding", "--limit", "3", "--json"}); err != nil {
		t.Fatalf("runs: %v", err)
	}
	if !strings.Contains(stdout.String(), `"kind": "embedding"`) {
		t.Fatalf("runs output = %q", stdout.String())
	}

	refresh := New()
	stdout.Reset()
	refresh.Stdout = &stdout
	if err := refresh.Run(ctx, []string{"--config", configPath, "refresh", "openclaw/openclaw", "--no-sync", "--no-embed", "--threshold", "0.7", "--min-size", "2", "--json"}); err != nil {
		t.Fatalf("refresh cluster-only: %v", err)
	}
	if !strings.Contains(stdout.String(), `"cluster":`) {
		t.Fatalf("refresh output = %q", stdout.String())
	}

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open final store: %v", err)
	}
	defer st.Close()
	threads, err := st.ThreadsByIDs(ctx, repoID, []int64{firstID, secondID})
	if err != nil {
		t.Fatalf("threads by ids: %v", err)
	}
	if len(threads) != 2 {
		t.Fatalf("threads by ids = %+v", threads)
	}
}

func TestNeighborsBackfillsMissingAndStaleTargetEmbedding(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	repoID, firstID, secondID := seedCommandFlowStore(t, dbPath)
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID: repoID,
		Basis:  "title_original",
		Model:  "text-embedding-3-small",
		Number: 102,
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tasks = %#v, want one task for seeded neighbor", tasks)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID:    firstID,
		Basis:       "old_basis",
		Model:       "old-model",
		Dimensions:  2,
		ContentHash: "old-hash",
		Vector:      []float64{0, 1},
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("seed old target vector: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID:    secondID,
		Basis:       "title_original",
		Model:       "text-embedding-3-small",
		Dimensions:  2,
		ContentHash: tasks[0].ContentHash,
		Vector:      []float64{0.95, 0.05},
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("seed neighbor vector: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	var calls atomic.Int64
	openAIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Fatalf("unexpected OpenAI path: %s", r.URL.Path)
		}
		calls.Add(1)
		var payload struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode embeddings request: %v", err)
		}
		if len(payload.Input) != 1 {
			t.Fatalf("neighbors backfill should embed one row, got %d inputs", len(payload.Input))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 0, "embedding": []float64{1, 0}}},
		})
	}))
	defer openAIServer.Close()
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", openAIServer.URL)

	runNeighbors := func() string {
		t.Helper()
		var stdout bytes.Buffer
		app := New()
		app.Stdout = &stdout
		if err := app.Run(ctx, []string{"--config", configPath, "neighbors", "openclaw/openclaw", "--number", "101", "--limit", "1", "--threshold", "0.1", "--json"}); err != nil {
			t.Fatalf("neighbors: %v", err)
		}
		return stdout.String()
	}
	if out := runNeighbors(); !strings.Contains(out, `"number": 102`) {
		t.Fatalf("neighbors output after missing backfill = %q", out)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("OpenAI calls after missing backfill = %d, want 1", got)
	}
	if out := runNeighbors(); !strings.Contains(out, `"number": 102`) {
		t.Fatalf("neighbors output after cached vector = %q", out)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("already-current embedding was re-embedded, calls = %d", got)
	}

	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{
		ThreadID:   firstID,
		Title:      "Gateway websocket stalls",
		Body:       "Gateway websocket stalls after a fresh body update.",
		RawText:    "Gateway websocket stalls after a fresh body update.",
		DedupeText: "gateway websocket stalls after a fresh body update",
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("update target document: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close stale update store: %v", err)
	}
	if out := runNeighbors(); !strings.Contains(out, `"number": 102`) {
		t.Fatalf("neighbors output after stale backfill = %q", out)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("stale target should be re-embedded once, calls = %d", got)
	}
}

func TestClustersReportUsesDisplaySummarySourceForCollidingIDs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	rawID, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "101", Number: 101, Kind: "issue", State: "open",
		Title: "Raw live cluster", Body: "raw cluster body",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/101",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "raw",
		UpdatedAt:     now,
	})
	if err != nil {
		t.Fatalf("seed raw thread: %v", err)
	}
	durableID, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "201", Number: 201, Kind: "issue", State: "closed",
		Title: "Durable historical cluster", Body: "durable cluster body",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/201",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "durable",
		UpdatedAt:     now,
	})
	if err != nil {
		t.Fatalf("seed durable thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_runs(id, repo_id, scope, status, started_at, finished_at, stats_json)
		values(9, ?, 'repo', 'completed', '2026-05-01T00:00:00Z', '2026-05-01T00:01:00Z', '{}')
	`, repoID); err != nil {
		t.Fatalf("seed raw cluster run: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into clusters(id, repo_id, cluster_run_id, representative_thread_id, member_count, created_at)
		values(77, ?, 9, ?, 1, '2026-05-01T00:01:00Z')
	`, repoID, rawID); err != nil {
		t.Fatalf("seed raw cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_members(cluster_id, thread_id, score_to_representative, created_at)
		values(77, ?, 1.0, '2026-05-01T00:01:00Z')
	`, rawID); err != nil {
		t.Fatalf("seed raw cluster member: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at, closed_at)
		values(77, ?, 'closed-durable', 'closed-durable', 'closed', ?, 'Durable historical cluster', '2026-04-01T00:00:00Z', '2026-04-02T00:00:00Z', '2026-04-02T00:00:00Z')
	`, repoID, durableID); err != nil {
		t.Fatalf("seed durable cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_closures(cluster_id, reason, actor_kind, created_at, updated_at)
		values(77, 'handled', 'local', '2026-04-02T00:00:00Z', '2026-04-02T00:00:00Z')
	`); err != nil {
		t.Fatalf("seed durable closure: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(77, ?, 'representative', 'active', 'system', '{}', '2026-04-01T00:00:00Z', '2026-04-01T00:00:00Z')
	`, durableID); err != nil {
		t.Fatalf("seed durable member: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "clusters-report", "openclaw/openclaw", "--sort", "size", "--min-size", "1", "--limit", "2", "--member-limit", "5", "--json"}); err != nil {
		t.Fatalf("clusters-report: %v", err)
	}
	var payload struct {
		Clusters []store.ClusterDetail `json:"clusters"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout.String())
	}
	if len(payload.Clusters) != 2 {
		t.Fatalf("report clusters = %#v", payload.Clusters)
	}
	if payload.Clusters[0].Cluster.Source != store.ClusterSourceRun || payload.Clusters[0].Members[0].Thread.Number != 101 {
		t.Fatalf("raw report detail used wrong source: %#v", payload.Clusters[0])
	}
	if payload.Clusters[1].Cluster.Source != store.ClusterSourceDurable || payload.Clusters[1].Members[0].Thread.Number != 201 {
		t.Fatalf("durable report detail used wrong source: %#v", payload.Clusters[1])
	}
}

func TestHybridSearchSkipsOpenAIWhenNoVectors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"index": 0, "embedding": []float64{1, 0}}}})
	}))
	defer server.Close()
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)

	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "search", "openclaw/openclaw", "--query", "gateway websocket", "--mode", "hybrid", "--limit", "5", "--json"}); err != nil {
		t.Fatalf("hybrid search: %v", err)
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("hybrid search made %d OpenAI calls without vectors", calls)
	}
	if !strings.Contains(stdout.String(), `"mode": "keyword"`) || !strings.Contains(stdout.String(), `"requested_mode": "hybrid"`) {
		t.Fatalf("hybrid fallback output = %q", stdout.String())
	}
}

func TestSyncCommandUsesConfiguredGitHubBaseURLAndHydratesComments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-gh-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch r.URL.Path {
		case "/repos/openclaw/openclaw":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 12345, "full_name": "openclaw/openclaw"})
		case "/repos/openclaw/openclaw/issues/101":
			_ = json.NewEncoder(w).Encode(githubIssueJSON(101, "issue", "Gateway websocket stalls"))
		case "/repos/openclaw/openclaw/issues/102":
			row := githubIssueJSON(102, "pull_request", "Fix Discord typing timeout")
			row["pull_request"] = map[string]any{"url": "https://api.github.test/pulls/102"}
			_ = json.NewEncoder(w).Encode(row)
		case "/repos/openclaw/openclaw/issues/101/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{githubCommentJSON(1001, "issue comment")})
		case "/repos/openclaw/openclaw/issues/102/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{githubCommentJSON(1002, "pr issue comment")})
		case "/repos/openclaw/openclaw/pulls/102/reviews":
			_ = json.NewEncoder(w).Encode([]map[string]any{githubCommentJSON(1003, "review body")})
		case "/repos/openclaw/openclaw/pulls/102/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{githubCommentJSON(1004, "review line")})
		default:
			t.Fatalf("unexpected GitHub path: %s", r.URL.String())
		}
	}))
	defer server.Close()
	t.Setenv("GITHUB_TOKEN", "test-gh-token")
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "sync", "openclaw/openclaw", "--numbers", "101,102", "--include-comments", "--json"}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	var stats struct {
		ThreadsSynced      int `json:"threads_synced"`
		PullRequestsSynced int `json:"pull_requests_synced"`
		CommentsSynced     int `json:"comments_synced"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode sync stats: %v\n%s", err, stdout.String())
	}
	if stats.ThreadsSynced != 2 || stats.PullRequestsSynced != 1 || stats.CommentsSynced != 4 {
		t.Fatalf("sync stats = %+v", stats)
	}

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.ThreadCount != 2 || status.RepositoryCount != 1 {
		t.Fatalf("status after sync = %+v", status)
	}
}

func TestRefreshRunsSyncEmbedAndClusterWithLocalServers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}

	githubServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/openclaw/openclaw":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 12345, "full_name": "openclaw/openclaw"})
		case "/repos/openclaw/openclaw/issues":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				githubIssueJSON(201, "issue", "Gateway reconnect loop"),
				githubIssueJSON(202, "pull_request", "Gateway reconnect timeout"),
			})
		case "/repos/openclaw/openclaw/issues/201/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{githubCommentJSON(2001, "same reconnect loop")})
		case "/repos/openclaw/openclaw/issues/202/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{githubCommentJSON(2002, "same reconnect timeout")})
		case "/repos/openclaw/openclaw/pulls/202/reviews",
			"/repos/openclaw/openclaw/pulls/202/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/openclaw/openclaw/pulls/202":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          202,
				"head":            map[string]any{"sha": "head-202", "ref": "feature", "repo": map[string]any{"full_name": "openclaw/openclaw"}},
				"base":            map[string]any{"sha": "base-202"},
				"mergeable_state": "clean",
				"changed_files":   1,
			})
		case "/repos/openclaw/openclaw/pulls/202/files":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"filename": "internal/gateway.go", "status": "modified", "patch": "@@ gateway"}})
		case "/repos/openclaw/openclaw/pulls/202/commits":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"sha":    "commit-202",
				"commit": map[string]any{"message": "fix: reconnect timeout"},
			}})
		case "/repos/openclaw/openclaw/commits/head-202/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []map[string]any{}})
		case "/repos/openclaw/openclaw/actions/runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": []map[string]any{}})
		case "/graphql":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{
				"reviewThreads": map[string]any{"nodes": []map[string]any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""}},
			}}}})
		default:
			t.Fatalf("unexpected GitHub path: %s", r.URL.String())
		}
	}))
	defer githubServer.Close()

	openAIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Fatalf("unexpected OpenAI path: %s", r.URL.Path)
		}
		var payload struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode embeddings request: %v", err)
		}
		data := make([]map[string]any, 0, len(payload.Input))
		for index := range payload.Input {
			data = append(data, map[string]any{"index": index, "embedding": []float64{1, 0.01 * float64(index)}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer openAIServer.Close()
	t.Setenv("GITHUB_TOKEN", "test-gh-token")
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", githubServer.URL)
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", openAIServer.URL)

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "refresh", "openclaw/openclaw", "--include-comments", "--with", "pr-details", "--limit", "2", "--threshold", "0.5", "--min-size", "1", "--json"}); err != nil {
		t.Fatalf("refresh: %v\nstderr=%s", err, stderr.String())
	}
	var payload refreshResult
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode refresh: %v\n%s", err, stdout.String())
	}
	if payload.Sync == nil ||
		payload.Sync.ThreadsSynced != 2 ||
		payload.Sync.CommentsSynced != 2 ||
		payload.Sync.PRDetailsSynced != 1 ||
		payload.Sync.PRFilesSynced != 1 ||
		payload.Sync.PRCommitsSynced != 1 {
		t.Fatalf("sync payload = %+v", payload.Sync)
	}
	if payload.Embed == nil || payload.Embed.Embedded != 2 {
		t.Fatalf("embed payload = %+v", payload.Embed)
	}
	if payload.Cluster == nil || int(payload.Cluster["vector_count"].(float64)) != 2 {
		t.Fatalf("cluster payload = %+v", payload.Cluster)
	}
	for _, want := range []string{"[refresh] sync", "[refresh] embed", "[refresh] cluster"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, stderr.String())
		}
	}
}

func TestEmbedErrorBranchesRecordFailures(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	init := New()
	if err := init.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCommandFlowStore(t, dbPath)
	t.Setenv("OPENAI_API_KEY", "")
	if err := New().Run(ctx, []string{"--config", configPath, "embed", "openclaw/openclaw"}); err == nil {
		t.Fatal("missing OpenAI key should fail")
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.EmbeddingBasis = "title_summary"
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save title summary config: %v", err)
	}
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	if err := New().Run(ctx, []string{"--config", configPath, "embed", "openclaw/openclaw"}); err == nil {
		t.Fatal("title_summary embed should fail")
	}

	cfg.EmbeddingBasis = "title_original"
	cfg.OpenAI.BatchSize = 0
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "embed failed", http.StatusInternalServerError)
	}))
	defer server.Close()
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)
	t.Setenv("GITCRAWL_OPENAI_RETRY_DISABLED", "1")
	if err := New().Run(ctx, []string{"--config", configPath, "embed", "openclaw/openclaw", "--limit", "1"}); err == nil {
		t.Fatal("OpenAI error should fail")
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	runs, err := st.ListRuns(ctx, repo.ID, "embedding", 1)
	if err != nil {
		t.Fatalf("list embedding runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "error" || runs[0].ErrorText == "" {
		t.Fatalf("embedding error run = %+v", runs)
	}
}

func TestEmbedRunPartialOnSomeFailedBatches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.OpenAI.BatchSize = 1
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var payload struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode: %v", err)
		}
		// First input is permanently bad — return non-retryable 400.
		if len(payload.Input) == 1 && strings.Contains(payload.Input[0], "Gateway websocket stalls") {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"message": "bad input", "type": "invalid_request_error"},
			})
			return
		}
		data := make([]map[string]any, 0, len(payload.Input))
		for index := range payload.Input {
			data = append(data, map[string]any{"index": index, "embedding": []float64{1, 0.5 * float64(index)}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer server.Close()
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)
	t.Setenv("GITCRAWL_OPENAI_RETRY_DISABLED", "1")

	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "embed", "openclaw/openclaw", "--json"}); err != nil {
		t.Fatalf("embed: %v", err)
	}

	var result embedResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode embed result: %v\n%s", err, stdout.String())
	}
	if result.Status != "partial" {
		t.Fatalf("status = %q, want partial", result.Status)
	}
	if result.Embedded != 2 {
		t.Fatalf("embedded = %d, want 2", result.Embedded)
	}
	if result.Failed != 1 {
		t.Fatalf("failed = %d, want 1", result.Failed)
	}
	if len(result.Failures) != 1 {
		t.Fatalf("failures = %+v", result.Failures)
	}
	if result.Failures[0].Status != http.StatusBadRequest {
		t.Fatalf("failure status = %d", result.Failures[0].Status)
	}

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	runs, err := st.ListRuns(ctx, repo.ID, "embedding", 1)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "partial" {
		t.Fatalf("run = %+v", runs)
	}
}

func TestEmbedRunCancelledRecordsCancelledStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancel()
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer server.Close()
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)
	t.Setenv("GITCRAWL_OPENAI_RETRY_DISABLED", "1")

	if err := New().Run(ctx, []string{"--config", configPath, "embed", "openclaw/openclaw"}); err == nil {
		t.Fatal("expected cancellation error")
	}

	st, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repo, err := st.RepositoryByFullName(context.Background(), "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	runs, err := st.ListRuns(context.Background(), repo.ID, "embedding", 1)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "cancelled" {
		t.Fatalf("expected cancelled run, got %+v", runs)
	}
}

func TestTruncatedEmbeddingTaskCount(t *testing.T) {
	tasks := []store.EmbeddingTask{
		{Number: 1},
		{Number: 2, TextTruncated: true},
		{Number: 3, TextTruncated: true},
	}
	if got := truncatedEmbeddingTaskCount(tasks); got != 2 {
		t.Fatalf("truncated count = %d, want 2", got)
	}
}

func githubIssueJSON(number int, kind string, title string) map[string]any {
	row := map[string]any{
		"id":         number + 10000,
		"number":     number,
		"state":      "open",
		"title":      title,
		"body":       title + " body",
		"html_url":   fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", number),
		"labels":     []map[string]any{{"name": "bug"}},
		"assignees":  []map[string]any{},
		"user":       map[string]any{"login": kind + "-author", "type": "User"},
		"created_at": "2026-04-30T01:00:00Z",
		"updated_at": "2026-04-30T02:00:00Z",
	}
	if kind == "pull_request" {
		row["html_url"] = fmt.Sprintf("https://github.com/openclaw/openclaw/pull/%d", number)
		row["pull_request"] = map[string]any{"url": fmt.Sprintf("https://api.github.com/repos/openclaw/openclaw/pulls/%d", number)}
	}
	return row
}

func githubCommentJSON(id int, body string) map[string]any {
	return map[string]any{
		"id":         id,
		"body":       body,
		"user":       map[string]any{"login": "commenter", "type": "User"},
		"created_at": "2026-04-30T03:00:00Z",
		"updated_at": "2026-04-30T04:00:00Z",
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

func TestClusterLocalOverrideCommands(t *testing.T) {
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
	if _, err := st.SaveDurableClusters(ctx, repoID, []store.DurableClusterInput{{
		StableKey:              "cli-cluster",
		StableSlug:             "cli-cluster",
		RepresentativeThreadID: firstID,
		Title:                  "CLI cluster",
		Members: []store.DurableClusterMemberInput{
			{ThreadID: firstID, Role: "canonical"},
			{ThreadID: secondID, Role: "member"},
		},
	}}); err != nil {
		t.Fatalf("save cluster: %v", err)
	}
	clusterIDValue, err := st.ClusterIDForThreadNumber(ctx, repoID, 101, false)
	if err != nil {
		t.Fatalf("cluster id: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	var stdout bytes.Buffer
	run := func(args ...string) string {
		t.Helper()
		stdout.Reset()
		cmd := New()
		cmd.Stdout = &stdout
		if err := cmd.Run(ctx, append([]string{"--config", configPath}, args...)); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return stdout.String()
	}
	clusterID := strconv.FormatInt(clusterIDValue, 10)
	if out := run("exclude-cluster-member", "openclaw/openclaw", "--id", clusterID, "--number", "102", "--reason", "duplicate", "--json"); !strings.Contains(out, `"action": "exclude"`) {
		t.Fatalf("exclude output = %q", out)
	}
	if out := run("include-cluster-member", "openclaw/openclaw", "--id", clusterID, "--number", "102", "--reason", "needed", "--json"); !strings.Contains(out, `"action": "include"`) {
		t.Fatalf("include output = %q", out)
	}
	if out := run("set-cluster-canonical", "openclaw/openclaw", "--id", clusterID, "--number", "102", "--reason", "better", "--json"); !strings.Contains(out, `"action": "canonical"`) {
		t.Fatalf("canonical output = %q", out)
	}
	if out := run("close-cluster", "openclaw/openclaw", "--id", clusterID, "--reason", "resolved", "--json"); !strings.Contains(out, `"closed": true`) {
		t.Fatalf("close cluster output = %q", out)
	}
	if out := run("reopen-cluster", "openclaw/openclaw", "--id", clusterID, "--json"); !strings.Contains(out, `"reopened": true`) {
		t.Fatalf("reopen cluster output = %q", out)
	}
}

func seedCommandFlowStore(t *testing.T, dbPath string) (int64, int64, int64) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:        "openclaw",
		Name:         "openclaw",
		FullName:     "openclaw/openclaw",
		GitHubRepoID: "12345",
		RawJSON:      `{"full_name":"openclaw/openclaw"}`,
		UpdatedAt:    now,
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	threads := []store.Thread{
		{
			RepoID:          repoID,
			GitHubID:        "101",
			Number:          101,
			Kind:            "issue",
			State:           "open",
			Title:           "Gateway websocket stalls",
			Body:            "Gateway websocket stalls when messages arrive.",
			AuthorLogin:     "alice",
			AuthorType:      "User",
			HTMLURL:         "https://github.com/openclaw/openclaw/issues/101",
			LabelsJSON:      `[{"name":"bug"}]`,
			AssigneesJSON:   "[]",
			RawJSON:         "{}",
			ContentHash:     "thread-101",
			UpdatedAtGitHub: "2026-04-30T01:00:00Z",
			UpdatedAt:       now,
		},
		{
			RepoID:          repoID,
			GitHubID:        "102",
			Number:          102,
			Kind:            "issue",
			State:           "open",
			Title:           "Discord typing timeout",
			Body:            "typing TTL stops while gateway websocket is slow.",
			AuthorLogin:     "bob",
			AuthorType:      "User",
			HTMLURL:         "https://github.com/openclaw/openclaw/issues/102",
			LabelsJSON:      `[]`,
			AssigneesJSON:   "[]",
			RawJSON:         "{}",
			ContentHash:     "thread-102",
			UpdatedAtGitHub: "2026-04-30T02:00:00Z",
			UpdatedAt:       now,
		},
		{
			RepoID:          repoID,
			GitHubID:        "103",
			Number:          103,
			Kind:            "pull_request",
			State:           "open",
			Title:           "Refactor portable cache",
			Body:            "Portable cache maintenance update.",
			AuthorLogin:     "carol",
			AuthorType:      "User",
			HTMLURL:         "https://github.com/openclaw/openclaw/pull/103",
			LabelsJSON:      `[]`,
			AssigneesJSON:   "[]",
			RawJSON:         "{}",
			ContentHash:     "thread-103",
			IsDraft:         true,
			UpdatedAtGitHub: "2026-04-30T03:00:00Z",
			UpdatedAt:       now,
		},
	}
	ids := make([]int64, 0, len(threads))
	for _, thread := range threads {
		id, err := st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("seed thread %d: %v", thread.Number, err)
		}
		ids = append(ids, id)
		if _, err := st.UpsertDocument(ctx, store.Document{
			ThreadID:   id,
			Title:      thread.Title,
			RawText:    thread.Title + "\n\n" + thread.Body,
			DedupeText: strings.ToLower(thread.Title + " " + thread.Body),
			UpdatedAt:  now,
		}); err != nil {
			t.Fatalf("seed document %d: %v", thread.Number, err)
		}
	}
	return repoID, ids[0], ids[1]
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

func TestClustersDefaultShowsActivePrimaryMembers(t *testing.T) {
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
	openID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "90",
		Number:        90,
		Kind:          "issue",
		State:         "open",
		Title:         "Open member",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/90",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-90",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed open thread: %v", err)
	}
	closedID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "91",
		Number:        91,
		Kind:          "issue",
		State:         "closed",
		Title:         "Closed historical member",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/91",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-91",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed closed thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(90, ?, 'cluster-90', 'cluster-90', 'active', ?, 'Cluster 90', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z');
	`, repoID, openID); err != nil {
		t.Fatalf("seed cluster group: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(90, ?, 'member', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z'),
		      (90, ?, 'member', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z');
	`, openID, closedID); err != nil {
		t.Fatalf("seed cluster memberships: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "--json", "clusters", "openclaw/openclaw", "--sort", "size", "--min-size", "1"}); err != nil {
		t.Fatalf("clusters: %v", err)
	}
	var active struct {
		Clusters []store.ClusterSummary `json:"clusters"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &active); err != nil {
		t.Fatalf("decode active clusters: %v\n%s", err, stdout.String())
	}
	if len(active.Clusters) != 1 || active.Clusters[0].MemberCount != 2 {
		t.Fatalf("default clusters should include closed historical members, got %#v", active.Clusters)
	}

	stdout.Reset()
	withClosed := New()
	withClosed.Stdout = &stdout
	if err := withClosed.Run(ctx, []string{"--config", configPath, "--json", "clusters", "openclaw/openclaw", "--sort", "size", "--min-size", "1", "--hide-closed"}); err != nil {
		t.Fatalf("clusters hide closed: %v", err)
	}
	var all struct {
		Clusters []store.ClusterSummary `json:"clusters"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &all); err != nil {
		t.Fatalf("decode all clusters: %v\n%s", err, stdout.String())
	}
	if len(all.Clusters) != 1 || all.Clusters[0].MemberCount != 1 {
		t.Fatalf("hide-closed should focus active members, got %#v", all.Clusters)
	}

	stdout.Reset()
	detail := New()
	detail.Stdout = &stdout
	if err := detail.Run(ctx, []string{"--config", configPath, "--json", "cluster-detail", "openclaw/openclaw", "--id", "90"}); err != nil {
		t.Fatalf("cluster-detail: %v", err)
	}
	var detailPayload struct {
		Members []store.ClusterMemberDetail `json:"members"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &detailPayload); err != nil {
		t.Fatalf("decode cluster detail: %v\n%s", err, stdout.String())
	}
	if len(detailPayload.Members) != 2 {
		t.Fatalf("default cluster-detail should match visible cluster members, got %#v", detailPayload.Members)
	}

	stdout.Reset()
	hideDetail := New()
	hideDetail.Stdout = &stdout
	if err := hideDetail.Run(ctx, []string{"--config", configPath, "--json", "cluster-detail", "openclaw/openclaw", "--id", "90", "--hide-closed"}); err != nil {
		t.Fatalf("cluster-detail hide closed: %v", err)
	}
	detailPayload.Members = nil
	if err := json.Unmarshal(stdout.Bytes(), &detailPayload); err != nil {
		t.Fatalf("decode hide-closed cluster detail: %v\n%s", err, stdout.String())
	}
	if len(detailPayload.Members) != 1 || detailPayload.Members[0].Thread.Number != 90 {
		t.Fatalf("hide-closed cluster-detail should focus open members, got %#v", detailPayload.Members)
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
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID: repoID, Basis: "title_original", Model: "text-embedding-3-small", Force: true,
	})
	if err != nil {
		t.Fatalf("list embedding tasks: %v", err)
	}
	hashByThread := map[int64]string{}
	for _, task := range tasks {
		hashByThread[task.ThreadID] = task.ContentHash
	}
	for _, vector := range []store.ThreadVector{
		{ThreadID: firstID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 2, ContentHash: hashByThread[firstID], Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: now},
		{ThreadID: secondID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 2, ContentHash: hashByThread[secondID], Vector: []float64{0.95, 0.05}, CreatedAt: now, UpdatedAt: now},
		{ThreadID: thirdID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 2, ContentHash: hashByThread[thirdID], Vector: []float64{0, 1}, CreatedAt: now, UpdatedAt: now},
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
	if !strings.Contains(stdout.String(), `"cluster_count": 2`) {
		t.Fatalf("cluster output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}
	memberCounts := []int{}
	for _, cluster := range clusters {
		memberCounts = append(memberCounts, cluster.MemberCount)
	}
	sort.Ints(memberCounts)
	if len(memberCounts) != 2 || memberCounts[0] != 1 || memberCounts[1] != 2 {
		t.Fatalf("expected duplicate cluster plus singleton, got %#v", clusters)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store before limited cluster: %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.90", "--limit", "2", "--json"}); err != nil {
		t.Fatalf("limited cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"processed": 2`) ||
		!strings.Contains(stdout.String(), `"partial": true`) ||
		!strings.Contains(stdout.String(), `"complete": false`) {
		t.Fatalf("limited cluster coverage output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store after limited cluster: %v", err)
	}
	clusters, err = st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list clusters after limited run: %v", err)
	}
	if len(clusters) != 2 {
		t.Fatalf("limited cluster run should not retire unprocessed clusters, got %#v", clusters)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after limited cluster: %v", err)
	}

	if err := run.Run(ctx, []string{"--config", configPath, "configure", "--embed-model", "text-embedding-3-large"}); err != nil {
		t.Fatalf("configure new embed model: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.90", "--json"}); err != nil {
		t.Fatalf("compatible model fallback cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"fallback": true`) ||
		!strings.Contains(stdout.String(), `"partial": true`) ||
		!strings.Contains(stdout.String(), `"complete": false`) {
		t.Fatalf("compatible model fallback coverage output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store before model migration cluster: %v", err)
	}
	tasks, err = st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID: repoID, Basis: "title_original", Model: "text-embedding-3-large", Force: true,
	})
	if err != nil {
		t.Fatalf("list migrated embedding tasks: %v", err)
	}
	hashByThread = map[int64]string{}
	for _, task := range tasks {
		hashByThread[task.ThreadID] = task.ContentHash
	}
	for _, vector := range []store.ThreadVector{
		{ThreadID: firstID, Basis: "title_original", Model: "text-embedding-3-large", Dimensions: 2, ContentHash: hashByThread[firstID], Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: now},
		{ThreadID: secondID, Basis: "title_original", Model: "text-embedding-3-large", Dimensions: 2, ContentHash: hashByThread[secondID], Vector: []float64{0.95, 0.05}, CreatedAt: now, UpdatedAt: now},
	} {
		if err := st.UpsertThreadVector(ctx, vector); err != nil {
			t.Fatalf("upsert migrated vector: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store before model migration cluster: %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.90", "--json"}); err != nil {
		t.Fatalf("compatible partial model migration cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"complete": false`) ||
		!strings.Contains(stdout.String(), `"partial": true`) ||
		!strings.Contains(stdout.String(), `"missing": 1`) {
		t.Fatalf("compatible partial cluster coverage output = %q", stdout.String())
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store after model migration cluster: %v", err)
	}
	clusters, err = st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list clusters after model migration run: %v", err)
	}
	if len(clusters) != 2 {
		t.Fatalf("failed model migration run should preserve clusters, got %#v", clusters)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after model migration cluster: %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.90", "--strict-vectors", "--json"}); err == nil || !strings.Contains(err.Error(), "vector coverage is incomplete") {
		t.Fatalf("strict model migration cluster error = %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.90", "--limit", "2", "--json"}); err != nil {
		t.Fatalf("explicit partial model migration cluster: %v", err)
	}
	if !strings.Contains(stdout.String(), `"complete": false`) ||
		!strings.Contains(stdout.String(), `"partial": true`) ||
		!strings.Contains(stdout.String(), `"missing": 1`) {
		t.Fatalf("partial cluster coverage output = %q", stdout.String())
	}

	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store before close-all cluster: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `update threads set state = 'closed', closed_at_gh = ?, updated_at = ? where repo_id = ?`, now, now, repoID); err != nil {
		t.Fatalf("close seeded threads: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store before close-all cluster: %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "0.90", "--json"}); err != nil {
		t.Fatalf("close-all cluster: %v", err)
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store after close-all cluster: %v", err)
	}
	clusters, err = st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20})
	if err != nil {
		t.Fatalf("list clusters after close-all run: %v", err)
	}
	if len(clusters) != 0 {
		t.Fatalf("complete zero-vector run should retire active clusters, got %#v", clusters)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after close-all cluster: %v", err)
	}
}

func TestClusterAndRefreshStrictVectorsRejectMissingCoverageBeforeMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	if _, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "missing vector", Body: "body",
		HTMLURL:    "https://github.com/openclaw/openclaw/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread", UpdatedAt: "2026-07-12T00:00:00Z",
	}); err != nil {
		t.Fatalf("thread: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	for _, args := range [][]string{
		{"--config", configPath, "cluster", "openclaw/openclaw", "--strict-vectors", "--json"},
		{"--config", configPath, "refresh", "openclaw/openclaw", "--no-sync", "--no-embed", "--strict-vectors", "--json"},
	} {
		err := New().Run(ctx, args)
		if err == nil || !strings.Contains(err.Error(), "no fresh vectors are available") {
			t.Fatalf("%v error = %v", args, err)
		}
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	var runs int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from cluster_runs`).Scan(&runs); err != nil {
		t.Fatalf("cluster runs: %v", err)
	}
	if runs != 0 {
		t.Fatalf("cluster runs = %d, want no mutation", runs)
	}
}

func TestCompleteClusterVectorCoverageRequiresFreshVectors(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	firstThread := store.Thread{
		RepoID: repoID, GitHubID: "501", Number: 501, Kind: "issue", State: "open",
		Title: "Fresh vector coverage", Body: "original body",
		HTMLURL: "https://github.com/openclaw/openclaw/issues/501", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: "{}", ContentHash: "hash-501", UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	firstThreadID, err := st.UpsertThread(ctx, firstThread)
	if err != nil {
		t.Fatalf("seed first thread: %v", err)
	}
	secondThread := store.Thread{
		RepoID: repoID, GitHubID: "502", Number: 502, Kind: "issue", State: "open",
		Title: "Stale vector coverage", Body: "original stale body",
		HTMLURL: "https://github.com/openclaw/openclaw/issues/502", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: "{}", ContentHash: "hash-502", UpdatedAt: "2026-04-26T00:00:00Z",
	}
	secondThreadID, err := st.UpsertThread(ctx, secondThread)
	if err != nil {
		t.Fatalf("seed second thread: %v", err)
	}
	query := store.ThreadVectorQuery{RepoID: repoID, Basis: "title_original", Model: "text-embedding-3-small"}
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{RepoID: repoID, Basis: query.Basis, Model: query.Model})
	if err != nil {
		t.Fatalf("list embedding tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("embedding tasks = %#v", tasks)
	}
	hashByNumber := map[int]string{}
	for _, task := range tasks {
		hashByNumber[task.Number] = task.ContentHash
	}
	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID: firstThreadID, Basis: query.Basis, Model: query.Model, Dimensions: 2,
		ContentHash: hashByNumber[501], Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert first vector: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID: secondThreadID, Basis: query.Basis, Model: query.Model, Dimensions: 2,
		ContentHash: "stale-hash", Vector: []float64{0, 1}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert stale second vector: %v", err)
	}
	vectors, err := st.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		t.Fatalf("list vectors: %v", err)
	}
	coverage, fresh, err := evaluateClusterVectorCoverage(ctx, st, query, vectors)
	if err != nil {
		t.Fatalf("stale older coverage: %v", err)
	}
	if coverage.Complete || coverage.Stale != 1 || len(fresh) != 1 {
		t.Fatalf("stale older coverage = %+v fresh=%d", coverage, len(fresh))
	}

	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID: secondThreadID, Basis: query.Basis, Model: query.Model, Dimensions: 2,
		ContentHash: hashByNumber[502], Vector: []float64{0, 1}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("refresh second vector: %v", err)
	}
	vectors, err = st.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		t.Fatalf("list fresh vectors: %v", err)
	}
	coverage, fresh, err = evaluateClusterVectorCoverage(ctx, st, query, vectors)
	if err != nil {
		t.Fatalf("fresh complete coverage: %v", err)
	}
	if !coverage.Complete || coverage.Fresh != 2 || len(fresh) != 2 {
		t.Fatalf("fresh complete coverage = %+v fresh=%d", coverage, len(fresh))
	}

	firstThread.Body = "changed body"
	firstThread.ContentHash = "hash-501-updated"
	firstThread.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.UpsertThread(ctx, firstThread); err != nil {
		t.Fatalf("update thread: %v", err)
	}
	coverage, fresh, err = evaluateClusterVectorCoverage(ctx, st, query, vectors)
	if err != nil {
		t.Fatalf("stale complete coverage: %v", err)
	}
	if coverage.Complete || coverage.Stale != 1 || len(fresh) != 1 {
		t.Fatalf("stale updated coverage = %+v fresh=%d", coverage, len(fresh))
	}
}

func TestClusterVectorCoverageRejectsMissingSummaryInputs(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	firstID, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "511", Number: 511, Kind: "issue", State: "open",
		Title: "Has key summary", Body: "body",
		HTMLURL: "https://github.com/openclaw/openclaw/issues/511", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: "{}", ContentHash: "hash-511", UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("seed first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "512", Number: 512, Kind: "issue", State: "open",
		Title: "No key summary", Body: "body",
		HTMLURL: "https://github.com/openclaw/openclaw/issues/512", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: "{}", ContentHash: "hash-512", UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("seed second thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(
			thread_id, source_updated_at, observation_sequence,
			content_hash, title_hash, body_hash, labels_hash, created_at
		)
		select id, ?, observation_sequence, 'content', 'title', 'body', 'labels', ?
		from threads
		where id = ?
	`, now, now, firstID); err != nil {
		t.Fatalf("seed revision: %v", err)
	}
	var revisionID int64
	if err := st.DB().QueryRowContext(ctx, `select id from thread_revisions where thread_id = ?`, firstID).Scan(&revisionID); err != nil {
		t.Fatalf("revision id: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(?, 'llm_key_summary', 'test', 'test', 'test', 'input', 'output', 'key summary text', ?)
	`, revisionID, now); err != nil {
		t.Fatalf("seed key summary: %v", err)
	}

	query := store.ThreadVectorQuery{RepoID: repoID, Basis: "llm_key_summary", Model: "text-embedding-3-small"}
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{RepoID: repoID, Basis: query.Basis, Model: query.Model})
	if err != nil {
		t.Fatalf("list embedding tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Number != 511 {
		t.Fatalf("embedding tasks = %#v", tasks)
	}
	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID: firstID, Basis: query.Basis, Model: query.Model, Dimensions: 2,
		ContentHash: tasks[0].ContentHash, Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert summary vector: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID: secondID, Basis: query.Basis, Model: query.Model, Dimensions: 2,
		ContentHash: "stale-summary-vector", Vector: []float64{0, 1}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert stale summary vector: %v", err)
	}
	vectors, err := st.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		t.Fatalf("list vectors: %v", err)
	}
	coverage, fresh, err := evaluateClusterVectorCoverage(ctx, st, query, vectors)
	if err != nil {
		t.Fatalf("summary coverage: %v", err)
	}
	if coverage.Complete || coverage.Eligible != 2 || coverage.Fresh != 1 || coverage.Missing != 0 || coverage.MissingInput != 1 || coverage.Stale != 1 || len(fresh) != 1 {
		t.Fatalf("summary coverage = %+v fresh=%d", coverage, len(fresh))
	}
	err = clusterVectorCoverageError("openclaw", "openclaw", query, coverage, "vector coverage is incomplete")
	if !strings.Contains(err.Error(), "populate missing key summaries") || !strings.Contains(err.Error(), "missing_input=1") {
		t.Fatalf("summary recovery error = %v", err)
	}
}

func TestClusterFallbackDropsStaleSupportedVectorsBeforeDeduplication(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	now := "2026-07-12T03:00:00Z"
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "520", Number: 520, Kind: "issue", State: "open",
		Title: "Fresh fallback vector", Body: "body",
		HTMLURL:    "https://github.com/openclaw/gitcrawl/issues/520",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread", UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("seed thread: %v", err)
	}
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID: repoID, Basis: "title_original", Model: "legacy-model", Force: true,
	})
	if err != nil {
		t.Fatalf("list embedding tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("embedding tasks = %+v", tasks)
	}
	vectors := []store.ThreadVector{
		{
			ThreadID: threadID, Basis: "title_original", Model: "configured-model", Dimensions: 2,
			ContentHash: "stale", Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: "2026-07-12T03:02:00Z",
		},
		{
			ThreadID: threadID, Basis: "title_original", Model: "legacy-model", Dimensions: 2,
			ContentHash: tasks[0].ContentHash, Vector: []float64{0, 1}, CreatedAt: now, UpdatedAt: "2026-07-12T03:01:00Z",
		},
	}
	fresh, err := freshFallbackClusterVectors(ctx, st, store.ThreadVectorQuery{RepoID: repoID}, vectors)
	if err != nil {
		t.Fatalf("fresh fallback vectors: %v", err)
	}
	if len(fresh) != 1 || fresh[0].Model != "legacy-model" || fresh[0].ContentHash != tasks[0].ContentHash {
		t.Fatalf("fresh fallback vectors = %+v", fresh)
	}
}

func TestClusterCommandAllowsExplicitUnsupportedBasisWithoutRetirement(t *testing.T) {
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "601", Number: 601, Kind: "issue", State: "open",
		Title: "Custom basis vector", Body: "custom vector body",
		HTMLURL: "https://github.com/openclaw/openclaw/issues/601", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: "{}", ContentHash: "hash-601", UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("seed thread: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID: threadID, Basis: "external_basis", Model: "external-model", Dimensions: 2,
		ContentHash: "external-hash", Vector: []float64{1, 0}, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert vector: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--basis", "external_basis", "--model", "external-model", "--json"}); err != nil {
		t.Fatalf("cluster explicit basis: %v", err)
	}
	if !strings.Contains(stdout.String(), `"cluster_count": 1`) {
		t.Fatalf("cluster output = %q", stdout.String())
	}

	configure := New()
	if err := configure.Run(ctx, []string{"--config", configPath, "configure", "--embed-model", "external-model", "--embedding-basis", "external_basis"}); err != nil {
		t.Fatalf("configure custom basis: %v", err)
	}
	stdout.Reset()
	configured := New()
	configured.Stdout = &stdout
	if err := configured.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--min-size", "1", "--json"}); err != nil {
		t.Fatalf("cluster configured custom basis: %v", err)
	}
	if !strings.Contains(stdout.String(), `"cluster_count": 1`) {
		t.Fatalf("configured cluster output = %q", stdout.String())
	}
	if err := configured.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--strict-vectors", "--json"}); err == nil || !strings.Contains(err.Error(), "vector coverage cannot be verified") {
		t.Fatalf("strict custom basis error = %v", err)
	}
}

func TestClusterCommandRejectsNonFiniteThresholdBeforeMutation(t *testing.T) {
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	for number, vector := range map[int][]float64{701: {1, 0}, 702: {0.95, 0.05}} {
		threadID, err := st.UpsertThread(ctx, store.Thread{
			RepoID: repoID, GitHubID: strconv.Itoa(number), Number: number, Kind: "issue", State: "open",
			Title: "Non finite threshold", HTMLURL: fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", number),
			LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: fmt.Sprintf("hash-%d", number), UpdatedAt: now,
		})
		if err != nil {
			t.Fatalf("seed thread %d: %v", number, err)
		}
		if err := st.UpsertThreadVector(ctx, store.ThreadVector{
			ThreadID: threadID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 2,
			ContentHash: fmt.Sprintf("hash-%d", number), Vector: vector, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("upsert vector %d: %v", number, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "cluster", "openclaw/openclaw", "--threshold", "NaN", "--json"}); err == nil {
		t.Fatal("cluster with NaN threshold should fail")
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	for _, table := range []string{"cluster_runs", "cluster_groups"} {
		var count int
		if err := st.DB().QueryRowContext(ctx, `select count(*) from `+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d, want 0", table, count)
		}
	}
}

func TestBuildDurableClusterInputsPrunesWeakCrossKindEdges(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	issueID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "201",
		Number:        201,
		Kind:          "issue",
		State:         "open",
		Title:         "Slack zero inbound events",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/201",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-201",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	prID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "202",
		Number:        202,
		Kind:          "pull_request",
		State:         "open",
		Title:         "Slack socket mode import fix",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/202",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-202",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed pull request: %v", err)
	}
	vectors := []store.ThreadVector{
		{ThreadID: issueID, Vector: []float64{1, 0}},
		{ThreadID: prID, Vector: []float64{0.9, 0.435889894}},
	}
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, vectors, clusterBuildOptions{
		Threshold:          0.82,
		MinSize:            2,
		MaxClusterSize:     defaultClusterMaxSize,
		Fanout:             16,
		CrossKindThreshold: 0.93,
	})
	if err != nil {
		t.Fatalf("build inputs: %v", err)
	}
	if edgeCount != 0 || len(inputs) != 0 {
		t.Fatalf("weak cross-kind edge should be pruned, edges=%d inputs=%#v", edgeCount, inputs)
	}
	inputs, edgeCount, err = buildDurableClusterInputs(ctx, st, repoID, vectors, clusterBuildOptions{
		Threshold:          0.82,
		MinSize:            2,
		MaxClusterSize:     defaultClusterMaxSize,
		Fanout:             16,
		CrossKindThreshold: 0.89,
	})
	if err != nil {
		t.Fatalf("build relaxed inputs: %v", err)
	}
	if edgeCount != 1 || len(inputs) != 1 {
		t.Fatalf("relaxed cross-kind threshold should keep edge, edges=%d inputs=%#v", edgeCount, inputs)
	}
}

func TestBuildDurableClusterInputsKeepsDeterministicReferenceEdges(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	issueID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "301",
		Number:        301,
		Kind:          "issue",
		State:         "open",
		Title:         "Gateway token regression",
		Body:          "Users cannot authorize device tokens.",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/301",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-301",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	prID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "302",
		Number:        302,
		Kind:          "pull_request",
		State:         "open",
		Title:         "Repair auth scope migration",
		Body:          "Fixes #301 by preserving the device-token scope during upgrade.",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/302",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-302",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed pull request: %v", err)
	}
	vectors := []store.ThreadVector{
		{ThreadID: issueID, Vector: []float64{1, 0}},
		{ThreadID: prID, Vector: []float64{0, 1}},
	}
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, vectors, clusterBuildOptions{
		Threshold:          0.99,
		MinSize:            2,
		MaxClusterSize:     defaultClusterMaxSize,
		Fanout:             16,
		CrossKindThreshold: 0.99,
	})
	if err != nil {
		t.Fatalf("build inputs: %v", err)
	}
	if edgeCount != 1 || len(inputs) != 1 {
		t.Fatalf("direct issue/PR reference should form an evidence edge, edges=%d inputs=%#v", edgeCount, inputs)
	}
}

func TestBuildDurableClusterInputsIgnoresCrossRepoQualifiedReferences(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	issueID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "901",
		Number:        901,
		Kind:          "issue",
		State:         "open",
		Title:         "Gateway token regression",
		Body:          "Users cannot authorize device tokens.",
		HTMLURL:       "https://github.com/openclaw/openclaw/issues/901",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-901",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	prID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "902",
		Number:        902,
		Kind:          "pull_request",
		State:         "open",
		Title:         "Repair auth scope migration",
		Body:          "Fixes otherorg/other#901 by preserving the device-token scope during upgrade.",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/902",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-902",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed pull request: %v", err)
	}
	vectors := []store.ThreadVector{
		{ThreadID: issueID, Vector: []float64{1, 0}},
		{ThreadID: prID, Vector: []float64{0, 1}},
	}
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, vectors, clusterBuildOptions{
		Threshold:          0.99,
		MinSize:            2,
		MaxClusterSize:     defaultClusterMaxSize,
		Fanout:             16,
		CrossKindThreshold: 0.99,
	})
	if err != nil {
		t.Fatalf("build inputs: %v", err)
	}
	if edgeCount != 0 || len(inputs) != 0 {
		t.Fatalf("cross-repo qualified refs should not form evidence edges, edges=%d inputs=%#v", edgeCount, inputs)
	}
}

func TestBuildDurableClusterInputsIgnoresBareOneDigitProseRefs(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	firstID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "401",
		Number:        401,
		Kind:          "pull_request",
		State:         "open",
		Title:         "Background task notification",
		Body:          "This is the #1 UX gap for orchestration.",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/401",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-401",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "402",
		Number:        402,
		Kind:          "pull_request",
		State:         "open",
		Title:         "Plugin config overlay",
		Body:          "This is #1 for locked-down deployments.",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/402",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-402",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed second thread: %v", err)
	}
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, []store.ThreadVector{
		{ThreadID: firstID, Vector: []float64{1, 0}},
		{ThreadID: secondID, Vector: []float64{0, 1}},
	}, clusterBuildOptions{
		Threshold:          0.99,
		MinSize:            2,
		MaxClusterSize:     defaultClusterMaxSize,
		Fanout:             16,
		CrossKindThreshold: 0.99,
	})
	if err != nil {
		t.Fatalf("build inputs: %v", err)
	}
	if edgeCount != 0 || len(inputs) != 0 {
		t.Fatalf("bare one-digit prose refs should not form evidence edges, edges=%d inputs=%#v", edgeCount, inputs)
	}
}

func TestBuildDurableClusterInputsPrunesBodyOnlyUnrelatedReferences(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	watchdogID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "601",
		Number:        601,
		Kind:          "pull_request",
		State:         "open",
		Title:         "feat: add external rescue watchdog",
		Body:          "Adds a rescue watchdog service.",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/601",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-601",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed watchdog thread: %v", err)
	}
	windowsID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "602",
		Number:        602,
		Kind:          "pull_request",
		State:         "open",
		Title:         "fix: align windows path tests with runtime behavior",
		Body:          strings.Repeat("Windows path normalization changed in this shard. ", 8) + "The Windows shard failures inherited by #601 are unrelated to the watchdog feature itself.",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/602",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-602",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed windows thread: %v", err)
	}
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, []store.ThreadVector{
		{ThreadID: watchdogID, Vector: []float64{1, 0}},
		{ThreadID: windowsID, Vector: []float64{0, 1}},
	}, clusterBuildOptions{
		Threshold:          0.99,
		MinSize:            2,
		MaxClusterSize:     defaultClusterMaxSize,
		Fanout:             16,
		CrossKindThreshold: 0.99,
	})
	if err != nil {
		t.Fatalf("build inputs: %v", err)
	}
	if edgeCount != 0 || len(inputs) != 0 {
		t.Fatalf("body-only reference without title overlap should not form evidence edge, edges=%d inputs=%#v", edgeCount, inputs)
	}
}

func TestBuildDurableClusterInputsPrunesWeakGenericTitleEdges(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	firstID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "501",
		Number:        501,
		Kind:          "pull_request",
		State:         "open",
		Title:         "fix: improve error handling and logging for security-critical operations",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/501",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-501",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      "502",
		Number:        502,
		Kind:          "pull_request",
		State:         "open",
		Title:         "fix(gateway): isolate control-plane write rate limits by connection",
		HTMLURL:       "https://github.com/openclaw/openclaw/pull/502",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-502",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("seed second thread: %v", err)
	}
	vectors := []store.ThreadVector{
		{ThreadID: firstID, Vector: []float64{1, 0}},
		{ThreadID: secondID, Vector: []float64{0.84, 0.5425863986500217}},
	}
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, vectors, clusterBuildOptions{
		Threshold:          0.82,
		MinSize:            2,
		MaxClusterSize:     defaultClusterMaxSize,
		Fanout:             16,
		CrossKindThreshold: defaultCrossKindMinScore,
	})
	if err != nil {
		t.Fatalf("build inputs: %v", err)
	}
	if edgeCount != 0 || len(inputs) != 0 {
		t.Fatalf("weak generic title edge should be pruned, edges=%d inputs=%#v", edgeCount, inputs)
	}
}

func TestKeepTopEdgesKeepsOneSidedNearestNeighbors(t *testing.T) {
	edges := keepTopEdges([]clusterer.Edge{
		{LeftThreadID: 1, RightThreadID: 2, Score: 0.95},
		{LeftThreadID: 1, RightThreadID: 3, Score: 0.90},
	}, 1)
	if len(edges) != 2 {
		t.Fatalf("one-sided top-k edges should be kept, got %#v", edges)
	}
}

func TestMergeHybridSearchHitsReservesKeywordSlots(t *testing.T) {
	got := mergeHybridSearchHits(
		[]store.SearchHit{{ThreadID: 10, Number: 10}, {ThreadID: 20, Number: 20}},
		[]store.SearchHit{{ThreadID: 30, Number: 30}, {ThreadID: 40, Number: 40}},
		3,
	)
	want := []int{10, 30, 20}
	if len(got) != len(want) {
		t.Fatalf("hybrid hits = %#v, want %v", got, want)
	}
	for index, hit := range got {
		if hit.Number != want[index] {
			t.Fatalf("hybrid hit %d = #%d, want #%d; all=%#v", index, hit.Number, want[index], got)
		}
	}

	got = mergeHybridSearchHits(
		[]store.SearchHit{{ThreadID: 10, Number: 10}},
		[]store.SearchHit{{ThreadID: 10, Number: 10}, {ThreadID: 30, Number: 30}},
		3,
	)
	want = []int{10, 30}
	if len(got) != len(want) || got[0].Number != want[0] || got[1].Number != want[1] {
		t.Fatalf("deduped hybrid hits = %#v, want %v", got, want)
	}
}

func TestCanFallbackFromSemanticSearchRejectsContextErrors(t *testing.T) {
	if canFallbackFromSemanticSearch(context.Canceled) {
		t.Fatal("canceled semantic search should not fall back to keyword results")
	}
	if canFallbackFromSemanticSearch(context.DeadlineExceeded) {
		t.Fatal("deadline semantic search should not fall back to keyword results")
	}
	if !canFallbackFromSemanticSearch(errors.New("missing semantic dependency")) {
		t.Fatal("ordinary semantic failure should allow keyword fallback")
	}
}

func TestRefreshEmbedsAndClustersWithoutSync(t *testing.T) {
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
		t.Fatalf("seed repository: %v", err)
	}
	seedEmbeddingDocument(t, ctx, st, repoID, 101, "Duplicate crash one")
	seedEmbeddingDocument(t, ctx, st, repoID, 102, "Duplicate crash two")
	seedEmbeddingDocument(t, ctx, st, repoID, 103, "Unrelated settings request")
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var request struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		response := struct {
			Data []struct {
				Index     int       `json:"index"`
				Embedding []float64 `json:"embedding"`
			} `json:"data"`
		}{}
		for index, input := range request.Input {
			vector := []float64{0, 1}
			if strings.Contains(strings.ToLower(input), "duplicate") {
				vector = []float64{1, 0.01}
			}
			response.Data = append(response.Data, struct {
				Index     int       `json:"index"`
				Embedding []float64 `json:"embedding"`
			}{Index: index, Embedding: vector})
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "refresh", "openclaw/openclaw", "--no-sync", "--threshold", "0.90", "--json"}); err != nil {
		t.Fatalf("refresh: %v\nstderr:\n%s", err, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, `"embedded": 3`) {
		t.Fatalf("refresh did not embed rows: %q", out)
	}
	if !strings.Contains(out, `"cluster_count": 2`) {
		t.Fatalf("refresh did not persist cluster: %q", out)
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
	memberCounts := []int{}
	for _, cluster := range clusters {
		memberCounts = append(memberCounts, cluster.MemberCount)
	}
	sort.Ints(memberCounts)
	if len(memberCounts) != 2 || memberCounts[0] != 1 || memberCounts[1] != 2 {
		t.Fatalf("expected duplicate cluster plus singleton, got %#v", clusters)
	}
}

func seedEmbeddingDocument(t *testing.T, ctx context.Context, st *store.Store, repoID int64, number int, title string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	threadID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      strconv.Itoa(number),
		Number:        number,
		Kind:          "issue",
		State:         "open",
		Title:         title,
		Body:          title,
		HTMLURL:       fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", number),
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   fmt.Sprintf("hash-%d", number),
		UpdatedAt:     now,
	})
	if err != nil {
		t.Fatalf("seed thread %d: %v", number, err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{
		ThreadID:   threadID,
		Title:      title,
		Body:       title,
		RawText:    title,
		DedupeText: strings.ToLower(title),
		UpdatedAt:  now,
	}); err != nil {
		t.Fatalf("seed document %d: %v", number, err)
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
