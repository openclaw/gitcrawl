package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestGHCacheDescriptorAndPolicyBranches(t *testing.T) {
	if got := canonicalGHCommandArgs(nil); got != nil {
		t.Fatalf("nil canonical args = %+v", got)
	}
	canonical := canonicalGHCommandArgs([]string{"pr", "view", "12", "--json", "title,number", "-R", " openclaw/openclaw ", "--method", "get", "--flag"})
	if strings.Join(canonical, " ") != "pr view 12 --flag --json=number,title --method=GET --repo=openclaw/openclaw" {
		t.Fatalf("canonical args = %+v", canonical)
	}
	if got := canonicalGHCommandArgs([]string{"pr", "view", "--repo"}); strings.Join(got, " ") != "pr view --repo" {
		t.Fatalf("missing value canonical args = %+v", got)
	}
	if !ghCacheTagsMatch([]string{"repo:openclaw/openclaw", "issues"}, stringSet([]string{"issues", "repo:openclaw/openclaw"})) {
		t.Fatal("specific issue tag should match")
	}
	if ghCacheTagsMatch([]string{"repo:openclaw/openclaw"}, stringSet([]string{"repo:openclaw/openclaw", "issues"})) {
		t.Fatal("repo tag alone should not match specific mutation")
	}
	app := New()
	t.Setenv("GH_REPO", "openclaw/from-env")
	tagCases := [][]string{
		app.ghCommandCacheTags(context.Background(), []string{"issue", "view", "https://github.com/openclaw/openclaw/issues/10", "-R", "openclaw/openclaw"}),
		app.ghCommandCacheTags(context.Background(), []string{"pr", "view", "12"}),
		app.ghMutationInvalidationTags(context.Background(), []string{"run", "rerun", "99", "-R", "openclaw/openclaw"}),
		app.ghCommandCacheTags(context.Background(), []string{"workflow", "view", "ci.yml", "-R", "openclaw/openclaw"}),
		app.ghCommandCacheTags(context.Background(), []string{"release", "view", "v0.7.0", "-R", "openclaw/openclaw"}),
		app.ghCommandCacheTags(context.Background(), []string{"api", "repos/openclaw/openclaw/actions/runs/99/jobs"}),
		app.ghMutationInvalidationTags(context.Background(), []string{"cache", "delete"}),
	}
	for _, tags := range tagCases {
		if len(tags) == 0 {
			t.Fatalf("empty tags")
		}
	}
	if repo := ghCommandRepo([]string{"repo", "view", "openclaw/openclaw"}); repo != "openclaw/openclaw" {
		t.Fatalf("repo view repo = %q", repo)
	}
	if repo := ghAPIRepo([]string{"https://api.github.com/repos/openclaw/openclaw/issues/10"}); repo != "openclaw/openclaw" {
		t.Fatalf("api repo = %q", repo)
	}
	if tags := ghAPITags([]string{"repos/openclaw/openclaw/releases/latest"}); len(tags) < 2 || tags[1] != "releases" {
		t.Fatalf("release api tags = %+v", tags)
	}
	if got := firstGHNumberArg([]string{"--repo", "openclaw/openclaw", "https://github.com/openclaw/openclaw/pull/12"}); got != "12" {
		t.Fatalf("first number = %q", got)
	}
	if got := uniqueStrings([]string{"", "a", " a ", "b"}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unique = %+v", got)
	}
	completedRun := ghCommandCacheEntry{Args: []string{"run", "view", "99"}, Stdout: `{"status":"completed"}`}
	if ttl := ghCompletedRunCacheTTL(completedRun); ttl != 12*time.Hour {
		t.Fatalf("run view ttl = %s", ttl)
	}
	completedList := ghCommandCacheEntry{Args: []string{"api", "repos/openclaw/openclaw/actions/runs"}, Stdout: `{"workflow_runs":[{"status":"completed"}]}`}
	if ttl := ghCompletedRunCacheTTL(completedList); ttl != 30*time.Minute {
		t.Fatalf("run list ttl = %s", ttl)
	}
	jobs := ghCommandCacheEntry{Args: []string{"api", "repos/openclaw/openclaw/actions/runs/99/jobs"}, Stdout: `{"jobs":[{"conclusion":"success"}]}`}
	if ttl := ghCompletedRunCacheTTL(jobs); ttl != 12*time.Hour {
		t.Fatalf("jobs ttl = %s", ttl)
	}
	if ghJSONStatusCompleted(`{`) || ghJSONCollectionCompleted(`[]`) || allGHStatusMapsCompleted([]map[string]any{{"status": "queued"}}) {
		t.Fatal("incomplete JSON status classified as completed")
	}
	if !cacheableGHRead([]string{"label", "list"}) || !cacheableGHRead([]string{"org", "list"}) || !cacheableGHRead([]string{"search", "repos"}) {
		t.Fatal("expected read-only gh commands to be cacheable")
	}
	if ghCommandName(nil) != "" || ghCommandName([]string{"pr"}) != "pr" || ghCommandName([]string{"api", "repos/x/y"}) != "api" {
		t.Fatal("gh command name mismatch")
	}
	if ghRunCacheTTL(nil) != 30*time.Second || ghRunCacheTTL([]string{"view", "--job", "1"}) != time.Minute || ghRunCacheTTL([]string{"rerun"}) != 30*time.Second {
		t.Fatal("run ttl mismatch")
	}
	if ttl := ghAPICacheTTL([]string{"repos/openclaw/openclaw/actions/runs/99/jobs"}); ttl != time.Minute {
		t.Fatalf("jobs ttl = %s", ttl)
	}
	if ttl := ghAPICacheTTL([]string{"repos/openclaw/openclaw/contents/file?ref=main"}); ttl != 30*time.Minute {
		t.Fatalf("unstable content ttl = %s", ttl)
	}
	if !ghAPIContentRefIsStableReleaseTag("refs/tags/v1.2.3") || !ghAPIContentRefIsStableReleaseTag("v1.2.3+build") || ghAPIContentRefIsStableReleaseTag("v1.2") {
		t.Fatal("version ref classification mismatch")
	}
}

func TestPortableRuntimeHelperBranches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "store")
	dbPath := filepath.Join(root, "data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir db dir: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir git dir: %v", err)
	}
	if err := os.WriteFile(dbPath, []byte("db-v1"), 0o644); err != nil {
		t.Fatalf("write db: %v", err)
	}
	app := New()
	app.configPath = filepath.Join(dir, "config.toml")
	mirror, err := app.portableRuntimeDBPath(dbPath)
	if err != nil {
		t.Fatalf("runtime path: %v", err)
	}
	changed, err := refreshPortableRuntimeDB(ctx, dbPath, mirror, false)
	if err != nil || !changed {
		t.Fatalf("initial runtime copy changed=%v err=%v", changed, err)
	}
	changed, err = refreshPortableRuntimeDB(ctx, dbPath, mirror, false)
	if err != nil || changed {
		t.Fatalf("second runtime copy changed=%v err=%v", changed, err)
	}
	if needs, err := portableRuntimeNeedsCopy(filepath.Join(dir, "missing.db"), mirror); err == nil || needs {
		t.Fatalf("missing source needs=%v err=%v", needs, err)
	}
	if _, ok := portableStoreRoot(filepath.Join(dir, "plain", "db.sqlite")); ok {
		t.Fatal("plain db should not have portable root")
	}
	if gitWorktreeClean(ctx, root) {
		t.Fatal("fake git directory should not be a clean worktree")
	}
	statePath := portableStoreRefreshStatePath(mirror)
	state := portableStoreRefreshState{LastSuccess: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := writePortableStoreRefreshState(statePath, state); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if got := readPortableStoreRefreshState(statePath); got.LastSuccess == "" {
		t.Fatalf("read state = %+v", got)
	}
	if got := readPortableStoreRefreshState(filepath.Join(dir, "missing.json")); got.LastSuccess != "" {
		t.Fatalf("missing state = %+v", got)
	}
	if !recentPortableRefresh(state.LastSuccess, time.Now().UTC(), time.Hour) || recentPortableRefresh("bad", time.Now().UTC(), time.Hour) || recentPortableRefresh("", time.Now().UTC(), time.Hour) {
		t.Fatal("recent refresh classification mismatch")
	}
	t.Setenv("GITCRAWL_PORTABLE_REFRESH_TTL", "0")
	if portableStoreRefreshInterval() != 0 {
		t.Fatal("zero refresh ttl not honored")
	}
	if err := copyFileAtomic(filepath.Join(dir, "missing"), filepath.Join(dir, "out", "db")); err == nil {
		t.Fatal("missing source copy should fail")
	}
}

func TestGHCacheClearMatchingBranches(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	app := New()
	app.configPath = configPath
	dir, err := app.ghCommandCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}
	entry := ghCommandCacheEntry{
		Args:      []string{"issue", "view", "10", "-R", "openclaw/openclaw"},
		Stdout:    "{}",
		Stderr:    "",
		ExitCode:  0,
		CreatedAt: time.Now(),
		Tags:      []string{"repo:openclaw/openclaw", "issues", "issue:10"},
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	entryPath := filepath.Join(dir, "entry.json")
	if err := os.WriteFile(entryPath, data, 0o644); err != nil {
		t.Fatalf("write entry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "entry.lock"), []byte("lock"), 0o644); err != nil {
		t.Fatalf("write lock: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write ignored entry: %v", err)
	}
	if err := app.clearGHCommandCacheMatching([]string{"issue:10"}); err != nil {
		t.Fatalf("clear matching: %v", err)
	}
	if _, err := os.Stat(entryPath); !os.IsNotExist(err) {
		t.Fatalf("entry still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "entry.lock")); !os.IsNotExist(err) {
		t.Fatalf("lock still exists: %v", err)
	}
	if err := os.WriteFile(entryPath, data, 0o644); err != nil {
		t.Fatalf("rewrite entry: %v", err)
	}
	if err := app.clearGHCommandCacheForMutation(ctx, []string{"cache", "delete"}); err != nil {
		t.Fatalf("clear global mutation: %v", err)
	}
	if _, err := os.Stat(entryPath); !os.IsNotExist(err) {
		t.Fatalf("global clear left entry: %v", err)
	}
	if cfg.CacheDir == "" {
		t.Fatal("seed config cache dir should not be empty")
	}
}

func TestGHMetricsSearchRunsAndXCacheBranches(t *testing.T) {
	var counters ghXCacheCounters
	if incrementGHXCacheCounters(&counters, "unknown", nil) {
		t.Fatal("unknown counter should not increment")
	}
	for _, name := range []string{"local_hits", "fallback_hits", "stale_hits", "low_budget_stale_hits", "live_bypasses", "backend_misses", "pass_through_writes"} {
		if !incrementGHXCacheCounters(&counters, name, []string{"api", "repos/openclaw/openclaw/actions/runs/99"}) {
			t.Fatalf("counter %s did not increment", name)
		}
	}
	var bucket ghXCacheCounterBucket
	for _, name := range []string{"local_hits", "fallback_hits", "stale_hits", "low_budget_stale_hits", "live_bypasses", "backend_misses", "pass_through_writes"} {
		if !incrementGHXCacheCounterBucket(&bucket, name, []string{"pr", "view", "12", "-R", "openclaw/openclaw"}) {
			t.Fatalf("bucket counter %s did not increment", name)
		}
	}
	if incrementGHXCacheCounterBucket(&bucket, "bad", nil) {
		t.Fatal("unknown bucket counter should not increment")
	}
	if got := ghCommandMissKey([]string{"pr", "view", strings.Repeat("x", 220)}); len(got) != 180 || !strings.HasSuffix(got, "...") {
		t.Fatalf("miss key = %q len=%d", got, len(got))
	}
	if route := ghCommandRoute([]string{"api", "repos/openclaw/openclaw/actions/runs/99"}); !strings.Contains(route, "/actions/runs/:id") {
		t.Fatalf("api route = %q", route)
	}
	if route := ghCommandRoute([]string{"pr"}); route != "pr" {
		t.Fatalf("single route = %q", route)
	}
	now := time.Now().UTC()
	counters.Hourly = map[string]ghXCacheCounterBucket{
		"old":  {StartedAt: now.Add(-2 * time.Hour), LocalHits: 9},
		"new":  {StartedAt: now.Add(-5 * time.Minute), LocalHits: 1, BackendMissesByCommand: map[string]int64{"api": 2}},
		"zero": {LocalHits: 8},
	}
	recent := counters.since(time.Hour, now)
	if recent.LocalHits != 1 || recent.BackendMissesByCommand["api"] != 2 {
		t.Fatalf("recent counters = %+v", recent)
	}
	mergeCounterMap(&recent.BackendMissesByRoute, map[string]int64{"r": 3})
	if recent.BackendMissesByRoute["r"] != 3 {
		t.Fatalf("merged counters = %+v", recent.BackendMissesByRoute)
	}
	buckets := map[string]ghXCacheCounterBucket{"old": {StartedAt: now.Add(-8 * 24 * time.Hour)}, "new": {StartedAt: now}}
	pruneGHXCacheBuckets(buckets, now.Add(-7*24*time.Hour))
	if _, ok := buckets["old"]; ok || buckets["new"].StartedAt.IsZero() {
		t.Fatalf("pruned buckets = %+v", buckets)
	}
	if _, start := ghXCacheCurrentBucket(now); !start.Equal(now.Truncate(time.Hour)) {
		t.Fatalf("bucket start = %s", start)
	}
	if staleGHCommandCacheLock(fakeFileInfo{mod: now.Add(-3 * time.Minute)}) != true || staleGHCommandCacheLock(fakeFileInfo{mod: now}) {
		t.Fatal("stale lock classification mismatch")
	}

	thread := store.Thread{
		GitHubID: "99", Number: 99, Title: "Title", State: "open", HTMLURL: "https://example.com/99",
		LabelsJSON: `["bug",""]`, AuthorLogin: "alice", AuthorType: "User", Body: "body",
		UpdatedAt: "2026-05-08T00:00:00Z", CreatedAtGitHub: "2026-05-07T00:00:00Z", ClosedAtGitHub: "", IsDraft: true,
	}
	fields := "number,id,title,state,url,updatedAt,createdAt,closedAt,mergedAt,labels,isDraft,author,body"
	rows, err := ghSearchJSONRows([]store.Thread{thread}, fields)
	if err != nil || rows[0]["number"] != 99 {
		t.Fatalf("search rows=%+v err=%v", rows, err)
	}
	if labels := ghLabelsFromJSON(`not-json`); labels != nil {
		t.Fatalf("bad labels = %+v", labels)
	}
	if labels := ghLabelsFromJSON(`[{"name":"bug","color":"red"}]`); len(labels) != 1 || labels[0].Name != "bug" {
		t.Fatalf("object labels = %+v", labels)
	}
	if _, err := ghSearchJSONRows([]store.Thread{thread}, "unsupported"); err == nil {
		t.Fatal("unsupported search json field should fail")
	}
	if _, err := ghSearchJSONRows([]store.Thread{thread}, " "); err == nil {
		t.Fatal("empty search json fields should fail")
	}
	query, repo, state := parseGHSearchQuery("repo:openclaw/openclaw is:pr is:open crash")
	if query != "crash" || repo != "openclaw/openclaw" || state != "open" {
		t.Fatalf("query=%q repo=%q state=%q", query, repo, state)
	}
	if !isGHSearchKind("pull-requests") || ghSearchKind("pulls") != "pull_request" || ghSearchKind("issues") != "issue" {
		t.Fatal("search kind mismatch")
	}
	if _, err := parseGHSearchDuration("0"); err == nil {
		t.Fatal("zero duration should fail")
	}
	if duration, err := parseGHSearchDuration("5"); err != nil || duration != 5*time.Second {
		t.Fatalf("seconds duration=%s err=%v", duration, err)
	}
	if _, err := parseGHSearchLimit("5", "6"); err == nil {
		t.Fatal("disagreeing limits should fail")
	}

	runs := []store.WorkflowRun{{
		RunID: "99", RunNumber: 7, WorkflowName: "CI", Status: "completed", Conclusion: "success",
		HTMLURL: "https://example.com/run", Event: "push", HeadBranch: "main", HeadSHA: "abc",
		CreatedAtGH: "2026-05-08T00:00:00Z", UpdatedAtGH: "2026-05-08T00:01:00Z",
	}, {RunID: "not-number", WorkflowName: "Deploy"}}
	runRows, err := ghWorkflowRunJSONRows(runs, "databaseId,id,number,workflowName,name,displayTitle,status,conclusion,url,event,headBranch,headSha,createdAt,updatedAt")
	if err != nil {
		t.Fatalf("run rows: %v", err)
	}
	if runRows[0]["databaseId"] != int64(99) || runRows[1]["databaseId"] != "not-number" {
		t.Fatalf("run rows = %+v", runRows)
	}

	dir := t.TempDir()
	entry := ghCommandCacheEntry{Args: []string{"run", "list"}, CreatedAt: time.Now().Add(-time.Hour), Stdout: "[]"}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "good.json"), data, 0o644); err != nil {
		t.Fatalf("write entry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{"), 0o644); err != nil {
		t.Fatalf("write bad entry: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	found := false
	for _, entry := range entries {
		if info, ok := ghCommandCacheKeyInfoFromDirEntry(dir, entry); ok && info.Key == "good" {
			found = true
		}
	}
	if !found {
		t.Fatal("cache key info did not parse good entry")
	}
	var buf bytes.Buffer
	printGHXCacheMisses(&buf, "Misses", map[string]int64{"b": 1, "a": 2})
	if !strings.Contains(buf.String(), "Misses") {
		t.Fatalf("miss output = %q", buf.String())
	}
}

type fakeFileInfo struct{ mod time.Time }

func (f fakeFileInfo) Name() string       { return "fake" }
func (f fakeFileInfo) Size() int64        { return 0 }
func (f fakeFileInfo) Mode() os.FileMode  { return 0 }
func (f fakeFileInfo) ModTime() time.Time { return f.mod }
func (f fakeFileInfo) IsDir() bool        { return false }
func (f fakeFileInfo) Sys() any           { return nil }
