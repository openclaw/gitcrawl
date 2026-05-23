package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestGHShimPRCacheAndPolicyHelperBranches(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	app := New()
	app.configPath = configPath
	var stdout bytes.Buffer
	app.Stdout = &stdout

	if err := app.Run(ctx, []string{"--config", configPath, "gh", "pr", "checks", "12", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("human pr checks: %v", err)
	}
	if !strings.Contains(stdout.String(), "test\tcompleted\tsuccess") {
		t.Fatalf("human checks = %q", stdout.String())
	}
	cache, err := app.localGHPullRequestCache(ctx, "openclaw/openclaw", 12)
	if err != nil {
		t.Fatalf("local pr cache: %v", err)
	}
	if _, err := app.loadGHPullRequestCache(ctx, "openclaw/openclaw", 12, false); err != nil {
		t.Fatalf("load cached pr detail without freshness: %v", err)
	}
	if _, err := app.loadGHPullRequestCache(ctx, "openclaw/openclaw", 12, true); err != nil {
		t.Fatalf("load fresh cached pr detail: %v", err)
	}
	if !ghPullRequestCacheFresh(cache) {
		t.Fatalf("seeded cache should be fresh: %+v", cache.Detail)
	}
	cache.Detail.RawJSON = `{"head":{"sha":"different"}}`
	if ghPullRequestCacheFresh(cache) {
		t.Fatal("mismatched raw head sha should be stale")
	}
	cache.Detail.RawJSON = `{"head":{"sha":"abc123"}}`
	cache.Detail.FetchedAt = "bad"
	if ghPullRequestCacheFresh(cache) {
		t.Fatal("bad fetched timestamp should be stale")
	}
	if !app.shouldAutoHydrateGHPRDetails(localGHUnsupported(errors.New("pull request detail: sql: no rows in result set"))) {
		t.Fatal("missing local PR cache should auto-hydrate")
	}
	t.Setenv("GITCRAWL_GH_AUTO_HYDRATE", "0")
	if app.shouldAutoHydrateGHThread(nil) {
		t.Fatal("auto-hydrate env disable not honored")
	}
	if _, err := app.loadGHPullRequestCache(ctx, "openclaw/openclaw", 9999, true); err == nil {
		t.Fatal("missing PR cache with auto-hydrate disabled should fail")
	}
	t.Setenv("GITCRAWL_GH_AUTO_HYDRATE", "")
	if isMissingLocalPRCache(nil) || !isMissingLocalPRCache(localGHUnsupported(errors.New("cached PR branch \"x\" was not found"))) {
		t.Fatal("missing cache classification mismatch")
	}
	number, err := app.findGHPullRequestNumberByBranch(ctx, "openclaw/openclaw", "manifest-cache")
	if err != nil || number != 12 {
		t.Fatalf("branch lookup number=%d err=%v", number, err)
	}
	if _, err := app.findGHPullRequestNumberByBranch(ctx, "openclaw/openclaw", "missing"); err == nil {
		t.Fatal("missing branch lookup should fail")
	}
	if got := ghPRHeadRefFromRawJSON(`{"head":{"ref":" feature/cache "}}`); got != "feature/cache" {
		t.Fatalf("head ref = %q", got)
	}
	if got := ghPRHeadRefFromRawJSON(`{`); got != "" {
		t.Fatalf("invalid head ref = %q", got)
	}
	if !ghPRFieldsNeedFresh([]string{"number", "statusCheckRollup"}) || !ghPRFieldsNeedFresh([]string{"mergeStateStatus"}) || !ghPRFieldsNeedFresh([]string{"reviewDecision"}) || ghPRFieldsNeedFresh([]string{"files"}) {
		t.Fatal("fresh field detection mismatch")
	}
	thread := store.Thread{IsDraft: true}
	for _, field := range []string{"headRepositoryOwner", "headRepository", "mergeStateStatus", "additions", "deletions", "changedFiles", "isDraft"} {
		if _, err := ghPRDetailJSONValue(thread, cache, field); err != nil {
			t.Fatalf("field %s: %v", field, err)
		}
	}
	if _, err := ghPRDetailJSONValue(thread, cache, "unsupported"); err == nil {
		t.Fatal("unsupported PR detail field should fail")
	}
	var out bytes.Buffer
	app.Stdout = &out
	if err := app.writeJSONValue(map[string]any{"value": 1}, ""); err != nil || !strings.Contains(out.String(), `"value": 1`) {
		t.Fatalf("write json out=%q err=%v", out.String(), err)
	}
	if err := app.writeJSONValue(make(chan int), ""); err == nil {
		t.Fatal("unmarshalable JSON value should fail")
	}
	out.Reset()
	if err := app.writeJSONValue(map[string]any{"value": 2}, ".value"); err != nil || strings.TrimSpace(out.String()) != "2" {
		t.Fatalf("write json jq out=%q err=%v", out.String(), err)
	}
	t.Setenv("PATH", "")
	if err := app.writeJSONValue(map[string]any{"value": 2}, ".value"); err == nil {
		t.Fatal("jq expression without jq executable should fail")
	}
}

func TestGHShimCachePolicyExtraBranches(t *testing.T) {
	if cacheableGHRead(nil) || cacheableGHRead([]string{"repo", "view", "--web"}) {
		t.Fatal("interactive or empty gh commands should not be cacheable")
	}
	if !cacheableGHRead([]string{"gist", "view", "1"}) || !cacheableGHRead([]string{"project", "item-list"}) || !cacheableGHRead([]string{"cache", "list"}) {
		t.Fatal("expected read-only command to be cacheable")
	}
	if ghAPIReadOnly([]string{"repos/openclaw/gitcrawl/issues", "-f", "title=x"}) || ghAPIReadOnly([]string{"repos/openclaw/gitcrawl", "-X"}) || ghAPIReadOnly([]string{"repos/openclaw/gitcrawl", "--method=PATCH"}) {
		t.Fatal("mutating or malformed API command should not be read-only")
	}
	if got := ghAPIPathArg([]string{"--paginate", "-H", "Accept: json", "--jq", ".[]", "--template", "{{.}}", "repos/openclaw/gitcrawl/issues"}); got != "repos/openclaw/gitcrawl/issues" {
		t.Fatalf("api path with skipped flags = %q", got)
	}
	if got := ghAPIPathArg([]string{"-f", "x=y"}); got != "" {
		t.Fatalf("api path with only fields = %q", got)
	}
	if !ghAPIReadOnly([]string{"repos/openclaw/gitcrawl", "--method=GET"}) {
		t.Fatal("GET API command should be read-only")
	}
	sanitizedSearch := sanitizeGHShimArgs([]string{"api", "search/issues", "-f", "q=repo:openclaw/gitcrawl+is:pr", "-f", "per_page=1"})
	if got := strings.Join(sanitizedSearch, " "); got != "api --method GET search/issues -f q=repo:openclaw/gitcrawl+is:pr -f per_page=1" {
		t.Fatalf("sanitized search api = %q", got)
	}
	if !cacheableGHRead(sanitizedSearch) {
		t.Fatal("sanitized search API command should be cacheable")
	}
	if got := strings.Join(sanitizeGHShimArgs([]string{"api", "repos/openclaw/gitcrawl/issues", "-f", "title=x"}), " "); got != "api repos/openclaw/gitcrawl/issues -f title=x" {
		t.Fatalf("non-search API command should stay unchanged: %q", got)
	}
	if ghGraphQLReadOnly([]string{"graphql"}) || ghGraphQLReadOnly([]string{"graphql", "-X"}) || ghGraphQLReadOnly([]string{"graphql", "-X", "PUT", "-f", "query={ viewer { login } }"}) || ghGraphQLReadOnly([]string{"graphql", "--field=query=@query.graphql"}) {
		t.Fatal("malformed or mutating GraphQL command should not be read-only")
	}
	if !ghGraphQLReadOnly([]string{"graphql", "--field=query=query { viewer { login } }"}) {
		t.Fatal("GraphQL query should be read-only")
	}
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "2m")
	if got := ghCommandCacheTTL([]string{"repo", "view"}); got != 2*time.Minute {
		t.Fatalf("env ttl = %s", got)
	}
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "")
	ttlCases := []struct {
		args []string
		want time.Duration
	}{
		{[]string{"api", "repos/openclaw/gitcrawl/pages/builds/latest"}, 2 * time.Minute},
		{[]string{"api", "repos/openclaw/gitcrawl/pages/health"}, 15 * time.Minute},
		{[]string{"api", "repos/openclaw/gitcrawl/actions/jobs/123/logs"}, 12 * time.Hour},
		{[]string{"api", "repos/openclaw/gitcrawl/actions/jobs/123"}, time.Minute},
		{[]string{"api", "repos/openclaw/gitcrawl/actions/runs/123/pending_deployments"}, 30 * time.Second},
		{[]string{"api", "repos/openclaw/gitcrawl/actions/workflows/ci.yml"}, 15 * time.Minute},
		{[]string{"api", "repos/openclaw/gitcrawl/releases/latest"}, time.Hour},
		{[]string{"api", "repos/openclaw/gitcrawl/branches/main"}, 10 * time.Minute},
		{[]string{"workflow", "list"}, 15 * time.Minute},
		{[]string{"issue", "view"}, 15 * time.Minute},
		{[]string{"unknown"}, 5 * time.Minute},
	}
	for _, tc := range ttlCases {
		if got := ghCommandCacheTTL(tc.args); got != tc.want {
			t.Fatalf("ttl %v = %s, want %s", tc.args, got, tc.want)
		}
	}
	if !ghAPIContentRefIsStable([]string{"repos/openclaw/gitcrawl/contents/a?ref=v1.2.3-beta+1"}) || ghAPIContentRefIsStable([]string{"repos/openclaw/gitcrawl/contents/a?ref=refs/heads/v1.2.3"}) || ghAPIContentRefIsStable([]string{"repos/openclaw/gitcrawl/contents/a?ref=v1.2"}) {
		t.Fatal("stable content ref classification mismatch")
	}
	t.Setenv("GH_REPO", "openclaw/from-env")
	repo, number, ok := parseGHPRDiffIdentityArgs([]string{"pr", "diff", "42"})
	if !ok || repo != "openclaw/from-env" || number != 42 {
		t.Fatalf("diff identity repo=%q number=%d ok=%v", repo, number, ok)
	}
	repo, number, ok = parseGHPRDiffIdentityArgs([]string{"pr", "diff", "https://github.com/openclaw/openclaw/pull/78601"})
	if !ok || repo != "openclaw/openclaw" || number != 78601 {
		t.Fatalf("diff URL identity repo=%q number=%d ok=%v", repo, number, ok)
	}
	repo, number, ok = parseGHPRDiffIdentityArgs([]string{"pr", "diff", "https://github.com/openclaw/openclaw/issues/78601"})
	if !ok || repo != "openclaw/openclaw" || number != 78601 {
		t.Fatalf("diff issue URL identity repo=%q number=%d ok=%v", repo, number, ok)
	}
	for _, args := range [][]string{{"issue", "close"}, {"pr", "merge"}, {"project", "item-add"}, {"release", "upload"}, {"repo", "delete"}, {"run", "rerun"}, {"secret", "set"}, {"variable", "delete"}, {"workflow", "disable"}, {"api", "repos/openclaw/gitcrawl/issues", "-f", "title=x"}} {
		if !mutatingGHCommand(args) {
			t.Fatalf("%v should be mutating", args)
		}
	}
	if mutatingGHCommand([]string{"pr", "checkout"}) || mutatingGHCommand([]string{"repo", "view"}) || mutatingGHCommand([]string{"api", "repos/openclaw/gitcrawl"}) {
		t.Fatal("read-only commands classified as mutating")
	}
	for _, remote := range []string{"git@github.com:openclaw/gitcrawl.git", "https://github.com/openclaw/gitcrawl.git", "ssh://git@github.com/openclaw/gitcrawl.git"} {
		if got, err := ownerRepoFromGitRemote(remote); err != nil || got != "openclaw/gitcrawl" {
			t.Fatalf("remote %q => %q err=%v", remote, got, err)
		}
	}
	if _, err := ownerRepoFromGitRemote("not-a-github-remote"); err == nil {
		t.Fatal("bad remote should fail")
	}
	app := New()
	if got, err := app.resolveGHRepo(context.Background(), " openclaw/explicit "); err != nil || got != "openclaw/explicit" {
		t.Fatalf("explicit repo = %q err=%v", got, err)
	}
	if got, err := app.resolveGHRepo(context.Background(), ""); err != nil || got != "openclaw/from-env" {
		t.Fatalf("env repo = %q err=%v", got, err)
	}
	t.Setenv("GH_REPO", "")
	repoDir := t.TempDir()
	if err := runGit(context.Background(), repoDir, "init", "-b", "main"); err != nil {
		t.Fatalf("init git repo: %v", err)
	}
	if err := runGit(context.Background(), repoDir, "remote", "add", "origin", "https://github.com/openclaw/gitcrawl.git"); err != nil {
		t.Fatalf("add origin: %v", err)
	}
	original, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() { _ = os.Chdir(original) }()
	if err := os.Chdir(repoDir); err != nil {
		t.Fatalf("chdir repo: %v", err)
	}
	if got, err := app.resolveGHRepo(context.Background(), ""); err != nil || got != "openclaw/gitcrawl" {
		t.Fatalf("git remote repo = %q err=%v", got, err)
	}
	ghPath := filepath.Join(t.TempDir(), "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho real-gh:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	var ghOut bytes.Buffer
	app.Stdout = &ghOut
	if err := app.runGHShim(context.Background(), nil); err != nil {
		t.Fatalf("empty gh shim fallback: %v", err)
	}
	if strings.TrimSpace(ghOut.String()) != "real-gh:" {
		t.Fatalf("empty gh shim output = %q", ghOut.String())
	}
	shimPath := filepath.Join(t.TempDir(), "gitcrawl-gh")
	if err := os.WriteFile(shimPath, []byte("#!/bin/sh\necho shim\n"), 0o755); err != nil {
		t.Fatalf("write fake shim: %v", err)
	}
	shimLink := filepath.Join(t.TempDir(), "gh")
	if err := os.Symlink(shimPath, shimLink); err != nil {
		t.Fatalf("symlink fake shim: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", shimLink)
	if _, err := resolveRealGHPath(); err == nil || !strings.Contains(err.Error(), "gitcrawl shim") {
		t.Fatalf("shim path should fail fast, err=%v", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", exe)
	if _, err := resolveRealGHPath(); err == nil || !strings.Contains(err.Error(), "gitcrawl shim") {
		t.Fatalf("current executable backend should fail fast, err=%v", err)
	}
	hardLink := filepath.Join(t.TempDir(), "gh")
	if err := os.Link(exe, hardLink); err != nil {
		t.Fatalf("hard-link executable: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", hardLink)
	if _, err := resolveRealGHPath(); err == nil || !strings.Contains(err.Error(), "gitcrawl shim") {
		t.Fatalf("hard-linked executable backend should fail fast, err=%v", err)
	}
	copiedPath := filepath.Join(t.TempDir(), "gh")
	copiedBytes, err := os.ReadFile(exe)
	if err != nil {
		t.Fatalf("read executable: %v", err)
	}
	if err := os.WriteFile(copiedPath, copiedBytes, 0o755); err != nil {
		t.Fatalf("copy executable: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", copiedPath)
	if _, err := resolveRealGHPath(); err == nil || !strings.Contains(err.Error(), "gitcrawl shim") {
		t.Fatalf("copied executable backend should fail fast, err=%v", err)
	}
	if ghBackendModeUsable(0o666, "darwin") {
		t.Fatal("unix backend without execute bits should be unusable")
	}
	if !ghBackendModeUsable(0o666, "windows") {
		t.Fatal("windows backend should not require unix execute bits")
	}
	t.Setenv("GITCRAWL_GH_STALE_GRACE", "3m")
	if got := ghCommandCacheStaleGrace([]string{"api", "users/octocat"}); got != 3*time.Minute {
		t.Fatalf("env stale grace = %s", got)
	}
	t.Setenv("GITCRAWL_GH_STALE_GRACE", "")
	if got := ghCommandCacheStaleGrace([]string{"api", "users/octocat"}); got != 24*time.Hour {
		t.Fatalf("user stale grace = %s", got)
	}
}
