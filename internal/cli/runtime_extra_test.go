package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPortableStoreRootPropagatesGitProbeFailure(t *testing.T) {
	dir := t.TempDir()
	if err := runGit(context.Background(), "", "init", dir); err != nil {
		t.Fatalf("git init: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := portableStoreRoot(ctx, filepath.Join(dir, "gitcrawl.db"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("portable store root error = %v, want context canceled", err)
	}
}

func TestPortableStoreRootBindsCandidateToOwningWorktree(t *testing.T) {
	ctx := context.Background()
	outer := t.TempDir()
	if err := runGit(ctx, "", "init", outer); err != nil {
		t.Fatalf("git init: %v", err)
	}
	nested := filepath.Join(outer, "data")
	if err := os.MkdirAll(filepath.Join(nested, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir stray Git metadata: %v", err)
	}

	root, ok, err := portableStoreRoot(ctx, filepath.Join(nested, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, outer) {
		t.Fatalf("portable store root = %q, ok=%v, want outer worktree %q", root, ok, outer)
	}
}

func TestPortableStoreRootIgnoresInheritedGitSelection(t *testing.T) {
	ctx := context.Background()
	portable := t.TempDir()
	if err := runGit(ctx, "", "init", portable); err != nil {
		t.Fatalf("git init portable: %v", err)
	}
	other := t.TempDir()
	if err := runGit(ctx, "", "init", other); err != nil {
		t.Fatalf("git init other: %v", err)
	}
	t.Setenv("GIT_DIR", filepath.Join(other, ".git"))
	t.Setenv("GIT_WORK_TREE", other)

	root, ok, err := portableStoreRoot(ctx, filepath.Join(portable, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, portable) {
		t.Fatalf("portable store root = %q, ok=%v, want %q", root, ok, portable)
	}
}

func TestPortableStoreRootIgnoresGitTraceOutput(t *testing.T) {
	ctx := context.Background()
	portable := t.TempDir()
	if err := runGit(ctx, "", "init", portable); err != nil {
		t.Fatalf("git init: %v", err)
	}
	t.Setenv("GIT_TRACE", "1")

	root, ok, err := portableStoreRoot(ctx, filepath.Join(portable, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, portable) {
		t.Fatalf("portable store root = %q, ok=%v, want %q", root, ok, portable)
	}
}

func TestPortableStoreRootIgnoresCommandScopeGitConfig(t *testing.T) {
	ctx := context.Background()
	portable := t.TempDir()
	if err := runGit(ctx, "", "init", portable); err != nil {
		t.Fatalf("git init: %v", err)
	}
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.bare")
	t.Setenv("GIT_CONFIG_VALUE_0", "true")

	root, ok, err := portableStoreRoot(ctx, filepath.Join(portable, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, portable) {
		t.Fatalf("portable store root = %q, ok=%v, want %q", root, ok, portable)
	}
}

func TestPortableRuntimeUtilityBranches(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	mirror := filepath.Join(dir, "runtime", "source.db")
	if _, err := portableRuntimeNeedsCopy(source, mirror); err == nil {
		t.Fatal("missing source should fail")
	}
	if err := os.WriteFile(source, []byte("v1"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	needs, err := portableRuntimeNeedsCopy(source, mirror)
	if err != nil || !needs {
		t.Fatalf("missing mirror needs copy=%v err=%v", needs, err)
	}
	if err := copyFileAtomic(source, mirror); err != nil {
		t.Fatalf("copy mirror: %v", err)
	}
	if err := os.WriteFile(mirror+"-wal", []byte("wal"), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	if err := os.WriteFile(mirror+"-shm", []byte("shm"), 0o644); err != nil {
		t.Fatalf("write shm: %v", err)
	}
	if err := os.Chtimes(mirror, time.Now().Add(time.Hour), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("age mirror: %v", err)
	}
	needs, err = portableRuntimeNeedsCopy(source, mirror)
	if err != nil || needs {
		t.Fatalf("fresh mirror needs copy=%v err=%v", needs, err)
	}
	if err := copyFileAtomic(source, mirror); err != nil {
		t.Fatalf("recopy mirror: %v", err)
	}
	if _, err := os.Stat(mirror + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("wal sidecar should be removed, err=%v", err)
	}
	if _, err := os.Stat(mirror + "-shm"); !os.IsNotExist(err) {
		t.Fatalf("shm sidecar should be removed, err=%v", err)
	}

	statePath := portableStoreRefreshStatePath(mirror)
	state := portableStoreRefreshState{LastAttempt: "attempt", LastSuccess: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := writePortableStoreRefreshState(statePath, state); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if got := readPortableStoreRefreshState(statePath); got.LastAttempt != "attempt" || got.LastSuccess == "" {
		t.Fatalf("state = %+v", got)
	}
	if err := os.WriteFile(statePath, []byte("{"), 0o600); err != nil {
		t.Fatalf("write invalid state: %v", err)
	}
	if got := readPortableStoreRefreshState(statePath); got.LastAttempt != "" {
		t.Fatalf("invalid state should decode empty, got %+v", got)
	}
	now := time.Now().UTC()
	if recentPortableRefresh("", now, time.Minute) || recentPortableRefresh("bad", now, time.Minute) || !recentPortableRefresh(now.Format(time.RFC3339Nano), now, time.Minute) {
		t.Fatal("recent refresh classification mismatch")
	}
	lockPath := filepath.Join(dir, "refresh.lock")
	if err := os.WriteFile(lockPath, []byte("123\n"), 0o600); err != nil {
		t.Fatalf("write lock: %v", err)
	}
	removeStalePortableRefreshLock(lockPath, now)
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("fresh lock should remain: %v", err)
	}
	old := now.Add(-3 * portableStoreRefreshTimeout)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatalf("age lock: %v", err)
	}
	removeStalePortableRefreshLock(lockPath, now)
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("stale lock should be removed, err=%v", err)
	}
	t.Setenv("GITCRAWL_PORTABLE_REFRESH_TTL", "0")
	if got := portableStoreRefreshInterval(); got != 0 {
		t.Fatalf("zero ttl = %s", got)
	}
	t.Setenv("GITCRAWL_PORTABLE_REFRESH_TTL", "bad")
	if got := portableStoreRefreshInterval(); got != portableStoreRefreshTTL {
		t.Fatalf("bad ttl fallback = %s", got)
	}
	if err := refreshPortableStoreForDB(context.Background(), source); err != nil {
		t.Fatalf("non-portable refresh should be no-op: %v", err)
	}
	configDir := filepath.Join(dir, "config-root")
	t.Setenv("GITCRAWL_CONFIG", filepath.Join(configDir, "config.toml"))
	defaultStore := filepath.Join(configDir, "stores", "gitcrawl-store")
	if err := os.MkdirAll(defaultStore, 0o755); err != nil {
		t.Fatalf("mkdir default store: %v", err)
	}
	if !portableStoreRepairAllowed(defaultStore, "") {
		t.Fatal("default portable store should be repairable")
	}
	if portableStoreRepairAllowed(configDir, "") {
		t.Fatal("config root should not be repairable without marker")
	}
	markedStore := filepath.Join(dir, "custom-store")
	markedInfo := filepath.Join(markedStore, ".git", "info")
	if err := os.MkdirAll(markedInfo, 0o755); err != nil {
		t.Fatalf("mkdir marked store info: %v", err)
	}
	if err := os.WriteFile(filepath.Join(markedInfo, portableStoreMarkerFile), []byte("gitcrawl portable store\n"), 0o644); err != nil {
		t.Fatalf("write marked store marker: %v", err)
	}
	if !portableStoreRepairAllowed(markedStore, "") {
		t.Fatal("marked portable store should be repairable")
	}
	explicitConfigDir := filepath.Join(dir, "explicit-config-root")
	explicitConfigPath := filepath.Join(explicitConfigDir, "nested", "config.toml")
	explicitStore := filepath.Join(explicitConfigDir, "nested", "stores", "gitcrawl-store")
	if err := os.MkdirAll(explicitStore, 0o755); err != nil {
		t.Fatalf("mkdir explicit default store: %v", err)
	}
	if !portableStoreRepairAllowed(explicitStore, explicitConfigPath) {
		t.Fatal("explicit-config default portable store should be repairable")
	}
	lockRoot := filepath.Join(dir, "locked-store")
	lockPath = filepath.Join(lockRoot, ".git", "index.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatalf("write index lock: %v", err)
	}
	oldLock := now.Add(-time.Minute)
	if err := os.Chtimes(lockPath, oldLock, oldLock); err != nil {
		t.Fatalf("age index lock: %v", err)
	}
	removed, err := removeStaleGitIndexLock(context.Background(), lockRoot, staleGitIndexLockAge)
	if err != nil || !removed {
		t.Fatalf("remove stale index lock removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("stale index lock should be removed, err=%v", err)
	}

	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatalf("rewrite index lock: %v", err)
	}
	if err := os.Chtimes(lockPath, oldLock, oldLock); err != nil {
		t.Fatalf("age index lock with failing lsof: %v", err)
	}
	fakeBin := filepath.Join(dir, "fake-bin")
	if err := os.MkdirAll(fakeBin, 0o755); err != nil {
		t.Fatalf("mkdir fake bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "lsof"), []byte("#!/bin/sh\nexit 2\n"), 0o755); err != nil {
		t.Fatalf("write fake lsof: %v", err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	removed, err = removeStaleGitIndexLock(context.Background(), lockRoot, staleGitIndexLockAge)
	if err != nil || removed {
		t.Fatalf("failing lsof should not remove lock, removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("index lock should remain when lsof fails: %v", err)
	}
}
