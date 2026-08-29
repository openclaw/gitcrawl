//go:build !windows

package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPortableGitCancellationAllowsOwnedCleanup(t *testing.T) {
	dir := t.TempDir()
	owned := filepath.Join(dir, "tmp_pack_owned")
	unknown := filepath.Join(dir, "tmp_pack_preexisting")
	ready := filepath.Join(dir, "ready")
	childPID := filepath.Join(dir, "child.pid")
	if err := os.WriteFile(unknown, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
trap 'wait; exit 143' TERM
sh -c 'trap '\''rm -f "$GITCRAWL_TEST_OWNED"; exit 0'\'' TERM
  echo $$ > "$GITCRAWL_TEST_CHILD_PID"
  echo owned > "$GITCRAWL_TEST_OWNED"
  echo ready > "$GITCRAWL_TEST_READY"
  sleep 30 & wait' &
wait
`
	git := filepath.Join(dir, "git")
	if err := os.WriteFile(git, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_TEST_OWNED", owned)
	t.Setenv("GITCRAWL_TEST_CHILD_PID", childPID)
	t.Setenv("GITCRAWL_TEST_READY", ready)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, err := portableGitContext(ctx, git)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runPortableGit(ctx, dir, io.Discard, "fetch") }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("child not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	started := time.Now()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("cleanup exceeded bound")
	}
	if _, err := os.Stat(owned); !os.IsNotExist(err) {
		t.Fatalf("owned child did not clean its pack: %v", err)
	}
	if data, err := os.ReadFile(unknown); err != nil || string(data) != "preserve" {
		t.Fatal("unknown file changed")
	}
	data, err := os.ReadFile(childPID)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatal("owned child remains alive")
	}
}

func TestPortableGitGrowthCancelsOnlyOwnedProcess(t *testing.T) {
	dir := t.TempDir()
	git := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(git, []byte("#!/bin/sh\ntrap 'exit 143' TERM\ndd if=/dev/zero of=owned-growth bs=1024 count=64 2>/dev/null\nsleep 30 & wait\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, err := portableGitContext(context.Background(), git)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := newPortableBudget(ctx, []string{dir}, 1, 32<<10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := budget.monitor(ctx)
	defer stop()
	started := time.Now()
	err = runPortableGit(ctx, dir, io.Discard, "fetch")
	if err == nil || !strings.Contains(err.Error(), "growth budget") {
		t.Fatalf("growth: %v", err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("growth cancellation exceeded bound")
	}
	if budget.snapshot().PeakGrowth < 64<<10 {
		t.Fatal("missing growth observation")
	}
}

func TestPortableGitTimeoutAndForcedCleanup(t *testing.T) {
	dir := t.TempDir()
	git := filepath.Join(dir, "git")
	if err := os.WriteFile(git, []byte("#!/bin/sh\ntrap '' TERM\nsleep 30 & wait\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ctx, err := portableGitContext(ctx, git)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = runPortableGit(ctx, dir, io.Discard, "fetch")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("forced cleanup exceeded bound")
	}
}

func TestPortableMaintenanceSuppressedAcrossEntryPoints(t *testing.T) {
	fixture := newPortableRefreshFixture(t, false)
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(t.TempDir(), "argv")
	wrapper := filepath.Join(t.TempDir(), "git")
	script := `#!/bin/sh
maintenance=0
gc=0
for arg do
  [ "$arg" = 'maintenance.auto=false' ] && maintenance=1
  [ "$arg" = 'gc.auto=0' ] && gc=1
done
[ "$maintenance$gc" = 11 ] || exit 91
printf '%s\n' "$@" >> "$GITCRAWL_TEST_GIT_LOG"
printf 'END\n' >> "$GITCRAWL_TEST_GIT_LOG"
exec "$GITCRAWL_TEST_REAL_GIT" "$@"
`
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_TEST_REAL_GIT", git)
	t.Setenv("GITCRAWL_TEST_GIT_LOG", log)
	t.Setenv("GITCRAWL_PORTABLE_GIT", wrapper)
	fixture.advance(t, true)
	if _, err := fixture.refresh(t, "--git", wrapper); err != nil {
		t.Fatal(err)
	}
	fixture.init(t)
	if err := os.WriteFile(filepath.Join(fixture.checkout, fixture.relative)+".gz", []byte("dirty"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPortableStore(context.Background(), fixture.remote, fixture.checkout); err != nil {
		t.Fatal(err)
	}
	if err := refreshPortableStoreForDB(context.Background(), filepath.Join(fixture.checkout, fixture.relative)); err != nil {
		t.Fatal(err)
	}
	if _, err := recloneMalformedPortableStoreForDB(context.Background(), filepath.Join(fixture.checkout, fixture.relative), fixture.configPath); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"fetch", "merge", "clone", "reset"} {
		if !bytes.Contains(data, []byte("\n"+command+"\n")) {
			t.Fatalf("entry point not exercised: %s", command)
		}
	}
	for _, call := range strings.Split(string(data), "END\n") {
		if strings.Contains(call, "\nfetch\n") && !strings.Contains(call, "\n--no-auto-maintenance\n") {
			t.Fatal("fetch omitted maintenance suppression")
		}
	}
}

func TestPortableRefreshReportsPartialRefFailure(t *testing.T) {
	fixture := newPortableRefreshFixture(t, false)
	initial, err := fixture.refresh(t)
	if err != nil {
		t.Fatal(err)
	}
	beforeMirror, _ := fileSHA256(fixture.mirror)
	fixture.advance(t, true)
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nfor arg do\n[ \"$arg\" = update-ref ] && exit 42\ndone\nexec \"$GITCRAWL_TEST_REAL_GIT\" \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_TEST_REAL_GIT", git)
	result, err := fixture.refresh(t, "--git", wrapper)
	if err == nil || result.Result != "partial" || result.AfterCommit != result.TargetCommit || result.AfterCommit == result.BeforeCommit {
		t.Fatalf("partial advancement: %+v %v", result, err)
	}
	if portableTestGit(t, fixture.checkout, "rev-parse", "HEAD") != result.TargetCommit || portableTestGit(t, fixture.checkout, "rev-parse", "refs/remotes/origin/main") != initial.AfterCommit {
		t.Fatal("partial result does not match actual refs")
	}
	if after, _ := fileSHA256(fixture.mirror); beforeMirror != after {
		t.Fatal("partial ref failure replaced last-good mirror")
	}
	if _, err := fixture.refresh(t); err != nil {
		t.Fatal("safe retry after partial result", err)
	}
}
