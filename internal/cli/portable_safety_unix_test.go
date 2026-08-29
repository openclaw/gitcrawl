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

func TestPortableConfigScopeIsolationAndRefusal(t *testing.T) {
	fixture := newPortableRefreshFixture(t, true)
	if _, err := fixture.refresh(t); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "helper-ran")
	helper := filepath.Join(dir, "helper")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\ntouch \"$GITCRAWL_TEST_MARKER\"\nexit 99\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_TEST_MARKER", marker)
	t.Setenv("GITCRAWL_TEST_REAL_GIT", realGit)
	// An operator-selected wrapper supplies a synthetic system scope without
	// modifying the machine's Git configuration or reading its values.
	wrapper := filepath.Join(dir, "git")
	script := "#!/bin/sh\nexport GIT_CONFIG_SYSTEM=\"${GIT_CONFIG_SYSTEM:-$GITCRAWL_TEST_SYSTEM_CONFIG}\"\nexec \"$GITCRAWL_TEST_REAL_GIT\" \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_PORTABLE_GIT", wrapper)
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "xdg"))
	for _, scope := range []string{"global", "system"} {
		t.Run(scope, func(t *testing.T) {
			configPath := filepath.Join(dir, ".gitconfig")
			if scope == "system" {
				configPath = filepath.Join(dir, "system.config")
				t.Setenv("GITCRAWL_TEST_SYSTEM_CONFIG", configPath)
				t.Setenv("GIT_CONFIG_SYSTEM", "")
				t.Setenv("GIT_CONFIG_NOSYSTEM", "")
			} else {
				t.Setenv("GIT_CONFIG_GLOBAL", "")
			}
			portableTestGit(t, fixture.checkout, "config", "--file", configPath, "filter.fixture.smudge", helper)
			before := portableTestSnapshot(t, filepath.Dir(fixture.configPath))
			result, err := fixture.refresh(t)
			if err == nil || result.Stage != "admission" || result.Reason != "unsupported portable Git configuration ("+scope+" scope: filters)" {
				t.Fatalf("synthetic %s scope not diagnosed: %+v %v", scope, result, err)
			}
			if !bytes.Equal(before, portableTestSnapshot(t, filepath.Dir(fixture.configPath))) {
				t.Fatal("config refusal mutated subscriber")
			}
			t.Setenv("GIT_CONFIG_"+strings.ToUpper(scope), os.DevNull)
			if result, err := fixture.refresh(t); err != nil || result.MirrorResult != "unchanged" {
				t.Fatalf("scope isolation was discarded: %+v %v", result, err)
			}
		})
	}
	// These settings are overridden for every portable operation. Actual hook
	// files, attributes, filters and redirection remain separate refusals.
	for _, key := range []string{"core.hooksPath", "core.fsmonitor", "core.attributesFile", "core.sshCommand"} {
		portableTestGit(t, fixture.checkout, "config", key, helper)
	}
	if _, err := fixture.refresh(t); err != nil {
		t.Fatalf("neutralized config refused: %v", err)
	}
	for _, key := range []string{"filter.fixture.smudge", "url.https://example.invalid/private-subsection.insteadOf", "remote.origin.uploadpack"} {
		t.Run(key, func(t *testing.T) {
			portableTestGit(t, fixture.checkout, "config", key, helper)
			before := portableTestSnapshot(t, filepath.Dir(fixture.configPath))
			result, err := fixture.refresh(t)
			if err == nil || result.Stage != "admission" || !strings.Contains(result.Reason, "local scope") || strings.Contains(result.Reason, "private-subsection") || strings.Contains(result.Reason, helper) {
				t.Fatalf("unsafe/unsanitized local config admission: %+v %v", result, err)
			}
			if !bytes.Equal(before, portableTestSnapshot(t, filepath.Dir(fixture.configPath))) {
				t.Fatal("local config refusal mutated subscriber")
			}
			portableTestGit(t, fixture.checkout, "config", "--unset", key)
		})
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Git executed a neutralized or rejected helper: %v", err)
	}
}

func TestPortableGitFailureDiagnostics(t *testing.T) {
	dir := t.TempDir()
	git := filepath.Join(dir, "git")
	if err := os.WriteFile(git, []byte("#!/bin/sh\nprintf '%s' \"$GITCRAWL_TEST_DIAGNOSTIC\" >&2\nexit 128\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, err := portableGitContext(context.Background(), git)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, message, want string }{
		{"missing-remote", "fatal: synthetic-private-path does not appear to be a git repository", "remote repository unavailable; verify the remote path and read access"},
		{"missing-local", "fatal: repository 'synthetic-private-path' does not exist", "remote repository unavailable"},
		{"not-found", "remote: Repository not found. synthetic-private-path", "remote repository unavailable"},
		{"authentication", "fatal: Authentication failed for synthetic-private-path", "check the credential helper or SSH identity"},
		{"ssh", "synthetic-private-path: Permission denied (publickey).", "Git authentication failed"},
		{"prompt", "fatal: could not read Username for synthetic-private-path: terminal prompts disabled", "Git authentication failed"},
		{"disk", "fatal: cannot write synthetic-private-path: No space left on device", "restore free-space headroom before retrying"},
		{"dns", "fatal: unable to access synthetic-private-path: Could not resolve host", "check network connectivity and remote availability"},
		{"connection", "Failed to connect to synthetic-private-path port 443", "Git connection failed"},
		{"index-lock", "fatal: Unable to create synthetic-private-path/index.lock: File exists", "index.lock file exists"},
		{"dirty", "Your local changes to synthetic-private-path would be overwritten by merge", "Your local changes would be overwritten by merge"},
		{"unknown", "unrecognized synthetic-private-path diagnostic", "verify Git version, repository state and remote access"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GITCRAWL_TEST_DIAGNOSTIC", tc.message)
			err := runPortableGit(ctx, dir, io.Discard, "fetch", "origin")
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 128 || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected safe classification %q with exit 128, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "synthetic-private-path") {
				t.Fatal("Git failure exposed raw diagnostics")
			}
		})
	}
}

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
