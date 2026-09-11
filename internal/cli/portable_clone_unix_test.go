//go:build darwin || linux

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncPortableStoreFailedCloneCanRetry(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing-empty-directory=%t", existing), func(t *testing.T) {
			fixture := newPortableRefreshFixture(t, false)
			target := filepath.Join(t.TempDir(), "checkout")
			var original os.FileInfo
			if existing {
				if err := os.Mkdir(target, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(target, 0o750); err != nil {
					t.Fatal(err)
				}
				var err error
				original, err = os.Stat(target)
				if err != nil {
					t.Fatal(err)
				}
			}
			checkOriginal := func() {
				t.Helper()
				if original != nil {
					current, err := os.Stat(target)
					if err != nil || !os.SameFile(original, current) || current.Mode() != original.Mode() {
						t.Fatalf("original destination metadata changed: %v %v", current, err)
					}
				}
			}
			realGit, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			wrapper := filepath.Join(t.TempDir(), "git")
			script := `#!/bin/sh
clone=false
for arg in "$@"; do
  if [ "$arg" = clone ]; then clone=true; fi
  target="$arg"
done
if "$clone"; then
  "$GITCRAWL_TEST_REAL_GIT" init -b main "$target" >/dev/null || exit $?
  "$GITCRAWL_TEST_REAL_GIT" -C "$target" remote add origin "$GITCRAWL_TEST_REMOTE" || exit $?
  printf partial > "$target/.git/index"
  printf preserve > "$target/concurrent-file"
  echo 'synthetic-private-path: No space left on device' >&2
  exit 79
fi
exec "$GITCRAWL_TEST_REAL_GIT" "$@"
`
			if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("GITCRAWL_TEST_REAL_GIT", realGit)
			t.Setenv("GITCRAWL_TEST_REMOTE", fixture.remote)
			t.Setenv("GITCRAWL_PORTABLE_GIT", wrapper)
			_, err = syncPortableStore(context.Background(), fixture.remote, target)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 79 || !strings.Contains(err.Error(), "insufficient disk space for Git") {
				t.Fatalf("expected original clone failure, got %v", err)
			}
			entries, err := os.ReadDir(target)
			if existing && (err != nil || len(entries) != 0) || !existing && !os.IsNotExist(err) {
				t.Fatalf("failed clone changed destination: entries=%v err=%v", entries, err)
			}
			checkOriginal()
			stagingParent := filepath.Dir(target)
			if existing {
				stagingParent = target
			}
			staging, err := filepath.Glob(filepath.Join(stagingParent, ".checkout.clone-*"))
			if err != nil {
				t.Fatal(err)
			}
			if len(staging) != 0 {
				t.Fatalf("failed clone left owned staging directories: %v", staging)
			}
			t.Setenv("GITCRAWL_PORTABLE_GIT", realGit)
			if action, err := syncPortableStore(context.Background(), fixture.remote, target); err != nil || action != "cloned" {
				t.Fatalf("retry: action=%q err=%v", action, err)
			}
			checkOriginal()
			if err := sqliteStoreHealth(context.Background(), filepath.Join(target, fixture.relative)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPortableCloneRetainsLegacyFilesystemSupport(t *testing.T) {
	for _, tc := range []struct{ existing, absentOnly bool }{{}, {existing: true}, {absentOnly: true}, {existing: true, absentOnly: true}} {
		t.Run(fmt.Sprintf("existing=%t/absent-only=%t", tc.existing, tc.absentOnly), func(t *testing.T) {
			fixture := newPortableRefreshFixture(t, false)
			target := filepath.Join(t.TempDir(), "checkout")
			if tc.existing {
				if err := os.Mkdir(target, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			ctx, release, err := acquirePortableOwner(context.Background(), target)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			calls := 0
			unsupported := func(string, string) error {
				calls++
				if tc.absentOnly && calls == 1 {
					return os.ErrExist
				}
				return errPortableExclusiveUnavailable
			}
			if err := clonePortableStoreWithRename(ctx, fixture.remote, target, unsupported); err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if tc.absentOnly {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("unsupported rename invoked %d times", calls)
			}
			if err := sqliteStoreHealth(ctx, filepath.Join(target, fixture.relative)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSyncPortableStorePreservesWritableDestinationUnderReadOnlyParent(t *testing.T) {
	fixture := newPortableRefreshFixture(t, false)
	parent := t.TempDir()
	target := filepath.Join(parent, "checkout")
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, ".checkout.gitcrawl.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	if action, err := syncPortableStore(context.Background(), fixture.remote, target); err != nil || action != "cloned" {
		t.Fatalf("clone beneath read-only parent: %q %v", action, err)
	}
	current, err := os.Stat(target)
	if err != nil || !os.SameFile(original, current) || original.Mode() != current.Mode() {
		t.Fatalf("original destination changed: %v", err)
	}
}

func TestSyncPortableStorePreservesConcurrentFile(t *testing.T) {
	fixture := newPortableRefreshFixture(t, false)
	target := filepath.Join(t.TempDir(), "checkout")
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(t.TempDir(), "git")
	script := `#!/bin/sh
clone=false
for arg in "$@"; do
  if [ "$arg" = clone ]; then clone=true; fi
done
"$GITCRAWL_TEST_REAL_GIT" "$@" || exit $?
if "$clone"; then
  printf preserve > "$GITCRAWL_TEST_DESTINATION/concurrent-file"
fi
`
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_TEST_REAL_GIT", realGit)
	t.Setenv("GITCRAWL_TEST_DESTINATION", target)
	t.Setenv("GITCRAWL_PORTABLE_GIT", wrapper)
	if _, err := syncPortableStore(context.Background(), fixture.remote, target); err == nil || !strings.Contains(err.Error(), "destination changed") {
		t.Fatalf("expected concurrent destination refusal, got %v", err)
	}
	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 1 || entries[0].Name() != "concurrent-file" {
		t.Fatalf("unexpected destination contents: %v %v", entries, err)
	}
	content, err := os.ReadFile(filepath.Join(target, "concurrent-file"))
	if err != nil || string(content) != "preserve" {
		t.Fatalf("concurrent file lost: %q %v", content, err)
	}
}
