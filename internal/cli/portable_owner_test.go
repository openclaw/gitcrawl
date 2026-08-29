package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPortableOwnerRejectsCrossStoreAndCanceledAcquisition(t *testing.T) {
	// Resolving the test executable is sufficient: ownership never runs Git.
	t.Setenv("GITCRAWL_PORTABLE_GIT", os.Args[0])
	root := t.TempDir()
	first, second := filepath.Join(root, "first"), filepath.Join(root, "second")
	ctx, release, err := acquirePortableOwner(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, otherRelease, err := acquirePortableOwner(ctx, second); err == nil || !strings.Contains(err.Error(), "already owns a different store") || otherRelease != nil {
		t.Fatalf("nested ownership changed stores: release=%t, %v", otherRelease != nil, err)
	}
	if _, competingRelease, err := acquirePortableOwner(context.Background(), first); err == nil {
		competingRelease()
		t.Fatal("cross-store refusal released the original lease")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, canceledRelease, err := acquirePortableOwner(canceled, second); !errors.Is(err, context.Canceled) || canceledRelease != nil {
		t.Fatalf("canceled acquisition: release=%t, %v", canceledRelease != nil, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".second.gitcrawl.lock")); !os.IsNotExist(err) {
		t.Fatalf("refused acquisition created a second lock: %v", err)
	}
	owner := ctx.Value(portableOwnerKey{}).(*portableOwner)
	if err := owner.check(); err != nil {
		t.Fatalf("original owner no longer valid: %v", err)
	}
	release()
	if err := owner.check(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("released file still represents ownership: %v", err)
	}
	if _, err := os.Stat(owner.file.Name()); err != nil {
		t.Fatalf("release removed permanent lock: %v", err)
	}
}

func TestPortableGitExecutableSelectionAndRevalidation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "selected-git")
	writePortableSafetyFile(t, path, []byte("original executable"))
	t.Setenv("GITCRAWL_PORTABLE_GIT", "relative-git")
	if _, err := portableGitContext(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("relative environment selector accepted: %v", err)
	}
	for _, test := range []struct{ path, want string }{
		{filepath.Join(root, "missing"), "resolve portable Git executable"},
		{root, "not a regular file"},
	} {
		if _, err := portableGitContext(context.Background(), test.path); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("invalid selector %q: %v", test.path, err)
		}
	}
	ctx, err := portableGitContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	// A pinned executable is not reselected from a changed environment.
	reused, err := portableGitContext(ctx, "")
	if err != nil || reused != ctx {
		t.Fatalf("pinned executable was re-resolved: %v", err)
	}
	if err := os.Rename(path, path+".previous"); err != nil {
		t.Fatal(err)
	}
	writePortableSafetyFile(t, path, []byte("replacement executable"))
	before := portableTestSnapshot(t, root)
	if err := runPortableGit(ctx, root, io.Discard, "status"); err == nil || err.Error() != "portable Git executable changed during operation" {
		t.Fatalf("replaced executable not refused before start: %v", err)
	}
	if string(before) != string(portableTestSnapshot(t, root)) {
		t.Fatal("executable refusal mutated files")
	}
	if err := runPortableGit(context.Background(), root, io.Discard, "status"); err == nil || err.Error() != "portable Git executable was not resolved" {
		t.Fatalf("unresolved executable accepted: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := runPortableGit(canceled, root, io.Discard, "status"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled command attempted executable validation/start: %v", err)
	}
}

func TestPortablePathAndRemoteAdmission(t *testing.T) {
	for _, path := range []string{"data/archive.db", "archive.db", "data/COM10.db"} {
		if err := validatePortableRelativePath(path); err != nil {
			t.Fatalf("valid logical path %q rejected: %v", path, err)
		}
	}
	for _, path := range []string{"", "/archive.db", "data\\archive.db", "../archive.db", "data//archive.db", "data/./archive.db", "data/.GiT/config", "data/archive.", "data/archive /db", "NUL.db", "data/com1.db", "LPT9/archive.db", "data/archive\n.db"} {
		if err := validatePortableRelativePath(path); err == nil {
			t.Fatalf("unsafe logical path %q accepted", path)
		}
	}
	for _, remote := range []string{"https://example.invalid/archive.git", "ssh://git@example.invalid/archive.git", "git@example.invalid:archive.git", "file:///synthetic/archive.git"} {
		if err := validatePortableRemote(remote); err != nil {
			t.Fatalf("valid remote %q rejected: %v", remote, err)
		}
	}
	for _, remote := range []string{"", " --upload-pack=helper", "-option", "ext::helper", "https://example.invalid/archive?token=synthetic", "https://example.invalid/archive#fragment", "https://%invalid/archive", "ftp://example.invalid/archive", "ssh://example.invalid/archive\nargument", "https://git@example.invalid/archive.git", "http://git@example.invalid/archive.git", "git://git@example.invalid/archive.git", "file://git@example.invalid/archive.git", "ssh://git:@example.invalid/archive.git", "ssh://@example.invalid/archive.git"} {
		if err := validatePortableRemote(remote); err == nil {
			t.Fatalf("unsafe remote %q accepted", remote)
		}
	}
}
