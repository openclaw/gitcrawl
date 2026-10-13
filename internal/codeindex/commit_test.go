package codeindex

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func commitFixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	writeFile(t, filepath.Join(root, "before.txt"), "original\n")
	writeFile(t, filepath.Join(root, "deleted.txt"), "delete me\n")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "first")
	sha, err := gitText(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return root, sha
}

func TestScanCommitPinnedDespiteDirtyWorktreeAndMovingMain(t *testing.T) {
	root, first := commitFixture(t)
	original, err := ScanCommit(context.Background(), Options{Path: root}, first)
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "mv", "before.txt", "after.txt")
	runGit(t, root, "rm", "deleted.txt")
	writeFile(t, filepath.Join(root, "new.go"), "package fresh\n")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "second")
	second, err := gitText(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "after.txt"), "uncommitted developer content")
	if err := os.Remove(filepath.Join(root, "new.go")); err != nil {
		t.Fatal(err)
	}
	again, err := ScanCommit(context.Background(), Options{Path: root}, first)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, again) || again.WorktreeDirty {
		t.Fatal("captured commit changed with worktree/HEAD")
	}
	next, err := ScanCommit(context.Background(), Options{Path: root}, second)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, d := range next.Documents {
		got[d.Path] = d.Text
	}
	if !reflect.DeepEqual(got, map[string]string{"after.txt": "original\n", "new.go": "package fresh\n"}) || next.WorktreeDirty || next.GitSHA != second {
		t.Fatalf("wrong immutable generation: %#v", next)
	}
}

func TestScanCommitLimitsModesAndBareSource(t *testing.T) {
	root, _ := commitFixture(t)
	writeFile(t, filepath.Join(root, "large.txt"), strings.Repeat("x", 30))
	if err := os.WriteFile(filepath.Join(root, "binary"), []byte{0, 1, 2}, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("before.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "limits")
	sha, err := gitText(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	bare := filepath.Join(t.TempDir(), "source.git")
	runGit(t, root, "clone", "--bare", "--no-hardlinks", root, bare)
	r, err := ScanCommit(context.Background(), Options{Path: bare, MaxFileBytes: 16}, sha)
	if err != nil {
		t.Fatal(err)
	}
	if r.FilesIndexed != 2 || r.SkippedLarge != 1 || r.SkippedBinary != 1 || r.SkippedNonRegular != 1 || r.WorktreeDirty {
		t.Fatalf("unexpected scope: %#v", r)
	}
	for _, o := range []Options{{Path: bare, MaxFiles: 1}, {Path: bare, MaxTotalBytes: 1}} {
		if _, err := ScanCommit(context.Background(), o, sha); err == nil {
			t.Fatal("accepted incomplete bounded corpus")
		}
	}
	for _, ref := range []string{"HEAD", sha[:12], "--all", strings.Repeat("f", 40)} {
		if _, err := ScanCommit(context.Background(), Options{Path: bare}, ref); err == nil {
			t.Fatal("accepted invalid/missing commit")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ScanCommit(ctx, Options{Path: bare}, sha); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestScanCommitIgnoresReplacementObjects(t *testing.T) {
	root, first := commitFixture(t)
	writeFile(t, filepath.Join(root, "before.txt"), "replacement\n")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "replacement")
	second, err := gitText(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "replace", first, second)
	r, err := ScanCommit(context.Background(), Options{Path: root}, first)
	if err != nil {
		t.Fatal(err)
	}
	if r.Documents[0].Text != "original\n" {
		t.Fatal("replacement ref changed captured content")
	}
}
