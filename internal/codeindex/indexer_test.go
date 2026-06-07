package codeindex

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestScanIndexesTrackedTextAndSkipsUnsafeContent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	writeFile(t, filepath.Join(root, "cmd", "main.go"), "package main\n")
	writeFile(t, filepath.Join(root, "README.md"), "readme")
	writeFile(t, filepath.Join(root, "large.txt"), "0123456789")
	if err := os.WriteFile(filepath.Join(root, "binary.dat"), []byte{'a', 0, 'b'}, 0o600); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	if err := os.Symlink("README.md", filepath.Join(root, "linked.md")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed")

	result, err := Scan(ctx, Options{Path: root, MaxFileBytes: 8, MaxFiles: 10})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.GitSHA == "" || result.WorktreeDirty {
		t.Fatalf("snapshot metadata = %#v", result)
	}
	if result.FilesSeen != 5 || result.FilesIndexed != 1 || result.SkippedLarge != 2 || result.SkippedBinary != 1 || result.SkippedNonRegular != 1 {
		t.Fatalf("scan stats = %#v", result)
	}
	if got := result.Documents[0]; got.Path != "README.md" || got.Language != "md" || got.ContentHash == "" {
		t.Fatalf("document = %#v", got)
	}

	writeFile(t, filepath.Join(root, "README.md"), "# Dirty\n")
	result, err = Scan(ctx, Options{Path: root, MaxFileBytes: 1024, MaxFiles: 10})
	if err != nil {
		t.Fatalf("dirty scan: %v", err)
	}
	if !result.WorktreeDirty {
		t.Fatal("dirty worktree was not reported")
	}
}

func TestScanRejectsFileLimit(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	writeFile(t, filepath.Join(root, "a.txt"), "a")
	writeFile(t, filepath.Join(root, "b.txt"), "b")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed")

	if _, err := Scan(context.Background(), Options{Path: root, MaxFiles: 1}); err == nil {
		t.Fatal("file limit unexpectedly accepted")
	}
	if _, err := Scan(context.Background(), Options{Path: root, MaxTotalBytes: 1}); err == nil {
		t.Fatal("total byte limit unexpectedly accepted")
	}
}

func TestScanRejectsNonRepository(t *testing.T) {
	if _, err := Scan(context.Background(), Options{Path: t.TempDir()}); err == nil {
		t.Fatal("non-repository scan unexpectedly succeeded")
	}
}

func TestScanRejectsRepositoryWithoutCommit(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	if _, err := Scan(context.Background(), Options{Path: root}); err == nil {
		t.Fatal("repository without HEAD unexpectedly succeeded")
	}
}

func TestScanDefaultsToCurrentDirectory(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	writeFile(t, filepath.Join(root, "README.md"), "readme")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed")
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(previous)

	result, err := Scan(context.Background(), Options{})
	if err != nil {
		t.Fatalf("scan current directory: %v", err)
	}
	if result.FilesIndexed != 1 || result.Documents[0].Path != "README.md" {
		t.Fatalf("scan result = %#v", result)
	}
}

func TestScanSkipsTrackedFilesMissingFromWorktree(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	writeFile(t, filepath.Join(root, "present.txt"), "present")
	writeFile(t, filepath.Join(root, "deleted.txt"), "deleted")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed")
	if err := os.Remove(filepath.Join(root, "deleted.txt")); err != nil {
		t.Fatalf("remove tracked file: %v", err)
	}

	result, err := Scan(context.Background(), Options{Path: root})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !result.WorktreeDirty || result.FilesSeen != 2 || result.FilesIndexed != 1 || result.SkippedMissing != 1 {
		t.Fatalf("scan stats = %#v", result)
	}
	if got := result.Documents[0].Path; got != "present.txt" {
		t.Fatalf("indexed path = %q", got)
	}
}

func TestScanSkipsTrackedPathsEscapingThroughSymlinkedDirectory(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	trackedDir := filepath.Join(root, "vendor")
	writeFile(t, filepath.Join(trackedDir, "config.txt"), "tracked")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed")

	external := t.TempDir()
	writeFile(t, filepath.Join(external, "config.txt"), "external secret")
	if err := os.RemoveAll(trackedDir); err != nil {
		t.Fatalf("remove tracked directory: %v", err)
	}
	if err := os.Symlink(external, trackedDir); err != nil {
		t.Fatalf("replace tracked directory with symlink: %v", err)
	}

	result, err := Scan(context.Background(), Options{Path: root})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.FilesSeen != 1 || result.FilesIndexed != 0 || result.SkippedOutsideRoot != 1 {
		t.Fatalf("scan stats = %#v", result)
	}
}

func TestReadFileAtMostBoundsBytesRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "growing.txt")
	writeFile(t, path, "0123456789")
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()
	data, tooLarge, err := readFileAtMost(root, "growing.txt", 4)
	if err != nil {
		t.Fatalf("read bounded file: %v", err)
	}
	if !tooLarge || data != nil {
		t.Fatalf("bounded read = %q, tooLarge=%v", data, tooLarge)
	}
	if _, _, err := readFileAtMost(root, "missing.txt", 4); !os.IsNotExist(err) {
		t.Fatalf("missing bounded read error = %v", err)
	}
}

func TestScanSkipsGitlinkReplacedByRegularFile(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-b", "main")
	writeFile(t, filepath.Join(root, "README.md"), "readme")
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed")
	head, err := gitText(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	runGit(t, root, "update-index", "--add", "--cacheinfo", "160000", head, "submodule")
	runGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "add gitlink")
	writeFile(t, filepath.Join(root, "submodule"), "not tracked blob content")

	result, err := Scan(context.Background(), Options{Path: root})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.FilesSeen != 2 || result.FilesIndexed != 1 || result.SkippedNonRegular != 1 {
		t.Fatalf("scan stats = %#v", result)
	}
	if result.Documents[0].Path != "README.md" {
		t.Fatalf("documents = %#v", result.Documents)
	}
}

func TestPathAndLanguageClassification(t *testing.T) {
	for _, path := range []string{"", "..", "../outside", "/absolute"} {
		if safeRelativePath(path) {
			t.Fatalf("unsafe path accepted: %q", path)
		}
	}
	for path, want := range map[string]string{
		"Dockerfile": "dockerfile",
		"Makefile":   "makefile",
		"LICENSE":    "text",
		"main.GO":    "go",
	} {
		if got := languageForPath(path); got != want {
			t.Fatalf("languageForPath(%q) = %q, want %q", path, got, want)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	if _, err := gitBytes(context.Background(), dir, args...); err != nil {
		t.Fatal(err)
	}
}
