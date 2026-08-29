package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

type portableRefreshFixture struct {
	remote     string
	checkout   string
	configPath string
	relative   string
	mirror     string
}

func portableTestGit(t *testing.T, root string, args ...string) string {
	t.Helper()
	output, err := gitOutput(context.Background(), root, args...)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func portableTestCommit(t *testing.T, root string) {
	t.Helper()
	portableTestGit(t, root, "add", "--all")
	portableTestGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false", "commit", "-m", "fixture generation")
}

func portableTestManifest(t *testing.T, path, relative string, compressed bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := fileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest := portableDBManifest{Schema: "gitcrawl-portable-sync-v2", OutputPath: relative, OutputBytes: int64(len(data)), SHA256: fmt.Sprintf("%x", digest), QuickCheck: "ok"}
	if compressed {
		var archive bytes.Buffer
		writer := gzip.NewWriter(&archive)
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".gz", archive.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		archiveSHA, err := fileSHA256(path + ".gz")
		if err != nil {
			t.Fatal(err)
		}
		manifest.Compression, manifest.ArchivePath = "gzip", filepath.Base(path)+".gz"
		manifest.ArchiveBytes, manifest.ArchiveSHA256 = int64(archive.Len()), fmt.Sprintf("%x", archiveSHA)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(portableDBManifestPath(path), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
}

func newPortableRefreshFixture(t *testing.T, compressed bool) portableRefreshFixture {
	t.Helper()
	dir := t.TempDir()
	fixture := portableRefreshFixture{remote: filepath.Join(dir, "publisher"), checkout: filepath.Join(dir, "subscriber"), configPath: filepath.Join(dir, "config.toml"), relative: "data/archive.db"}
	if err := os.MkdirAll(filepath.Join(fixture.remote, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	portableTestGit(t, fixture.remote, "init", "-b", "main")
	seedPortableThread(t, filepath.Join(fixture.remote, fixture.relative), 1, "first generation")
	portableTestManifest(t, filepath.Join(fixture.remote, fixture.relative), fixture.relative, compressed)
	portableTestCommit(t, fixture.remote)
	fixture.init(t)
	app := New()
	app.configPath = fixture.configPath
	mirror, err := app.portableRuntimeDBPath(context.Background(), filepath.Join(fixture.checkout, fixture.relative))
	if err != nil {
		t.Fatal(err)
	}
	fixture.mirror, err = canonicalPortablePath(mirror)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture portableRefreshFixture) init(t *testing.T) {
	t.Helper()
	app := New()
	app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
	if err := app.Run(context.Background(), []string{"--config", fixture.configPath, "init", "--portable-store", fixture.remote, "--store-dir", fixture.checkout, "--portable-db", fixture.relative, "--json"}); err != nil {
		t.Fatal(err)
	}
}

func (fixture portableRefreshFixture) refresh(t *testing.T, extra ...string) (portableRefreshResult, error) {
	t.Helper()
	app := New()
	var stdout, stderr bytes.Buffer
	app.Stdout, app.Stderr = &stdout, &stderr
	args := []string{"--config", fixture.configPath, "portable", "refresh", "--expected-remote", fixture.remote, "--min-free-bytes", "1", "--max-growth-bytes", "67108864", "--json"}
	err := app.Run(context.Background(), append(args, extra...))
	var result portableRefreshResult
	if decodeErr := json.Unmarshal(stdout.Bytes(), &result); decodeErr != nil {
		t.Fatalf("result: %v; command error: %v; stdout=%s stderr=%s", decodeErr, err, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "stage=") {
		t.Fatalf("missing progress: %s", stderr.String())
	}
	return result, err
}

func (fixture portableRefreshFixture) advance(t *testing.T, compressed bool) {
	t.Helper()
	path := filepath.Join(fixture.remote, fixture.relative)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		temp, err := stagePortableSQLiteSourceTemp(path, path, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(temp, path); err != nil {
			t.Fatal(err)
		}
	}
	seedPortableThread(t, path, 2, "second generation")
	portableTestManifest(t, path, fixture.relative, compressed)
	portableTestCommit(t, fixture.remote)
}

func TestPortableInitGzipRepeatedAndTransition(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			fixture := newPortableRefreshFixture(t, compressed)
			fixture.init(t)
			fixture.advance(t, true)
			fixture.init(t)
			fixture.init(t)
			cfg, err := config.Load(fixture.configPath)
			if err != nil {
				t.Fatal(err)
			}
			logical := filepath.Join(fixture.checkout, fixture.relative)
			if cfg.DBPath != logical {
				t.Fatalf("logical DB changed: %s", cfg.DBPath)
			}
			if _, err := os.Stat(logical); !os.IsNotExist(err) {
				t.Fatalf("gzip init required a raw DB: %v", err)
			}
		})
	}
}

func TestPortableInitValidatesBeforeGitAndPreservesConfig(t *testing.T) {
	fixture := newPortableRefreshFixture(t, true)
	before, _ := os.ReadFile(fixture.configPath)
	head := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD")
	fixture.advance(t, true)
	for _, path := range []string{"../escape.db", "/absolute.db", "data/../archive.db", "C:\\archive.db", ".git/config", "data//archive.db"} {
		app := New()
		app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
		err := app.Run(context.Background(), []string{"--config", fixture.configPath, "init", "--portable-store", fixture.remote, "--store-dir", fixture.checkout, "--portable-db", path})
		if err == nil {
			t.Fatalf("accepted %q", path)
		}
		if got := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD"); got != head {
			t.Fatal("invalid syntax mutated Git")
		}
	}
	if err := os.WriteFile(filepath.Join(fixture.remote, fixture.relative)+".gz", []byte("invalid artifact"), 0o644); err != nil {
		t.Fatal(err)
	}
	portableTestCommit(t, fixture.remote)
	app := New()
	app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
	if err := app.Run(context.Background(), []string{"--config", fixture.configPath, "init", "--portable-store", fixture.remote, "--store-dir", fixture.checkout, "--portable-db", fixture.relative}); err == nil {
		t.Fatal("invalid artifact accepted")
	}
	after, _ := os.ReadFile(fixture.configPath)
	if !bytes.Equal(before, after) {
		t.Fatal("failed init replaced config")
	}
}

func TestPortableRefreshSuccessNoopAndLocalMirror(t *testing.T) {
	fixture := newPortableRefreshFixture(t, false)
	initial, err := fixture.refresh(t)
	if err != nil || initial.Result != "no-op" || initial.MirrorResult != "promoted" {
		t.Fatalf("initial: %+v %v", initial, err)
	}
	beforeConfig, _ := os.ReadFile(fixture.configPath)
	fixture.advance(t, true)
	updated, err := fixture.refresh(t, "--store-dir", fixture.checkout, "--portable-db", fixture.relative)
	if err != nil || updated.Result != "updated" || updated.BeforeCommit == updated.AfterCommit || updated.SHA256 == "" {
		t.Fatalf("update: %+v %v", updated, err)
	}
	if updated.Capacity.PeakGrowth <= 0 || updated.Mirror != fixture.mirror {
		t.Fatalf("observations: %+v", updated)
	}
	if err := sqliteStoreImmutableHealth(context.Background(), fixture.mirror); err != nil {
		t.Fatal(err)
	}
	seedPortableThread(t, fixture.mirror, 3, "local writable runtime state")
	localSHA, _ := fileSHA256(fixture.mirror)
	result, err := fixture.refresh(t)
	if err != nil || result.Result != "no-op" || result.MirrorResult != "preserved-local" {
		t.Fatalf("local: %+v %v", result, err)
	}
	afterSHA, _ := fileSHA256(fixture.mirror)
	if localSHA != afterSHA {
		t.Fatal("local runtime changed")
	}
	afterConfig, _ := os.ReadFile(fixture.configPath)
	if !bytes.Equal(beforeConfig, afterConfig) {
		t.Fatal("refresh rewrote config")
	}
}

func TestPortableRefreshRefusalsPreserveLastGood(t *testing.T) {
	cases := []string{"invalid", "dirty", "index", "ignored", "divergent", "ahead", "origin", "hook", "filter", "lock", "runtime-lock", "orphan", "low-space", "growth", "timeout", "submodule"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := newPortableRefreshFixture(t, false)
			if _, err := fixture.refresh(t); err != nil {
				t.Fatal(err)
			}
			fixture.advance(t, true)
			extra := []string{}
			switch name {
			case "invalid":
				if err := os.WriteFile(filepath.Join(fixture.remote, fixture.relative)+".gz", []byte("bad gzip"), 0o644); err != nil {
					t.Fatal(err)
				}
				portableTestCommit(t, fixture.remote)
			case "dirty", "index":
				if err := os.WriteFile(filepath.Join(fixture.checkout, fixture.relative), []byte("local edit"), 0o644); err != nil {
					t.Fatal(err)
				}
				if name == "index" {
					portableTestGit(t, fixture.checkout, "add", fixture.relative)
				}
			case "ignored":
				if err := os.WriteFile(filepath.Join(fixture.checkout, ".git", "info", "exclude"), []byte("collision\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(fixture.checkout, "collision"), []byte("keep ignored data"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(fixture.remote, "collision"), []byte("incoming"), 0o644); err != nil {
					t.Fatal(err)
				}
				portableTestCommit(t, fixture.remote)
			case "divergent", "ahead":
				if err := os.WriteFile(filepath.Join(fixture.checkout, "local"), []byte("local commit"), 0o644); err != nil {
					t.Fatal(err)
				}
				portableTestCommit(t, fixture.checkout)
				if name == "ahead" {
					portableTestGit(t, fixture.remote, "update-ref", "refs/heads/main", portableTestGit(t, fixture.remote, "rev-parse", "HEAD^"))
				}
			case "origin":
				portableTestGit(t, fixture.checkout, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "unrelated"))
			case "hook":
				if err := os.WriteFile(filepath.Join(fixture.checkout, ".git", "hooks", "post-merge"), []byte("#!/bin/sh\nexit 99\n"), 0o755); err != nil {
					t.Fatal(err)
				}
			case "filter":
				portableTestGit(t, fixture.checkout, "config", "filter.test.smudge", "false")
			case "lock":
				if err := os.WriteFile(filepath.Join(fixture.checkout, ".git", "index.lock"), []byte("live"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "orphan":
				if err := os.WriteFile(filepath.Join(fixture.checkout, ".git", "objects", "pack", "tmp_pack_unknown"), []byte("preserve"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "runtime-lock":
				if err := os.WriteFile(portableStoreRefreshStatePath(fixture.mirror)+".lock", []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "low-space":
				extra = []string{"--min-free-bytes", "18446744073709551615"}
			case "growth":
				extra = []string{"--max-growth-bytes", "1"}
			case "timeout":
				extra = []string{"--timeout", "1ns"}
			case "submodule":
				portableTestGit(t, fixture.remote, "update-index", "--add", "--cacheinfo", "160000,"+portableTestGit(t, fixture.remote, "rev-parse", "HEAD")+",module")
				portableTestGit(t, fixture.remote, "-c", "user.name=Test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false", "commit", "-m", "gitlink")
			}
			head := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD")
			tracking := portableTestGit(t, fixture.checkout, "rev-parse", "refs/remotes/origin/main")
			mirrorSHA, _ := fileSHA256(fixture.mirror)
			configSHA, _ := fileSHA256(fixture.configPath)
			if name == "timeout" {
				// A deadline can expire during read-only flag validation, before
				// the structured operation starts.
				app := New()
				app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
				if err := app.Run(context.Background(), []string{"--config", fixture.configPath, "portable", "refresh", "--expected-remote", fixture.remote, "--timeout", "1ns", "--json"}); err == nil {
					t.Fatal("timeout accepted")
				}
			} else {
				result, err := fixture.refresh(t, extra...)
				if err == nil || result.Result != "refused" {
					t.Fatalf("refusal: %+v %v", result, err)
				}
			}
			if portableTestGit(t, fixture.checkout, "rev-parse", "HEAD") != head || portableTestGit(t, fixture.checkout, "rev-parse", "refs/remotes/origin/main") != tracking {
				t.Fatal("refusal changed refs")
			}
			if sha, _ := fileSHA256(fixture.mirror); sha != mirrorSHA {
				t.Fatal("refusal changed mirror")
			}
			if sha, _ := fileSHA256(fixture.configPath); sha != configSHA {
				t.Fatal("refusal changed config")
			}
			staging, _ := filepath.Glob(filepath.Join(filepath.Dir(fixture.mirror), ".gitcrawl-refresh-*"))
			if len(staging) != 0 {
				t.Fatalf("owned staging leaked: %v", staging)
			}
		})
	}
}

func TestPortableOwnershipCrossProcessAndSymlink(t *testing.T) {
	if root := os.Getenv("GITCRAWL_TEST_LOCK_ROOT"); root != "" {
		_, release, err := acquirePortableOwner(context.Background(), root)
		if err == nil {
			release()
			os.Exit(19)
		}
		os.Exit(0)
	}
	root := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, release, err := acquirePortableOwner(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	owner := ctx.Value(portableOwnerKey{}).(*portableOwner)
	old := time.Now().Add(-24 * time.Hour)
	if err := os.Chtimes(owner.file.Name(), old, old); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPortableOwnershipCrossProcessAndSymlink$")
	command.Env = append(os.Environ(), "GITCRAWL_TEST_LOCK_ROOT="+root)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("competing child stole live lock: %v %s", err, out)
	}
	alias := filepath.Join(filepath.Dir(root), "alias")
	if err := os.Symlink(root, alias); err == nil {
		if _, closeAlias, err := acquirePortableOwner(context.Background(), alias); err == nil {
			closeAlias()
			t.Fatal("symlink bypassed lock")
		}
	}
	if err := owner.check(); err != nil {
		t.Fatal(err)
	}
	_, nestedRelease, err := acquirePortableOwner(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	nestedRelease()
	if _, err := os.Stat(owner.file.Name()); err != nil {
		t.Fatal("live lock removed", err)
	}
}

func TestPortableBudgetCountsGrowthWithoutDeletionCredit(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "old")
	if err := os.WriteFile(old, bytes.Repeat([]byte("x"), 100), 0o600); err != nil {
		t.Fatal(err)
	}
	budget, err := newPortableBudget(context.Background(), []string{root}, 1, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(old); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "new"), bytes.Repeat([]byte("x"), 33), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := budget.check(context.Background()); err == nil {
		t.Fatal("deletion credited against growth")
	}
}

func TestPortableStagingCleanupPreservesReplacement(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "owned")
	if err := os.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(staging)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staging, staging+"-moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(staging, "unknown")
	if err := os.WriteFile(unknown, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeOwnedPortableStaging(staging, info); err == nil {
		t.Fatal("cleanup accepted a replacement directory")
	}
	if data, err := os.ReadFile(unknown); err != nil || string(data) != "keep" {
		t.Fatal("cleanup touched unrelated contents")
	}
}

func TestPortableOwnershipBeforeCloneConvergesThroughLink(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "future-store")
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	_, release, err := acquirePortableOwner(context.Background(), alias)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, otherRelease, err := acquirePortableOwner(context.Background(), root); err == nil {
		otherRelease()
		t.Fatal("first clone could acquire competing locks through a dangling alias")
	}
}

func TestPortableCommandOwnershipRetainsOnlyWritableSessions(t *testing.T) {
	for _, retain := range []bool{false, true} {
		t.Run(fmt.Sprint(retain), func(t *testing.T) {
			root := t.TempDir()
			session := &portableCommandSession{retain: retain}
			defer session.close()
			ctx := context.WithValue(context.Background(), portableCommandKey{}, session)
			_, release, err := acquirePortableOwner(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			release()
			_, otherRelease, err := acquirePortableOwner(context.Background(), root)
			if err == nil {
				otherRelease()
			}
			if retain && err == nil {
				t.Fatal("writable session released ownership before close")
			}
			if !retain && err != nil {
				t.Fatal("reader retained ownership after preparation", err)
			}
		})
	}
}

func TestPortableRefreshUsesExistingRuntimeForStoreAlias(t *testing.T) {
	fixture := newPortableRefreshFixture(t, true)
	alias := filepath.Join(filepath.Dir(fixture.checkout), "linked-store")
	if err := os.Symlink(fixture.checkout, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	cfg, err := config.Load(fixture.configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBPath = filepath.Join(alias, fixture.relative)
	if err := config.Save(fixture.configPath, cfg); err != nil {
		t.Fatal(err)
	}
	app := New()
	app.configPath = fixture.configPath
	expected, err := app.portableRuntimeDBPath(context.Background(), cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	expected, err = canonicalPortablePath(expected)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.refresh(t, "--store-dir", alias)
	if err != nil || result.Mirror != expected {
		t.Fatalf("alias runtime moved: %+v %v; expected %s", result, err, expected)
	}
}
