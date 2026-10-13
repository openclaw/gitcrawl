//go:build native_code_overlay

package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/codeindex"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestCodeCommitGatePinsSourceAndReplacesAtomically(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[fail], func(t *testing.T) {
			root := t.TempDir()
			repo := filepath.Join(root, "source")
			if e := os.Mkdir(repo, 0700); e != nil {
				t.Fatal(e)
			}
			git := func(args ...string) string {
				t.Helper()
				c := exec.Command("git", append([]string{"-C", repo}, args...)...)
				b, e := c.CombinedOutput()
				if e != nil {
					t.Fatalf("fixture git: %v %s", e, b)
				}
				return string(b)
			}
			git("init", "-b", "main")
			if e := os.WriteFile(filepath.Join(repo, "current.go"), []byte("package captured\n"), 0600); e != nil {
				t.Fatal(e)
			}
			git("add", ".")
			git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "captured")
			sha := git("rev-parse", "HEAD")
			sha = sha[:len(sha)-1]
			scan, e := codeindex.ScanCommit(context.Background(), codeindex.Options{Path: repo}, sha)
			if e != nil {
				t.Fatal(e)
			}
			dbpath := filepath.Join(root, "archive.db")
			s, e := store.Open(context.Background(), dbpath)
			if e != nil {
				t.Fatal(e)
			}
			id, e := s.UpsertRepository(context.Background(), store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", GitHubRepoID: "42", RawJSON: "{}", UpdatedAt: "t"})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.DB().Exec("CREATE TABLE IF NOT EXISTS analytics_coverage(repository TEXT PRIMARY KEY,through TEXT,issues INTEGER,pull_requests INTEGER,complete INTEGER,observed_at TEXT)")
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.DB().Exec("INSERT INTO analytics_coverage(repository,through,issues,pull_requests,complete,observed_at) VALUES('openclaw/openclaw','fixture',0,0,1,?)", time.Now().UTC().Format(time.RFC3339Nano))
			if e != nil {
				t.Fatal(e)
			}
			old, e := s.ReplaceCodeSnapshot(context.Background(), store.CodeSnapshot{RepoID: id, SourceRoot: "old", GitSHA: "old", FileCount: 1, ByteCount: 3, IndexedAt: "old"}, []store.CodeDocument{{RepoID: id, Path: "obsolete.go", Text: "old", ByteSize: 3, ContentHash: "old"}})
			if e != nil {
				t.Fatal(e)
			}
			if fail {
				_, e = s.DB().Exec("CREATE TRIGGER fixture_refuse BEFORE INSERT ON code_documents WHEN NEW.path='current.go' BEGIN SELECT RAISE(ABORT,'fixture'); END")
				if e != nil {
					t.Fatal(e)
				}
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			proof := filepath.Join(root, "source.json")
			raw, _ := json.Marshal(map[string]any{"format": "gitcrawl.code-upstream.v1", "repository_url": "https://github.com/openclaw/openclaw.git", "branch": "main", "commit": sha, "git_dir": scan.SourceRoot, "observed_at": time.Now().UTC().Format(time.RFC3339Nano)})
			if e = os.WriteFile(proof, raw, 0600); e != nil {
				t.Fatal(e)
			}
			gate := filepath.Join(root, "commit.gate")
			reader, writer, e := os.Pipe()
			if e != nil {
				t.Fatal(e)
			}
			stdout := os.Stdout
			os.Stdout = writer
			defer func() { os.Stdout = stdout; reader.Close(); writer.Close() }()
			done := make(chan error, 1)
			go func() {
				done <- codeRun([]string{"--db", dbpath, "--source", repo, "--commit", sha, "--upstream-receipt", proof, "--commit-gate", gate})
			}()
			_ = reader.SetReadDeadline(time.Now().Add(15 * time.Second))
			decoder := json.NewDecoder(reader)
			var prepared map[string]any
			if e = decoder.Decode(&prepared); e != nil {
				t.Fatal(e)
			}
			if prepared["phase"] != "prepared" || prepared["git_sha"] != sha || prepared["tracked_dirty"] != false {
				t.Fatal("wrong prepared source binding")
			}
			// Advance main and dirty the worktree AFTER preparation, BEFORE the gate.
			if e = os.WriteFile(filepath.Join(repo, "current.go"), []byte("package later\n"), 0600); e != nil {
				t.Fatal(e)
			}
			git("add", ".")
			git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "later")
			if e = os.WriteFile(gate, []byte(prepared["content_digest"].(string)), 0600); e != nil {
				t.Fatal(e)
			}
			select {
			case e = <-done:
			case <-time.After(15 * time.Second):
				t.Fatal("native commit did not settle")
			}
			os.Stdout = stdout
			if (e != nil) != fail {
				t.Fatalf("commit error=%v want_failure=%v", e, fail)
			}
			ro, e := store.OpenReadOnly(context.Background(), dbpath)
			if e != nil {
				t.Fatal(e)
			}
			defer ro.Close()
			snap, e := ro.LatestCodeSnapshot(context.Background(), id)
			if e != nil {
				t.Fatal(e)
			}
			var path, text string
			if e = ro.DB().QueryRow("SELECT path,text_content FROM code_documents WHERE snapshot_id=?", snap.ID).Scan(&path, &text); e != nil {
				t.Fatal(e)
			}
			if fail {
				if snap.ID != old || path != "obsolete.go" || text != "old" {
					t.Fatal("failed publication damaged prior generation")
				}
			} else {
				if snap.GitSHA != sha || snap.WorktreeDirty || path != "current.go" || text != "package captured\n" {
					t.Fatal("publication mixed captured and later commit")
				}
			}
		})
	}
}
