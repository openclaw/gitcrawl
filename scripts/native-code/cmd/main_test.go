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
	for _, tc := range []struct {
		name                                   string
		incomplete, rollback, stale, regressed bool
	}{
		{name: "commit"},
		{name: "rollback", rollback: true},
		{name: "unavailable_item", incomplete: true},
		{name: "unavailable_rollback", incomplete: true, rollback: true},
		{name: "stale_after_prepare", incomplete: true, stale: true},
		{name: "regressed_after_prepare", incomplete: true, regressed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fail := tc.rollback || tc.stale || tc.regressed
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
			clock := time.Now().UTC().Format(time.RFC3339Nano)
			complete := 1
			if tc.incomplete {
				complete = 0
			}
			_, e = s.DB().Exec("INSERT INTO analytics_coverage(repository,through,issues,pull_requests,complete,observed_at) VALUES('openclaw/openclaw',?,0,0,?,?)", clock, complete, clock)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.SetAnalyticsState(context.Background(), "core_baseline_verified:openclaw/openclaw", "1"); e != nil {
				t.Fatal(e)
			}
			if tc.incomplete {
				_, e = s.DB().Exec("INSERT INTO analytics_retries(repository,number,operation,first_seen_at,last_seen_at,next_attempt_at,attempts) VALUES('openclaw/openclaw',177425,'graphql_history',?,?,?,8)", clock, clock, clock)
				if e != nil {
					t.Fatal(e)
				}
			}
			old, e := s.ReplaceCodeSnapshot(context.Background(), store.CodeSnapshot{RepoID: id, SourceRoot: "old", GitSHA: "old", FileCount: 1, ByteCount: 3, IndexedAt: "old"}, []store.CodeDocument{{RepoID: id, Path: "obsolete.go", Text: "old", ByteSize: 3, ContentHash: "old"}})
			if e != nil {
				t.Fatal(e)
			}
			if tc.rollback {
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
				err := codeRun([]string{"--db", dbpath, "--source", repo, "--commit", sha, "--upstream-receipt", proof, "--commit-gate", gate})
				done <- err
				if err != nil {
					writer.Close()
				}
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
			progress, ok := prepared["core_progress_at_prepare"].(map[string]any)
			if !ok || progress["coverage_complete"] != !tc.incomplete || progress["through"] != clock {
				t.Fatal("prepare receipt lost truthful core progress")
			}
			if tc.stale || tc.regressed {
				s, err := store.Open(context.Background(), dbpath)
				if err != nil {
					t.Fatal(err)
				}
				changed := time.Now().UTC().Add(-6 * time.Minute)
				if tc.regressed {
					parsed, err := time.Parse(time.RFC3339Nano, clock)
					if err != nil {
						t.Fatal(err)
					}
					changed = parsed.Add(-time.Nanosecond)
				}
				_, err = s.DB().Exec("UPDATE analytics_coverage SET through=?", changed.Format(time.RFC3339Nano))
				s.Close()
				if err != nil {
					t.Fatal(err)
				}
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
			var retainedComplete int
			if e = ro.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='openclaw/openclaw'").Scan(&retainedComplete); e != nil || retainedComplete != complete {
				t.Fatalf("coverage changed: %d %v", retainedComplete, e)
			}
			if tc.incomplete {
				var attempts int
				var next string
				if e = ro.DB().QueryRow("SELECT attempts,next_attempt_at FROM analytics_retries WHERE repository='openclaw/openclaw' AND number=177425 AND resolved_at IS NULL").Scan(&attempts, &next); e != nil || attempts != 8 || next != clock {
					t.Fatalf("retry changed: %d %s %v", attempts, next, e)
				}
			}
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
				var committed map[string]any
				if err := decoder.Decode(&committed); err != nil {
					t.Fatal(err)
				}
				progress, ok := committed["core_progress_at_commit"].(map[string]any)
				if !ok || progress["coverage_complete"] != !tc.incomplete || progress["through"] != clock {
					t.Fatal("commit receipt lost truthful core progress")
				}
				if tc.incomplete {
					// No-op needs neither another gate nor another publication.
					os.Stdout = writer
					if err := codeRun([]string{"--db", dbpath, "--source", repo, "--commit", sha, "--upstream-receipt", proof, "--commit-gate", gate}); err != nil {
						t.Fatal(err)
					}
					os.Stdout = stdout
					var noop map[string]any
					if err := decoder.Decode(&noop); err != nil {
						t.Fatal(err)
					}
					if noop["phase"] != "unchanged" || noop["snapshot_id"] != float64(snap.ID) {
						t.Fatal("incomplete-coverage no-op changed generation")
					}
				}
			}
		})
	}
}

func TestCodeCoreProgressAdmission(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC()
	valid := now.Add(-time.Minute).Format(time.RFC3339Nano)
	for _, tc := range []struct {
		name, baseline, through, observed string
		complete                          int
		accept                            bool
	}{
		{"complete", "1", valid, valid, 1, true},
		{"incomplete", "1", valid, valid, 0, true},
		{"boundary", "1", now.Add(-5 * time.Minute).Format(time.RFC3339Nano), valid, 0, true},
		{"baseline_missing", "", valid, valid, 0, false},
		{"frontier_invalid", "1", "fixture", valid, 0, false},
		{"observation_invalid", "1", valid, "bad", 0, false},
		{"frontier_stale", "1", now.Add(-5*time.Minute - time.Nanosecond).Format(time.RFC3339Nano), valid, 0, false},
		{"observation_stale", "1", now.Add(-7 * time.Minute).Format(time.RFC3339Nano), now.Add(-6 * time.Minute).Format(time.RFC3339Nano), 0, false},
		{"future", "1", valid, now.Add(time.Nanosecond).Format(time.RFC3339Nano), 0, false},
		{"frontier_after_observation", "1", now.Format(time.RFC3339Nano), valid, 0, false},
		{"invalid_completeness", "1", valid, valid, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.SetAnalyticsState(ctx, "core_baseline_verified:openclaw/openclaw", tc.baseline); err != nil {
				t.Fatal(err)
			}
			_, err := s.DB().Exec("INSERT OR REPLACE INTO analytics_coverage(repository,through,complete,observed_at) VALUES('openclaw/openclaw',?,?,?)", tc.through, tc.complete, tc.observed)
			if err != nil {
				t.Fatal(err)
			}
			p, err := readCodeCoreProgress(ctx, s, now)
			if (err == nil) != tc.accept {
				t.Fatalf("admission=%v want=%v error=%v", err == nil, tc.accept, err)
			}
			if err == nil && p.CoverageComplete != (tc.complete == 1) {
				t.Fatal("coverage relabelled")
			}
		})
	}
	if _, err = s.DB().Exec("DELETE FROM analytics_coverage"); err != nil {
		t.Fatal(err)
	}
	if _, err = readCodeCoreProgress(ctx, s, now); err == nil {
		t.Fatal("missing receipt accepted")
	}
}
