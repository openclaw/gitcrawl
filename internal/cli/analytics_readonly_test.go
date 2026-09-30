package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestAnalyticsCommandsRespectOwnershipAndExplicitRepair(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	s, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	repoID, err := s.UpsertRepository(ctx, store.Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: `{}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := s.UpsertThread(ctx, store.Thread{RepoID: repoID, GitHubID: "1", Number: 1, Kind: "pull_request", State: "open", Title: "retained", HTMLURL: "https://github.com/fixture/repo/pull/1", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{}`, ContentHash: "h", UpdatedAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpsertComment(ctx, store.Comment{ThreadID: tid, GitHubID: "review", CommentType: "pull_review", RawJSON: `{"submitted_at":"2026-01-02T00:00:00Z"}`, CreatedAtGitHub: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().ExecContext(ctx, "UPDATE comments SET submitted_at_gh=NULL,publication_at_gh=NULL; UPDATE comment_revisions SET submitted_at_gh=NULL,publication_at_gh=NULL"); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-01-01T00:00:00Z", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordAnalyticsAttempt(ctx, store.AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "review_state", Status: "failed", ErrorClass: "validation", ErrorText: "private-rejection-sentinel", Evidence: json.RawMessage(`{"private":"private-rejection-sentinel"}`), StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z"}); err != nil {
		t.Fatal(err)
	}
	cfg := writeDoctorTestConfig(t, dir, path)
	lock, err := os.OpenFile(filepath.Join(dir, "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = lockPortableFile(lock); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected provider request", 500)
	}))
	defer server.Close()
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
	run := func(args ...string) (map[string]any, error) {
		t.Helper()
		a := New()
		var out bytes.Buffer
		a.Stdout = &out
		a.Stderr = io.Discard
		err := a.Run(ctx, append([]string{"--config", cfg, "analytics", "fixture/repo", "--json"}, args...))
		if err != nil {
			return nil, err
		}
		if strings.Contains(out.String(), "private-rejection-sentinel") {
			t.Fatal("status exposed private rejection evidence")
		}
		var payload map[string]any
		if err = json.Unmarshal(out.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		return payload, nil
	}
	status, err := run("--status")
	if err != nil {
		t.Fatal(err)
	}
	coverage, ok := status["coverage"].(map[string]any)
	if !ok || coverage["complete"] != true || status["unresolved_retries"] != float64(1) || status["core_unresolved_retries"] != float64(0) {
		t.Fatalf("review failure hid core coverage: %+v", status)
	}
	audit, err := run()
	if err != nil {
		t.Fatal(err)
	}
	if audit["would_change"] != float64(2) || audit["changed"] != float64(0) {
		t.Fatalf("audit mutated or missed repair: %+v", audit)
	}
	if _, err = run("--apply"); err == nil || !strings.Contains(err.Error(), "ownership lock busy") {
		t.Fatalf("mutation bypassed collector owner: %v", err)
	}
	if _, err = run("--status", "--once"); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("mixed read/write mode accepted: %v", err)
	}
	var modified, attempts int
	if err = s.DB().QueryRowContext(ctx, "SELECT (SELECT count(*) FROM comments WHERE publication_at_gh IS NOT NULL)+(SELECT count(*) FROM comment_revisions WHERE publication_at_gh IS NOT NULL)").Scan(&modified); err != nil || modified != 0 {
		t.Fatalf("audit applied publication repair: %d %v", modified, err)
	}
	if err = s.DB().QueryRowContext(ctx, "SELECT count(*) FROM analytics_fetch_attempts").Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("read created a collection receipt: %d %v", attempts, err)
	}
	// An explicit repair succeeds only after the fixture collector releases its
	// lock. It normalizes retained evidence without inventing a new revision.
	var rawBefore, recordedBefore string
	if err = s.DB().QueryRowContext(ctx, "SELECT raw_json,recorded_at FROM comment_revisions").Scan(&rawBefore, &recordedBefore); err != nil {
		t.Fatal(err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	repaired, err := run("--apply")
	if err != nil || repaired["changed"] != float64(2) {
		t.Fatalf("explicit repair=%+v err=%v", repaired, err)
	}
	generation, err := s.AnalyticsState(ctx, "publication_repair_generation")
	if err != nil || generation == "" {
		t.Fatalf("missing repair generation: %q %v", generation, err)
	}
	var rawAfter, recordedAfter, published string
	var revisions int
	if err = s.DB().QueryRowContext(ctx, "SELECT count(*),raw_json,recorded_at,publication_at_gh FROM comment_revisions").Scan(&revisions, &rawAfter, &recordedAfter, &published); err != nil {
		t.Fatal(err)
	}
	if revisions != 1 || rawBefore != rawAfter || recordedBefore != recordedAfter || published != "2026-01-02T00:00:00Z" {
		t.Fatal("repair changed retained source history or publication time")
	}
	replay, err := run("--apply")
	if err != nil || replay["changed"] != float64(0) {
		t.Fatalf("repair replay=%+v err=%v", replay, err)
	}
	again, err := s.AnalyticsState(ctx, "publication_repair_generation")
	if err != nil || generation != again {
		t.Fatalf("idempotent replay changed repair generation: %q %q %v", generation, again, err)
	}
	if requests.Load() != 0 {
		t.Fatalf("read-only modes contacted provider %d times", requests.Load())
	}
}
