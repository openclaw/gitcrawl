package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type defaultSweepGitHub struct {
	fakeGitHub
	requests []gh.ListIssuesOptions
	closed   bool
	omit     bool
	fail     error
	updated  string
}

func (f *defaultSweepGitHub) ListRepositoryIssues(_ context.Context, _, _ string, opts gh.ListIssuesOptions, _ gh.Reporter) ([]map[string]any, error) {
	f.requests = append(f.requests, opts)
	state := "open"
	if f.closed {
		state = "closed"
	}
	row := map[string]any{
		"id": 1, "number": 1, "state": state, "title": "fixture",
		"body": "retained", "updated_at": "2026-01-02T00:00:00Z",
	}
	if f.updated != "" {
		row["updated_at"] = f.updated
	}
	if opts.State == "closed" && f.fail != nil {
		return []map[string]any{row}, f.fail
	}
	if f.omit || (opts.State != state && opts.State != "all") {
		return nil, nil
	}
	return []map[string]any{row}, nil
}

func TestDefaultClosedSweepPreservesOfflineCoverageAndFailureCheckpoint(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &defaultSweepGitHub{}
	s := New(client, st)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	opts := Options{Owner: "fixture", Repo: "repo"}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	started := now
	now = now.Add(40 * 24 * time.Hour)
	client.closed, client.fail = true, errors.New("denied after partial page")
	if _, err := s.Sync(ctx, opts); !errors.Is(err, client.fail) {
		t.Fatalf("partial sweep error=%v", err)
	}
	watermark, err := st.ClosedSweepWatermark(ctx, repo.ID)
	if err != nil || !watermark.Equal(started) {
		t.Fatalf("failed sweep advanced watermark=%v err=%v", watermark, err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 1 || threads[0].State != "open" {
		t.Fatalf("partial response changed state=%+v err=%v", threads, err)
	}
	client.fail = nil
	client.omit = true
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	threads, err = st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 1 || threads[0].State != "open" {
		t.Fatalf("omission retired thread=%+v err=%v", threads, err)
	}
	client.omit = false
	now = now.Add(time.Hour)
	client.updated = now.Format(time.RFC3339Nano)
	stats, err := s.Sync(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	threads, err = st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 1 || threads[0].State != "closed" {
		t.Fatalf("observed closure not applied=%+v err=%v", threads, err)
	}
	if stats.ClosedSweepThrough != now.Format(time.RFC3339Nano) {
		t.Fatalf("completed watermark=%q", stats.ClosedSweepThrough)
	}
	if client.requests[0].State != "open" || client.requests[0].Since != "" ||
		client.requests[2].State != "open" || client.requests[2].Since != "" {
		t.Fatalf("default open listing changed: %+v", client.requests)
	}
	if client.requests[3].Since != started.Add(-time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("offline sweep since=%q", client.requests[3].Since)
	}
}

func TestDefaultClosedSweepBootstrapsLegacyArchiveAndRollsBackCheckpoint(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &defaultSweepGitHub{}
	s := New(client, st)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	opts := Options{Owner: "fixture", Repo: "repo"}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	// An older archive has open observations but no sweep-aware run metadata.
	if _, err := st.DB().ExecContext(ctx, `update sync_runs set stats_json = '{}'`); err != nil {
		t.Fatal(err)
	}
	watermark, err := st.ClosedSweepWatermark(ctx, repo.ID)
	if err != nil || !watermark.Equal(now) {
		t.Fatalf("legacy watermark=%v err=%v", watermark, err)
	}
	started := now
	now = now.Add(60 * 24 * time.Hour)
	client.closed = true
	client.updated = now.Format(time.RFC3339Nano)
	if _, err := st.DB().ExecContext(ctx, `create trigger fail_run before insert on sync_runs
		begin select raise(abort, 'synthetic persistence failure'); end`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(ctx, opts); err == nil {
		t.Fatal("expected checkpoint persistence failure")
	}
	watermark, err = st.ClosedSweepWatermark(ctx, repo.ID)
	if err != nil || !watermark.Equal(started) {
		t.Fatalf("failed persistence advanced watermark=%v err=%v", watermark, err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 1 || threads[0].State != "open" {
		t.Fatalf("failed transaction changed threads=%+v err=%v", threads, err)
	}
	if _, err := st.DB().ExecContext(ctx, `drop trigger fail_run`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	if got := client.requests[len(client.requests)-1].Since; got != started.Add(-time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("legacy sweep since=%q", got)
	}
}

func TestClosedSweepPreservesExplicitScopeContracts(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"since", Options{Since: "1h"}},
		{"limited", Options{Limit: 1}},
		{"number", Options{Numbers: []int{1}}},
		{"all", Options{State: "all"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			client := &defaultSweepGitHub{}
			s := New(client, st)
			now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
			s.now = func() time.Time { return now }
			opts := tc.opts
			opts.Owner, opts.Repo = "fixture", "repo"
			stats, err := s.Sync(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name != "all" && stats.ClosedSweepThrough != "" {
				t.Fatalf("restricted run advanced default coverage=%q", stats.ClosedSweepThrough)
			}
			switch tc.name {
			case "since":
				if len(client.requests) != 2 {
					t.Fatalf("requests=%+v", client.requests)
				}
				for _, request := range client.requests {
					if request.Since != now.Add(-time.Hour).Format(time.RFC3339Nano) {
						t.Fatalf("explicit since changed=%+v", request)
					}
				}
			case "limited", "all":
				if len(client.requests) != 1 || client.requests[0].Since != "" {
					t.Fatalf("listing changed=%+v", client.requests)
				}
			case "number":
				if len(client.requests) != 0 {
					t.Fatalf("targeted run listed=%+v", client.requests)
				}
			}
		})
	}
}

func TestLegacyCheckpointSurvivesCommittedClosureAndFailedRunRecord(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	client := &defaultSweepGitHub{}
	s := New(client, st)
	started := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := started
	s.now = func() time.Time { return now }
	opts := Options{Owner: "fixture", Repo: "repo"}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `update sync_runs set stats_json = '{}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `create trigger fail_success before insert on sync_runs
		when new.status = 'success'
		begin select raise(abort, 'final run rejected'); end`); err != nil {
		t.Fatal(err)
	}
	now = now.Add(60 * 24 * time.Hour)
	client.closed, client.updated = true, now.Format(time.RFC3339Nano)
	stats, err := s.Sync(ctx, opts)
	if err == nil || stats.ThreadsClosed != 1 || stats.ThreadsSynced != 1 || stats.ClosedSweepThrough != "" {
		t.Fatalf("final record failure lost truthful commits: %+v err=%v", stats, err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 1 || threads[0].State != "closed" {
		t.Fatalf("committed closure lost=%+v err=%v", threads, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	watermark, err := st.ClosedSweepWatermark(ctx, repo.ID)
	if err != nil || !watermark.Equal(started) {
		t.Fatalf("legacy checkpoint lost after reopen=%v err=%v", watermark, err)
	}
	last, err := st.LastSuccessfulSyncAt(ctx, repo.ID)
	if err != nil || !last.Equal(started) {
		t.Fatalf("failed final record advanced freshness=%v err=%v", last, err)
	}
	if _, err := st.DB().ExecContext(ctx, `drop trigger fail_success`); err != nil {
		t.Fatal(err)
	}
	s = New(client, st)
	s.now = func() time.Time { return now }
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	if got := client.requests[len(client.requests)-1].Since; got != started.Add(-time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("retry lost original window=%q", got)
	}
	watermark, err = st.ClosedSweepWatermark(ctx, repo.ID)
	if err != nil || !watermark.Equal(now) {
		t.Fatalf("complete retry did not advance=%v err=%v", watermark, err)
	}
}

func TestNewArchivePartialCommitPreservesInitialSweepLowerBound(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &partialGitHub{failNumber: 8, operation: "issue"}
	s := New(client, st)
	started := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return started }
	stats, err := s.Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{7, 8}})
	if err == nil || stats.ThreadsSynced != 1 {
		t.Fatalf("partial sync=%+v err=%v", stats, err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	watermark, err := st.ClosedSweepWatermark(ctx, repo.ID)
	if err != nil || !watermark.Equal(started.Add(-24*time.Hour)) {
		t.Fatalf("partial observation advanced initial lower bound=%v err=%v", watermark, err)
	}
	assertNoSuccessfulSync(t, st, repo.ID)
	runs, err := st.ListRuns(ctx, repo.ID, "sync", 10)
	if err != nil || len(runs) != 1 || runs[0].Status != "checkpoint" {
		t.Fatalf("checkpoint=%+v err=%v", runs, err)
	}
}
