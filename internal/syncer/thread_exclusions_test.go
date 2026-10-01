package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestOwnerExcludedSelectionNeverReachesProviderOrRetryLedger(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','{}','2026-01-01T00:00:00Z');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES
 (1,1,'one',10,'pull_request','open','removed','','[]','[]','{}','h','2026-01-01T00:00:00Z'),
 (2,1,'two',20,'pull_request','open','keeper','','[]','[]','{}','h','2026-01-01T00:00:00Z')`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var crawl atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !crawl.Load() {
			http.Error(w, "unexpected request", 400)
			return
		}
		switch r.URL.Path {
		case "/repos/fixture/repo":
			fmt.Fprint(w, `{"id":100,"full_name":"fixture/repo","owner":{"login":"fixture"},"name":"repo"}`)
		case "/repos/fixture/repo/issues":
			fmt.Fprint(w, `[{"id":"one","number":10,"title":"removed","state":"open","body":"do not restore","pull_request":{}},{"id":"two","number":20,"title":"keeper","state":"open","body":"keep","pull_request":{}}]`)
		case "/repos/fixture/repo/issues/20/comments", "/repos/fixture/repo/pulls/20/reviews", "/repos/fixture/repo/pulls/20/comments":
			fmt.Fprint(w, `[]`)
		default:
			t.Errorf("excluded or unexpected fetch: %s", r.URL.Path)
			http.Error(w, "unexpected", 400)
		}
	}))
	defer server.Close()
	client := gh.New(gh.Options{BaseURL: server.URL})
	for _, graphql := range []bool{false, true} {
		for _, review := range []bool{false, true} {
			if review && !graphql {
				continue
			}
			operation := "graphql_history"
			if review {
				operation = "review_state"
			}
			result, err := New(client, s).Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{10}, State: "all", GraphQLHistory: graphql, ReviewStateOnly: review, ReceiptOperation: operation, IncludeComments: true, IncludePRMetadata: true})
			if err != nil || result.ThreadsSynced != 0 || result.OwnerExcluded != 1 {
				t.Fatalf("excluded selection: %+v %v", result, err)
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("owner-excluded provider access", calls.Load())
	}
	crawl.Store(true)
	stats, err := New(client, s).Sync(ctx, Options{Owner: "fixture", Repo: "repo", State: "all", IncludeComments: true})
	if err != nil || stats.ThreadsSynced != 1 || stats.OwnerExcluded != 1 {
		t.Fatalf("normal crawl: %+v %v", stats, err)
	}
	var n int
	if err := s.DB().QueryRow("SELECT count(*) FROM threads WHERE number=10").Scan(&n); err != nil || n != 0 {
		t.Fatal("crawl restored excluded thread", err)
	}
	mixed := &excludedHistoryClient{Client: client}
	_, err = New(mixed, s).Sync(ctx, Options{Owner: "fixture", Repo: "repo", State: "all", Numbers: []int{10, 20}, GraphQLHistory: true, IncludeComments: true, IncludePRMetadata: true})
	if err == nil || fmt.Sprint(mixed.numbers) != "[20]" {
		t.Fatal("mixed GraphQL selection not filtered", mixed.numbers, err)
	}
	for _, table := range []string{"analytics_fetch_attempts", "analytics_retries"} {
		var n int
		s.DB().QueryRow("select count(*) from " + table + " where number=10").Scan(&n)
		if n != 0 {
			t.Fatal("owner exclusion turned into retry/success", table, n)
		}
	}
}

type excludedHistoryClient struct {
	*gh.Client
	numbers []int
}

func (c *excludedHistoryClient) FetchGraphQLHistory(_ context.Context, _, _ string, numbers []int, _ gh.Reporter) (gh.HistoryBatch, error) {
	c.numbers = numbers
	return gh.HistoryBatch{}, fmt.Errorf("fixture provider failure")
}

// A late receipt for owner removal is neither a provider deletion nor recovery.
func TestOwnerExcludedLateReceipt(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.DB().Exec("INSERT INTO thread_exclusions VALUES('fixture/repo',10,'issue',1,'one','now','owner_requested','fixture')"); err != nil {
		t.Fatal(err)
	}
	for _, result := range []string{"failed", "success"} {
		err = s.RecordAnalyticsAttempt(ctx, store.AnalyticsAttempt{Repository: "FIXTURE/REPO", Number: 10, Operation: "graphql_history", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: result, Evidence: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err = s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts").Scan(&n); err != nil || n != 0 {
		t.Fatal("late receipt created work", err)
	}
}
