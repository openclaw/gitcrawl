package syncer

import (
	"context"
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
	_, err = s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','{}','2026-01-01');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES
 (1,1,'one',10,'pull_request','open','removed','','[]','[]','{}','h','2026-01-01'),
 (2,1,'two',20,'pull_request','open','keeper','','[]','[]','{}','h','2026-01-01')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, "fixture"); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "unexpected request", 400) }))
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
			if err != nil || result.ThreadsSynced != 0 {
				t.Fatalf("excluded selection: %+v %v", result, err)
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("owner-excluded provider access", calls.Load())
	}
	for _, table := range []string{"analytics_fetch_attempts", "analytics_retries"} {
		var n int
		s.DB().QueryRow("select count(*) from " + table + " where number=10").Scan(&n)
		if n != 0 {
			t.Fatal("owner exclusion turned into retry/success", table, n)
		}
	}
}
