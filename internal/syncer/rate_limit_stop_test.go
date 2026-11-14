package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

// GitHub asks clients to stop sending requests while rate limited. Once a
// rate-limit failure survives the client's own wait and retry, a sequential
// sync must not move on to fetch the remaining siblings.
func TestSyncStopsAcquisitionAfterRateLimitFailure(t *testing.T) {
	secondary := func(w http.ResponseWriter, status int) {
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-RateLimit-Remaining", "4900")
		w.WriteHeader(status)
		fmt.Fprint(w, `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`)
	}
	primary := func(w http.ResponseWriter, status int) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Unix(), 10))
		w.WriteHeader(status)
		fmt.Fprint(w, `{"message":"API rate limit exceeded"}`)
	}
	permission := func(w http.ResponseWriter, status int) {
		w.Header().Set("X-RateLimit-Remaining", "4900")
		w.WriteHeader(status)
		fmt.Fprint(w, `{"message":"Resource not accessible by integration"}`)
	}
	for _, tc := range []struct {
		name      string
		options   Options
		failPath  string
		fail      func(http.ResponseWriter, int)
		status    int
		siblings  []string
		wantFetch bool
		reserve   int
	}{
		{"secondary 403 on exact issue", Options{Numbers: []int{1, 2, 3}}, "/repos/fixture/repo/issues/1", secondary, http.StatusForbidden, []string{"/repos/fixture/repo/issues/2", "/repos/fixture/repo/issues/3"}, false, 0},
		{"primary exhaustion on exact issue", Options{Numbers: []int{1, 2, 3}}, "/repos/fixture/repo/issues/1", primary, http.StatusForbidden, []string{"/repos/fixture/repo/issues/2", "/repos/fixture/repo/issues/3"}, false, 0},
		{"secondary 429 on comments", Options{State: "all", IncludeComments: true}, "/repos/fixture/repo/issues/1/comments", secondary, http.StatusTooManyRequests, []string{"/repos/fixture/repo/issues/2/comments", "/repos/fixture/repo/issues/3/comments"}, false, 0},
		{"secondary 403 with quota reserve", Options{Numbers: []int{1, 2, 3}}, "/repos/fixture/repo/issues/1", secondary, http.StatusForbidden, []string{"/repos/fixture/repo/issues/2", "/repos/fixture/repo/issues/3"}, false, 10},
		{"permission 403 control", Options{Numbers: []int{1, 2, 3}}, "/repos/fixture/repo/issues/1", permission, http.StatusForbidden, []string{"/repos/fixture/repo/issues/2", "/repos/fixture/repo/issues/3"}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var mu sync.Mutex
			requested := map[string]int{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requested[r.URL.Path]++
				mu.Unlock()
				issue := func(n int) string {
					return fmt.Sprintf(`{"id":%d,"number":%d,"title":"issue %d","state":"open","updated_at":"2026-11-01T00:00:00Z"}`, 100+n, n, n)
				}
				switch {
				case r.URL.Path == tc.failPath:
					tc.fail(w, tc.status)
				case r.URL.Path == "/rate_limit":
					fmt.Fprintf(w, `{"resources":{"core":{"limit":5000,"remaining":4900,"reset":%d}}}`, time.Now().Add(time.Hour).Unix())
				case r.URL.Path == "/repos/fixture/repo":
					fmt.Fprint(w, `{"id":1,"name":"repo","full_name":"fixture/repo","owner":{"login":"fixture"}}`)
				case r.URL.Path == "/repos/fixture/repo/issues":
					fmt.Fprintf(w, `[%s,%s,%s]`, issue(1), issue(2), issue(3))
				case strings.HasSuffix(r.URL.Path, "/comments"):
					fmt.Fprint(w, `[]`)
				case strings.HasPrefix(r.URL.Path, "/repos/fixture/repo/issues/"):
					n, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/repos/fixture/repo/issues/"))
					fmt.Fprint(w, issue(n))
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.Error(w, "unexpected", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			options := tc.options
			options.Owner, options.Repo = "fixture", "repo"
			_, err = New(gh.New(gh.Options{BaseURL: server.URL, RateLimitReserve: tc.reserve}), s).Sync(ctx, options)
			if err == nil {
				t.Fatal("rate-limited or forbidden item did not fail the sync")
			}
			mu.Lock()
			defer mu.Unlock()
			attempts := 2 // the client's own wait and single retry
			if tc.wantFetch {
				attempts = 1 // permission failures are not retried
			}
			if requested[tc.failPath] != attempts {
				t.Fatalf("failing request sent %d times, want %d", requested[tc.failPath], attempts)
			}
			if tc.reserve > 0 && requested["/rate_limit"] != 3 {
				// One reserve probe each for the repository, the failing request
				// and its retry; none for siblings that are no longer fetched.
				t.Fatalf("reserve probes = %d, want 3 (requests %v)", requested["/rate_limit"], requested)
			}
			for _, sibling := range tc.siblings {
				if fetched := requested[sibling] > 0; fetched != tc.wantFetch {
					t.Fatalf("sibling %s fetched=%t after %s, want %t (requests %v)", sibling, fetched, tc.name, tc.wantFetch, requested)
				}
			}
		})
	}
}

// GitHub reports GraphQL primary-quota exhaustion as HTTP 200 with a
// RATE_LIMITED error rather than as a 403 or 429. Review-thread hydration must
// stop on it like any other rate limit instead of querying every remaining PR.
func TestSyncStopsAcquisitionAfterGraphQLRateLimited(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var mu sync.Mutex
	reviewThreadQueries := map[int]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/fixture/repo":
			fmt.Fprint(w, `{"id":1,"name":"repo","full_name":"fixture/repo","owner":{"login":"fixture"}}`)
		case strings.HasPrefix(r.URL.Path, "/repos/fixture/repo/issues/"):
			n, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/repos/fixture/repo/issues/"))
			fmt.Fprintf(w, `{"id":%d,"number":%d,"title":"pr %d","state":"open","pull_request":{},"updated_at":"2026-11-01T00:00:00Z"}`, 100+n, n, n)
		case r.URL.Path == "/graphql":
			var request struct {
				Variables struct {
					PR int `json:"pr"`
				} `json:"variables"`
			}
			_ = json.NewDecoder(r.Body).Decode(&request)
			mu.Lock()
			reviewThreadQueries[request.Variables.PR]++
			mu.Unlock()
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
			fmt.Fprint(w, `{"data":null,"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded for user ID 1."}]}`)
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	_, err = New(gh.New(gh.Options{BaseURL: server.URL}), s).Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{1, 2, 3}, IncludePRDetails: true})
	if err == nil || !gh.IsRateLimited(err) {
		t.Fatalf("GraphQL rate limit not reported as a rate limit: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if reviewThreadQueries[1] != 1 || reviewThreadQueries[2] != 0 || reviewThreadQueries[3] != 0 {
		t.Fatalf("review-thread queries by PR = %v, want only PR 1", reviewThreadQueries)
	}
}
