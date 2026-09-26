package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestAnalyticsRecoveryUsesQuotaAfterCoreAndResumesCancellation(t *testing.T) {
	for _, mode := range []string{"available", "reserved", "quota_drops", "quota_races", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dir := t.TempDir()
			s, err := store.Open(ctx, filepath.Join(dir, "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_, err = s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo',1,'{}','2026-01-01')`)
			if err != nil {
				t.Fatal(err)
			}
			oldItems := 48
			if mode == "quota_races" {
				oldItems = 6001
			}
			tx, err := s.DB().BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			for n := 1; n <= oldItems; n++ {
				_, err = tx.Exec(`INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at,last_pulled_at) VALUES(?,1,?,?,'pull_request','open','','','[]','[]','{}','fixture','2026-01-01','2026-01-01T00:00:00Z')`, n, fmt.Sprint(n), n)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			baseline := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
			if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", baseline, 0, oldItems); err != nil {
				t.Fatal(err)
			}
			var coreSeen, cancelled atomic.Bool
			var recovered, probes, recoveryProbes atomic.Int64
			quota := func() int {
				if mode == "quota_races" && recoveryProbes.Load() > 1 {
					return 2999
				}
				if mode == "reserved" || (mode == "quota_drops" && recovered.Load() >= 16) {
					return 3020
				}
				return 19000
			}
			conn := func(nodes ...any) map[string]any {
				if nodes == nil {
					nodes = []any{}
				}
				return map[string]any{"totalCount": len(nodes), "nodes": nodes, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": "end"}}
			}
			numbers := regexp.MustCompile(`n([0-9]+): issueOrPullRequest\(number:([0-9]+)\)`)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/rate_limit" {
					probes.Add(1)
					if coreSeen.Load() {
						recoveryProbes.Add(1)
					}
					fmt.Fprintf(w, `{"resources":{"graphql":{"limit":20000,"remaining":%d,"reset":4102444800},"core":{"limit":20000,"remaining":19999,"reset":4102444800}}}`, quota())
					return
				}
				if r.URL.Path != "/graphql" {
					t.Errorf("unexpected endpoint %s", r.URL.Path)
					http.Error(w, "unexpected", 400)
					return
				}
				var req struct{ Query string }
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				if coreSeen.Load() && !strings.Contains(req.Query, "orderBy") && !strings.Contains(req.Query, "issueOrPullRequest") {
					recoveryProbes.Add(1)
				}
				data := map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": quota(), "limit": 20000, "resetAt": "2099-01-01T00:00:00Z"}}
				if strings.Contains(req.Query, "orderBy") {
					kind, page := "issues", conn()
					if strings.Contains(req.Query, "pullRequests(first:") {
						kind, page = "pullRequests", conn(map[string]any{"number": 10000, "updatedAt": time.Now().UTC().Format(time.RFC3339Nano)})
					}
					data["repository"] = map[string]any{kind: page}
				} else if matches := numbers.FindAllStringSubmatch(req.Query, -1); len(matches) > 0 {
					repo := map[string]any{"id": "repo", "databaseId": 1, "nameWithOwner": "fixture/repo"}
					for _, m := range matches {
						n, _ := strconv.Atoi(m[2])
						if n == 10000 {
							coreSeen.Store(true)
						} else {
							if !coreSeen.Load() {
								t.Error("recovery ran before ordinary capture")
							}
							var complete int
							if e := s.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&complete); e != nil || complete != 1 {
								t.Errorf("core coverage unavailable during review: %d %v", complete, e)
							}
							if mode == "cancel" && cancelled.CompareAndSwap(false, true) {
								cancel()
								<-r.Context().Done()
								return
							}
							recovered.Add(1)
						}
						node := map[string]any{"id": fmt.Sprint("PR-", n), "fullDatabaseId": fmt.Sprint(n), "__typename": "PullRequest", "number": n, "title": "fixture", "body": "retained", "state": "OPEN", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": baseline, "url": fmt.Sprintf("https://github.com/fixture/repo/pull/%d", n), "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "author": map[string]any{"id": "actor", "login": "fixture", "__typename": "User"}, "labels": conn(), "assignees": conn(), "comments": conn(), "reviews": conn(), "reviewThreads": conn()}
						repo["n"+m[1]] = node
					}
					data["repository"] = repo
				}
				json.NewEncoder(w).Encode(map[string]any{"data": data})
			}))
			defer server.Close()
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			t.Setenv("GITHUB_TOKEN", "test-token-placeholder")
			a := New()
			a.Stderr = io.Discard
			a.configPath = writeDoctorTestConfig(t, dir, filepath.Join(dir, "archive.db"))
			client := gh.New(gh.Options{BaseURL: server.URL, Token: "test-token-placeholder"})
			err = a.analyticsCycle(ctx, s, client, "fixture", "repo")
			if mode == "cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v", err)
				}
				var failed int
				if e := s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE operation='review_state' AND status='failed'").Scan(&failed); e != nil || failed == 0 {
					t.Fatalf("missing cancellation receipts: %d %v", failed, e)
				}
				if _, err = s.DB().Exec("UPDATE analytics_retries SET next_attempt_at='2000-01-01T00:00:00Z' WHERE resolved_at IS NULL"); err != nil {
					t.Fatal(err)
				}
				// Resume from the real committed cursor and retry rows; no reset/reseed.
				err = a.analyticsCycle(context.Background(), s, client, "fixture", "repo")
			}
			if err != nil {
				t.Fatal(err)
			}
			var resolved, pending, scanned, complete int
			if err = s.DB().QueryRow("SELECT count(*) FROM analytics_retries WHERE operation='review_state' AND resolved_at IS NOT NULL").Scan(&resolved); err != nil {
				t.Fatal(err)
			}
			if err = s.DB().QueryRow("SELECT pending_items,scanned,complete FROM analytics_review_state_coverage").Scan(&pending, &scanned, &complete); err != nil {
				t.Fatal(err)
			}
			want := oldItems
			if mode == "reserved" || mode == "quota_races" {
				want = 0
			}
			if mode == "quota_drops" {
				want = 16
			}
			if resolved != want || pending != oldItems-want || scanned != oldItems+1 || (complete == 1) != (want == oldItems) {
				t.Fatalf("resolved=%d pending=%d scanned=%d complete=%d, want recovered%d", resolved, pending, scanned, complete, want)
			}
			if mode == "quota_races" {
				var deferred int
				if e := s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE error_class='rate_limit'").Scan(&deferred); e != nil || deferred == 0 {
					t.Fatalf("native reserve guard not exercised: %d %v", deferred, e)
				}
			}
			if probes.Load() == 0 {
				t.Fatal("never obtained actual quota")
			}
			if err = s.DB().QueryRow("SELECT complete FROM analytics_coverage").Scan(&complete); err != nil || complete != 1 {
				t.Fatalf("core coverage affected by recovery: %d %v", complete, err)
			}
		})
	}
}

func TestAnalyticsReviewBudgetRejectsMissingAndExpiredQuota(t *testing.T) {
	now := time.Now()
	for _, limits := range [][]gh.RateLimitSnapshot{nil, {{Resource: "core", Limit: 20000, Remaining: 19000, ResetAt: now.Add(time.Hour)}}, {{Resource: "graphql", Limit: 20000, Remaining: 19000, ResetAt: now.Add(-time.Second)}}} {
		if _, _, err := analyticsReviewBudget(limits, now); err == nil {
			t.Fatal("invalid quota admitted recovery")
		}
	}
}

func TestAnalyticsRecoveryScansPastOneChunkWhenQuotaIsReserved(t *testing.T) {
	for _, quotaJSON := range []string{`{"data":{"rateLimit":{"cost":1,"limit":20000,"remaining":3020,"resetAt":"2099-01-01T00:00:00Z"}}}`, `{"data":{}}`, `{"data":{"rateLimit":{"cost":1,"limit":20000,"remaining":19000,"resetAt":"2000-01-01T00:00:00Z"}}}`} {
		ctx := context.Background()
		s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		_, err = s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo',1,'{}','2026-01-01');
 WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<6001)
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at,last_pulled_at)
 SELECT x,1,printf('%d',x),x,'pull_request','open','','','[]','[]','{}','fixture','2026-01-01','2026-01-01T00:00:00Z' FROM n`)
		if err != nil {
			t.Fatal(err)
		}
		var requests atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			if r.URL.Path != "/graphql" {
				t.Errorf("provider work admitted inside reserve: %s", r.URL.Path)
			}
			fmt.Fprint(w, quotaJSON)
		}))
		defer server.Close()
		a := New()
		a.Stderr = io.Discard
		c := gh.New(gh.Options{BaseURL: server.URL})
		if err = a.analyticsReviewRecovery(ctx, s, c, "fixture", "repo", time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		var scanned, pending, done, complete int
		if err = s.DB().QueryRow("SELECT scanned,pending_items,scan_complete,complete FROM analytics_review_state_coverage").Scan(&scanned, &pending, &done, &complete); err != nil {
			t.Fatal(err)
		}
		if scanned != 6001 || pending != 6001 || done != 1 || complete != 0 || requests.Load() != 1 {
			t.Fatalf("scan stalled or reserve breached: %d %d %d %d requests=%d", scanned, pending, done, complete, requests.Load())
		}
	}
}

func TestAnalyticsRecoveryDeadlineYieldsWithDurableReceipt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := store.Open(ctx, filepath.Join(dir, "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo',1,'{}','2026-01-01');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at,last_pulled_at) VALUES(1,1,'1',1,'pull_request','open','','','[]','[]','{}','fixture','2026-01-01','2026-01-01T00:00:00Z')`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-01-01T00:00:00Z", 0, 1); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			fmt.Fprint(w, `{"resources":{"graphql":{"limit":20000,"remaining":19000,"reset":4102444800}}}`)
			return
		}
		var req struct{ Query string }
		if e := json.NewDecoder(r.Body).Decode(&req); e != nil {
			t.Error(e)
			return
		}
		if !strings.Contains(req.Query, "issueOrPullRequest") {
			fmt.Fprint(w, `{"data":{"rateLimit":{"cost":1,"limit":20000,"remaining":19000,"resetAt":"2099-01-01T00:00:00Z"}}}`)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
	t.Setenv("GITHUB_TOKEN", "test-token-placeholder")
	a := New()
	a.Stderr = io.Discard
	a.configPath = writeDoctorTestConfig(t, dir, filepath.Join(dir, "archive.db"))
	c := gh.New(gh.Options{BaseURL: server.URL, Token: "test-token-placeholder"})
	if err = a.analyticsReviewRecovery(ctx, s, c, "fixture", "repo", time.Now().Add(6*time.Second)); err != nil {
		t.Fatal("window yield failed core cycle:", err)
	}
	var failed, pending, core int
	if err = s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE status='failed' AND error_class='cancelled'").Scan(&failed); err != nil {
		t.Fatal(err)
	}
	if err = s.DB().QueryRow("SELECT pending_items FROM analytics_review_state_coverage").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err = s.DB().QueryRow("SELECT complete FROM analytics_coverage").Scan(&core); err != nil {
		t.Fatal(err)
	}
	if failed != 1 || pending != 1 || core != 1 {
		t.Fatalf("lost cancellation state: failed=%d pending=%d core=%d", failed, pending, core)
	}
	if analyticsCancellationOnly(errors.Join(context.DeadlineExceeded, errors.New("receipt failed"))) {
		t.Fatal("storage error hidden as yield")
	}
	if !analyticsCancellationOnly(errors.Join(fmt.Errorf("request: %w", context.DeadlineExceeded), context.Canceled)) {
		t.Fatal("normal cancellation not recognized")
	}
}
