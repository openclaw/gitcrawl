package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestAnalyticsPoisonItemAndDiscoveryLaneDoNotStarvePeers(t *testing.T) {
	for _, brokenDiscovery := range []bool{false, true} {
		t.Run(fmt.Sprint(brokenDiscovery), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			path := filepath.Join(dir, "archive.db")
			s, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			at := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
			baseline := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
			if err = s.SetAnalyticsState(ctx, "through:fixture/repo", baseline); err != nil {
				t.Fatal(err)
			}
			if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", baseline, 1, 1); err != nil {
				t.Fatal(err)
			}
			var broken atomic.Bool
			broken.Store(true)
			var revised atomic.Bool
			conn := func(nodes ...any) map[string]any {
				if nodes == nil {
					nodes = []any{}
				}
				return map[string]any{"nodes": nodes, "totalCount": len(nodes), "pageInfo": map[string]any{"hasNextPage": false, "endCursor": "end"}}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/rate_limit" {
					fmt.Fprint(w, `{"resources":{"graphql":{"limit":50000,"remaining":49000,"reset":4102444800},"core":{"limit":50000,"remaining":49000,"reset":4102444800}}}`)
					return
				}
				if r.URL.Path != "/graphql" {
					t.Errorf("unexpected fallback: %s", r.URL.Path)
					http.Error(w, "no fallback", 400)
					return
				}
				var req struct{ Query string }
				if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
					t.Error(decodeErr)
					return
				}
				data := map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": 48000, "resetAt": "2099-01-01T00:00:00Z"}}
				if strings.Contains(req.Query, "orderBy") {
					kind, n := "issues", 1
					if strings.Contains(req.Query, "pullRequests(first:") {
						kind, n = "pullRequests", 2
					}
					page := conn(map[string]any{"number": n, "updatedAt": at})
					if brokenDiscovery && broken.Load() && n == 1 {
						delete(page, "pageInfo")
					}
					data["repository"] = map[string]any{kind: page}
				} else if strings.Contains(req.Query, "issueOrPullRequest") {
					n, typ := 1, "Issue"
					if strings.Contains(req.Query, "number:2)") {
						n, typ = 2, "PullRequest"
					}
					node := map[string]any{"id": fmt.Sprint("node-", n), "fullDatabaseId": fmt.Sprint(n), "__typename": typ, "number": n, "title": "fixture", "body": "retained body", "state": "OPEN", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": at, "url": fmt.Sprintf("https://github.com/fixture/repo/issues/%d", n), "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "author": map[string]any{"id": "actor", "login": "fixture", "__typename": "User"}, "labels": conn(), "assignees": conn(), "comments": conn()}
					if revised.Load() && n == 2 {
						node["body"] = "fresh core body"
					}
					if n == 2 {
						node["reviews"] = conn()
						node["reviewThreads"] = conn()
					}
					if !brokenDiscovery && broken.Load() && n == 1 {
						node["comments"].(map[string]any)["totalCount"] = 1
					}
					data["repository"] = map[string]any{"id": "repo", "databaseId": 1, "nameWithOwner": "fixture/repo", "n0": node}
				}
				json.NewEncoder(w).Encode(map[string]any{"data": data})
			}))
			defer server.Close()
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			t.Setenv("GITHUB_TOKEN", "test-token-placeholder")
			a := New()
			a.Stderr = io.Discard
			a.configPath = writeDoctorTestConfig(t, dir, path)
			client := gh.New(gh.Options{BaseURL: server.URL, Token: "test-token-placeholder"})
			reviewRetry := store.AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "review_state", StartedAt: "2099-01-01T00:00:00Z", FinishedAt: "2099-01-01T00:00:01Z", Status: "failed", Evidence: json.RawMessage(`{}`)}
			if !brokenDiscovery {
				if err = s.RecordAnalyticsAttempt(ctx, reviewRetry); err != nil {
					t.Fatal(err)
				}
			} else {
				// A transient core failure can precede first lane migration. Its
				// incomplete flag must not erase the verified historical baseline.
				failure := store.AnalyticsAttempt{Repository: "fixture/repo", Operation: "discover_issues", StartedAt: baseline, FinishedAt: baseline, Status: "failed", Evidence: json.RawMessage(`{}`)}
				if err = s.RecordAnalyticsAttempt(ctx, failure); err != nil {
					t.Fatal(err)
				}
				var complete int
				if err = s.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&complete); err != nil || complete != 0 {
					t.Fatalf("failure did not clear core coverage: complete=%d err=%v", complete, err)
				}
			}
			if !brokenDiscovery {
				// Exercise the real first-watch entry point with no coverage row or
				// completed-update watermark: the completed discovery receipt owns it.
				if _, err = s.DB().Exec("DELETE FROM analytics_coverage"); err != nil {
					t.Fatal(err)
				}
				s.SetAnalyticsState(ctx, "through:fixture/repo", "")
				receipt, _ := json.Marshal(map[string]any{"phase": "complete", "discovery": []map[string]any{{"kind": "issues", "done": 1, "total": 1, "updated_at": baseline}, {"kind": "pullRequests", "done": 1, "total": 1, "updated_at": baseline}}})
				if err = os.WriteFile(filepath.Join(dir, "status.json"), receipt, 0600); err != nil {
					t.Fatal(err)
				}
				a.Stdout = io.Discard
				err = a.runAnalytics(ctx, []string{"fixture/repo", "--once", "--json"})
			} else {
				err = a.analyticsCycle(ctx, s, client, "fixture", "repo")
			}
			if err == nil {
				t.Fatal("incomplete coverage reported complete")
			}
			if !brokenDiscovery && !errors.Is(err, errAnalyticsIncomplete) {
				t.Fatal(err)
			}
			var count int
			s.DB().QueryRow("SELECT count(*) FROM threads WHERE number=2").Scan(&count)
			if count != 1 {
				t.Fatal("independent PR never persisted")
			}
			s.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&count)
			if count != 0 {
				t.Fatal("poison item hidden by complete=true")
			}
			if !brokenDiscovery {
				var before, after int
				s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE number=1 AND operation='graphql_history'").Scan(&before)
				if err = a.analyticsCycle(ctx, s, client, "fixture", "repo"); !errors.Is(err, errAnalyticsIncomplete) {
					t.Fatalf("unexpected repeat outcome %v", err)
				}
				s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE number=1 AND operation='graphql_history'").Scan(&after)
				if before != after {
					t.Fatal("review-only failure bypassed core retry backoff repeatedly")
				}
			}
			broken.Store(false)
			// Simulate a due retry after a process restart, without dropping its history.
			if _, err = s.DB().Exec("UPDATE analytics_retries SET next_attempt_at='2000-01-01T00:00:00Z'"); err != nil {
				t.Fatal(err)
			}
			s.Close()
			s, err = store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.analyticsCycle(ctx, s, client, "fixture", "repo"); err != nil {
				t.Fatal(err)
			}
			if outstanding, err := s.AnalyticsOutstanding(ctx, "fixture/repo"); err != nil || outstanding != 0 {
				t.Fatalf("not recovered: %d %v", outstanding, err)
			}
			if err = s.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&count); err != nil || count != 1 {
				t.Fatalf("verified core baseline did not recover: complete=%d err=%v", count, err)
			}
			s.DB().QueryRow("SELECT count(*) FROM threads").Scan(&count)
			if count != 2 {
				t.Fatalf("wrong recovered content count %d", count)
			}
			s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE status='failed'").Scan(&count)
			if count < 1 {
				t.Fatal("failure history erased")
			}
			reviewRetry.Number = 2
			if err = s.RecordAnalyticsAttempt(ctx, reviewRetry); err != nil {
				t.Fatal(err)
			}
			revised.Store(true)
			if err = a.analyticsCycle(ctx, s, client, "fixture", "repo"); err != nil {
				t.Fatal(err)
			}
			var body string
			s.DB().QueryRow("SELECT body FROM threads WHERE number=2").Scan(&body)
			if body != "fresh core body" {
				t.Fatal("review recovery backoff hid a newly discovered core update")
			}
		})
	}
}

func TestAnalyticsLaneMigrationRetainsInflightCursorAndLegacyReceipt(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	legacy := `{"started":"2026-01-01T12:00:00Z","since":"2026-01-01T10:55:00Z","kind":1,"cursor":"opaque-provider-cursor","issues":10,"prs":20}`
	if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-01-01T11:00:00Z", 10, 20); err != nil {
		t.Fatal(err)
	}
	s.SetAnalyticsState(ctx, "through:fixture/repo", "2026-01-01T11:00:00Z")
	s.SetAnalyticsState(ctx, "updates:fixture/repo", legacy)
	if err = migrateAnalyticsLanes(ctx, s, "fixture/repo"); err != nil {
		t.Fatal(err)
	}
	value, err := s.AnalyticsState(ctx, "updates:fixture/repo:pullRequests")
	if err != nil {
		t.Fatal(err)
	}
	var cp updateCheckpoint
	if err = json.Unmarshal([]byte(value), &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Cursor != "opaque-provider-cursor" || cp.Since != "2026-01-01T10:55:00Z" {
		t.Fatal("in-flight provider cursor reset")
	}
	total, _ := s.AnalyticsState(ctx, "total:fixture/repo:issues")
	if total != "10" {
		t.Fatalf("completed lane total lost: %s", total)
	}
	old, _ := s.AnalyticsState(ctx, "updates:fixture/repo")
	if old != legacy {
		t.Fatal("legacy evidence overwritten")
	}
	s.SetAnalyticsState(ctx, "updates:fixture/repo:pullRequests", "new-progress")
	if err = migrateAnalyticsLanes(ctx, s, "fixture/repo"); err != nil {
		t.Fatal(err)
	}
	value, _ = s.AnalyticsState(ctx, "updates:fixture/repo:pullRequests")
	if value != "new-progress" {
		t.Fatal("restart reseeded an existing checkpoint")
	}
}

func TestAnalyticsDiscoveryLeavesDueItemsToBoundedRetryScheduler(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 1; i <= 24; i++ {
		a := store.AnalyticsAttempt{Repository: "fixture/repo", Number: i, Operation: "graphql_history", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", Evidence: json.RawMessage(`{}`)}
		if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	due, err := s.DueAnalyticsRetries(ctx, "fixture/repo", "2099-01-01T00:00:00Z", 8, "graphql_history")
	if err != nil || len(due) != 8 {
		t.Fatalf("retry bound: %v %v", due, err)
	}
	// This app has no usable configuration. Any attempted collection would fail;
	// discovery must skip even due items not selected by the retry budget.
	a := New()
	numbers := []int{}
	for i := 1; i <= 24; i++ {
		numbers = append(numbers, i)
	}
	if err = a.analyticsNumbers(ctx, s, "fixture", "repo", numbers, true, "graphql_history"); err != nil {
		t.Fatal(err)
	}
	var attempts int
	s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts").Scan(&attempts)
	if attempts != 24 {
		t.Fatal("discovery bypassed the retry scheduler")
	}
}
