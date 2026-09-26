package cli

import (
	"context"
	"encoding/json"
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

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestAnalyticsPairedPartialRejectionIsolatesPeerAndPreservesUnavailableHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbpath := filepath.Join(dir, "archive.db")
	s, err := store.Open(ctx, dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var reject atomic.Bool
	beforeAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	afterAt := time.Now().UTC().Format(time.RFC3339Nano)
	conn := func(nodes ...any) map[string]any {
		if nodes == nil {
			nodes = []any{}
		}
		return map[string]any{"totalCount": len(nodes), "nodes": nodes, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": "end"}}
	}
	aliases := regexp.MustCompile(`n([0-9]+): issueOrPullRequest\(number:([0-9]+)\)`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			fmt.Fprint(w, `{"resources":{"graphql":{"limit":20000,"remaining":19000,"reset":4102444800},"core":{"limit":20000,"remaining":19000,"reset":4102444800}}}`)
			return
		}
		var req struct{ Query string }
		if e := json.NewDecoder(r.Body).Decode(&req); e != nil {
			t.Error(e)
			return
		}
		data := map[string]any{"rateLimit": map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": "2099-01-01T00:00:00Z"}}
		response := map[string]any{"data": data}
		repo := map[string]any{"id": "repo", "databaseId": 1, "nameWithOwner": "fixture/repo"}
		var failures []any
		matches := aliases.FindAllStringSubmatch(req.Query, -1)
		for _, m := range matches {
			n, _ := strconv.Atoi(m[2])
			alias := "n" + m[1]
			if reject.Load() && n == 16945 && len(matches) == 1 {
				var retainedBody string
				if e := s.DB().QueryRow("SELECT body FROM threads WHERE number=16945").Scan(&retainedBody); e != nil || retainedBody != "retained original" {
					t.Errorf("paired partial response was applied before isolated success: %q %v", retainedBody, e)
				}
			}
			if reject.Load() && n == 16944 {
				repo[alias] = nil
				failures = append(failures, map[string]any{"type": "NOT_FOUND", "path": []any{"repository", alias}, "message": "private provider prose"})
				continue
			}
			at, body := beforeAt, "retained original"
			if reject.Load() {
				at, body = afterAt, "isolated success"
			}
			comment := map[string]any{"id": fmt.Sprint("C", n), "__typename": "IssueComment", "fullDatabaseId": fmt.Sprint(n + 1000000), "body": "retained comment", "createdAt": beforeAt, "updatedAt": beforeAt, "publishedAt": beforeAt, "author": map[string]any{"id": "actor", "login": "fixture", "__typename": "User"}, "url": fmt.Sprintf("https://github.com/fixture/repo/pull/%d#comment", n)}
			repo[alias] = map[string]any{"id": fmt.Sprint("PR", n), "fullDatabaseId": fmt.Sprint(n), "__typename": "PullRequest", "number": n, "title": "fixture", "body": body, "state": "CLOSED", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": at, "url": fmt.Sprintf("https://github.com/fixture/repo/pull/%d", n), "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "author": map[string]any{"id": "actor", "login": "fixture", "__typename": "User"}, "labels": conn(), "assignees": conn(), "comments": conn(comment), "reviews": conn(), "reviewThreads": conn()}
		}
		if len(repo) > 3 {
			data["repository"] = repo
		}
		if len(failures) > 0 {
			response["errors"] = failures
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
	t.Setenv("GITHUB_TOKEN", "test-token-placeholder")
	a := New()
	a.Stderr = io.Discard
	a.configPath = writeDoctorTestConfig(t, dir, dbpath)
	if err = a.syncAnalyticsBatch(ctx, s, "fixture", "repo", []int{16944, 16945}, "graphql_history"); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", beforeAt, 0, 2); err != nil {
		t.Fatal(err)
	}
	// Reproduce a retained legacy PR with unknown review membership.
	if _, err = s.DB().Exec("DELETE FROM pull_request_review_thread_syncs WHERE thread_id=(SELECT id FROM threads WHERE number=16944)"); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		tables := []string{"threads", "thread_revisions", "comments", "comment_revisions"}
		out := map[string][][]any{}
		for _, table := range tables {
			where := "thread_id=(SELECT id FROM threads WHERE number=16944)"
			if table == "threads" {
				where = "number=16944"
			} else if table == "comment_revisions" {
				where = "comment_id IN (SELECT id FROM comments WHERE thread_id=(SELECT id FROM threads WHERE number=16944))"
			}
			rows, e := s.DB().Query("SELECT * FROM " + table + " WHERE " + where + " ORDER BY id")
			if e != nil {
				t.Fatal(e)
			}
			cols, _ := rows.Columns()
			values := [][]any{}
			for rows.Next() {
				row := make([]any, len(cols))
				dest := make([]any, len(cols))
				for i := range row {
					dest[i] = &row[i]
				}
				if e = rows.Scan(dest...); e != nil {
					t.Fatal(e)
				}
				values = append(values, row)
			}
			if e = rows.Err(); e != nil {
				t.Fatal(e)
			}
			rows.Close()
			out[table] = values
		}
		b, _ := json.Marshal(out)
		return string(b)
	}
	retained := snapshot()
	reject.Store(true)
	if err = a.analyticsIsolatedBatch(ctx, s, "fixture", "repo", []int{16944, 16945}, "review_state"); err != nil {
		t.Fatal(err)
	}
	if snapshot() != retained {
		t.Fatal("unavailable PR history changed")
	}
	var unknown, complete int
	s.DB().QueryRow("SELECT count(*) FROM pull_request_review_thread_syncs WHERE thread_id=(SELECT id FROM threads WHERE number=16944)").Scan(&unknown)
	s.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&complete)
	if unknown != 0 || complete != 1 {
		t.Fatalf("fabricated membership or invalidated core: %d %d", unknown, complete)
	}
	var evidence string
	if err = s.DB().QueryRow("SELECT evidence_json FROM analytics_fetch_attempts WHERE number=16944 AND operation='review_state' ORDER BY id DESC LIMIT 1").Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(evidence, `"type":"NOT_FOUND"`) || !strings.Contains(evidence, `"path":["repository","n0"]`) || strings.Contains(evidence, "private provider prose") {
		t.Fatal("missing or unsafe durable cause", evidence)
	}
	var pending, peerResolved, success int
	s.DB().QueryRow("SELECT count(*) FROM analytics_retries WHERE number=16944 AND operation='review_state' AND resolved_at IS NULL AND attempts=2").Scan(&pending)
	s.DB().QueryRow("SELECT count(*) FROM analytics_retries WHERE number=16945 AND operation='review_state' AND resolved_at IS NOT NULL").Scan(&peerResolved)
	s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE number=16945 AND operation='review_state' AND status='success'").Scan(&success)
	var membership, body string
	s.DB().QueryRow("SELECT x.review_thread_ids_json,t.body FROM threads t JOIN pull_request_review_thread_syncs x ON x.thread_id=t.id WHERE t.number=16945").Scan(&membership, &body)
	if pending != 1 || peerResolved != 1 || success != 1 || membership != "[]" || body != "isolated success" {
		t.Fatalf("isolation/retry proof failed: %d %d %d %s %s", pending, peerResolved, success, membership, body)
	}
}
