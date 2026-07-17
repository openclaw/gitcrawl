package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func writeRateLimits(t *testing.T, w http.ResponseWriter, resetAt time.Time, coreRemaining, graphqlRemaining int) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(map[string]any{"resources": map[string]any{
		"core":    map[string]any{"limit": 5000, "remaining": coreRemaining, "reset": resetAt.Unix()},
		"graphql": map[string]any{"limit": 5000, "remaining": graphqlRemaining, "reset": resetAt.Unix()},
	}}); err != nil {
		t.Fatalf("encode rate limits: %v", err)
	}
}

func TestListRepositoryIssuesPaginatesAndLimits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("missing auth header: %q", r.Header.Get("Authorization"))
		}
		switch r.URL.Query().Get("page") {
		case "":
			w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next", <`+serverURL(r)+`?page=2>; rel="last"`)
			_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 1}, {"number": 2}})
		case "2":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 3}})
		default:
			t.Fatalf("unexpected page: %s", r.URL.RawQuery)
		}
	}))
	defer server.Close()

	var messages []string
	reporter := Reporter(func(message string) { messages = append(messages, message) })
	client := New(Options{Token: "token", BaseURL: server.URL, PageDelay: -1})
	rows, err := client.ListRepositoryIssues(context.Background(), "openclaw", "gitcrawl", ListIssuesOptions{Limit: 3}, reporter)
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows: got %d want 3", len(rows))
	}
	if intValue(rows[2]["number"]) != 3 {
		t.Fatalf("last number: %#v", rows[2]["number"])
	}
	joined := strings.Join(messages, "\n")
	if !strings.Contains(joined, "page 1/2 fetched") || !strings.Contains(joined, "page 2/2 fetched") {
		t.Fatalf("expected page X/Y log lines, got:\n%s", joined)
	}
}

func TestListRepositoryIssuesUsesExpectedTotalWhenNoLastLink(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("page") {
		case "":
			// Cursor-style pagination: only "next", no "last".
			w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next"`)
			rows := make([]map[string]any, 100)
			for i := range rows {
				rows[i] = map[string]any{"number": i + 1}
			}
			_ = json.NewEncoder(w).Encode(rows)
		case "2":
			rows := make([]map[string]any, 50)
			for i := range rows {
				rows[i] = map[string]any{"number": 100 + i + 1}
			}
			_ = json.NewEncoder(w).Encode(rows)
		default:
			t.Fatalf("unexpected page: %s", r.URL.RawQuery)
		}
	}))
	defer server.Close()

	var messages []string
	reporter := Reporter(func(message string) { messages = append(messages, message) })
	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	rows, err := client.ListRepositoryIssues(context.Background(), "openclaw", "gitcrawl", ListIssuesOptions{ExpectedTotal: 150}, reporter)
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	if len(rows) != 150 {
		t.Fatalf("rows: got %d want 150", len(rows))
	}
	joined := strings.Join(messages, "\n")
	if !strings.Contains(joined, "page 1/2 fetched") || !strings.Contains(joined, "page 2/2 fetched") {
		t.Fatalf("expected page X/Y log lines from hint, got:\n%s", joined)
	}
}

func TestPaginateRaisesTotalWhenActualExceedsHint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("page") {
		case "":
			w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next"`)
			_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 1}})
		case "2":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 2}})
		default:
			t.Fatalf("unexpected page: %s", r.URL.RawQuery)
		}
	}))
	defer server.Close()

	var messages []string
	reporter := Reporter(func(message string) { messages = append(messages, message) })
	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	// Hint underestimates (1 page) but the API actually returns 2.
	if _, err := client.ListRepositoryIssues(context.Background(), "o", "r", ListIssuesOptions{ExpectedTotal: 1}, reporter); err != nil {
		t.Fatalf("list: %v", err)
	}
	joined := strings.Join(messages, "\n")
	if !strings.Contains(joined, "page 2/2 fetched") {
		t.Fatalf("expected total to be raised to actual page count, got:\n%s", joined)
	}
}

func TestRequestErrorIncludesStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	_, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	requestErr, ok := err.(*RequestError)
	if !ok {
		t.Fatalf("error type: %T", err)
	}
	if requestErr.Status != http.StatusUnauthorized {
		t.Fatalf("status: got %d want %d", requestErr.Status, http.StatusUnauthorized)
	}
	if !strings.Contains((&RequestError{Method: "GET", URL: "https://example.test", Status: 500}).Error(), "status 500") {
		t.Fatal("request error without body missing status")
	}
	if !strings.Contains((&RequestError{Method: "POST", URL: "https://example.test", Status: 400, Body: "bad"}).Error(), "bad") {
		t.Fatal("request error with body missing body")
	}
}

func TestClientSingleResourceAndCollectionEndpoints(t *testing.T) {
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests[r.URL.Path]++
		if r.Header.Get("Accept") != "application/vnd.github+json" {
			t.Fatalf("accept header = %q", r.Header.Get("Accept"))
		}
		switch r.URL.Path {
		case "/repos/openclaw/gitcrawl":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		case "/repos/openclaw/gitcrawl/issues/7":
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 7})
		case "/repos/openclaw/gitcrawl/pulls/8":
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 8})
		case "/repos/openclaw/gitcrawl/issues/7/comments",
			"/repos/openclaw/gitcrawl/pulls/8/reviews",
			"/repos/openclaw/gitcrawl/pulls/8/comments",
			"/repos/openclaw/gitcrawl/pulls/8/files",
			"/repos/openclaw/gitcrawl/pulls/8/commits":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1}})
		case "/repos/openclaw/gitcrawl/commits/abc/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []map[string]any{{"name": "test"}}})
		case "/repos/openclaw/gitcrawl/actions/runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": []map[string]any{{"id": 99}}})
		case "/repos/openclaw/gitcrawl/actions/runs/99":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		default:
			t.Fatalf("unexpected path: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	ctx := context.Background()
	if row, err := client.GetRepo(ctx, "openclaw", "gitcrawl", nil); err != nil || intValue(row["id"]) != 1 {
		t.Fatalf("get repo = %#v %v", row, err)
	}
	if row, err := client.GetIssue(ctx, "openclaw", "gitcrawl", 7, nil); err != nil || intValue(row["number"]) != 7 {
		t.Fatalf("get issue = %#v %v", row, err)
	}
	if row, err := client.GetPull(ctx, "openclaw", "gitcrawl", 8, nil); err != nil || intValue(row["number"]) != 8 {
		t.Fatalf("get pull = %#v %v", row, err)
	}
	for name, fn := range map[string]func() ([]map[string]any, error){
		"comments": func() ([]map[string]any, error) { return client.ListIssueComments(ctx, "openclaw", "gitcrawl", 7, nil) },
		"reviews":  func() ([]map[string]any, error) { return client.ListPullReviews(ctx, "openclaw", "gitcrawl", 8, nil) },
		"review-comments": func() ([]map[string]any, error) {
			return client.ListPullReviewComments(ctx, "openclaw", "gitcrawl", 8, nil)
		},
		"files":   func() ([]map[string]any, error) { return client.ListPullFiles(ctx, "openclaw", "gitcrawl", 8, nil) },
		"commits": func() ([]map[string]any, error) { return client.ListPullCommits(ctx, "openclaw", "gitcrawl", 8, nil) },
		"checks": func() ([]map[string]any, error) {
			return client.ListCommitCheckRuns(ctx, "openclaw", "gitcrawl", "abc", nil)
		},
		"runs": func() ([]map[string]any, error) {
			return client.ListWorkflowRuns(ctx, "openclaw", "gitcrawl", ListWorkflowRunsOptions{HeadSHA: "abc"}, nil)
		},
	} {
		rows, err := fn()
		if err != nil || len(rows) != 1 {
			t.Fatalf("%s rows = %+v err=%v", name, rows, err)
		}
	}
	if run, err := client.GetWorkflowRun(ctx, "openclaw", "gitcrawl", "99", nil); err != nil ||
		intValue(run["id"]) != 99 {
		t.Fatalf("get workflow run = %+v err=%v", run, err)
	}
	if len(requests) != 11 {
		t.Fatalf("requests = %+v", requests)
	}
}

func TestCheckRunsAndWorkflowRunsPaginate(t *testing.T) {
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests[r.URL.Path]++
		page := r.URL.Query().Get("page")
		switch r.URL.Path {
		case "/repos/openclaw/gitcrawl/commits/abc/check-runs":
			switch page {
			case "":
				w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next"`)
				_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []map[string]any{{"name": "test-1"}, {"name": "test-2"}}})
			case "2":
				_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []map[string]any{{"name": "test-3"}}})
			default:
				t.Fatalf("unexpected check-runs page: %s", r.URL.RawQuery)
			}
		case "/repos/openclaw/gitcrawl/actions/runs":
			switch page {
			case "":
				w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next"`)
				_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": []map[string]any{{"id": 1}, {"id": 2}}})
			case "2":
				_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": []map[string]any{{"id": 3}, {"id": 4}}})
			default:
				t.Fatalf("unexpected workflow-runs page: %s", r.URL.RawQuery)
			}
		default:
			t.Fatalf("unexpected path: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	ctx := context.Background()
	checks, err := client.ListCommitCheckRuns(ctx, "openclaw", "gitcrawl", "abc", nil)
	if err != nil {
		t.Fatalf("list checks: %v", err)
	}
	if len(checks) != 3 || checks[2]["name"] != "test-3" {
		t.Fatalf("checks = %#v", checks)
	}
	runs, err := client.ListWorkflowRuns(ctx, "openclaw", "gitcrawl", ListWorkflowRunsOptions{HeadSHA: "abc", Limit: 3}, nil)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 3 || intValue(runs[2]["id"]) != 3 {
		t.Fatalf("runs = %#v", runs)
	}
	if requests["/repos/openclaw/gitcrawl/commits/abc/check-runs"] != 2 || requests["/repos/openclaw/gitcrawl/actions/runs"] != 2 {
		t.Fatalf("requests = %+v", requests)
	}
}

func TestListPullReviewThreadsDecodesGraphQLEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/graphql" {
			t.Fatalf("unexpected path: %s", r.URL.String())
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content-type = %q", r.Header.Get("Content-Type"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{
			"reviewThreads": map[string]any{
				"nodes": []map[string]any{{
					"id":         "PRRT_1",
					"isResolved": false,
					"comments":   map[string]any{"nodes": []map[string]any{{"id": "PRRC_1"}}},
				}},
				"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
			},
		}}}})
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	rows, err := client.ListPullReviewThreads(context.Background(), "openclaw", "gitcrawl", 8, nil)
	if err != nil {
		t.Fatalf("list review threads: %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != "PRRT_1" {
		t.Fatalf("rows = %#v", rows)
	}
}

func TestListPullReviewThreadsUsesEnterpriseGraphQLEndpoint(t *testing.T) {
	var graphQLPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		graphQLPath = r.URL.Path
		if r.URL.Path != "/api/graphql" {
			http.Error(w, "wrong graphql endpoint", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{
			"reviewThreads": map[string]any{
				"nodes":    []map[string]any{},
				"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
			},
		}}}})
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL + "/api/v3", PageDelay: -1})
	if _, err := client.ListPullReviewThreads(context.Background(), "openclaw", "gitcrawl", 8, nil); err != nil {
		t.Fatalf("list review threads: %v", err)
	}
	if graphQLPath != "/api/graphql" {
		t.Fatalf("graphql path = %q, want /api/graphql", graphQLPath)
	}
}

func TestListPullReviewThreadsRetriesWithBody(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode graphql request %d: %v", atomic.LoadInt32(&calls)+1, err)
		}
		if body.Query == "" || body.Variables["owner"] != "openclaw" || body.Variables["repo"] != "gitcrawl" {
			t.Fatalf("graphql request body = %+v", body)
		}
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{
			"reviewThreads": map[string]any{
				"nodes":    []map[string]any{},
				"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
			},
		}}}})
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	if _, err := client.ListPullReviewThreads(context.Background(), "openclaw", "gitcrawl", 8, nil); err != nil {
		t.Fatalf("list review threads: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func TestListPullReviewThreadsPaginatesReviewThreadComments(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/graphql" {
			t.Fatalf("unexpected path: %s", r.URL.String())
		}
		var body graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch calls {
		case 1:
			if body.Variables["threadID"] != nil {
				t.Fatalf("first request should fetch review threads, variables=%+v", body.Variables)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{
				"reviewThreads": map[string]any{
					"nodes": []map[string]any{{
						"id":         "PRRT_1",
						"isResolved": false,
						"comments": map[string]any{
							"nodes":    []map[string]any{{"id": "PRRC_1"}},
							"pageInfo": map[string]any{"hasNextPage": true, "endCursor": "comment-cursor-1"},
						},
					}},
					"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
				},
			}}}})
		case 2:
			if body.Variables["threadID"] != "PRRT_1" || body.Variables["cursor"] != "comment-cursor-1" {
				t.Fatalf("comment page variables = %+v", body.Variables)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"node": map[string]any{
					"comments": map[string]any{
						"nodes":    []map[string]any{{"id": "PRRC_2"}},
						"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
					},
				},
			}})
		default:
			t.Fatalf("unexpected graphql call %d", calls)
		}
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	rows, err := client.ListPullReviewThreads(context.Background(), "openclaw", "gitcrawl", 8, nil)
	if err != nil {
		t.Fatalf("list review threads: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
	if len(rows) != 1 || rows[0]["id"] != "PRRT_1" {
		t.Fatalf("rows = %#v", rows)
	}
	comments, ok := rows[0]["comments"].(map[string]any)
	if !ok {
		t.Fatalf("comments = %#v", rows[0]["comments"])
	}
	nodes, ok := comments["nodes"].([]map[string]any)
	if !ok || len(nodes) != 2 || nodes[1]["id"] != "PRRC_2" {
		t.Fatalf("comment nodes = %#v", comments["nodes"])
	}
}

func TestNextPageAndReporterBranches(t *testing.T) {
	header := `<https://api.github.test/repos/o/r/issues?page=2&state=open>; rel="next", <https://api.github.test/repos/o/r/issues?page=9>; rel="last"`
	if got := nextPage(header, "https://api.github.test"); got != "/repos/o/r/issues?page=2&state=open" {
		t.Fatalf("next page = %q", got)
	}
	gheHeader := `<https://ghe.example/api/v3/repos/o/r/issues?page=2>; rel="next"`
	if got := nextPage(gheHeader, "https://ghe.example/api/v3"); got != "/repos/o/r/issues?page=2" {
		t.Fatalf("ghe next page = %q", got)
	}
	if got := nextPage(`<bad-url>; rel="last"`, "https://api.github.test"); got != "" {
		t.Fatalf("bad next page = %q", got)
	}
	if got := lastPage(header); got != 9 {
		t.Fatalf("last page = %d", got)
	}
	if got := lastPage(`<https://api.github.test/x?page=3>; rel="next"`); got != 0 {
		t.Fatalf("last page without rel=last = %d", got)
	}
	if got := lastPage(`<%zz>; rel="last"`); got != 0 {
		t.Fatalf("last page bad url = %d", got)
	}
	var messages []string
	Reporter(func(message string) { messages = append(messages, message) }).Printf("hello %d", 1)
	if len(messages) != 1 || messages[0] != "hello 1" {
		t.Fatalf("messages = %+v", messages)
	}
	Reporter(nil).Printf("ignored")
}

func TestClientErrorAndHelperBranches(t *testing.T) {
	client := New(Options{})
	if client.baseURL != "https://api.github.com" || client.userAgent != "gitcrawl" {
		t.Fatalf("defaults = %+v", client)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/openclaw/gitcrawl":
			_, _ = w.Write([]byte("{"))
		case "/repos/openclaw/gitcrawl/issues":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 1}, {"number": 2}, {"number": 3}})
		default:
			t.Fatalf("unexpected path: %s", r.URL.String())
		}
	}))
	defer server.Close()
	client = New(Options{BaseURL: server.URL, PageDelay: -1})
	if _, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil); err == nil {
		t.Fatal("bad json should fail")
	}
	rows, err := client.ListRepositoryIssues(context.Background(), "openclaw", "gitcrawl", ListIssuesOptions{State: "closed", Since: "2026-04-30T00:00:00Z", Limit: 2}, nil)
	if err != nil {
		t.Fatalf("limited issues: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("limited rows = %+v", rows)
	}
	if got := pathEscape("owner/name"); got != "owner%2Fname" {
		t.Fatalf("escaped path = %q", got)
	}
	if got := intValue(json.Number("7")); got != 7 {
		t.Fatalf("json int = %d", got)
	}
	if got := intValue("bad"); got != 0 {
		t.Fatalf("bad int = %d", got)
	}
}

func TestRateLimitRetriesOn403WithRemainingZero(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Unix()))
			http.Error(w, "rate limited", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	row, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil)
	if err != nil {
		t.Fatalf("get repo: %v", err)
	}
	if intValue(row["id"]) != 1 {
		t.Fatalf("row = %#v", row)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d want 2", got)
	}
}

func TestGetRateLimitsDecodesCoreAndGraphQLResources(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rate_limit" {
			t.Fatalf("path = %q, want /rate_limit", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"resources": map[string]any{
			"core":    map[string]any{"limit": 5000, "remaining": 101, "reset": resetAt},
			"graphql": map[string]any{"limit": 5000, "remaining": 202, "reset": resetAt},
		}})
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL})
	snapshots, err := client.GetRateLimits(context.Background(), nil)
	if err != nil {
		t.Fatalf("get rate limits: %v", err)
	}
	byResource := make(map[string]RateLimitSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		byResource[snapshot.Resource] = snapshot
	}
	if got := byResource["core"]; got.Limit != 5000 || got.Remaining != 101 || got.ResetAt.Unix() != resetAt {
		t.Fatalf("core rate limit = %+v", got)
	}
	if got := byResource["graphql"]; got.Limit != 5000 || got.Remaining != 202 || got.ResetAt.Unix() != resetAt {
		t.Fatalf("graphql rate limit = %+v", got)
	}
}

func TestRateLimitReserveStopsCoreRequestBeforeCrossing(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).UTC()
	var rateCalls atomic.Int32
	var coreCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			remaining := 12 - int(rateCalls.Add(1))
			writeRateLimits(t, w, resetAt, remaining, 100)
			return
		}
		call := coreCalls.Add(1)
		if call > 1 {
			http.Error(w, "reserve crossed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "10")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
		w.Header().Set("X-RateLimit-Resource", "core")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{{
			Limit: 5000, Remaining: 11, ResetAt: resetAt, Resource: "core",
		}},
	})
	if _, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil); err != nil {
		t.Fatalf("first request: %v", err)
	}
	_, err := client.GetIssue(context.Background(), "openclaw", "gitcrawl", 1, nil)
	var reserveErr *RateLimitReserveError
	if !errors.As(err, &reserveErr) {
		t.Fatalf("second request error = %v, want RateLimitReserveError", err)
	}
	if reserveErr.RateLimit.Resource != "core" || reserveErr.RateLimit.Remaining != 10 || reserveErr.Reserve != 10 {
		t.Fatalf("reserve error = %+v", reserveErr)
	}
	if got := rateCalls.Load(); got != 2 {
		t.Fatalf("rate status requests = %d, want 2", got)
	}
	if got := coreCalls.Load(); got != 1 {
		t.Fatalf("core requests = %d, want 1", got)
	}
}

func TestRateLimitReserveSerializesConcurrentRequestsAtThreshold(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).UTC()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var rateCalls atomic.Int32
	var coreCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			remaining := 12 - int(rateCalls.Add(1))
			writeRateLimits(t, w, resetAt, remaining, 100)
			return
		}
		if coreCalls.Add(1) > 1 {
			http.Error(w, "reserve crossed", http.StatusInternalServerError)
			return
		}
		close(firstStarted)
		<-releaseFirst
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{{
			Limit: 5000, Remaining: 11, ResetAt: resetAt, Resource: "core",
		}},
	})
	errs := make(chan error, 2)
	go func() {
		_, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil)
		errs <- err
	}()
	<-firstStarted
	go func() {
		_, err := client.GetIssue(context.Background(), "openclaw", "gitcrawl", 1, nil)
		errs <- err
	}()
	close(releaseFirst)

	var succeeded int
	var stopped int
	for range 2 {
		err := <-errs
		if err == nil {
			succeeded++
			continue
		}
		var reserveErr *RateLimitReserveError
		if errors.As(err, &reserveErr) {
			stopped++
			continue
		}
		t.Fatalf("request error = %v", err)
	}
	if succeeded != 1 || stopped != 1 {
		t.Fatalf("succeeded = %d, stopped = %d", succeeded, stopped)
	}
	if got := rateCalls.Load(); got != 2 {
		t.Fatalf("rate status requests = %d, want 2", got)
	}
	if got := coreCalls.Load(); got != 1 {
		t.Fatalf("core requests = %d, want 1", got)
	}
}

func TestRateLimitReserveSeparateClientsObserveSharedQuota(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).UTC()
	firstDispatched := make(chan struct{})
	releaseFirst := make(chan struct{})
	var rateCalls atomic.Int32
	var coreCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			rateCalls.Add(1)
			writeRateLimits(t, w, resetAt, 11-int(coreCalls.Load()), 100)
			return
		}
		if coreCalls.Add(1) > 1 {
			http.Error(w, "reserve crossed", http.StatusInternalServerError)
			return
		}
		close(firstDispatched)
		<-releaseFirst
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	firstClient := New(Options{BaseURL: server.URL, RateLimitReserve: 10})
	secondClient := New(Options{BaseURL: server.URL, RateLimitReserve: 10})
	firstErr := make(chan error, 1)
	go func() {
		_, err := firstClient.GetRepo(context.Background(), "openclaw", "gitcrawl", nil)
		firstErr <- err
	}()
	<-firstDispatched

	_, err := secondClient.GetIssue(context.Background(), "openclaw", "gitcrawl", 1, nil)
	var reserveErr *RateLimitReserveError
	if !errors.As(err, &reserveErr) {
		close(releaseFirst)
		<-firstErr
		t.Fatalf("second client error = %v, want RateLimitReserveError", err)
	}
	close(releaseFirst)
	if err := <-firstErr; err != nil {
		t.Fatalf("first client request: %v", err)
	}
	if reserveErr.RateLimit.Remaining != 10 {
		t.Fatalf("reserve error = %+v", reserveErr)
	}
	if got := rateCalls.Load(); got != 2 {
		t.Fatalf("rate status requests = %d, want 2", got)
	}
	if got := coreCalls.Load(); got != 1 {
		t.Fatalf("core requests = %d, want 1", got)
	}
}

func TestRateLimitReserveSerializesConcurrentRateStatus(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).UTC()
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	var coreCalls atomic.Int32
	var rateCalls atomic.Int32
	var observedMu sync.Mutex
	var observed []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			call := rateCalls.Add(1)
			if call == 1 {
				close(probeStarted)
				<-releaseProbe
			}
			remaining := 11
			if call >= 3 {
				remaining = 10
			}
			writeRateLimits(t, w, resetAt, remaining, 100)
			return
		}
		if coreCalls.Add(1) > 1 {
			http.Error(w, "reserve crossed", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{{
			Limit: 5000, Remaining: 11, ResetAt: resetAt, Resource: "core",
		}},
		RateLimit: func(snapshot RateLimitSnapshot) {
			observedMu.Lock()
			defer observedMu.Unlock()
			observed = append(observed, snapshot.Remaining)
		},
	})
	probeErr := make(chan error, 1)
	go func() {
		_, err := client.GetRateLimits(context.Background(), nil)
		probeErr <- err
	}()
	<-probeStarted
	if client.reserve.requestMu.TryLock() {
		client.reserve.requestMu.Unlock()
		close(releaseProbe)
		<-probeErr
		t.Fatal("rate status request did not hold the guarded request lock")
	}
	requestErr := make(chan error, 1)
	go func() {
		_, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil)
		requestErr <- err
	}()
	close(releaseProbe)
	if err := <-probeErr; err != nil {
		t.Fatalf("rate status request: %v", err)
	}
	if err := <-requestErr; err != nil {
		t.Fatalf("guarded request: %v", err)
	}
	observedMu.Lock()
	gotObserved := append([]int(nil), observed...)
	observedMu.Unlock()
	if len(gotObserved) != 3 || gotObserved[0] != 11 || gotObserved[1] != 11 || gotObserved[2] != 10 {
		t.Fatalf("observed remaining = %v, want [11 11 10]", gotObserved)
	}

	_, err := client.GetIssue(context.Background(), "openclaw", "gitcrawl", 1, nil)
	var reserveErr *RateLimitReserveError
	if !errors.As(err, &reserveErr) {
		t.Fatalf("request after delayed status error = %v, want RateLimitReserveError", err)
	}
	if got := coreCalls.Load(); got != 1 {
		t.Fatalf("core requests = %d, want 1", got)
	}
}

func TestRateLimitReserveStopsBeforeFollowingRedirect(t *testing.T) {
	var calls atomic.Int32
	resetAt := time.Now().Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			writeRateLimits(t, w, resetAt, 11, 100)
			return
		}
		calls.Add(1)
		if r.URL.Path == "/repos/openclaw/gitcrawl" {
			http.Redirect(w, r, "/repos/openclaw/redirected", http.StatusFound)
			return
		}
		http.Error(w, "reserve crossed", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{{
			Limit: 5000, Remaining: 11, ResetAt: time.Now().Add(time.Hour), Resource: "core",
		}},
	})
	_, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || requestErr.Status != http.StatusFound {
		t.Fatalf("redirect error = %v, want status %d RequestError", err, http.StatusFound)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestRateLimitReserveBootstrapStopsBeforeFollowingRedirect(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/rate_limit" {
			http.Redirect(w, r, "/costful", http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, "reserve crossed", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, RateLimitReserve: 10})
	_, err := client.GetRateLimits(context.Background(), nil)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || requestErr.Status != http.StatusTemporaryRedirect {
		t.Fatalf("bootstrap redirect error = %v, want status %d RequestError", err, http.StatusTemporaryRedirect)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestRateLimitReserveChargesNonGETRateLimitRequest(t *testing.T) {
	var calls atomic.Int32
	resetAt := time.Now().Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/rate_limit" {
			writeRateLimits(t, w, resetAt, 10, 100)
			return
		}
		calls.Add(1)
		http.Error(w, "reserve crossed", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{{
			Limit: 5000, Remaining: 10, ResetAt: time.Now().Add(time.Hour), Resource: "core",
		}},
	})
	var out map[string]any
	err := client.doJSON(context.Background(), http.MethodPost, "/rate_limit", strings.NewReader(`{}`), nil, &out)
	var reserveErr *RateLimitReserveError
	if !errors.As(err, &reserveErr) {
		t.Fatalf("POST rate_limit error = %v, want RateLimitReserveError", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}

func TestRateLimitObserverUsesFinalRedirectResource(t *testing.T) {
	var snapshot RateLimitSnapshot
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/graphql" {
			http.Redirect(w, r, "/repos/openclaw/gitcrawl", http.StatusFound)
			return
		}
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "99")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	client := New(Options{
		BaseURL: server.URL,
		RateLimit: func(value RateLimitSnapshot) {
			snapshot = value
		},
	})
	var out map[string]any
	if err := client.doJSON(context.Background(), http.MethodPost, client.graphQLURL, strings.NewReader(`{}`), nil, &out); err != nil {
		t.Fatalf("redirected request: %v", err)
	}
	if snapshot.Resource != "core" || snapshot.Remaining != 99 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestRateLimitReserveCountsRepositoryNamedRateLimit(t *testing.T) {
	var calls atomic.Int32
	resetAt := time.Now().Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			writeRateLimits(t, w, resetAt, 10, 100)
			return
		}
		calls.Add(1)
		http.Error(w, "reserve crossed", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{{
			Limit: 5000, Remaining: 10, ResetAt: time.Now().Add(time.Hour), Resource: "core",
		}},
	})
	_, err := client.GetRepo(context.Background(), "openclaw", "rate_limit", nil)
	var reserveErr *RateLimitReserveError
	if !errors.As(err, &reserveErr) {
		t.Fatalf("repository request error = %v, want RateLimitReserveError", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}

func TestRateLimitReserveRefreshesSharedTokenBeforeEveryRequest(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).UTC()
	var rateStatusCalls atomic.Int32
	var coreCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			remaining := 12 - int(rateStatusCalls.Add(1))
			writeRateLimits(t, w, resetAt, remaining, 100)
			return
		}
		call := coreCalls.Add(1)
		if call > 1 {
			http.Error(w, "reserve crossed", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{{
			Limit: 5000, Remaining: 100, ResetAt: time.Now().Add(-time.Minute), Resource: "core",
		}},
	})
	if _, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil); err != nil {
		t.Fatalf("first request after refresh: %v", err)
	}
	_, err := client.GetIssue(context.Background(), "openclaw", "gitcrawl", 1, nil)
	var reserveErr *RateLimitReserveError
	if !errors.As(err, &reserveErr) {
		t.Fatalf("second request error = %v, want RateLimitReserveError", err)
	}
	if reserveErr.RateLimit.Remaining != 10 {
		t.Fatalf("reserve error = %+v", reserveErr)
	}
	if got := rateStatusCalls.Load(); got != 2 {
		t.Fatalf("rate status requests = %d, want 2", got)
	}
	if got := coreCalls.Load(); got != 1 {
		t.Fatalf("core requests = %d, want 1", got)
	}
}

func TestRateLimitReserveTracksGraphQLSeparately(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).UTC()
	var calls atomic.Int32
	var rateCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			graphqlRemaining := 12 - int(rateCalls.Add(1))
			writeRateLimits(t, w, resetAt, 100, graphqlRemaining)
			return
		}
		call := calls.Add(1)
		if call > 1 {
			http.Error(w, "reserve crossed", http.StatusInternalServerError)
			return
		}
		if r.URL.Path != "/graphql" {
			t.Fatalf("path = %q, want /graphql", r.URL.Path)
		}
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "10")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
		w.Header().Set("X-RateLimit-Resource", "graphql")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"repository": map[string]any{"pullRequest": map[string]any{"reviewThreads": map[string]any{
				"nodes": []map[string]any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
			}}},
		}})
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:          server.URL,
		RateLimitReserve: 10,
		InitialRateLimits: []RateLimitSnapshot{
			{Limit: 5000, Remaining: 100, ResetAt: resetAt, Resource: "core"},
			{Limit: 5000, Remaining: 11, ResetAt: resetAt, Resource: "graphql"},
		},
	})
	if _, err := client.ListPullReviewThreads(context.Background(), "openclaw", "gitcrawl", 1, nil); err != nil {
		t.Fatalf("first GraphQL request: %v", err)
	}
	_, err := client.ListPullReviewThreads(context.Background(), "openclaw", "gitcrawl", 1, nil)
	var reserveErr *RateLimitReserveError
	if !errors.As(err, &reserveErr) {
		t.Fatalf("second GraphQL request error = %v, want RateLimitReserveError", err)
	}
	if reserveErr.RateLimit.Resource != "graphql" || reserveErr.RateLimit.Remaining != 10 {
		t.Fatalf("reserve error = %+v", reserveErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("GraphQL requests = %d, want 1", got)
	}
}

func TestRateLimitObserverIncludesAPIHost(t *testing.T) {
	var snapshot RateLimitSnapshot
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "4999")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer server.Close()

	client := New(Options{
		BaseURL:   server.URL,
		PageDelay: -1,
		RateLimit: func(value RateLimitSnapshot) {
			snapshot = value
		},
	})
	if _, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil); err != nil {
		t.Fatalf("get repo: %v", err)
	}
	if snapshot.Host != strings.TrimPrefix(server.URL, "http://") || snapshot.Remaining != 4999 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestRateLimitRetriesOn429WithRetryAfter(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 2})
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	row, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil)
	if err != nil {
		t.Fatalf("get repo: %v", err)
	}
	if intValue(row["id"]) != 2 {
		t.Fatalf("row = %#v", row)
	}
}

func TestRateLimitRespectsContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()))
		http.Error(w, "rate limited", http.StatusForbidden)
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := client.GetRepo(ctx, "openclaw", "gitcrawl", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestNonRateLimit403IsNotRetried(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer server.Close()

	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	if _, err := client.GetRepo(context.Background(), "openclaw", "gitcrawl", nil); err == nil {
		t.Fatal("expected error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d want 1", got)
	}
}

func serverURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + r.URL.Path
}
