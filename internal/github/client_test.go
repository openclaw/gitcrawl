package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
			"/repos/openclaw/gitcrawl/pulls/8/files":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1}})
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
		"files": func() ([]map[string]any, error) { return client.ListPullFiles(ctx, "openclaw", "gitcrawl", 8, nil) },
	} {
		rows, err := fn()
		if err != nil || len(rows) != 1 {
			t.Fatalf("%s rows = %+v err=%v", name, rows, err)
		}
	}
	if len(requests) != 7 {
		t.Fatalf("requests = %+v", requests)
	}
}

func TestNextPageAndReporterBranches(t *testing.T) {
	header := `<https://api.github.test/repos/o/r/issues?page=2&state=open>; rel="next", <https://api.github.test/repos/o/r/issues?page=9>; rel="last"`
	if got := nextPage(header); got != "/repos/o/r/issues?page=2&state=open" {
		t.Fatalf("next page = %q", got)
	}
	if got := nextPage(`<bad-url>; rel="last"`); got != "" {
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

func serverURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + r.URL.Path
}
