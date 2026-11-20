package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRESTPageOctopoolResumeAndFullRows(t *testing.T) {
	calls := 0
	full := strings.Repeat("full body λ\x00", 1000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req struct {
			Pool, Method, Path string
			Query              map[string][]string
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if r.Header.Get("Authorization") != "Bearer fixture" || req.Pool != "fixture" || req.Method != "GET" {
			t.Error("relay contract changed")
		}
		if req.Path == "/repos/example/project" {
			json.NewEncoder(w).Encode(map[string]any{"status": 200, "body": map[string]any{"id": 42}, "body_encoding": "json"})
			return
		}
		if req.Path != "/repos/example/project/pulls/7/reviews" || req.Query["per_page"][0] != "2" {
			t.Errorf("unexpected request: %+v", req)
		}
		headers := map[string]string{}
		rows := []map[string]any{{"id": json.Number("9007199254740993"), "body": full, "state": "APPROVED", "submitted_at": "2026-01-02T03:04:05Z"}, {"id": 2, "body": "private draft", "state": "PENDING"}}
		if len(req.Query["page"]) == 0 {
			headers["Link"] = `<https://api.github.com/repositories/42/pulls/7/reviews?per_page=2&page=2>; rel="next"`
		} else {
			rows = []map[string]any{}
		}
		json.NewEncoder(w).Encode(map[string]any{"status": 200, "body": rows, "body_encoding": "json", "headers": headers})
	}))
	defer server.Close()
	client, err := NewOctopool(server.URL, "fixture", "fixture", "example/project")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.GetRepo(context.Background(), "example", "project", nil); err != nil {
		t.Fatal(err)
	}
	request := RESTPageRequest{Owner: "example", Repo: "project", Family: "pull_review", Number: 7, PerPage: 2}
	page, err := client.ReadRESTPage(context.Background(), request, nil)
	if err != nil || len(page.Rows) != 1 || fmt.Sprint(page.Rows[0]["id"]) != "9007199254740993" || page.Rows[0]["body"] != full || page.NextCursor == "" {
		t.Fatalf("first page: %+v %v", page, err)
	}
	// A restarted client rebinds numeric provider paths with GetRepo, then
	// resumes the persisted cursor without a local inventory store.
	client, err = NewOctopool(server.URL, "fixture", "fixture", "example/project")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.GetRepo(context.Background(), "example", "project", nil); err != nil {
		t.Fatal(err)
	}
	request.Cursor = page.NextCursor
	page, err = client.ReadRESTPage(context.Background(), request, nil)
	if err != nil || len(page.Rows) != 0 || page.NextCursor != "" || calls != 4 {
		t.Fatalf("terminal page: %+v %v calls=%d", page, err, calls)
	}
}

func TestRESTPageRejectsChangedCursorBeforeRequest(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; t.Error("invalid cursor dispatched") }))
	defer server.Close()
	client := New(Options{BaseURL: server.URL})
	for _, cursor := range []string{
		"https://other.invalid/repos/example/project/issues?per_page=1&state=all&sort=updated&direction=desc&page=2",
		"/repos/example/other/issues?per_page=1&state=all&sort=updated&direction=desc&page=2",
		"/repos/example/project/issues?per_page=1&state=closed&sort=updated&direction=desc&page=2",
		"/repos/example/project/issues?per_page=1&state=all&sort=updated&direction=desc&page=2&page=3",
		"/repos/example/project/issues?per_page=1&state=all&sort=updated&direction=desc&page=2&labels=hidden",
	} {
		_, err := client.ReadRESTPage(context.Background(), RESTPageRequest{Owner: "example", Repo: "project", Family: "issues", PerPage: 1, Cursor: cursor}, nil)
		if err == nil {
			t.Errorf("accepted cursor %s", cursor)
		}
	}
	if calls != 0 {
		t.Fatal(calls)
	}
}

func TestRESTPageDoesNotRestartAfterExternalProgress(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(424)
		w.Write([]byte(oversizedRejection))
	}))
	defer server.Close()
	client, _ := NewOctopool(server.URL, "fixture", "fixture", "example/project")
	_, err := client.ReadRESTPage(context.Background(), RESTPageRequest{Owner: "example", Repo: "project", Family: "issue_comment", Number: 7, PerPage: 100, Cursor: "/repos/example/project/issues/7/comments?per_page=100&page=2"}, nil)
	var oversized *octopoolResponseTooLargeError
	if !errors.As(err, &oversized) || calls != 1 {
		t.Fatalf("unexpected resize/retry: %v calls=%d", err, calls)
	}
}

func TestRESTPageDecodeBounds(t *testing.T) {
	for _, body := range []string{"null", `[{"id":1}] {}`, strings.Repeat(" ", MaxRESTPageBytes+1), `[` + strings.TrimSuffix(strings.Repeat(`{},`, 101), ",") + `]`} {
		if _, err := decodeRESTPage(strings.NewReader(body)); err == nil {
			t.Fatal("invalid page accepted")
		}
	}
}
