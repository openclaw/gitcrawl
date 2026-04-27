package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListRepositoryIssuesPaginatesAndLimits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("missing auth header: %q", r.Header.Get("Authorization"))
		}
		switch r.URL.Query().Get("page") {
		case "":
			w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next"`)
			_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 1}, {"number": 2}})
		case "2":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 3}})
		default:
			t.Fatalf("unexpected page: %s", r.URL.RawQuery)
		}
	}))
	defer server.Close()

	client := New(Options{Token: "token", BaseURL: server.URL, PageDelay: -1})
	rows, err := client.ListRepositoryIssues(context.Background(), "openclaw", "gitcrawl", ListIssuesOptions{Limit: 3}, nil)
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows: got %d want 3", len(rows))
	}
	if intValue(rows[2]["number"]) != 3 {
		t.Fatalf("last number: %#v", rows[2]["number"])
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
}

func serverURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + r.URL.Path
}
