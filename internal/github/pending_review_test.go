package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GitHub returns the authenticated user's own unsubmitted review and its draft
// comments to that user only. They are not part of the public conversation.
func TestListPullReviewsOmitsViewerPendingReview(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `[{"id":1,"state":"APPROVED","body":"ship it"},{"id":2,"state":"PENDING","body":"private draft"}]`)
	}))
	defer server.Close()
	rows, err := New(Options{BaseURL: server.URL}).ListPullReviews(context.Background(), "o", "r", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["state"] != "APPROVED" {
		t.Fatalf("reviews = %v, want only the submitted review", rows)
	}
}

func TestListPullReviewThreadsOmitsViewerPendingComments(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var request struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(body, &request)
		queries = append(queries, request.Query)
		fmt.Fprint(w, `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[
			{"id":"T1","comments":{"nodes":[
				{"id":"C1","body":"submitted","state":"SUBMITTED"},
				{"id":"C2","body":"private reply draft","state":"PENDING"}],"pageInfo":{"hasNextPage":false,"endCursor":""}}},
			{"id":"T2","comments":{"nodes":[
				{"id":"C3","body":"private thread draft","state":"PENDING"}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}
		],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`)
	}))
	defer server.Close()
	threads, err := New(Options{BaseURL: server.URL}).ListPullReviewThreads(context.Background(), "o", "r", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(threads)
	if len(threads) != 1 || threads[0]["id"] != "T1" || strings.Contains(string(encoded), "private") || !strings.Contains(string(encoded), "submitted") {
		t.Fatalf("threads = %s, want T1 with only its submitted comment", encoded)
	}
	if len(queries) != 1 || !strings.Contains(queries[0], "state") {
		t.Fatalf("review-thread query does not request comment state: %q", queries)
	}
}
