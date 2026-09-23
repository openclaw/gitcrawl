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
	"time"
)

func historyTestConnection(nodes ...any) map[string]any {
	if nodes == nil {
		nodes = []any{}
	}
	return map[string]any{"totalCount": len(nodes), "nodes": nodes, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": "end"}}
}
func historyTestNode() map[string]any {
	return map[string]any{"id": "PR_fixture", "__typename": "PullRequest", "fullDatabaseId": "9007199254740993", "number": 1, "title": "history", "body": "body", "state": "MERGED", "author": map[string]any{"login": "helper", "__typename": "Bot"}, "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "labels": historyTestConnection(), "assignees": historyTestConnection(), "comments": historyTestConnection(), "reviews": historyTestConnection(), "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "mergedAt": "2026-01-02T00:00:00Z"}
}
func TestGraphQLHistoryBatchNoRESTAndExactIDs(t *testing.T) {
	node := historyTestNode()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/graphql" || r.Method != "POST" {
			t.Errorf("unexpected REST request %s", r.URL)
			http.Error(w, "no REST", 400)
			return
		}
		var request graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		data := map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": 19999 - calls, "resetAt": "2099-01-01T01:00:00Z"}}
		if strings.Contains(request.Query, "issueOrPullRequest") {
			data["repository"] = map[string]any{"id": "R_fixture", "databaseId": 42, "nameWithOwner": "fixture/repo", "n0": node}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer server.Close()
	var log []string
	batch, err := New(Options{BaseURL: server.URL}).FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, func(s string) { log = append(log, s) })
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(batch.Items) != 1 {
		t.Fatalf("calls=%d batch=%+v", calls, batch)
	}
	item := batch.Items[0]
	if item.Thread["id"] != "PR_fixture" || item.Pull["id"] != "9007199254740993" || item.Thread["state"] != "closed" {
		t.Fatalf("identity/state %+v", item)
	}
	if historyMap(item.Thread["user"])["login"] != "helper[bot]" {
		t.Fatal("bot normalization")
	}
	if historyMap(item.Thread["_graphql"])["fullDatabaseId"] != "9007199254740993" {
		t.Fatal("raw payload lost")
	}
	if !strings.Contains(strings.Join(log, "\n"), "[github] graphql timing 2 ") {
		t.Fatal("missing request duration")
	}
	if !strings.Contains(strings.Join(log, "\n"), "[github] graphql cost 2 1") {
		t.Fatal("missing actual quota cost")
	}
}
func TestGraphQLHistoryNestedPaginationAndRejection(t *testing.T) {
	for _, mode := range []string{"complete", "repeated", "partial-error", "missing-child"} {
		t.Run(mode, func(t *testing.T) {
			node := historyTestNode()
			comment := map[string]any{"id": "C1", "__typename": "PullRequestReviewComment", "fullDatabaseId": "9007199254740994", "body": "inline"}
			review := map[string]any{"id": "V1", "__typename": "PullRequestReview", "fullDatabaseId": "9007199254740995", "body": "", "state": "APPROVED", "comments": historyTestConnection()}
			review["comments"].(map[string]any)["pageInfo"] = map[string]any{"hasNextPage": true, "endCursor": "first"}
			node["reviews"] = historyTestConnection(review)
			pages := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req graphqlEnvelope
				json.NewDecoder(r.Body).Decode(&req)
				data := map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": 19000, "resetAt": "2099-01-01T01:00:00Z"}}
				if strings.Contains(req.Query, "issueOrPullRequest") {
					data["repository"] = map[string]any{"databaseId": 42, "nameWithOwner": "fixture/repo", "n0": node}
					if mode == "missing-child" {
						delete(node, "comments")
					}
				}
				if strings.Contains(req.Query, "node(id:") {
					pages++
					conn := historyTestConnection(comment)
					if mode == "repeated" {
						conn["pageInfo"] = map[string]any{"hasNextPage": true, "endCursor": "first"}
					}
					data["node"] = map[string]any{"id": "V1", "comments": conn}
				}
				resp := map[string]any{"data": data}
				if mode == "partial-error" && strings.Contains(req.Query, "issueOrPullRequest") {
					resp["errors"] = []any{map[string]any{"message": "unavailable"}}
				}
				json.NewEncoder(w).Encode(resp)
			}))
			defer server.Close()
			batch, err := New(Options{BaseURL: server.URL}).FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil)
			if mode == "complete" {
				if err != nil {
					t.Fatal(err)
				}
				if pages != 1 || len(batch.Items[0].ReviewComments) != 1 || len(batch.Items[0].Reviews) != 1 {
					t.Fatal("nested pagination incomplete")
				}
			} else if err == nil {
				t.Fatalf("%s did not fail closed: %+v", mode, batch)
			}
		})
	}
}
func TestGraphQLHistoryReserveStopsBeforeBatch(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprint(w, `{"data":{"rateLimit":{"cost":1,"remaining":499,"resetAt":"2099-01-01T01:00:00Z"}}}`)
	}))
	defer server.Close()
	_, err := New(Options{BaseURL: server.URL}).FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil)
	if err == nil || calls != 1 {
		t.Fatalf("reserve failed calls=%d error=%v", calls, err)
	}
}

func TestGraphQLHistoryTransientRetriesKeepQueryAndAccountEveryAttempt(t *testing.T) {
	for _, kind := range []string{"gateway", "truncated", "empty"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			var queries []string
			var waits []time.Duration
			var logs []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var req graphqlEnvelope
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				queries = append(queries, req.Query)
				if calls < 3 {
					switch kind {
					case "gateway":
						w.Header().Set("Retry-After", "7")
						http.Error(w, "temporary gateway failure", 503)
					case "truncated":
						fmt.Fprint(w, `{"data":`)
					case "empty":
						w.WriteHeader(200)
					}
					return
				}
				fmt.Fprint(w, `{"data":{"rateLimit":{"cost":6,"remaining":19999,"resetAt":"2099-01-01T01:00:00Z"},"marker":"complete"}}`)
			}))
			defer server.Close()
			h := historySession{client: New(Options{BaseURL: server.URL}), remaining: 20000, reporter: func(s string) { logs = append(logs, s) }, retrySleep: func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }}
			data, err := h.request(context.Background(), "query { fixture }", nil, 16)
			if err != nil || data["marker"] != "complete" || calls != 3 {
				t.Fatalf("calls=%d data=%v err=%v", calls, data, err)
			}
			if len(waits) != 2 || queries[0] != queries[1] || queries[1] != queries[2] {
				t.Fatal("retry changed query or bounds")
			}
			want := []time.Duration{time.Second, 2 * time.Second}
			if kind == "gateway" {
				want = []time.Duration{7 * time.Second, 7 * time.Second}
			}
			if waits[0] != want[0] || waits[1] != want[1] {
				t.Fatalf("waits %v, want %v", waits, want)
			}
			if h.remaining != 19962 {
				t.Fatalf("failed attempts refunded: %d", h.remaining)
			}
			text := strings.Join(logs, "\n")
			if strings.Count(text, "[github] graphql budget ") != 3 || strings.Count(text, "[github] graphql timing ") != 3 || strings.Count(text, "[github] graphql cost ") != 1 {
				t.Fatalf("invalid accounting: %s", text)
			}
		})
	}
}

func TestGraphQLHistoryRetryBoundAndCancellation(t *testing.T) {
	for _, status := range []int{502, 401, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; http.Error(w, "failure", status) }))
			defer server.Close()
			h := historySession{client: New(Options{BaseURL: server.URL}), remaining: 20000, retrySleep: func(context.Context, time.Duration) error { return nil }}
			_, err := h.request(context.Background(), "query { fixture }", nil, 16)
			want := 1
			if status == 502 {
				want = 3
			}
			if err == nil || calls != want {
				t.Fatalf("calls=%d want=%d err=%v", calls, want, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepHistoryRetry(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled retry: %v", err)
	}
}
