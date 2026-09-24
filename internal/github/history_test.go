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
	return map[string]any{"id": "PR_fixture", "__typename": "PullRequest", "fullDatabaseId": "9007199254740993", "number": 1, "title": "history", "body": "body", "state": "MERGED", "author": map[string]any{"login": "helper", "__typename": "Bot"}, "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "labels": historyTestConnection(), "assignees": historyTestConnection(), "comments": historyTestConnection(), "reviews": historyTestConnection(), "reviewThreads": historyTestConnection(), "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "mergedAt": "2026-01-02T00:00:00Z"}
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
			review["comments"].(map[string]any)["totalCount"] = 1
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

func TestGraphQLHistoryRejectsShortConnection(t *testing.T) {
	node := historyTestNode()
	node["comments"].(map[string]any)["totalCount"] = 1
	h := historySession{}
	if err := h.hydrate(context.Background(), node); err == nil || !strings.Contains(err.Error(), "incomplete history comments count") {
		t.Fatalf("short terminal connection accepted: %v", err)
	}
}

func TestGraphQLHistoryIssueDiscussion(t *testing.T) {
	node := historyTestNode()
	node["__typename"] = "Issue"
	delete(node, "reviews")
	delete(node, "reviewThreads")
	node["comments"] = historyTestConnection(map[string]any{"id": "D1", "__typename": "IssueComment", "fullDatabaseId": json.Number("9007199254740993"), "body": "minimized discussion", "isMinimized": true, "author": nil})
	h := historySession{}
	if err := h.hydrate(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	item, err := historyItem(node)
	if err != nil || len(item.Comments) != 1 || item.Pull != nil || len(item.Reviews) != 0 {
		t.Fatalf("issue discussion: %+v, %v", item, err)
	}
	if item.Comments[0]["id"] != json.Number("9007199254740993") || historyMap(item.Comments[0]["_graphql"])["isMinimized"] != true {
		t.Fatal("issue comment identity or minimized state lost")
	}
}

func TestGraphQLHistoryBotIdentities(t *testing.T) {
	for _, login := range []string{"helper", "helper[bot]", "copilot-pull-request-reviewer"} {
		actor := map[string]any{"login": login, "__typename": "Bot"}
		want := strings.TrimSuffix(login, "[bot]") + "[bot]"
		if got := historyActorMap(actor); got["login"] != want || got["type"] != "Bot" || actor["login"] != login {
			t.Fatalf("bot identity: raw=%+v normalized=%+v", actor, got)
		}
	}
}

func TestGraphQLHistoryReviewThreadCompleteness(t *testing.T) {
	for _, mode := range []string{"complete", "repeated-thread", "repeated-comment", "partial-error", "missing-threads", "missing-comments", "short-page", "missing-identity"} {
		t.Run(mode, func(t *testing.T) {
			node := historyTestNode()
			standalone := map[string]any{"id": "C_standalone", "__typename": "PullRequestReviewComment", "fullDatabaseId": "9007199254740994", "body": "minimized standalone", "isMinimized": true, "minimizedReason": "OUTDATED", "author": nil, "pullRequestReview": nil}
			associated := map[string]any{"id": "C_review", "__typename": "PullRequestReviewComment", "fullDatabaseId": "9007199254740995", "body": "inline", "pullRequestReview": map[string]any{"id": "V1", "fullDatabaseId": "9007199254740996"}, "author": map[string]any{"login": "helper[bot]", "__typename": "Bot"}}
			review := map[string]any{"id": "V1", "__typename": "PullRequestReview", "fullDatabaseId": "9007199254740996", "body": "", "state": "APPROVED", "comments": historyTestConnection(associated)}
			discussion := map[string]any{"id": "D1", "__typename": "IssueComment", "fullDatabaseId": "9007199254740997", "body": "discussion", "author": nil}
			node["comments"] = historyTestConnection(discussion)
			node["reviews"] = historyTestConnection(review)
			thread1 := map[string]any{"id": "T1", "__typename": "PullRequestReviewThread", "comments": historyTestConnection(associated)}
			thread2 := map[string]any{"id": "T2", "__typename": "PullRequestReviewThread", "comments": historyTestConnection()}
			thread2["comments"].(map[string]any)["totalCount"] = 1
			thread2["comments"].(map[string]any)["pageInfo"] = map[string]any{"hasNextPage": true, "endCursor": "comment-first"}
			node["reviewThreads"] = historyTestConnection(thread1)
			node["reviewThreads"].(map[string]any)["totalCount"] = 2
			node["reviewThreads"].(map[string]any)["pageInfo"] = map[string]any{"hasNextPage": true, "endCursor": "thread-first"}
			if mode == "missing-threads" {
				delete(node, "reviewThreads")
			}
			if mode == "missing-comments" {
				delete(thread2, "comments")
			}
			if mode == "missing-identity" {
				delete(standalone, "fullDatabaseId")
			}
			pages := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req graphqlEnvelope
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				data := map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": 19000, "resetAt": "2099-01-01T01:00:00Z"}}
				if strings.Contains(req.Query, "issueOrPullRequest") {
					if !strings.Contains(req.Query, "reviewThreads(first:20)") || !strings.Contains(req.Query, "pullRequestReview { id fullDatabaseId }") {
						t.Error("query omits independent review-thread acquisition")
					}
					data["repository"] = map[string]any{"databaseId": 42, "nameWithOwner": "fixture/repo", "n0": node}
				}
				if strings.Contains(req.Query, "node(id:") {
					pages++
					switch req.Variables["id"] {
					case "PR_fixture":
						conn := historyTestConnection(thread2)
						if mode == "repeated-thread" {
							conn["pageInfo"] = map[string]any{"hasNextPage": true, "endCursor": "thread-first"}
						}
						data["node"] = map[string]any{"id": "PR_fixture", "reviewThreads": conn}
					case "T2":
						conn := historyTestConnection(standalone)
						if mode == "repeated-comment" {
							conn["pageInfo"] = map[string]any{"hasNextPage": true, "endCursor": "comment-first"}
						}
						if mode == "short-page" {
							conn["nodes"] = []any{}
						}
						data["node"] = map[string]any{"id": "T2", "comments": conn}
					default:
						t.Errorf("unexpected continuation: %v", req.Variables)
					}
				}
				response := map[string]any{"data": data}
				if mode == "partial-error" && pages > 0 {
					response["errors"] = []any{map[string]any{"message": "partial continuation"}}
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			batch, err := New(Options{BaseURL: server.URL}).FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil)
			if mode != "complete" {
				if err == nil {
					t.Fatalf("%s accepted incomplete history", mode)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			item := batch.Items[0]
			if pages != 2 || len(item.ReviewComments) != 2 || len(item.Reviews) != 1 || len(item.Comments) != 1 {
				t.Fatalf("incomplete or duplicate conversation: pages=%d item=%+v", pages, item)
			}
			if item.Reviews[0]["state"] != "APPROVED" || item.Reviews[0]["body"] != "" || item.Comments[0]["body"] != "discussion" {
				t.Fatal("review metadata or discussion lost")
			}
			if item.ReviewComments[0]["pull_request_review_id"] != "9007199254740996" || historyMap(item.ReviewComments[0]["user"])["login"] != "helper[bot]" {
				t.Fatal("review association or bot identity lost")
			}
			orphan := item.ReviewComments[1]
			if orphan["id"] != "9007199254740994" || orphan["pull_request_review_id"] != nil || historyMap(orphan["user"]) != nil || historyMap(orphan["_graphql"])["isMinimized"] != true {
				t.Fatalf("standalone/minimized/deleted-author comment lost: %+v", orphan)
			}
		})
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
