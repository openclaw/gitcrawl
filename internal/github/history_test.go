package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
