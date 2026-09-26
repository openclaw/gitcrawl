package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReviewStateOnlyStrictPaginationAndReplyFields(t *testing.T) {
	for _, mode := range []string{"complete", "null", "short", "cursor", "wrong_parent", "missing_state"} {
		t.Run(mode, func(t *testing.T) {
			conn := func(total int, more bool, cursor string, nodes ...any) map[string]any {
				if nodes == nil {
					nodes = []any{}
				}
				return map[string]any{"totalCount": total, "nodes": nodes, "pageInfo": map[string]any{"hasNextPage": more, "endCursor": cursor}}
			}
			comment := func(id string, reply any) map[string]any {
				return map[string]any{"id": id, "__typename": "PullRequestReviewComment", "fullDatabaseId": id, "body": "inline retained", "author": map[string]any{"id": "actor", "login": "fixture", "__typename": "User"}, "url": "https://github.com/fixture/repo/pull/7#comment", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "replyTo": reply}
			}
			thread := func(id string, comments any) map[string]any {
				return map[string]any{"id": id, "__typename": "PullRequestReviewThread", "isResolved": true, "isOutdated": false, "viewerCanResolve": false, "viewerCanUnresolve": true, "viewerCanReply": true, "path": "file.go", "line": 9, "comments": comments}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Query     string
					Variables map[string]any
				}
				json.NewDecoder(r.Body).Decode(&req)
				data := map[string]any{"rateLimit": map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}}
				if strings.Contains(req.Query, "issueOrPullRequest") {
					if strings.Contains(req.Query, "title") || strings.Contains(req.Query, "labels") || strings.Contains(req.Query, "reviews(") {
						t.Error("unrelated history requested")
					}
					first := thread("RT1", conn(2, true, "c1", comment("C1", nil)))
					if mode == "missing_state" {
						delete(first, "isResolved")
					}
					node := map[string]any{"__typename": "PullRequest", "id": "PR7", "number": 7, "updatedAt": "2026-01-02T00:00:00Z", "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "reviewThreads": conn(2, true, "rt1", first)}
					if mode == "short" {
						node["reviewThreads"] = conn(2, false, "end", first)
					}
					var selected any = node
					if mode == "null" {
						selected = nil
					}
					data["repository"] = map[string]any{"id": "R1", "databaseId": 1, "nameWithOwner": "fixture/repo", "n0": selected}
				} else if req.Variables["id"] == "PR7" {
					parent := "PR7"
					if mode == "wrong_parent" {
						parent = "OTHER"
					}
					data["node"] = map[string]any{"id": parent, "reviewThreads": conn(2, mode == "cursor", "rt1", thread("RT2", conn(0, false, "")))}
				} else if req.Variables["id"] == "RT1" {
					data["node"] = map[string]any{"id": "RT1", "comments": conn(2, false, "c2", comment("C2", map[string]any{"id": "C1", "fullDatabaseId": "C1"}))}
				}
				json.NewEncoder(w).Encode(map[string]any{"data": data})
			}))
			defer server.Close()
			items, err := New(Options{BaseURL: server.URL}).FetchGraphQLReviewState(context.Background(), "fixture", "repo", []int{7}, nil)
			if mode != "complete" {
				if err == nil {
					t.Fatal("incomplete evidence accepted", mode)
				}
				return
			}
			if err != nil || len(items) != 1 || len(items[0].Threads) != 2 {
				t.Fatalf("%+v %v", items, err)
			}
			comments := historyNodes(items[0].Threads[0], "comments")
			if len(comments) != 2 || historyMap(comments[1]["replyTo"])["id"] != "C1" || comments[0]["body"] != "inline retained" || historyMap(comments[0]["author"])["login"] != "fixture" {
				t.Fatal("reply/body/author evidence lost")
			}
		})
	}
}
func TestReviewStateResponseByteLimitRejectsWithoutPartialResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"padding":"` + strings.Repeat("x", 8192) + `"}}`))
	}))
	defer server.Close()
	var out map[string]any
	err := New(Options{BaseURL: server.URL, GraphQLResponseLimit: 1024}).doGraphQL(context.Background(), "query { rateLimit {cost} }", nil, nil, &out)
	class, _, _ := HistoryFailureDetails(err)
	if err == nil || class != "response_size" || out != nil {
		t.Fatalf("oversize response accepted: %v %s", err, class)
	}
}

func TestReviewStateGraphQLGuardRequiresObservedCredentialQuota(t *testing.T) {
	for _, mode := range []string{"normal", "low_probe", "low_page", "rotation", "expired", "unprobed"} {
		t.Run(mode, func(t *testing.T) {
			calls, rest, tokenCalls := 0, 0, 0
			reset := time.Now().UTC().Add(time.Hour)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/graphql" {
					rest++
					t.Error("redundant REST quota request")
					http.Error(w, "unexpected", 500)
					return
				}
				calls++
				remaining := 19000
				if mode == "low_probe" || mode == "low_page" && calls == 2 {
					remaining = 3000
				}
				if mode == "expired" {
					reset = time.Now().Add(-time.Second)
				}
				json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"rateLimit": map[string]any{"cost": 1, "limit": 20000, "remaining": remaining, "resetAt": reset.Format(time.RFC3339)}}})
			}))
			defer server.Close()
			c := New(Options{BaseURL: server.URL, GraphQLQuotaGuard: true, RateLimitReserve: 3000, TokenProvider: func(context.Context) (string, error) {
				tokenCalls++
				if mode == "rotation" && tokenCalls > 1 {
					return "changed-fixture", nil
				}
				return "fixture", nil
			}})
			h := historySession{client: c, remaining: 20000}
			if mode != "unprobed" {
				if _, err := h.quota(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			_, err := h.request(context.Background(), `query { node(id:"fixture"){id} rateLimit{cost remaining limit resetAt}}`, nil, 16)
			if mode == "normal" || mode == "low_page" {
				if err != nil {
					t.Fatal(err)
				}
				_, err = h.request(context.Background(), `query { node(id:"page"){id} rateLimit{cost remaining limit resetAt}}`, nil, 16)
			}
			if mode == "normal" {
				if err != nil || calls != 3 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			} else if err == nil {
				t.Fatal("unguarded content accepted")
			}
			expected := 1
			if mode == "normal" {
				expected = 3
			}
			if mode == "low_page" {
				expected = 2
			}
			if mode == "unprobed" {
				expected = 0
			}
			if calls != expected || rest != 0 {
				t.Fatalf("calls=%d rest=%d", calls, rest)
			}
			if mode == "low_probe" || mode == "low_page" {
				var reserve *RateLimitReserveError
				if !errors.As(err, &reserve) {
					t.Fatalf("not reserve error: %v", err)
				}
			}
		})
	}
}
