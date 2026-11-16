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

func conversationConnection(nodes ...any) map[string]any {
	if nodes == nil {
		nodes = []any{}
	}
	return map[string]any{"nodes": nodes, "totalCount": len(nodes), "pageInfo": map[string]any{"hasNextPage": false}}
}
func conversationFixture() map[string]any {
	return map[string]any{"__typename": "PullRequest", "id": "PR_fixture", "number": 7, "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "comments": conversationConnection(map[string]any{"id": "IC_fixture", "fullDatabaseId": json.Number("9007199254740993"), "body": strings.Repeat("full λ body ", 1000)}), "reviews": conversationConnection(), "reviewThreads": conversationConnection()}
}
func conversationTestClient(t *testing.T, node map[string]any, route, identity string, graphqlErrors []any) (*OctopoolConversations, *int) {
	t.Helper()
	calls := new(int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		var request struct {
			Method  string `json:"method"`
			Path    string `json:"path"`
			GraphQL struct {
				Query string `json:"query"`
			} `json:"graphql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Method != "POST" || request.Path != "/graphql" {
			t.Errorf("unexpected request %s %s", request.Method, request.Path)
		}
		if strings.Contains(request.GraphQL.Query, "rateLimit") || len(request.GraphQL.Query) > 16<<10 {
			t.Error("unsupported query")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "text", "body": conversationResponseText(t, node, graphqlErrors), "relay": map[string]any{"route_kind": route, "cache": "miss"}, "identity": map[string]any{"kind": identity}})
	}))
	t.Cleanup(server.Close)
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	client.issues[7] = map[string]any{"id": json.Number("12345678912345678"), "node_id": "PR_fixture", "number": 7, "pull_request": map[string]any{}}
	client.metadata = map[string]any{"id": 1}
	return client, calls
}
func TestOctopoolConversationPreservesIdentityAndFullText(t *testing.T) {
	node := conversationFixture()
	client, calls := conversationTestClient(t, node, "graphql_read", "github_app", nil)
	if err := client.Prepare(context.Background(), []int{7}, nil); err != nil {
		t.Fatal(err)
	}
	row, err := client.GetIssue(context.Background(), "fixture", "repo", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(row["id"]) != "12345678912345678" {
		t.Fatalf("REST identity replaced: %v", row)
	}
	comments, err := client.ListIssueComments(context.Background(), "fixture", "repo", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(comments) != 1 || fmt.Sprint(comments[0]["id"]) != "9007199254740993" || comments[0]["body"] != historyNodes(node, "comments")[0]["body"] || comments[0]["_graphql"] == nil {
		t.Fatal("lost full comment content or identity")
	}
	if *calls != 1 {
		t.Fatalf("wanted one batch request, got %d", *calls)
	}
}
func TestOctopoolConversationRejectsUnqualifiedResponses(t *testing.T) {
	for _, test := range []struct {
		name, route, identity string
		mutate                func(map[string]any)
		graphqlErrors         []any
	}{
		{name: "route", route: "rest_read", identity: "github_app"},
		{name: "identity", route: "graphql_read", identity: "oauth_user"},
		{name: "errors", route: "graphql_read", identity: "github_app", graphqlErrors: []any{map[string]any{"message": "failure"}}},
		{name: "missing", route: "graphql_read", identity: "github_app", mutate: func(m map[string]any) { delete(m, "id") }},
		{name: "wrong kind", route: "graphql_read", identity: "github_app", mutate: func(m map[string]any) { m["__typename"] = "Issue" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := conversationFixture()
			if test.mutate != nil {
				test.mutate(node)
			}
			client, _ := conversationTestClient(t, node, test.route, test.identity, test.graphqlErrors)
			if err := client.Prepare(context.Background(), []int{7}, nil); err == nil {
				t.Fatal("accepted incomplete/unqualified response")
			}
			if len(client.conversations) != 0 {
				t.Fatal("published incomplete batch")
			}
		})
	}
}
func TestOctopoolConversationFallsBackToPaginatedPoolREST(t *testing.T) {
	node := conversationFixture()
	historyMap(historyMap(node["comments"])["pageInfo"])["hasNextPage"] = true
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path  string              `json:"path"`
			Query map[string][]string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		paths = append(paths, request.Path)
		var body any
		headers := map[string]string{}
		if request.Path == "/graphql" {
			body = map[string]any{"data": map[string]any{"repository": map[string]any{"n0": node}}}
		} else if strings.HasSuffix(request.Path, "/issues/7/comments") {
			if len(request.Query["page"]) == 0 {
				body = []any{map[string]any{"id": 1, "body": "page one"}}
				headers["link"] = `<https://api.github.com/repos/fixture/repo/issues/7/comments?page=2>; rel="next"`
			} else {
				body = []any{map[string]any{"id": 2, "body": ""}}
			}
		} else {
			body = []any{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "headers": headers, "body": body, "relay": map[string]any{"route_kind": "graphql_read", "cache": "miss"}, "identity": map[string]any{"kind": "github_app"}})
	}))
	defer server.Close()
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	client.issues[7] = map[string]any{"node_id": "PR_fixture", "pull_request": map[string]any{}}
	if err := client.Prepare(context.Background(), []int{7}, nil); err != nil {
		t.Fatal(err)
	}
	if len(client.conversations[7].Comments) != 2 || len(paths) != 5 {
		t.Fatalf("incomplete fallback: %v %v", client.conversations[7], paths)
	}
}
func TestProjectConversationRetainsEmptyAndRejectsConflictingDuplicates(t *testing.T) {
	comment := map[string]any{"id": "RC_1", "fullDatabaseId": json.Number("456"), "body": "", "state": "SUBMITTED"}
	review := map[string]any{"id": "R_1", "fullDatabaseId": json.Number("789"), "body": "", "state": "APPROVED", "comments": conversationConnection(comment)}
	node := conversationFixture()
	node["reviews"] = conversationConnection(review)
	node["reviewThreads"] = conversationConnection(map[string]any{"comments": conversationConnection(comment)})
	item, err := projectConversation(node)
	if err != nil {
		t.Fatal(err)
	}
	if len(item.ReviewComments) != 1 || item.ReviewComments[0]["body"] != "" {
		t.Fatal("lost empty or duplicated inline comment")
	}
	historyMap(historyNodes(node, "reviewThreads")[0]["comments"])["nodes"] = []any{map[string]any{"id": "RC_1", "fullDatabaseId": json.Number("456"), "body": "conflicting"}}
	if _, err := projectConversation(node); err == nil {
		t.Fatal("accepted conflicting duplicates")
	}
}

func conversationResponseText(t *testing.T, node map[string]any, errors []any) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"data": map[string]any{"repository": map[string]any{"n0": node}}, "errors": errors})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
func TestConversationConnectionRejectsDuplicateOrMissingNodes(t *testing.T) {
	for _, nodes := range [][]any{{map[string]any{"id": "same"}, map[string]any{"id": "same"}}, {map[string]any{}}} {
		if completeConversationConnection(map[string]any{"comments": conversationConnection(nodes...)}, "comments") {
			t.Fatal("accepted duplicate or missing identity")
		}
	}
}
