package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReviewStateOmitsViewerPendingDrafts(t *testing.T) {
	comment := func(id, state string) map[string]any {
		return map[string]any{"id": id, "__typename": "PullRequestReviewComment", "state": state, "body": id, "fullDatabaseId": id}
	}
	thread := func(id string, comments ...map[string]any) map[string]any {
		nodes := make([]any, len(comments))
		for i, comment := range comments {
			nodes[i] = comment
		}
		return map[string]any{"id": id, "__typename": "PullRequestReviewThread", "isResolved": false, "isOutdated": false, "viewerCanResolve": false, "viewerCanUnresolve": false, "viewerCanReply": true, "comments": historyTestConnection(nodes...)}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Query string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		data := map[string]any{"rateLimit": map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}}
		if strings.Contains(req.Query, "issueOrPullRequest") {
			data["repository"] = map[string]any{"id": "R1", "databaseId": 1, "nameWithOwner": "fixture/repo", "n0": map[string]any{"id": "PR7", "__typename": "PullRequest", "number": 7, "updatedAt": "2026-01-02T00:00:00Z", "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "reviewThreads": historyTestConnection(thread("T1", comment("1", "SUBMITTED"), comment("2", "PENDING")), thread("T2", comment("3", "PENDING")))}}
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"data": data}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	items, err := New(Options{BaseURL: server.URL}).FetchGraphQLReviewState(context.Background(), "fixture", "repo", []int{7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(items)
	if len(items) != 1 || len(items[0].Threads) != 1 || strings.Contains(string(encoded), "PENDING") {
		t.Fatalf("review state retains drafts: %s", encoded)
	}
	if n := len(historyNodes(items[0].Threads[0], "comments")); n != 1 {
		t.Fatalf("comment count=%d", n)
	}
}
