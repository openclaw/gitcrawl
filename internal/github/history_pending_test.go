package github

import (
	"encoding/json"
	"strings"
	"testing"
)

// The GraphQL history profile sees the authenticated user's own unsubmitted
// review and its draft inline comments, which GitHub shows to nobody else.
func TestGraphQLHistoryOmitsViewerPendingReviewDrafts(t *testing.T) {
	node := historyTestNode()
	inline := func(id, state string) map[string]any {
		return map[string]any{"id": "C" + id, "__typename": "PullRequestReviewComment", "fullDatabaseId": id, "body": "inline " + id, "state": state}
	}
	node["reviews"] = historyTestConnection(
		map[string]any{"id": "V1", "__typename": "PullRequestReview", "fullDatabaseId": "11", "body": "submitted review", "state": "COMMENTED", "comments": historyTestConnection(inline("101", "SUBMITTED"))},
		map[string]any{"id": "V2", "__typename": "PullRequestReview", "fullDatabaseId": "12", "body": "private draft review", "state": "PENDING", "comments": historyTestConnection(inline("102", "PENDING"))},
	)
	node["reviewThreads"] = historyTestConnection(
		map[string]any{"id": "T1", "__typename": "PullRequestReviewThread", "comments": historyTestConnection(inline("101", "SUBMITTED"), inline("103", "PENDING"))},
		map[string]any{"id": "T2", "__typename": "PullRequestReviewThread", "comments": historyTestConnection(inline("102", "PENDING"))},
	)
	item, err := historyItem(node)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(item)
	for _, draft := range []string{"private draft review", "inline 102", "inline 103"} {
		if strings.Contains(string(encoded), draft) {
			t.Fatalf("history item kept viewer draft %q: reviews=%d review_comments=%d threads=%d", draft, len(item.Reviews), len(item.ReviewComments), len(item.ReviewThreads))
		}
	}
	if len(item.Reviews) != 1 || len(item.ReviewComments) != 1 || len(item.ReviewThreads) != 1 || item.ReviewThreads[0]["id"] != "T1" {
		t.Fatalf("submitted evidence changed: reviews=%d review_comments=%d threads=%v", len(item.Reviews), len(item.ReviewComments), item.ReviewThreads)
	}
	if total, _ := historyInt(historyMap(item.ReviewThreads[0]["comments"])["totalCount"]); total != 1 {
		t.Fatalf("filtered thread totalCount = %d, want 1", total)
	}
}
