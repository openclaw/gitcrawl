package github

import "strings"

// GitHub returns the authenticated user's own unsubmitted (PENDING) review and
// its draft comments to that user only. Nobody else can see them and their
// author may still discard them, so they are not part of the archived
// conversation.
func isPendingReview(state any) bool {
	value, _ := state.(string)
	return strings.EqualFold(value, "PENDING")
}

func withoutPendingReviews(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if !isPendingReview(row["state"]) {
			out = append(out, row)
		}
	}
	return out
}

// withoutPendingThreadComments drops draft comments and any thread that exists
// only in the authenticated user's unsubmitted review.
func withoutPendingThreadComments(threads []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(threads))
	for _, thread := range threads {
		comments := reviewThreadCommentsFromMap(thread["comments"])
		kept := make([]map[string]any, 0, len(comments.Nodes))
		for _, comment := range comments.Nodes {
			if !isPendingReview(comment["state"]) {
				kept = append(kept, comment)
			}
		}
		if len(kept) == len(comments.Nodes) {
			out = append(out, thread)
			continue
		}
		if len(kept) == 0 {
			continue
		}
		thread["comments"] = map[string]any{
			"nodes": kept,
			"pageInfo": map[string]any{
				"hasNextPage": comments.PageInfo.HasNextPage,
				"endCursor":   comments.PageInfo.EndCursor,
			},
		}
		out = append(out, thread)
	}
	return out
}
