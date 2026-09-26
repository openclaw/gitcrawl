package github

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ReviewStateItem contains only review evidence. It must never enter the
// full-thread projection, since canonical bodies/comments were not requested.
type ReviewStateItem struct {
	Number           int
	NodeID           string
	RepositoryID     string
	RepositoryNodeID string
	UpdatedAt        string
	Threads          []map[string]any
}

func (c *Client) FetchGraphQLReviewState(ctx context.Context, owner, repo string, numbers []int, reporter Reporter) ([]ReviewStateItem, error) {
	if len(numbers) == 0 || len(numbers) > 8 {
		return nil, fmt.Errorf("review-state query requires 1..8 numbers")
	}
	h := historySession{client: c, reporter: reporter, remaining: 20000}
	if _, err := h.quota(ctx); err != nil {
		return nil, err
	}
	var fields strings.Builder
	for i, n := range numbers {
		if n < 1 {
			return nil, fmt.Errorf("invalid review-state number")
		}
		fmt.Fprintf(&fields, `n%d: issueOrPullRequest(number:%d) {__typename ... on PullRequest{id number updatedAt repository{nameWithOwner} %s}} `, i, n, historyConnection("reviewThreads", historyReviewThread, ""))
	}
	data, err := h.request(ctx, `query($owner:String!,$repo:String!){repository(owner:$owner,name:$repo){id databaseId nameWithOwner `+fields.String()+`} rateLimit{cost remaining limit used resetAt}}`, map[string]any{"owner": owner, "repo": repo}, 16)
	if err != nil {
		return nil, err
	}
	r := historyMap(data["repository"])
	if !strings.EqualFold(historyString(r["nameWithOwner"]), owner+"/"+repo) || historyString(r["id"]) == "" {
		return nil, historyFailure("identity", 0, r, fmt.Errorf("review-state repository identity mismatch"))
	}
	repoID, validRepoID := historyInt(r["databaseId"])
	if !validRepoID || repoID <= 0 {
		return nil, historyFailure("identity", 0, r, fmt.Errorf("missing repository database identity"))
	}
	out := make([]ReviewStateItem, 0, len(numbers))
	for i, n := range numbers {
		node := historyMap(r[fmt.Sprint("n", i)])
		got, valid := historyInt(node["number"])
		if !valid || got != n || historyString(node["__typename"]) != "PullRequest" || historyString(node["id"]) == "" || !strings.EqualFold(historyString(historyMap(node["repository"])["nameWithOwner"]), owner+"/"+repo) {
			return nil, historyFailure("identity", n, node, fmt.Errorf("review-state PR identity mismatch"))
		}
		updated := historyString(node["updatedAt"])
		if _, err = time.Parse(time.RFC3339Nano, updated); err != nil {
			return nil, historyFailure("validation", n, node, fmt.Errorf("invalid review-state source time"))
		}
		if historyMap(node["reviewThreads"]) == nil {
			return nil, historyFailure("validation", n, node, fmt.Errorf("missing history reviewThreads"))
		}
		if err = h.hydrateConnections(ctx, map[string]any{"__typename": "PullRequest", "id": node["id"], "reviewThreads": node["reviewThreads"]}); err != nil {
			return nil, historyFailure("validation", n, node, err)
		}
		out = append(out, ReviewStateItem{Number: n, RepositoryID: fmt.Sprint(repoID), RepositoryNodeID: historyString(r["id"]), NodeID: historyString(node["id"]), UpdatedAt: updated, Threads: historyNodes(node, "reviewThreads")})
	}
	return out, nil
}
