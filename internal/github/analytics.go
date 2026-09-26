package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// AnalyticsNodes reads public provider evidence for the requested native node IDs.
func (c *Client) AnalyticsNodes(ctx context.Context, ids []string, profiles bool) ([]map[string]any, error) {
	fields := `... on Issue {author{login __typename ... on Node{id}}} ... on PullRequest {author{login __typename ... on Node{id}}} ... on IssueComment {author{login __typename ... on Node{id}}} ... on PullRequestReview {author{login __typename ... on Node{id}}} ... on PullRequestReviewComment {author{login __typename ... on Node{id}}}`
	if profiles {
		fields = `... on User {login name bio url createdAt} ... on Bot {login url createdAt} ... on Mannequin {login url createdAt}`
	}
	payload, e := json.Marshal(graphqlEnvelope{Query: `query($ids:[ID!]!){rateLimit{cost remaining limit used resetAt} nodes(ids:$ids){id __typename ` + fields + `}}`, Variables: map[string]any{"ids": ids}})
	if e != nil {
		return nil, e
	}
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Path    []any  `json:"path"`
		} `json:"errors"`
	}
	if e = c.doJSON(ctx, http.MethodPost, c.graphQLURL, bytes.NewReader(payload), nil, &envelope); e != nil {
		return nil, e
	}
	for _, failure := range envelope.Errors {
		if failure.Type != "NOT_FOUND" || len(failure.Path) < 2 || failure.Path[0] != "nodes" {
			return nil, fmt.Errorf("actor evidence GraphQL error: %s", failure.Message)
		}
	}
	var data map[string]any
	if e = json.Unmarshal(envelope.Data, &data); e != nil {
		return nil, e
	}

	nodes, ok := data["nodes"].([]any)
	if !ok || len(nodes) != len(ids) {
		return nil, fmt.Errorf("incomplete actor identity response")
	}
	out := make([]map[string]any, 0, len(ids))
	for i, v := range nodes {
		n, ok := v.(map[string]any)
		if !ok {
			n = map[string]any{"id": ids[i], "__typename": "Unavailable", "unavailable": true}
		}
		if n["id"] != ids[i] {
			return nil, fmt.Errorf("actor node identity mismatch")
		}
		out = append(out, n)
	}
	return out, nil
}

type UpdatedPage struct {
	Numbers []int
	Cursor  string
	More    bool
	Total   int
	Oldest  time.Time
}

// AnalyticsRateLimit reads the same GraphQL balance charged by history queries.
// REST resource counters can differ and must not admit recovery on that basis.
func (c *Client) AnalyticsRateLimit(ctx context.Context) (effective, observed RateLimitSnapshot, err error) {
	h := historySession{client: c, remaining: 20000}
	data, err := h.request(ctx, `query { rateLimit { cost limit remaining used resetAt } }`, nil, 1)
	if err != nil {
		return RateLimitSnapshot{}, RateLimitSnapshot{}, err
	}
	rate := historyMap(data["rateLimit"])
	limit, ok := historyInt(rate["limit"])
	if !ok || limit <= 0 {
		return RateLimitSnapshot{}, RateLimitSnapshot{}, fmt.Errorf("GraphQL quota limit unavailable")
	}
	remaining, _ := historyInt(rate["remaining"])
	reset, _ := time.Parse(time.RFC3339, historyString(rate["resetAt"]))
	observed = RateLimitSnapshot{Resource: "graphql", Limit: limit, Remaining: remaining, ResetAt: reset}
	return c.reserve.observeGraphQL(observed, time.Now()), observed, nil
}

func (c *Client) UpdatedNumbers(ctx context.Context, owner, repo, kind, after string, since time.Time) (UpdatedPage, error) {
	if kind != "issues" && kind != "pullRequests" {
		return UpdatedPage{}, fmt.Errorf("invalid discovery kind")
	}
	h := historySession{client: c, remaining: 20000}
	var cursor any
	if after != "" {
		cursor = after
	}
	data, e := h.request(ctx, `query($owner:String!,$repo:String!,$after:String){rateLimit{cost remaining limit used resetAt} repository(owner:$owner,name:$repo){`+kind+`(first:100,after:$after,orderBy:{field:UPDATED_AT,direction:DESC}){totalCount pageInfo{hasNextPage endCursor} nodes{number updatedAt}}}}`, map[string]any{"owner": owner, "repo": repo, "after": cursor}, 1)
	if e != nil {
		return UpdatedPage{}, e
	}
	r := historyMap(historyMap(data["repository"])[kind])
	p := UpdatedPage{}
	var valid bool
	p.Total, valid = historyInt(r["totalCount"])
	if !valid || p.Total < 0 {
		return p, historyFailure("discovery_validation", 0, r, fmt.Errorf("missing update-discovery count"))
	}
	info := historyMap(r["pageInfo"])
	var hasPageFlag bool
	p.More, hasPageFlag = info["hasNextPage"].(bool)
	nodes, hasNodes := r["nodes"].([]any)
	// totalCount covers the whole connection. A resumed final page can become
	// empty after deletion/reordering; it must not pin its cursor forever.
	if !hasPageFlag || !hasNodes || (len(nodes) == 0 && p.Total > 0 && after == "") || len(nodes) > p.Total {
		return p, historyFailure("discovery_validation", 0, r, fmt.Errorf("incomplete update-discovery page"))
	}
	p.Cursor = historyString(info["endCursor"])
	for _, n := range historyNodes(historyMap(data["repository"]), kind) {
		number, ok := historyInt(n["number"])
		at, e := time.Parse(time.RFC3339Nano, historyString(n["updatedAt"]))
		if !ok || number < 1 || e != nil {
			return p, historyFailure("discovery_validation", 0, r, fmt.Errorf("invalid update-discovery evidence"))
		}
		if !at.Before(since) {
			p.Numbers = append(p.Numbers, number)
		}
		if p.Oldest.IsZero() || at.Before(p.Oldest) {
			p.Oldest = at
		}
	}
	if p.More && (p.Cursor == "" || p.Cursor == after || p.Oldest.IsZero()) {
		return p, historyFailure("discovery_validation", 0, r, fmt.Errorf("update cursor did not advance"))
	}
	return p, nil
}
