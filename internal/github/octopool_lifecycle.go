package github

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type LifecycleQuery struct {
	NodeID, Kind, Cursor string
	Number               int
	Since                time.Time
}

// LifecyclePage retains original event objects, including unknown provider
// fields. PostgreSQL owns their interpretation, cursor chain and publication.
type LifecyclePage struct {
	Events     []json.RawMessage
	NextCursor string
	Terminal   bool
}

func (c *OctopoolConversations) ReadLifecycle(ctx context.Context, q LifecycleQuery, reporter Reporter) (LifecyclePage, error) {
	var empty LifecyclePage
	if q.Number < 1 || q.NodeID == "" || len(q.NodeID) > 512 || strings.ContainsAny(q.NodeID, "\x00\r\n") ||
		(q.Kind != "issue" && q.Kind != "pull_request") || q.Since.IsZero() || len(q.Cursor) > 4096 || strings.ContainsAny(q.Cursor, "\x00\r\n") {
		return empty, errors.New("invalid lifecycle page selection")
	}
	kind, types, extra := "Issue", "CLOSED_EVENT,REOPENED_EVENT", ""
	if q.Kind == "pull_request" {
		kind, types = "PullRequest", types+",MERGED_EVENT"
		extra = "... on MergedEvent { id createdAt actor { login __typename ... on Node { id } } commit { oid } }"
	}
	owner, repo, _ := strings.Cut(c.repository, "/")
	// Octopool mints a repository-scoped App token from the query root.
	query := `query($owner:String!,$repo:String!,$number:Int!,$since:DateTime!,$cursor:String) {
 repository(owner:$owner,name:$repo) { nameWithOwner
 issueOrPullRequest(number:$number) { __typename ... on ` + kind + ` {
 id number repository { nameWithOwner }
 timelineItems(first:100,after:$cursor,since:$since,itemTypes:[` + types + `]) {
  nodes { __typename
   ... on ClosedEvent { id createdAt actor { login __typename ... on Node { id } } closer { __typename ... on PullRequest { id mergedAt } ... on Commit { oid } } }
   ... on ReopenedEvent { id createdAt actor { login __typename ... on Node { id } } }
   ` + extra + `
  } pageInfo { hasNextPage endCursor }
 }
} } } }`
	var cursor any
	if q.Cursor != "" {
		cursor = q.Cursor
	}
	raw, err := c.requestConversationData(ctx, query, map[string]any{"owner": owner, "repo": repo, "number": q.Number, "since": q.Since.UTC().Format(time.RFC3339Nano), "cursor": cursor}, reporter)
	if err != nil {
		return empty, err
	}
	var data struct {
		Repository struct {
			Name string `json:"nameWithOwner"`
			Node *struct {
				NodeID     string `json:"id"`
				NodeType   string `json:"__typename"`
				Number     int    `json:"number"`
				Repository struct {
					Name string `json:"nameWithOwner"`
				} `json:"repository"`
				Timeline *struct {
					Nodes []json.RawMessage `json:"nodes"`
					Page  struct {
						More *bool   `json:"hasNextPage"`
						Next *string `json:"endCursor"`
					} `json:"pageInfo"`
				} `json:"timelineItems"`
			} `json:"issueOrPullRequest"`
		} `json:"repository"`
	}
	if json.Unmarshal(raw, &data) != nil || !strings.EqualFold(data.Repository.Name, c.repository) {
		return empty, errors.New("lifecycle repository identity changed or unavailable")
	}
	node := data.Repository.Node
	if node == nil || node.NodeID != q.NodeID || node.NodeType != kind || node.Number != q.Number ||
		!strings.EqualFold(node.Repository.Name, c.repository) || node.Timeline == nil {
		return empty, errors.New("lifecycle parent identity changed or unavailable")
	}
	connection := node.Timeline
	if connection.Nodes == nil || len(connection.Nodes) > 100 || connection.Page.More == nil {
		return empty, errors.New("invalid lifecycle connection")
	}
	next := ""
	if connection.Page.Next != nil {
		next = *connection.Page.Next
	}
	if len(next) > 4096 || strings.ContainsAny(next, "\x00\r\n") || (*connection.Page.More && (next == "" || next == q.Cursor || len(connection.Nodes) == 0)) {
		return empty, errors.New("lifecycle cursor did not advance")
	}
	seen := make(map[string]bool, len(connection.Nodes))
	for _, raw := range connection.Nodes {
		var event struct {
			ID        string `json:"id"`
			CreatedAt string `json:"createdAt"`
			Type      string `json:"__typename"`
		}
		if json.Unmarshal(raw, &event) != nil || event.ID == "" || seen[event.ID] ||
			(event.Type != "ClosedEvent" && event.Type != "ReopenedEvent" && (event.Type != "MergedEvent" || q.Kind != "pull_request")) {
			return empty, errors.New("invalid or duplicate lifecycle event")
		}
		at, err := time.Parse(time.RFC3339Nano, event.CreatedAt)
		if err != nil || at.Before(q.Since) {
			return empty, errors.New("invalid lifecycle event time")
		}
		seen[event.ID] = true
	}
	if !*connection.Page.More {
		next = ""
	}
	return LifecyclePage{Events: connection.Nodes, NextCursor: next, Terminal: !*connection.Page.More}, nil
}
