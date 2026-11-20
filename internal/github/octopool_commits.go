package github

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
)

var commitObjectID = regexp.MustCompile(`^[a-f0-9]{40}$`)

type CommitHead struct{ Repository, NodeID, Branch, OID string }
type CommitQuery struct {
	Head         CommitHead
	Since, Until time.Time
	Cursor       string
}
type CommitRead struct {
	RawJSON    json.RawMessage
	NextCursor string
	Terminal   bool
}
type CommitAttributionRead struct {
	RawJSON    json.RawMessage
	NextCursor string
	Terminal   bool
}

func (c *OctopoolConversations) ReadCommitHead(ctx context.Context, reporter Reporter) (CommitHead, error) {
	var out CommitHead
	owner, name, ok := strings.Cut(c.repository, "/")
	if !ok {
		return out, errors.New("invalid repository")
	}
	raw, err := c.requestConversationData(ctx, `query($owner:String!,$name:String!){ repository(owner:$owner,name:$name,followRenames:false){id nameWithOwner defaultBranchRef{name target{... on Commit{oid}}}} }`, map[string]any{"owner": owner, "name": name}, reporter)
	if err != nil {
		return out, err
	}
	var d struct {
		Repository *struct {
			ID, NameWithOwner string
			DefaultBranchRef  *struct {
				Name   string
				Target struct{ OID string }
			}
		}
	}
	if json.Unmarshal(raw, &d) != nil || d.Repository == nil || d.Repository.DefaultBranchRef == nil || !strings.EqualFold(d.Repository.NameWithOwner, c.repository) || d.Repository.ID == "" || d.Repository.DefaultBranchRef.Name == "" || !commitObjectID.MatchString(d.Repository.DefaultBranchRef.Target.OID) {
		return out, errors.New("default branch identity unavailable")
	}
	return CommitHead{Repository: c.repository, NodeID: d.Repository.ID, Branch: d.Repository.DefaultBranchRef.Name, OID: d.Repository.DefaultBranchRef.Target.OID}, nil
}
func (c *OctopoolConversations) ReadCommit(ctx context.Context, q CommitQuery, reporter Reporter) (CommitRead, error) {
	var out CommitRead
	if q.Head.Repository != c.repository || q.Head.NodeID == "" || !commitObjectID.MatchString(q.Head.OID) || q.Since.IsZero() || !q.Since.Before(q.Until) || len(q.Cursor) > 4096 || strings.ContainsAny(q.Cursor, "\x00\r\n") {
		return out, errors.New("invalid commit selection")
	}
	owner, name, _ := strings.Cut(c.repository, "/")
	var cursor any
	if q.Cursor != "" {
		cursor = q.Cursor
	}
	raw, err := c.requestConversationData(ctx, `query($owner:String!,$name:String!,$oid:GitObjectID!,$since:GitTimestamp!,$until:GitTimestamp!,$cursor:String){repository(owner:$owner,name:$name,followRenames:false){id nameWithOwner object(oid:$oid){__typename oid ... on Commit{history(first:1,after:$cursor,since:$since,until:$until){nodes{__typename id oid committedDate authoredDate message url author{name user{login}} repository{id nameWithOwner}} pageInfo{hasNextPage endCursor}}}}}}`, map[string]any{"owner": owner, "name": name, "oid": q.Head.OID, "since": q.Since.UTC().Format(time.RFC3339Nano), "until": q.Until.UTC().Format(time.RFC3339Nano), "cursor": cursor}, reporter)
	if err != nil {
		return out, err
	}
	var d struct {
		Repository *struct {
			ID, NameWithOwner string
			Object            *struct {
				Type    string `json:"__typename"`
				OID     string
				History *struct {
					Nodes    []json.RawMessage
					PageInfo struct {
						HasNextPage *bool
						EndCursor   *string
					}
				}
			}
		}
	}
	if json.Unmarshal(raw, &d) != nil || d.Repository == nil || d.Repository.ID != q.Head.NodeID || !strings.EqualFold(d.Repository.NameWithOwner, c.repository) || d.Repository.Object == nil || d.Repository.Object.Type != "Commit" || d.Repository.Object.OID != q.Head.OID || d.Repository.Object.History == nil {
		return out, errors.New("commit head identity changed")
	}
	h := d.Repository.Object.History
	if h.Nodes == nil || len(h.Nodes) > 1 || h.PageInfo.HasNextPage == nil {
		return out, errors.New("invalid commit history page")
	}
	out.Terminal = !*h.PageInfo.HasNextPage
	if !out.Terminal {
		if h.PageInfo.EndCursor == nil || *h.PageInfo.EndCursor == "" || *h.PageInfo.EndCursor == q.Cursor || len(*h.PageInfo.EndCursor) > 4096 || len(h.Nodes) == 0 {
			return out, errors.New("invalid commit history cursor")
		}
		out.NextCursor = *h.PageInfo.EndCursor
	}
	if len(h.Nodes) == 1 {
		var n struct {
			ID, OID, CommittedDate string
			Repository             struct{ ID, NameWithOwner string }
		}
		if json.Unmarshal(h.Nodes[0], &n) != nil || n.ID == "" || !commitObjectID.MatchString(n.OID) || n.Repository.ID != q.Head.NodeID || !strings.EqualFold(n.Repository.NameWithOwner, c.repository) {
			return out, errors.New("invalid commit identity")
		}
		at, e := time.Parse(time.RFC3339Nano, n.CommittedDate)
		if e != nil || at.Before(q.Since) || at.After(q.Until) {
			return out, errors.New("commit outside requested window")
		}
		// GitHub history uses an inclusive upper bound; reports use [since, until).
		if at.Before(q.Until) {
			out.RawJSON = h.Nodes[0]
		}
	}
	return out, nil
}
func (c *OctopoolConversations) ReadCommitAttribution(ctx context.Context, nodeID, oid, cursor string, reporter Reporter) (CommitAttributionRead, error) {
	var out CommitAttributionRead
	if nodeID == "" || len(nodeID) > 512 || !commitObjectID.MatchString(oid) || len(cursor) > 4096 || strings.ContainsAny(nodeID+cursor, "\x00\r\n") {
		return out, errors.New("invalid commit attribution selection")
	}
	var after any
	if cursor != "" {
		after = cursor
	}
	raw, err := c.requestConversationData(ctx, `query($id:ID!,$cursor:String){node(id:$id){... on Commit{id oid associatedPullRequests(first:100,after:$cursor){nodes{id mergedAt mergedBy{login} mergeCommit{oid} repository{id nameWithOwner}} pageInfo{hasNextPage endCursor}}}}}`, map[string]any{"id": nodeID, "cursor": after}, reporter)
	if err != nil {
		return out, err
	}
	var d struct{ Node json.RawMessage }
	var n struct {
		ID, OID                string
		AssociatedPullRequests *struct {
			Nodes    []json.RawMessage
			PageInfo struct {
				HasNextPage *bool
				EndCursor   *string
			}
		}
	}
	if json.Unmarshal(raw, &d) != nil || json.Unmarshal(d.Node, &n) != nil || n.ID != nodeID || n.OID != oid || n.AssociatedPullRequests == nil || n.AssociatedPullRequests.Nodes == nil || len(n.AssociatedPullRequests.Nodes) > 100 || n.AssociatedPullRequests.PageInfo.HasNextPage == nil {
		return out, errors.New("invalid commit attribution page")
	}
	p := n.AssociatedPullRequests
	out.Terminal = !*p.PageInfo.HasNextPage
	if !out.Terminal {
		if p.PageInfo.EndCursor == nil || *p.PageInfo.EndCursor == "" || *p.PageInfo.EndCursor == cursor || len(*p.PageInfo.EndCursor) > 4096 || len(p.Nodes) == 0 {
			return out, errors.New("invalid commit attribution cursor")
		}
		out.NextCursor = *p.PageInfo.EndCursor
	}
	out.RawJSON = d.Node
	return out, nil
}
