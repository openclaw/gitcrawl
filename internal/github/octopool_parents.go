package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParentSnapshot retains the selected GraphQL fields without manufacturing a
// REST issue. A PullRequest database ID is not its backing Issue database ID;
// callers must bind that identity from their independently retained selection.
type ParentSnapshot struct {
	Kind, NodeID, GraphQLDatabaseID, BackingIssueID string
	Number                                          int
	ProviderFields                                  map[string]any
}

const parentFields = `id __typename fullDatabaseId number title body
 author { login __typename url ... on Node { id } }
 authorAssociation createdAt updatedAt closedAt url locked activeLockReason
 repository { nameWithOwner }
 milestone { number title state dueOn createdAt updatedAt url }`

// ReadParents reads only the requested parent records. It does not inventory the
// repository, fetch child connections, populate conversation caches or infer
// child completeness. Oversized batches fail for the caller to split explicitly.
func (c *OctopoolConversations) ReadParents(ctx context.Context, numbers []int, reporter Reporter) ([]ParentSnapshot, error) {
	if len(numbers) == 0 || len(numbers) > 50 {
		return nil, errors.New("parent batch requires 1..50 numbers")
	}
	owner, repo, ok := strings.Cut(c.repository, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return nil, errors.New("expected owner/repository")
	}
	var aliases strings.Builder
	seen := make(map[int]bool, len(numbers))
	for i, number := range numbers {
		if number <= 0 || seen[number] {
			return nil, errors.New("parent batch requires unique positive numbers")
		}
		seen[number] = true
		fmt.Fprintf(&aliases, "n%d: issueOrPullRequest(number:%d) { ...ParentIssue ...ParentPull }\n", i, number)
	}
	query := `query($owner:String!,$repo:String!) { repository(owner:$owner,name:$repo) { nameWithOwner ` + aliases.String() + ` } }
 fragment ParentIssue on Issue { ` + parentFields + ` issueState: state stateReason }
 fragment ParentPull on PullRequest { ` + parentFields + ` pullState: state isDraft merged mergedAt
 mergedBy { login __typename url ... on Node { id } } mergeCommit { oid }
 mergeable mergeStateStatus maintainerCanModify additions deletions changedFiles
 headRefName headRefOid baseRefName baseRefOid
 headRepository { nameWithOwner } baseRepository { nameWithOwner } }`
	data, err := c.requestConversations(ctx, query, map[string]any{"owner": owner, "repo": repo}, reporter)
	if err != nil {
		return nil, err
	}
	repository := historyMap(data["repository"])
	if !strings.EqualFold(historyString(repository["nameWithOwner"]), c.repository) {
		return nil, errors.New("parent batch repository identity mismatch")
	}
	parents := make([]ParentSnapshot, 0, len(numbers))
	nodes := make(map[string]bool, len(numbers))
	for i, number := range numbers {
		row := historyMap(repository[fmt.Sprintf("n%d", i)])
		got, valid := historyInt(row["number"])
		nodeID := historyString(row["id"])
		if !valid || got != number || nodeID == "" || nodes[nodeID] ||
			!strings.EqualFold(historyString(historyMap(row["repository"])["nameWithOwner"]), c.repository) {
			return nil, errors.New("parent batch item unavailable or identity mismatch")
		}
		kind := historyString(row["__typename"])
		if kind != "Issue" && kind != "PullRequest" {
			return nil, errors.New("parent batch item has invalid kind")
		}
		if _, ok := row["title"].(string); !ok {
			return nil, errors.New("parent batch item missing full title")
		}
		if _, ok := row["body"].(string); !ok {
			return nil, errors.New("parent batch item missing full body")
		}
		for _, field := range []string{"createdAt", "updatedAt"} {
			if _, err := time.Parse(time.RFC3339Nano, historyString(row[field])); err != nil {
				return nil, errors.New("parent batch item missing provider timestamp")
			}
		}
		// These fields have different enum types in GitHub's schema. Distinct
		// response names are required even in mutually exclusive fragments.
		stateField := "issueState"
		if kind == "PullRequest" {
			stateField = "pullState"
		}
		state := historyString(row[stateField])
		if historyString(row["url"]) == "" || (state != "OPEN" && state != "CLOSED" && (kind != "PullRequest" || state != "MERGED")) {
			return nil, errors.New("parent batch item missing provider metadata")
		}
		var databaseID string
		switch value := row["fullDatabaseId"].(type) {
		case string:
			databaseID = value
		case json.Number:
			databaseID = string(value)
		}
		id, err := strconv.ParseInt(databaseID, 10, 64)
		if err != nil || id <= 0 || strconv.FormatInt(id, 10) != databaseID {
			return nil, errors.New("parent batch item missing exact database identity")
		}
		parent := ParentSnapshot{Kind: "issue", Number: number, NodeID: nodeID,
			GraphQLDatabaseID: databaseID, BackingIssueID: databaseID, ProviderFields: row}
		if kind == "PullRequest" {
			parent.Kind, parent.BackingIssueID = "pull_request", ""
		}
		parents = append(parents, parent)
		nodes[nodeID] = true
	}
	return parents, nil
}
