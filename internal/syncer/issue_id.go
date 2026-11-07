package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
)

// The cache is per sync and bounded even for a large REST traversal. It never
// carries an identity across repositories, credentials, or sync invocations.
const issueIDCacheLimit = 256

type issueIDResolver struct {
	client  GitHubClient
	options Options
	cache   map[int]string
	order   []int
}

func canonicalIssueID(value any) (string, error) {
	if n, ok := value.(float64); ok && (n <= 0 || n > 9007199254740991 || math.Trunc(n) != n) {
		return "", fmt.Errorf("invalid or inexact Issue database ID")
	}
	id := jsonID(value)
	if id == "" || id[0] == '0' {
		return "", fmt.Errorf("missing or invalid Issue database ID")
	}
	for _, c := range id {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("invalid Issue database ID")
		}
	}
	return id, nil
}

func (r *issueIDResolver) resolve(ctx context.Context, row map[string]any) (map[string]any, error) {
	number := intValue(row["number"])
	if number <= 0 {
		return nil, fmt.Errorf("missing Issue number")
	}
	value := row["id"]
	if stringValue(row["_gitcrawl_source"]) == "graphql" && issueKind(row) == "pull_request" {
		if id, ok := r.cache[number]; ok {
			value = id
		} else {
			// GetIssue uses the same credential, request, and REST quota guards as
			// ordinary sync. Never substitute a PR database ID or an opaque node ID.
			issue, err := r.client.GetIssue(ctx, r.options.Owner, r.options.Repo, number, r.options.Reporter)
			if err != nil {
				return nil, fmt.Errorf("resolve backing Issue #%d: %w", number, err)
			}
			if intValue(issue["number"]) != number || issueKind(issue) != "pull_request" {
				return nil, fmt.Errorf("backing Issue #%d identity mismatch", number)
			}
			id, err := canonicalIssueID(issue["id"])
			if err != nil {
				return nil, err
			}
			if r.cache == nil {
				r.cache = make(map[int]string)
			}
			if len(r.order) == issueIDCacheLimit {
				delete(r.cache, r.order[0])
				r.order = r.order[1:]
			}
			r.cache[number] = id
			r.order = append(r.order, number)
			value = id
		}
	}
	id, err := canonicalIssueID(value)
	if err != nil {
		return nil, err
	}
	if stringValue(row["_gitcrawl_source"]) != "graphql" {
		return row, nil
	}
	resolved := maps.Clone(row)
	resolved["id"] = json.Number(id)
	return resolved, nil
}
