package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const pullReviewThreadsQuery = `
query($owner: String!, $repo: String!, $pr: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      reviewThreads(first: 100, after: $cursor) {
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          startLine
          viewerCanResolve
          viewerCanUnresolve
          viewerCanReply
          comments(first: 100) {
            nodes {
              id
              databaseId
              body
              author { login __typename }
              path
              diffHunk
              createdAt
              updatedAt
              url
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}
`

const pullReviewThreadCommentsQuery = `
query($threadID: ID!, $cursor: String) {
  node(id: $threadID) {
    ... on PullRequestReviewThread {
      comments(first: 100, after: $cursor) {
        nodes {
          id
          databaseId
          body
          author { login __typename }
          path
          diffHunk
          createdAt
          updatedAt
          url
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}
`

type pullReviewThreadsResponse struct {
	Repository struct {
		PullRequest *struct {
			ReviewThreads struct {
				Nodes    []map[string]any `json:"nodes"`
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
			} `json:"reviewThreads"`
		} `json:"pullRequest"`
	} `json:"repository"`
}

type pullReviewThreadCommentsResponse struct {
	Node *struct {
		Comments reviewThreadCommentsConnection `json:"comments"`
	} `json:"node"`
}

type reviewThreadCommentsConnection struct {
	Nodes    []map[string]any `json:"nodes"`
	PageInfo struct {
		HasNextPage bool   `json:"hasNextPage"`
		EndCursor   string `json:"endCursor"`
	} `json:"pageInfo"`
}

type graphqlEnvelope struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

type graphqlResponseEnvelope struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// ListPullReviewThreads fetches GitHub's review-thread graph for a pull request.
func (c *Client) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	var out []map[string]any
	var cursor string
	for {
		vars := map[string]any{
			"owner": owner,
			"repo":  repo,
			"pr":    number,
		}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		var resp pullReviewThreadsResponse
		if err := c.doGraphQL(ctx, pullReviewThreadsQuery, vars, reporter, &resp); err != nil {
			return nil, err
		}
		if resp.Repository.PullRequest == nil {
			return nil, fmt.Errorf("pull request #%d not found in %s/%s", number, owner, repo)
		}
		page := resp.Repository.PullRequest.ReviewThreads
		for _, thread := range page.Nodes {
			if err := c.completeReviewThreadComments(ctx, thread, reporter); err != nil {
				return nil, err
			}
		}
		out = append(out, page.Nodes...)
		if !page.PageInfo.HasNextPage {
			break
		}
		cursor = page.PageInfo.EndCursor
	}
	return out, nil
}

func (c *Client) completeReviewThreadComments(ctx context.Context, thread map[string]any, reporter Reporter) error {
	threadID, _ := thread["id"].(string)
	if threadID == "" {
		return nil
	}
	comments := reviewThreadCommentsFromMap(thread["comments"])
	if !comments.PageInfo.HasNextPage {
		return nil
	}
	for comments.PageInfo.HasNextPage {
		cursor := comments.PageInfo.EndCursor
		if cursor == "" {
			return fmt.Errorf("review thread %s comments page missing endCursor", threadID)
		}
		vars := map[string]any{"threadID": threadID, "cursor": cursor}
		var resp pullReviewThreadCommentsResponse
		if err := c.doGraphQL(ctx, pullReviewThreadCommentsQuery, vars, reporter, &resp); err != nil {
			return err
		}
		if resp.Node == nil {
			return fmt.Errorf("review thread %s not found", threadID)
		}
		comments.Nodes = append(comments.Nodes, resp.Node.Comments.Nodes...)
		comments.PageInfo = resp.Node.Comments.PageInfo
	}
	thread["comments"] = map[string]any{
		"nodes": comments.Nodes,
		"pageInfo": map[string]any{
			"hasNextPage": comments.PageInfo.HasNextPage,
			"endCursor":   comments.PageInfo.EndCursor,
		},
	}
	return nil
}

func reviewThreadCommentsFromMap(value any) reviewThreadCommentsConnection {
	var out reviewThreadCommentsConnection
	raw, ok := value.(map[string]any)
	if !ok {
		return out
	}
	switch nodes := raw["nodes"].(type) {
	case []map[string]any:
		out.Nodes = append(out.Nodes, nodes...)
	case []any:
		out.Nodes = make([]map[string]any, 0, len(nodes))
		for _, node := range nodes {
			if mapped, ok := node.(map[string]any); ok {
				out.Nodes = append(out.Nodes, mapped)
			}
		}
	}
	pageInfo, _ := raw["pageInfo"].(map[string]any)
	if hasNextPage, ok := pageInfo["hasNextPage"].(bool); ok {
		out.PageInfo.HasNextPage = hasNextPage
	}
	if endCursor, ok := pageInfo["endCursor"].(string); ok {
		out.PageInfo.EndCursor = endCursor
	}
	return out
}

func (c *Client) doGraphQL(ctx context.Context, query string, variables map[string]any, reporter Reporter, out any) error {
	payload, err := json.Marshal(graphqlEnvelope{Query: query, Variables: variables})
	if err != nil {
		return fmt.Errorf("encode graphql request: %w", err)
	}
	var envelope graphqlResponseEnvelope
	if err := c.doJSON(ctx, http.MethodPost, "/graphql", bytes.NewReader(payload), reporter, &envelope); err != nil {
		return err
	}
	if len(envelope.Errors) > 0 {
		messages := make([]string, 0, len(envelope.Errors))
		for _, graphqlErr := range envelope.Errors {
			messages = append(messages, graphqlErr.Message)
		}
		return fmt.Errorf("github graphql: %s", strings.Join(messages, "; "))
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return fmt.Errorf("github graphql response missing data")
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("decode github graphql data: %w", err)
	}
	return nil
}
