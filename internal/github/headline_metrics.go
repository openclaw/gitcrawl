package github

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Pointer counts distinguish an unavailable field from an actual zero.
type RepositoryHeadline struct {
	Stars              *float64 `json:"stargazers_count"`
	Forks              *float64 `json:"forks_count"`
	Watchers           *float64 `json:"subscribers_count"`
	OpenIssuesAndPulls *float64 `json:"open_issues_count"`
}

type PullCount struct {
	Count      *float64 `json:"total_count"`
	Incomplete bool     `json:"incomplete_results"`
}

type CloneTraffic struct {
	Clones []struct {
		Timestamp string   `json:"timestamp"`
		Count     *float64 `json:"count"`
	} `json:"clones"`
}

type Release struct {
	ID         int64  `json:"id"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
	Published  string `json:"published_at"`
	Created    string `json:"created_at"`
	Name       string `json:"name"`
	Tag        string `json:"tag_name"`
	URL        string `json:"html_url"`
}

func (c *Client) RepositoryHeadline(ctx context.Context, repository string) (RepositoryHeadline, error) {
	var out RepositoryHeadline
	err := c.doJSON(ctx, http.MethodGet, "/repos/"+repository, nil, nil, &out)
	return out, err
}

func (c *Client) OpenPullCount(ctx context.Context, repository string) (PullCount, error) {
	var out PullCount
	err := c.doJSON(ctx, http.MethodGet, "/search/issues?q="+url.QueryEscape("repo:"+repository+" is:pr is:open")+"&per_page=1", nil, nil, &out)
	return out, err
}

func (c *Client) CloneTraffic(ctx context.Context, repository string) (CloneTraffic, error) {
	var out CloneTraffic
	err := c.doJSON(ctx, http.MethodGet, "/repos/"+repository+"/traffic/clones?per=day", nil, nil, &out)
	return out, err
}

func (c *Client) ReleasePage(ctx context.Context, repository string, page int) ([]Release, error) {
	var out []Release
	err := c.doJSON(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/releases?per_page=100&page=%d", repository, page), nil, nil, &out)
	return out, err
}
