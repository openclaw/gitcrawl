package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Reporter func(message string)

type Client struct {
	httpClient *http.Client
	baseURL    string
	token      string
	userAgent  string
	pageDelay  time.Duration
}

type Options struct {
	Token      string
	BaseURL    string
	UserAgent  string
	HTTPClient *http.Client
	PageDelay  time.Duration
}

type ListIssuesOptions struct {
	State         string
	Since         string
	Limit         int
	ExpectedTotal int
}

type RequestError struct {
	Method string
	URL    string
	Status int
	Body   string
}

func (e *RequestError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("github %s %s failed with status %d", e.Method, e.URL, e.Status)
	}
	return fmt.Sprintf("github %s %s failed with status %d: %s", e.Method, e.URL, e.Status, e.Body)
}

func New(options Options) *Client {
	baseURL := strings.TrimRight(options.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	userAgent := options.UserAgent
	if userAgent == "" {
		userAgent = "gitcrawl"
	}
	pageDelay := options.PageDelay
	return &Client{
		httpClient: httpClient,
		baseURL:    baseURL,
		token:      options.Token,
		userAgent:  userAgent,
		pageDelay:  pageDelay,
	}
}

func (c *Client) GetRepo(ctx context.Context, owner, repo string, reporter Reporter) (map[string]any, error) {
	var out map[string]any
	if err := c.doJSON(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s", pathEscape(owner), pathEscape(repo)), nil, reporter, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) GetIssue(ctx context.Context, owner, repo string, number int, reporter Reporter) (map[string]any, error) {
	var out map[string]any
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", pathEscape(owner), pathEscape(repo), number)
	if err := c.doJSON(ctx, http.MethodGet, path, nil, reporter, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) GetPull(ctx context.Context, owner, repo string, number int, reporter Reporter) (map[string]any, error) {
	var out map[string]any
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", pathEscape(owner), pathEscape(repo), number)
	if err := c.doJSON(ctx, http.MethodGet, path, nil, reporter, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) ListRepositoryIssues(ctx context.Context, owner, repo string, options ListIssuesOptions, reporter Reporter) ([]map[string]any, error) {
	values := url.Values{}
	state := strings.TrimSpace(options.State)
	if state == "" {
		state = "open"
	}
	values.Set("state", state)
	values.Set("sort", "updated")
	values.Set("direction", "desc")
	values.Set("per_page", "100")
	if options.Since != "" {
		values.Set("since", options.Since)
	}
	path := fmt.Sprintf("/repos/%s/%s/issues?%s", pathEscape(owner), pathEscape(repo), values.Encode())
	return c.paginate(ctx, path, options.Limit, options.ExpectedTotal, reporter)
}

func (c *Client) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments?per_page=100", pathEscape(owner), pathEscape(repo), number)
	return c.paginate(ctx, path, 0, 0, reporter)
}

func (c *Client) ListPullReviews(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews?per_page=100", pathEscape(owner), pathEscape(repo), number)
	return c.paginate(ctx, path, 0, 0, reporter)
}

func (c *Client) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/comments?per_page=100", pathEscape(owner), pathEscape(repo), number)
	return c.paginate(ctx, path, 0, 0, reporter)
}

func (c *Client) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/files?per_page=100", pathEscape(owner), pathEscape(repo), number)
	return c.paginate(ctx, path, 0, 0, reporter)
}

func (c *Client) paginate(ctx context.Context, firstPath string, limit int, expectedItems int, reporter Reporter) ([]map[string]any, error) {
	var out []map[string]any
	nextPath := firstPath
	page := 0
	totalPages := 0
	if expectedItems > 0 {
		totalPages = (expectedItems + 99) / 100
	}
	for nextPath != "" {
		page++
		var rows []map[string]any
		resp, err := c.do(ctx, http.MethodGet, nextPath, nil, reporter)
		if err != nil {
			return nil, err
		}
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decode github page: %w", err)
		}
		_ = resp.Body.Close()
		if limit > 0 && len(out)+len(rows) > limit {
			rows = rows[:limit-len(out)]
		}
		out = append(out, rows...)
		linkHeader := resp.Header.Get("Link")
		if last := lastPage(linkHeader); last > totalPages {
			totalPages = last
		}
		if totalPages > 0 && page > totalPages {
			totalPages = page
		}
		if totalPages > 0 {
			reporter.Printf("[github] page %d/%d fetched count=%d accumulated=%d", page, totalPages, len(rows), len(out))
		} else {
			reporter.Printf("[github] page %d fetched count=%d accumulated=%d", page, len(rows), len(out))
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		nextPath = nextPage(linkHeader)
		if nextPath != "" && c.pageDelay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.pageDelay):
			}
		}
	}
	return out, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, body io.Reader, reporter Reporter, out any) error {
	resp, err := c.do(ctx, method, path, body, reporter)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode github response: %w", err)
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, reporter Reporter) (*http.Response, error) {
	fullURL := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", c.userAgent)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	reporter.Printf("[github] request %s %s", method, path)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github request: %w", err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return nil, &RequestError{Method: method, URL: path, Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
}

func nextPage(linkHeader string) string {
	for _, part := range strings.Split(linkHeader, ",") {
		sections := strings.Split(part, ";")
		if len(sections) < 2 {
			continue
		}
		if strings.TrimSpace(sections[1]) != `rel="next"` {
			continue
		}
		rawURL := strings.Trim(strings.TrimSpace(sections[0]), "<>")
		parsed, err := url.Parse(rawURL)
		if err != nil {
			return ""
		}
		if parsed.RawQuery == "" {
			return parsed.Path
		}
		return parsed.Path + "?" + parsed.RawQuery
	}
	return ""
}

func lastPage(linkHeader string) int {
	for _, part := range strings.Split(linkHeader, ",") {
		sections := strings.Split(part, ";")
		if len(sections) < 2 {
			continue
		}
		if strings.TrimSpace(sections[1]) != `rel="last"` {
			continue
		}
		rawURL := strings.Trim(strings.TrimSpace(sections[0]), "<>")
		parsed, err := url.Parse(rawURL)
		if err != nil {
			return 0
		}
		page, _ := strconv.Atoi(parsed.Query().Get("page"))
		return page
	}
	return 0
}

func pathEscape(value string) string {
	return url.PathEscape(value)
}

func (r Reporter) Printf(format string, args ...any) {
	if r != nil {
		r(fmt.Sprintf(format, args...))
	}
}

func intValue(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case json.Number:
		parsed, _ := strconv.Atoi(string(typed))
		return parsed
	default:
		return 0
	}
}
