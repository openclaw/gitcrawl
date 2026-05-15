package github

import (
	"bytes"
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
	graphQLURL string
	token      string
	userAgent  string
	pageDelay  time.Duration
	rateLimit  RateLimitObserver
}

type Options struct {
	Token      string
	BaseURL    string
	UserAgent  string
	HTTPClient *http.Client
	PageDelay  time.Duration
	RateLimit  RateLimitObserver
}

type RateLimitObserver func(RateLimitSnapshot)

type RateLimitSnapshot struct {
	Host      string
	Limit     int
	Remaining int
	ResetAt   time.Time
	Resource  string
}

type ListIssuesOptions struct {
	State         string
	Since         string
	Limit         int
	ExpectedTotal int
}

type ListWorkflowRunsOptions struct {
	Branch  string
	HeadSHA string
	Limit   int
}

type RequestError struct {
	Method  string
	URL     string
	Status  int
	Body    string
	Headers http.Header
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
	return &Client{
		httpClient: httpClient,
		baseURL:    baseURL,
		graphQLURL: graphQLURLForBaseURL(baseURL),
		token:      options.Token,
		userAgent:  userAgent,
		pageDelay:  options.PageDelay,
		rateLimit:  options.RateLimit,
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

func (c *Client) ListPullCommits(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/commits?per_page=100", pathEscape(owner), pathEscape(repo), number)
	return c.paginate(ctx, path, 0, 0, reporter)
}

func (c *Client) ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter Reporter) ([]map[string]any, error) {
	path := fmt.Sprintf("/repos/%s/%s/commits/%s/check-runs?per_page=100", pathEscape(owner), pathEscape(repo), pathEscape(ref))
	return c.paginateEnvelope(ctx, path, 0, 0, "check_runs", reporter)
}

func (c *Client) ListWorkflowRuns(ctx context.Context, owner, repo string, options ListWorkflowRunsOptions, reporter Reporter) ([]map[string]any, error) {
	values := url.Values{}
	values.Set("per_page", "100")
	if options.Branch != "" {
		values.Set("branch", options.Branch)
	}
	if options.HeadSHA != "" {
		values.Set("head_sha", options.HeadSHA)
	}
	path := fmt.Sprintf("/repos/%s/%s/actions/runs?%s", pathEscape(owner), pathEscape(repo), values.Encode())
	return c.paginateEnvelope(ctx, path, options.Limit, 0, "workflow_runs", reporter)
}

func (c *Client) paginate(ctx context.Context, firstPath string, limit int, expectedItems int, reporter Reporter) ([]map[string]any, error) {
	return c.paginatePages(ctx, firstPath, limit, expectedItems, reporter, func(resp *http.Response) ([]map[string]any, error) {
		var rows []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			return nil, fmt.Errorf("decode github page: %w", err)
		}
		return rows, nil
	})
}

func (c *Client) paginateEnvelope(ctx context.Context, firstPath string, limit int, expectedItems int, field string, reporter Reporter) ([]map[string]any, error) {
	return c.paginatePages(ctx, firstPath, limit, expectedItems, reporter, func(resp *http.Response) ([]map[string]any, error) {
		var payload map[string]json.RawMessage
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, fmt.Errorf("decode github page: %w", err)
		}
		raw, ok := payload[field]
		if !ok {
			return nil, fmt.Errorf("decode github page: missing %q", field)
		}
		var rows []map[string]any
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, fmt.Errorf("decode github page %q: %w", field, err)
		}
		return rows, nil
	})
}

func (c *Client) paginatePages(ctx context.Context, firstPath string, limit int, expectedItems int, reporter Reporter, decode func(*http.Response) ([]map[string]any, error)) ([]map[string]any, error) {
	var out []map[string]any
	nextPath := firstPath
	page := 0
	totalPages := 0
	if expectedItems > 0 {
		totalPages = (expectedItems + 99) / 100
	}
	for nextPath != "" {
		page++
		resp, err := c.do(ctx, http.MethodGet, nextPath, nil, reporter)
		if err != nil {
			return nil, err
		}
		rows, err := decode(resp)
		if err != nil {
			_ = resp.Body.Close()
			return nil, err
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
		nextPath = nextPage(linkHeader, c.baseURL)
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
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, fmt.Errorf("read github request body: %w", err)
		}
	}
	bodyReader := func() io.Reader {
		if body == nil {
			return nil
		}
		return bytes.NewReader(bodyBytes)
	}
	resp, err := c.doOnce(ctx, method, path, bodyReader(), reporter)
	if err == nil {
		return resp, nil
	}
	wait, ok := rateLimitWait(err)
	if !ok {
		return nil, err
	}
	reporter.Printf("[github] rate-limit retry wait=%s", wait.Round(time.Second))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	return c.doOnce(ctx, method, path, bodyReader(), reporter)
}

func (c *Client) doOnce(ctx context.Context, method, path string, body io.Reader, reporter Reporter) (*http.Response, error) {
	fullURL := path
	if !isAbsoluteURL(path) {
		fullURL = c.baseURL + path
	}
	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", c.userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	reporter.Printf("[github] request %s %s", method, path)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github request: %w", err)
	}
	c.observeRateLimit(resp.Header)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return nil, &RequestError{
		Method:  method,
		URL:     path,
		Status:  resp.StatusCode,
		Body:    strings.TrimSpace(string(data)),
		Headers: resp.Header,
	}
}

func isAbsoluteURL(value string) bool {
	return strings.HasPrefix(value, "https://") || strings.HasPrefix(value, "http://")
}

func graphQLURLForBaseURL(baseURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return trimmed + "/graphql"
	}
	path := strings.TrimRight(parsed.Path, "/")
	if strings.HasSuffix(path, "/api/v3") {
		parsed.Path = strings.TrimSuffix(path, "/api/v3") + "/api/graphql"
	} else {
		parsed.Path = path + "/graphql"
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func (c *Client) observeRateLimit(header http.Header) {
	if c.rateLimit == nil {
		return
	}
	remaining, err := strconv.Atoi(strings.TrimSpace(header.Get("X-RateLimit-Remaining")))
	if err != nil {
		return
	}
	limit, _ := strconv.Atoi(strings.TrimSpace(header.Get("X-RateLimit-Limit")))
	var resetAt time.Time
	if raw := strings.TrimSpace(header.Get("X-RateLimit-Reset")); raw != "" {
		if secs, err := strconv.ParseInt(raw, 10, 64); err == nil {
			resetAt = time.Unix(secs, 0).UTC()
		}
	}
	c.rateLimit(RateLimitSnapshot{
		Host:      rateLimitHostForBaseURL(c.baseURL),
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   resetAt,
		Resource:  strings.TrimSpace(header.Get("X-RateLimit-Resource")),
	})
}

func rateLimitHostForBaseURL(baseURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Host == "" {
		return ""
	}
	if strings.EqualFold(parsed.Hostname(), "api.github.com") {
		return "github.com"
	}
	return parsed.Host
}

func rateLimitWait(err error) (time.Duration, bool) {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return 0, false
	}
	if reqErr.Status != http.StatusForbidden && reqErr.Status != http.StatusTooManyRequests {
		return 0, false
	}
	if v := strings.TrimSpace(reqErr.Headers.Get("Retry-After")); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second, true
		}
	}
	if reqErr.Headers.Get("X-RateLimit-Remaining") != "0" {
		return 0, false
	}
	secs, err := strconv.ParseInt(strings.TrimSpace(reqErr.Headers.Get("X-RateLimit-Reset")), 10, 64)
	if err != nil {
		return 0, false
	}
	if wait := time.Until(time.Unix(secs, 0)); wait > 0 {
		return wait, true
	}
	return time.Second, true
}

func nextPage(linkHeader, baseURL string) string {
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
		return requestPathFromLink(parsed, baseURL)
	}
	return ""
}

func requestPathFromLink(parsed *url.URL, baseURL string) string {
	path := parsed.EscapedPath()
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err == nil && parsed.Host != "" && strings.EqualFold(parsed.Host, base.Host) {
		basePath := strings.TrimRight(base.EscapedPath(), "/")
		if basePath != "" && (path == basePath || strings.HasPrefix(path, basePath+"/")) {
			path = strings.TrimPrefix(path, basePath)
			if path == "" {
				path = "/"
			}
		}
	}
	if parsed.RawQuery == "" {
		return path
	}
	return path + "?" + parsed.RawQuery
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
