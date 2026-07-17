package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Reporter receives synchronous request progress. A reporter used by a
// reserve-guarded client must not call back into that same Client.
type Reporter func(message string)

type Client struct {
	httpClient *http.Client
	baseURL    string
	graphQLURL string
	token      string
	userAgent  string
	pageDelay  time.Duration
	rateLimit  RateLimitObserver
	reserve    *rateLimitReserve
}

type Options struct {
	Token      string
	BaseURL    string
	UserAgent  string
	HTTPClient *http.Client
	PageDelay  time.Duration
	RateLimit  RateLimitObserver
	// RateLimitReserve preserves a best-effort observed floor for the shared
	// token. Guarded requests refresh /rate_limit before dispatch so other token
	// consumers are observed, but unrelated consumers cannot be locked between
	// that probe and dispatch.
	RateLimitReserve  int
	InitialRateLimits []RateLimitSnapshot
}

// RateLimitObserver receives synchronous quota snapshots. An observer used by
// a reserve-guarded client must not call back into that same Client.
type RateLimitObserver func(RateLimitSnapshot)

type RateLimitSnapshot struct {
	Host      string
	Limit     int
	Remaining int
	ResetAt   time.Time
	Resource  string
}

type RateLimitReserveError struct {
	RateLimit RateLimitSnapshot
	Reserve   int
}

func (e *RateLimitReserveError) Error() string {
	return fmt.Sprintf(
		"github %s rate limit reserve %d reached with %d remaining",
		e.RateLimit.Resource,
		e.Reserve,
		e.RateLimit.Remaining,
	)
}

type rateLimitReserve struct {
	requestMu sync.Mutex
	mu        sync.Mutex
	reserve   int
	snapshots map[string]RateLimitSnapshot
}

type rateLimitRequestLockKey struct{}

type rateLimitStatusExpiredError struct {
	RateLimit RateLimitSnapshot
}

func (e *rateLimitStatusExpiredError) Error() string {
	return fmt.Sprintf("github %s rate limit status expired at %s", e.RateLimit.Resource, e.RateLimit.ResetAt.Format(time.RFC3339))
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
	client := &Client{
		httpClient: httpClient,
		baseURL:    baseURL,
		graphQLURL: graphQLURLForBaseURL(baseURL),
		token:      options.Token,
		userAgent:  userAgent,
		pageDelay:  options.PageDelay,
		rateLimit:  options.RateLimit,
	}
	if options.RateLimitReserve > 0 {
		client.reserve = newRateLimitReserve(options.RateLimitReserve, options.InitialRateLimits)
	}
	return client
}

func newRateLimitReserve(reserve int, initial []RateLimitSnapshot) *rateLimitReserve {
	guard := &rateLimitReserve{
		reserve:   reserve,
		snapshots: make(map[string]RateLimitSnapshot, len(initial)),
	}
	guard.replace(initial)
	return guard
}

func (r *rateLimitReserve) beforeRequest(resource string, cost int) error {
	if r == nil || cost <= 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot, ok := r.snapshots[resource]
	if !ok {
		return fmt.Errorf("github %s rate limit status unavailable; cannot preserve reserve %d", resource, r.reserve)
	}
	if !snapshot.ResetAt.IsZero() && !time.Now().UTC().Before(snapshot.ResetAt) {
		return &rateLimitStatusExpiredError{RateLimit: snapshot}
	}
	if snapshot.Remaining-cost < r.reserve {
		return &RateLimitReserveError{RateLimit: snapshot, Reserve: r.reserve}
	}
	snapshot.Remaining -= cost
	r.snapshots[resource] = snapshot
	return nil
}

func (r *rateLimitReserve) observe(snapshot RateLimitSnapshot) {
	if r == nil || strings.TrimSpace(snapshot.Resource) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshots[snapshot.Resource] = snapshot
}

func (r *rateLimitReserve) replace(snapshots []RateLimitSnapshot) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshots = make(map[string]RateLimitSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		if strings.TrimSpace(snapshot.Resource) != "" {
			r.snapshots[snapshot.Resource] = snapshot
		}
	}
}

func (r *rateLimitReserve) snapshot(resource string) (RateLimitSnapshot, bool) {
	if r == nil {
		return RateLimitSnapshot{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot, ok := r.snapshots[resource]
	return snapshot, ok
}

func (c *Client) GetRepo(ctx context.Context, owner, repo string, reporter Reporter) (map[string]any, error) {
	var out map[string]any
	if err := c.doJSON(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s", pathEscape(owner), pathEscape(repo)), nil, reporter, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) GetRateLimits(ctx context.Context, reporter Reporter) ([]RateLimitSnapshot, error) {
	if c.reserve != nil {
		lockedReserve, _ := ctx.Value(rateLimitRequestLockKey{}).(*rateLimitReserve)
		if lockedReserve != c.reserve {
			c.reserve.requestMu.Lock()
			defer c.reserve.requestMu.Unlock()
			ctx = context.WithValue(ctx, rateLimitRequestLockKey{}, c.reserve)
		}
	}
	var payload struct {
		Resources map[string]struct {
			Limit     int   `json:"limit"`
			Remaining int   `json:"remaining"`
			Reset     int64 `json:"reset"`
		} `json:"resources"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/rate_limit", nil, reporter, &payload); err != nil {
		return nil, err
	}
	host := rateLimitHostForBaseURL(c.baseURL)
	out := make([]RateLimitSnapshot, 0, len(payload.Resources))
	for resource, value := range payload.Resources {
		snapshot := RateLimitSnapshot{
			Host:      host,
			Limit:     value.Limit,
			Remaining: value.Remaining,
			Resource:  resource,
		}
		if value.Reset > 0 {
			snapshot.ResetAt = time.Unix(value.Reset, 0).UTC()
		}
		out = append(out, snapshot)
	}
	c.reserve.replace(out)
	if c.rateLimit != nil {
		for _, snapshot := range out {
			if snapshot.Resource == "core" {
				c.rateLimit(snapshot)
				break
			}
		}
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

func (c *Client) GetWorkflowRun(
	ctx context.Context,
	owner string,
	repo string,
	runID string,
	reporter Reporter,
) (map[string]any, error) {
	path := fmt.Sprintf(
		"/repos/%s/%s/actions/runs/%s",
		pathEscape(owner),
		pathEscape(repo),
		pathEscape(runID),
	)
	var run map[string]any
	if err := c.doJSON(ctx, http.MethodGet, path, nil, reporter, &run); err != nil {
		return nil, err
	}
	return run, nil
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
	resource, cost := c.requestRateLimit(method, fullURL)
	if c.reserve != nil {
		lockedReserve, _ := ctx.Value(rateLimitRequestLockKey{}).(*rateLimitReserve)
		if lockedReserve != c.reserve {
			c.reserve.requestMu.Lock()
			defer c.reserve.requestMu.Unlock()
			ctx = context.WithValue(ctx, rateLimitRequestLockKey{}, c.reserve)
		}
	}
	if c.reserve != nil && cost > 0 {
		if _, err := c.GetRateLimits(ctx, reporter); err != nil {
			return nil, fmt.Errorf("refresh GitHub rate limit status: %w", err)
		}
	}
	if err := c.reserve.beforeRequest(resource, cost); err != nil {
		var expired *rateLimitStatusExpiredError
		if !errors.As(err, &expired) {
			return nil, err
		}
		_, refreshErr := c.GetRateLimits(ctx, reporter)
		if refreshErr != nil {
			return nil, fmt.Errorf("refresh GitHub rate limit status: %w", refreshErr)
		}
		if err := c.reserve.beforeRequest(resource, cost); err != nil {
			return nil, err
		}
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
	resp, err := c.guardedHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("github request: %w", err)
	}
	responseResource, responseCost := c.requestRateLimit(resp.Request.Method, resp.Request.URL.String())
	if responseCost > 0 && !c.observeRateLimit(resp.Header, responseResource) {
		c.observeReservedRateLimit(responseResource)
	}
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

func (c *Client) guardedHTTPClient() *http.Client {
	if c.reserve == nil {
		return c.httpClient
	}
	client := *c.httpClient
	checkRedirect := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if checkRedirect != nil {
			if err := checkRedirect(req, via); err != nil {
				return err
			}
		}
		return http.ErrUseLastResponse
	}
	return &client
}

func (c *Client) requestRateLimit(method, fullURL string) (string, int) {
	if method == http.MethodPost && fullURL == c.graphQLURL {
		// The read-only GraphQL queries in this client each have a calculated cost of one point.
		return "graphql", 1
	}
	if method == http.MethodGet && fullURL == c.baseURL+"/rate_limit" {
		return "core", 0
	}
	return "core", 1
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

func (c *Client) observeRateLimit(header http.Header, fallbackResource string) bool {
	remaining, err := strconv.Atoi(strings.TrimSpace(header.Get("X-RateLimit-Remaining")))
	if err != nil {
		return false
	}
	limit, _ := strconv.Atoi(strings.TrimSpace(header.Get("X-RateLimit-Limit")))
	var resetAt time.Time
	if raw := strings.TrimSpace(header.Get("X-RateLimit-Reset")); raw != "" {
		if secs, err := strconv.ParseInt(raw, 10, 64); err == nil {
			resetAt = time.Unix(secs, 0).UTC()
		}
	}
	resource := strings.TrimSpace(header.Get("X-RateLimit-Resource"))
	if resource == "" {
		resource = fallbackResource
	}
	snapshot := RateLimitSnapshot{
		Host:      rateLimitHostForBaseURL(c.baseURL),
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   resetAt,
		Resource:  resource,
	}
	c.reserve.observe(snapshot)
	if c.rateLimit != nil {
		c.rateLimit(snapshot)
	}
	return true
}

func (c *Client) observeReservedRateLimit(resource string) {
	snapshot, ok := c.reserve.snapshot(resource)
	if !ok || c.rateLimit == nil {
		return
	}
	snapshot.Host = rateLimitHostForBaseURL(c.baseURL)
	c.rateLimit(snapshot)
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
