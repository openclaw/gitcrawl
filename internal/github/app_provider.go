package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// NewAppAcquisition shares the existing REST and GraphQL acquisition readers.
// The key remains with the mint supervisor; this process cannot refresh tokens.
func NewAppAcquisition(ctx context.Context, token AcquisitionToken) (*Client, *OctopoolConversations, error) {
	return newAppAcquisition(ctx, token, http.DefaultTransport)
}

func newAppAcquisition(ctx context.Context, token AcquisitionToken, base http.RoundTripper) (*Client, *OctopoolConversations, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, nil, errors.New("App acquisition requires an invocation deadline")
	}
	if err := token.validate(token.Repository, deadline); err != nil {
		return nil, nil, err
	}
	transport := &appReadTransport{base: base, repositoryPath: "/repos/" + token.Repository, repositoryID: strconv.FormatInt(token.RepositoryID, 10)}
	client := New(Options{HTTPClient: &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: appNoRedirect},
		GraphQLResponseLimit: 3 << 20, RateLimitReserve: 1,
		TokenProvider: func(context.Context) (string, error) {
			if !token.ExpiresAt.After(time.Now().Add(time.Minute)) {
				return "", errors.New("installation token expired; mint a new token between cycles")
			}
			return token.Token, nil
		}})
	client.reserve.percent = 50
	// Current bounded acquisition queries have at most 50 parent aliases and
	// bounded child connections. Reserve 100 points before each GraphQL read.
	client.graphQLRequestCost = 100
	var accessible struct {
		TotalCount   int             `json:"total_count"`
		Repositories []appRepository `json:"repositories"`
	}
	if err := client.doJSON(ctx, http.MethodGet, "/installation/repositories?per_page=2", nil, nil, &accessible); err != nil {
		return nil, nil, err
	}
	app := AcquisitionApp{token.AppID, token.InstallationID, token.RepositoryID, token.Repository}
	if accessible.TotalCount != 1 || len(accessible.Repositories) != 1 || !accessible.Repositories[0].matches(app) {
		return nil, nil, errors.New("installation token is not restricted to the admitted public repository")
	}
	owner, repo, _ := strings.Cut(token.Repository, "/")
	if _, err := client.GetRepo(ctx, owner, repo, nil); err != nil {
		return nil, nil, err
	}
	for _, resource := range []string{"core", "graphql"} {
		err := client.reserve.beforeRequest(resource, 100)
		var expired *rateLimitStatusExpiredError
		if resource == "core" && errors.As(err, &expired) {
			err = client.refreshExpiredAppCoreQuota(ctx, nil, token.Token, 100)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	graph := &OctopoolConversations{Client: client, repository: token.Repository, issues: map[int]map[string]any{}, conversations: map[int]HistoryItem{}}
	return client, graph, nil
}

// The REST reset can fall between the quota probe and the reservation.
// Refresh the same credential once before admitting the protected request.
func (c *Client) refreshExpiredAppCoreQuota(ctx context.Context, reporter Reporter, token string, cost int) error {
	_, probeToken, err := c.getRateLimits(ctx, reporter, &token)
	if err != nil {
		return requestFailureAt("rest_preflight", "", fmt.Errorf("refresh GitHub rate limit status: %w", err))
	}
	currentToken, err := c.requestToken(ctx)
	if err != nil {
		return err
	}
	if probeToken != token || currentToken != token {
		return requestFailureAt("dispatch_guard", "credential_changed", errors.New("GitHub token changed during rate limit reservation"))
	}
	return c.reserve.beforeRequest("core", cost)
}

// No custom host, redirect, arbitrary method or second repository can receive
// the token, including provider pagination URLs. GraphQL is only called by the
// fixed acquisition query readers and is restricted server-side to one repo.
type appReadTransport struct {
	base                         http.RoundTripper
	repositoryPath, repositoryID string
}

func (t *appReadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	path := t.repositoryResource(req.URL.EscapedPath())
	read := req.Method == http.MethodGet && (path == "/rate_limit" || path == "/installation/repositories" || path == t.repositoryPath || strings.HasPrefix(path, t.repositoryPath+"/"))
	graph := req.Method == http.MethodPost && path == "/graphql"
	if req.URL.Scheme != "https" || req.URL.Host != "api.github.com" || req.URL.User != nil || req.URL.Fragment != "" || (!read && !graph) {
		return nil, errors.New("App acquisition supports only selected-repository GitHub reads")
	}
	response, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, errors.New("GitHub App read transport failed")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		response.Body.Close()
		response.Body = io.NopCloser(strings.NewReader("GitHub App read rejected; check installation scope, expiry and quota"))
	}
	return response, nil
}

func (t *appReadTransport) repositoryResource(path string) string {
	prefix := "/repositories/" + t.repositoryID
	if path == prefix || strings.HasPrefix(path, prefix+"/") {
		return t.repositoryPath + strings.TrimPrefix(path, prefix)
	}
	return path
}

// The shared GraphQL decoder retains provider prose for other consumers. This
// unattended path emits only local labels, while preserving actionable types.
func safeAppGraphQLError(err error) error {
	var reserve *RateLimitReserveError
	if errors.As(err, &reserve) {
		return reserve
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, cause) {
			return cause
		}
	}
	var failure *HistoryFailure
	if errors.As(err, &failure) {
		var cause error = errors.New("GitHub App GraphQL response rejected")
		if IsRateLimited(err) {
			cause = graphQLRateLimitedError{cause}
		}
		return &HistoryFailure{Cause: cause, Stage: failure.Stage, Number: failure.Number, Evidence: failure.Evidence}
	}
	var response *RequestError
	if errors.As(err, &response) {
		return &RequestError{Method: http.MethodPost, URL: "/graphql", Status: response.Status, Headers: response.Headers}
	}
	return errors.New("GitHub App GraphQL request failed")
}
