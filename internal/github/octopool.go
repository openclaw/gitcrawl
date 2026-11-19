package github

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NewOctopool uses the existing caller-token relay contract. The relay owns
// provider credentials and quota; this client never falls back to GitHub.
func NewOctopool(endpoint, pool, token, repository string) (*Client, error) {
	if token == "" {
		return nil, errors.New("--octopool requires OCTOPOOL_TOKEN for the existing caller session")
	}
	if endpoint == "" {
		endpoint = "https://octopool.dev"
	}
	if pool == "" {
		pool = "maintainers"
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("OCTOPOOL_URL must be an HTTPS base URL without credentials, query or fragment")
	}
	loopback := u.Hostname() == "localhost" || net.ParseIP(u.Hostname()).IsLoopback()
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, errors.New("OCTOPOOL_URL requires HTTPS; HTTP is supported only on loopback")
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	transport := &octopoolTransport{
		endpoint: strings.TrimRight(u.String(), "/") + "/v1/github/request",
		pool:     pool, token: token, repositoryPath: "/repos/" + repository,
		client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: noRedirect},
	}
	return New(Options{HTTPClient: &http.Client{
		Transport: transport, Timeout: 30 * time.Second, CheckRedirect: noRedirect,
	}}), nil
}

type octopoolTransport struct {
	endpoint, pool, token, repositoryPath string
	client                                *http.Client
	repositoryMu                          sync.RWMutex
	repositoryID                          string
}

type octopoolReadBudgetKey struct{}
type octopoolReadBudget struct{ posts int }

// REST's outer content retry loop shares this allowance. A storage rejection
// followed by a 502 must not multiply the three-post budget. Callers admit only
// repository GETs or the internally constructed conversation GraphQL query.
func (t *octopoolTransport) read(request *http.Request) (*http.Response, error) {
	ctx := request.Context()
	budget, _ := ctx.Value(octopoolReadBudgetKey{}).(*octopoolReadBudget)
	if budget == nil {
		budget = &octopoolReadBudget{}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		budget.posts++
		response, err := t.client.Do(request)
		if err != nil || response.StatusCode != http.StatusFailedDependency {
			return response, err
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, (16<<10)+1))
		response.Body.Close()
		if readErr != nil {
			body = nil
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		if readErr != nil || octopoolRejectionReason(body) != "relay_storage_unavailable" || budget.posts >= 3 || request.GetBody == nil {
			return response, nil
		}
		wait := 250 * time.Millisecond * time.Duration(1<<(budget.posts-1))
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= wait {
			return response, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		request = request.Clone(ctx)
		request.Body, err = request.GetBody()
		if err != nil {
			return nil, err
		}
	}
}

// Retain the HTTP failure while allowing pagination to distinguish the relay's
// exact size rejection from other 424 responses or transport failures.
type octopoolResponseTooLargeError struct{ *RequestError }

func (e *octopoolResponseTooLargeError) Unwrap() error { return e.RequestError }

// Preserve transport classification without exposing private URLs or diagnostics.
type octopoolTransportError struct{ cause error }

func (e *octopoolTransportError) Unwrap() error { return e.cause }
func (e *octopoolTransportError) Error() string {
	return "Octopool relay request failed: " + transportFailureReason(e.cause) + "; no direct GitHub fallback"
}

func (t *octopoolTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	path := req.URL.EscapedPath()
	t.repositoryMu.RLock()
	repositoryID := t.repositoryID
	t.repositoryMu.RUnlock()
	if repositoryID != "" {
		numericPath := "/repositories/" + repositoryID
		if path == numericPath || strings.HasPrefix(path, numericPath+"/") {
			path = t.repositoryPath + strings.TrimPrefix(path, numericPath)
		}
	}
	if req.Method != http.MethodGet || req.URL.Scheme != "https" || req.URL.Host != "api.github.com" || req.URL.User != nil ||
		(path != t.repositoryPath && !strings.HasPrefix(path, t.repositoryPath+"/")) {
		return nil, errors.New("Octopool sync supports only REST reads of the selected repository")
	}
	payload, err := json.Marshal(struct {
		Pool    string            `json:"pool"`
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Query   url.Values        `json:"query"`
		Headers map[string]string `json:"headers"`
	}{t.pool, req.Method, path, req.URL.Query(), map[string]string{
		"accept": req.Header.Get("Accept"), "x-github-api-version": req.Header.Get("X-GitHub-Api-Version"),
		"cache-control": "max-age=0",
	}})
	if err != nil {
		return nil, err
	}
	relayRequest, err := http.NewRequestWithContext(req.Context(), http.MethodPost, t.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.New("invalid Octopool request")
	}
	relayRequest.Header.Set("Authorization", "Bearer "+t.token)
	relayRequest.Header.Set("Content-Type", "application/json")
	response, err := t.read(relayRequest)
	if err != nil {
		return nil, &octopoolTransportError{cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		// Preserve status/backoff while exposing only known, content-free diagnostics.
		body, reason := octopoolRejection(response)
		if response.StatusCode == http.StatusFailedDependency && reason == "github_response_too_large" {
			return nil, &octopoolResponseTooLargeError{&RequestError{
				Method: req.Method, URL: req.URL.RequestURI(), Status: response.StatusCode,
				Body: string(body), Headers: response.Header,
			}}
		}
		return octopoolResponse(req, response.StatusCode, response.Header, body), nil
	}
	const maxEnvelopeBytes = 8 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, maxEnvelopeBytes+1))
	if err != nil {
		return nil, errors.New("read Octopool response failed")
	}
	if len(data) > maxEnvelopeBytes {
		return nil, errors.New("Octopool response exceeds 8 MiB")
	}
	var envelope struct {
		Status       int               `json:"status"`
		Headers      map[string]string `json:"headers"`
		Body         json.RawMessage   `json:"body"`
		BodyEncoding string            `json:"body_encoding"`
		Relay        struct {
			Cache   string `json:"cache"`
			StaleOK bool   `json:"stale_ok"`
		} `json:"relay"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Status < 200 || envelope.Status > 599 {
		return nil, errors.New("invalid Octopool response envelope")
	}
	if envelope.Relay.StaleOK || envelope.Relay.Cache == "stale" {
		return nil, errors.New("Octopool returned stale data; sync remains incomplete")
	}
	body, err := decodeOctopoolBody(envelope.Body, envelope.BodyEncoding)
	if err != nil {
		return nil, err
	}
	if path == t.repositoryPath && envelope.Status == http.StatusOK {
		var repository struct {
			ID json.Number `json:"id"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		if err := decoder.Decode(&repository); err != nil {
			return nil, errors.New("invalid Octopool repository identity")
		}
		id, err := strconv.ParseInt(string(repository.ID), 10, 64)
		if err != nil || id <= 0 {
			return nil, errors.New("invalid Octopool repository identity")
		}
		t.repositoryMu.Lock()
		if t.repositoryID != "" && t.repositoryID != string(repository.ID) {
			t.repositoryMu.Unlock()
			return nil, errors.New("Octopool repository identity changed")
		}
		t.repositoryID = string(repository.ID)
		t.repositoryMu.Unlock()
	}
	headers := make(http.Header)
	for key, value := range envelope.Headers {
		headers.Set(key, value)
	}
	return octopoolResponse(req, envelope.Status, headers, body), nil
}

// Only fixed fallback reason literals from Octopool's error contract may reach
// logs. Messages, request IDs, arbitrary details and future codes stay private.
func octopoolRejection(response *http.Response) ([]byte, string) {
	const generic = "Octopool relay rejected the request; no direct GitHub fallback"
	if response.StatusCode != http.StatusFailedDependency {
		return []byte(generic), ""
	}
	const limit = 16 << 10
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || len(body) > limit {
		return []byte(generic), ""
	}
	if reason := octopoolRejectionReason(body); reason != "" {
		return []byte(generic + " (fallback_local: " + reason + ")"), reason
	}
	return []byte(generic), ""
}

func octopoolRejectionReason(body []byte) string {
	if len(body) > 16<<10 {
		return ""
	}
	var failure struct {
		Error struct {
			Code    string `json:"code"`
			Details struct {
				Reason string `json:"reason"`
			} `json:"details"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &failure) != nil || failure.Error.Code != "fallback_local" {
		return ""
	}
	reason := failure.Error.Details.Reason
	switch reason {
	case "github_response_too_large", "identities_cooling_down", "logs_denied", "no_identity",
		"owner_denied", "relay_overloaded", "repo_not_public", "repo_public_check_failed",
		"route_denied", "search_denied", "relay_storage_unavailable", "github_identity_unauthorized",
		"github_rate_limited", "github_identity_depleted", "github_identity_forbidden",
		"github_app_repo_token_unavailable", "local_credentials_required", "web_only_unavailable",
		"pagination_exhausted":
		return reason
	default:
		return ""
	}
}

func decodeOctopoolBody(raw json.RawMessage, encoding string) ([]byte, error) {
	body := []byte(raw)
	var err error
	switch encoding {
	case "json":
		if len(body) == 0 {
			return nil, errors.New("Octopool JSON body is missing")
		}
	case "text", "base64":
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, errors.New("invalid Octopool body encoding")
		}
		body = []byte(text)
		if encoding == "base64" {
			body, err = base64.StdEncoding.DecodeString(text)
			if err != nil {
				return nil, errors.New("invalid Octopool base64 body")
			}
		}
	default:
		return nil, errors.New("unsupported Octopool body encoding")
	}
	return body, nil
}

func octopoolResponse(req *http.Request, status int, headers http.Header, body []byte) *http.Response {
	kept := make(http.Header)
	for _, name := range []string{"Content-Type", "Link", "ETag", "Last-Modified", "Retry-After", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset", "X-RateLimit-Resource", "X-GitHub-Request-Id"} {
		if value := headers.Get(name); value != "" {
			kept.Set(name, value)
		}
	}
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: kept,
		Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: req}
}
