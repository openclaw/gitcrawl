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
	"strings"
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
}

func (t *octopoolTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.URL.Scheme != "https" || req.URL.Host != "api.github.com" || req.URL.User != nil ||
		(req.URL.Path != t.repositoryPath && !strings.HasPrefix(req.URL.Path, t.repositoryPath+"/")) {
		return nil, errors.New("Octopool sync supports only REST reads of the selected repository")
	}
	payload, err := json.Marshal(struct {
		Pool    string            `json:"pool"`
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Query   url.Values        `json:"query"`
		Headers map[string]string `json:"headers"`
	}{t.pool, req.Method, req.URL.EscapedPath(), req.URL.Query(), map[string]string{
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
	response, err := t.client.Do(relayRequest)
	if err != nil {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		return nil, errors.New("Octopool relay request failed; no direct GitHub fallback")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		// Relay diagnostics are not GitHub content and may include private data.
		// Preserve status/backoff for the native retry owner without logging them.
		return octopoolResponse(req, response.StatusCode, response.Header,
			[]byte("Octopool relay rejected the request; no direct GitHub fallback")), nil
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
	body := []byte(envelope.Body)
	switch envelope.BodyEncoding {
	case "json":
		if len(body) == 0 {
			return nil, errors.New("Octopool JSON body is missing")
		}
	case "text", "base64":
		var text string
		if err := json.Unmarshal(envelope.Body, &text); err != nil {
			return nil, errors.New("invalid Octopool body encoding")
		}
		body = []byte(text)
		if envelope.BodyEncoding == "base64" {
			body, err = base64.StdEncoding.DecodeString(text)
			if err != nil {
				return nil, errors.New("invalid Octopool base64 body")
			}
		}
	default:
		return nil, errors.New("unsupported Octopool body encoding")
	}
	headers := make(http.Header)
	for key, value := range envelope.Headers {
		headers.Set(key, value)
	}
	return octopoolResponse(req, envelope.Status, headers, body), nil
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
