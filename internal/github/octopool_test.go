package github

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOctopoolPreservesProviderErrorHeadersAndScope(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"status":403,"headers":{"retry-after":"0","x-ratelimit-remaining":"4","x-github-request-id":"fixture-request","etag":"fixture-etag"},"body":{"message":"fixture denial"},"body_encoding":"json","relay":{"cache":"miss"}}`)
	}))
	defer server.Close()
	client, err := NewOctopool(server.URL, "fixture", "fixture-caller", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetRepo(context.Background(), "fixture", "repo", nil)
	var failure *RequestError
	if !errors.As(err, &failure) || failure.Status != 403 {
		t.Fatalf("lost upstream status: %v", err)
	}
	for name, want := range map[string]string{"Retry-After": "0", "X-RateLimit-Remaining": "4", "X-GitHub-Request-Id": "fixture-request", "ETag": "fixture-etag"} {
		if failure.Headers.Get(name) != want {
			t.Errorf("lost %s", name)
		}
	}
	_, err = client.GetRepo(context.Background(), "fixture", "other", nil)
	if err == nil || !strings.Contains(err.Error(), "selected repository") || calls.Load() != 1 {
		t.Fatalf("scope escaped: calls=%d error=%v", calls.Load(), err)
	}
}

func TestOctopoolRejectsCredentialBearingOrInsecureEndpoints(t *testing.T) {
	for _, endpoint := range []string{"http://example.test", "https://user:fixture@example.test", "https://example.test?token=fixture", "https://example.test#fragment"} {
		if _, err := NewOctopool(endpoint, "fixture", "fixture-caller", "fixture/repo"); err == nil {
			t.Errorf("accepted unsafe endpoint")
		}
	}
}
