package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
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

func TestOctopoolNumericPaginationIsBoundToVerifiedRepository(t *testing.T) {
	var paths []string
	var pages [][]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path  string              `json:"path"`
			Query map[string][]string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		paths = append(paths, request.Path)
		pages = append(pages, request.Query["page"])
		body := `[]`
		headers := `{}`
		if request.Path == "/repos/fixture/repo" {
			body = `{"id":123}`
		} else if len(paths) == 2 {
			headers = `{"link":"<https://api.github.com/repositories/123/issues/7/comments?page=2>; rel=\"next\""}`
		}
		_, _ = fmt.Fprintf(w, `{"status":200,"body_encoding":"json","body":%s,"headers":%s,"relay":{"cache":"miss"}}`, body, headers)
	}))
	defer server.Close()
	client, err := NewOctopool(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.GetRepo(context.Background(), "fixture", "repo", nil); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ListIssueComments(context.Background(), "fixture", "repo", 7, nil); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 || paths[2] != "/repos/fixture/repo/issues/7/comments" {
		t.Fatalf("pagination not normalized: %v", paths)
	}
	if len(pages[2]) != 1 || pages[2][0] != "2" {
		t.Fatalf("pagination cursor lost: %v", pages)
	}
	transport := client.httpClient.Transport.(*octopoolTransport)
	for _, path := range []string{"/repositories/124/issues/7/comments", "/repositories/1234/issues/7/comments", "/repositories/123%2fissues/7/comments"} {
		request, _ := http.NewRequest(http.MethodGet, "https://api.github.com"+path, nil)
		if _, err = transport.RoundTrip(request); err == nil {
			t.Fatalf("accepted foreign repository path %s", path)
		}
	}
	if len(paths) != 3 {
		t.Fatal("foreign numeric path reached relay")
	}
}

func TestOctopoolRESTTransportPreservesCauseWithoutPrivateDiagnostics(t *testing.T) {
	for _, test := range []struct {
		cause  error
		reason string
	}{
		{context.Canceled, "context canceled"},
		{context.DeadlineExceeded, "context deadline exceeded"},
		{&net.OpError{Op: "private-operation", Net: "private-network", Err: os.ErrDeadlineExceeded}, "network timeout"},
		{syscall.ECONNRESET, syscall.ECONNRESET.Error()},
		{syscall.EPIPE, syscall.EPIPE.Error()},
		{io.EOF, "EOF"},
		{io.ErrUnexpectedEOF, "unexpected EOF"},
		{errors.New("private-token-and-response"), "transport failure (*url.Error)"},
	} {
		cause := test.cause
		calls := 0
		transport := &octopoolTransport{
			endpoint: "https://private.invalid/relay", pool: "fixture", token: "private-token",
			repositoryPath: "/repos/fixture/repo",
			client: &http.Client{Transport: contentRetryTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, &url.Error{Op: "Post", URL: "https://private.invalid/private-token", Err: cause}
			})},
		}
		request, err := http.NewRequest(http.MethodGet, "https://api.github.com/repos/fixture/repo", nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = transport.RoundTrip(request)
		wantCalls := 1
		if errors.Is(cause, syscall.ECONNRESET) {
			wantCalls = 3
		}
		if calls != wantCalls {
			t.Fatalf("transport attempts=%d want=%d", calls, wantCalls)
		}
		if !errors.Is(err, cause) {
			t.Fatalf("lost transport cause: %v", err)
		}
		var want, got net.Error
		if errors.As(cause, &want) && (!errors.As(err, &got) || got.Timeout() != want.Timeout()) {
			t.Fatalf("lost network classification: %v", err)
		}
		if want := "Octopool relay request failed: " + test.reason + "; no direct GitHub fallback"; err.Error() != want {
			t.Fatalf("transport diagnosis: got %q, want %q", err, want)
		}
		for _, secret := range []string{"private.invalid", "private-token", "private-operation", "private-network"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("private diagnostics exposed: %s", err)
			}
		}
	}
}

func TestConnectionResetPlatformCodes(t *testing.T) {
	for _, test := range []struct {
		cause error
		want  bool
	}{
		{syscall.ECONNRESET, true},
		{syscall.Errno(10054), runtime.GOOS == "windows"},
		{syscall.EPIPE, false},
		{context.Canceled, false},
		{errors.New("connection reset by peer"), false},
	} {
		if got := isConnectionReset(fmt.Errorf("wrapped: %w", test.cause)); got != test.want {
			t.Fatalf("cause %v: reset=%t want=%t", test.cause, got, test.want)
		}
	}
}
