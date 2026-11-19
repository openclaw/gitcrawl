package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const storageRejection = `{"error":{"code":"fallback_local","message":"private-canary","details":{"reason":"relay_storage_unavailable"}}}`

func octopoolRetryRead(t *testing.T, endpoint, protocol string) func(context.Context) error {
	t.Helper()
	client, err := NewOctopoolConversations(endpoint, "fixture", "fixture-caller", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if protocol == "REST" {
		return func(ctx context.Context) error {
			_, err := client.Client.ListPullReviews(ctx, "fixture", "repo", 7, nil)
			return err
		}
	}
	return func(ctx context.Context) error {
		_, err := client.PrepareRows(ctx, []map[string]any{{"number": 7, "id": 123,
			"node_id": "PR_fixture", "updated_at": "2026-01-01T00:00:00Z", "pull_request": map[string]any{}}}, nil)
		return err
	}
}

func TestOctopoolStorageRetryReadPaths(t *testing.T) {
	for _, protocol := range []string{"REST", "GraphQL"} {
		for _, sequence := range [][]int{{424, 424, 200}, {424, 424, 424}, {424, 403}, {424, 502, 424}, {502, 424, 424},
			{0, 200}, {0, 0, 200}, {0, 0, 0}, {0, 424, 200}, {424, 0, 502}, {502, 0, 424}, {0, 403}} {
			t.Run(fmt.Sprintf("%s/%v", protocol, sequence), func(t *testing.T) {
				var calls atomic.Int32
				var first []byte
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					call := int(calls.Add(1))
					body, _ := io.ReadAll(r.Body)
					if call == 1 {
						first = body
					} else if !bytes.Equal(first, body) {
						t.Error("retry changed the relay envelope")
					}
					if r.Method != http.MethodPost || r.URL.Path != "/v1/github/request" || r.Header.Get("Authorization") != "Bearer fixture-caller" {
						t.Error("retry changed relay or caller")
					}
					if call > len(sequence) {
						t.Error("retry exceeded the shared three-post allowance")
						w.WriteHeader(400)
						return
					}
					status := sequence[call-1]
					if status == 0 {
						resetOctopoolConnection(t, w)
						return
					}
					if status != 200 {
						w.Header().Set("X-RateLimit-Remaining", "17")
						w.WriteHeader(status)
						_, _ = io.WriteString(w, storageRejection)
						return
					}
					var result any = []any{}
					if protocol == "GraphQL" {
						result = map[string]any{"data": map[string]any{"repository": map[string]any{"n0": conversationFixture()}}}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": result,
						"relay": map[string]any{"route_kind": "graphql_read", "cache": "miss"}, "identity": map[string]any{"kind": "github_app"}})
				}))
				defer server.Close()
				err := octopoolRetryRead(t, server.URL, protocol)(context.Background())
				wantCalls := len(sequence)
				if protocol == "GraphQL" {
					for i, status := range sequence {
						if status != 424 && status != 0 {
							wantCalls = i + 1
							break
						}
					}
				}
				if int(calls.Load()) != wantCalls {
					t.Fatalf("calls=%d want=%d error=%v", calls.Load(), wantCalls, err)
				}
				resetOnly := wantCalls == 3
				for _, status := range sequence[:wantCalls] {
					resetOnly = resetOnly && status == 0
				}
				if IsExhaustedOctopoolReset(err) != resetOnly {
					t.Fatalf("exhausted reset classification=%t want=%t error=%v", IsExhaustedOctopoolReset(err), resetOnly, err)
				}
				if sequence[wantCalls-1] == 200 {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					if err == nil || strings.Contains(err.Error(), "private-canary") {
						t.Fatalf("unsafe/missing failure: %v", err)
					}
					if sequence[wantCalls-1] == 0 && !isConnectionReset(err) {
						t.Fatalf("lost connection reset: %v", err)
					}
					if sequence[wantCalls-1] == 424 && !strings.Contains(err.Error(), "fallback_local: relay_storage_unavailable") {
						t.Fatalf("lost final typed rejection: %v", err)
					}
				}
			})
		}
	}
}

func TestOctopoolStorageRetryRejectsOtherReasons(t *testing.T) {
	for _, body := range []string{
		strings.ReplaceAll(storageRejection, "relay_storage_unavailable", "owner_denied"),
		strings.ReplaceAll(storageRejection, "relay_storage_unavailable", "private-unknown"),
		strings.ReplaceAll(storageRejection, "fallback_local", "other"),
		storageRejection + "trailing",
		storageRejection + strings.Repeat(" ", 16<<10),
	} {
		for _, protocol := range []string{"REST", "GraphQL"} {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(424)
				_, _ = io.WriteString(w, body)
			}))
			err := octopoolRetryRead(t, server.URL, protocol)(context.Background())
			server.Close()
			if calls.Load() != 1 || err == nil || strings.Contains(err.Error(), "private-") {
				t.Fatalf("%s retried or exposed unqualified rejection: calls=%d error=%v", protocol, calls.Load(), err)
			}
		}
	}
}

func TestOctopoolStorageRetryDeadline(t *testing.T) {
	for _, protocol := range []string{"REST", "GraphQL"} {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(424)
			_, _ = io.WriteString(w, storageRejection)
		}))
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := octopoolRetryRead(t, server.URL, protocol)(ctx)
		cancel()
		server.Close()
		if calls.Load() != 1 || err == nil || !strings.Contains(err.Error(), "relay_storage_unavailable") {
			t.Fatalf("%s extended the deadline or lost rejection: calls=%d error=%v", protocol, calls.Load(), err)
		}
	}
}

type cancelOnCloseBody struct {
	io.Reader
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error { b.cancel(); return nil }

func TestOctopoolStorageRetryClosesBodyAndHonorsCancellation(t *testing.T) {
	client, err := NewOctopoolConversations("https://relay.example", "fixture", "fixture-caller", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	client.relay.client.Transport = contentRetryTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 424, Header: http.Header{},
			Body: &cancelOnCloseBody{Reader: strings.NewReader(storageRejection), cancel: cancel}, Request: r}, nil
	})
	_, err = client.requestConversations(ctx, "query { viewer { login } }", nil, nil)
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation did not stop retry after closing body: calls=%d error=%v", calls, err)
	}
}

func resetOctopoolConnection(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	// Send a real TCP reset after reading the complete relay request.
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
		t.Error(err)
	}
	conn.Close()
}

func TestOctopoolResetHonorsDeadlineAndCancellation(t *testing.T) {
	for _, protocol := range []string{"REST", "GraphQL"} {
		for _, cancelDuringRequest := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel=%t", protocol, cancelDuringRequest), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					io.Copy(io.Discard, r.Body)
					if cancelDuringRequest {
						cancel()
					}
					resetOctopoolConnection(t, w)
				}))
				defer server.Close()
				err := octopoolRetryRead(t, server.URL, protocol)(ctx)
				if calls.Load() != 1 || err == nil {
					t.Fatalf("retried bounded request: calls=%d error=%v", calls.Load(), err)
				}
				if IsExhaustedOctopoolReset(err) {
					t.Fatal("incomplete retry budget classified as exhausted")
				}
				if cancelDuringRequest {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("lost cancellation: %v", err)
					}
				} else if !isConnectionReset(err) {
					t.Fatalf("lost reset when remaining deadline cannot fit backoff: %v", err)
				}
			})
		}
	}
}
