package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func sequenceTokenProvider(t *testing.T, tokens ...string) func(context.Context) (string, error) {
	t.Helper()
	i := 0
	return func(context.Context) (string, error) {
		if i >= len(tokens) {
			t.Fatal("unexpected credential invocation")
		}
		token := tokens[i]
		i++
		return token, nil
	}
}

func TestTokenProviderRefreshesRESTPagesAndGraphQL(t *testing.T) {
	var seen, observed []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.Header().Set("X-RateLimit-Remaining", "100")
		switch r.URL.Path {
		case "/pages":
			w.Header().Set("Link", `</second>; rel="next"`)
			fmt.Fprint(w, `[{"id":1}]`)
		case "/second":
			fmt.Fprint(w, `[{"id":2}]`)
		case "/graphql":
			fmt.Fprint(w, `{"data":{}}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := New(Options{
		BaseURL: server.URL, Token: "unused-static",
		TokenProvider: sequenceTokenProvider(t, "first", "second", "third"),
		RateLimit:     func(token string, _ RateLimitSnapshot) { observed = append(observed, token) },
	})
	rows, err := client.paginate(context.Background(), "/pages", 0, 0, nil)
	if err != nil || len(rows) != 2 {
		t.Fatalf("pages: count=%d err=%v", len(rows), err)
	}
	var result map[string]any
	if err := client.doJSON(context.Background(), http.MethodPost, client.graphQLURL, strings.NewReader(`{"query":"query { viewer { login } }"}`), nil, &result); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []string{"Bearer first", "Bearer second", "Bearer third"}) ||
		!reflect.DeepEqual(observed, []string{"first", "second", "third"}) {
		t.Fatalf("request/observation credentials differ: %v %v", seen, observed)
	}
}

func TestTokenProviderRefreshesAfterRateLimitWait(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		if len(seen) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `{"id":1}`)
	}))
	defer server.Close()
	client := New(Options{BaseURL: server.URL, TokenProvider: sequenceTokenProvider(t, "before", "after")})
	if _, err := client.GetRepo(context.Background(), "owner", "repo", nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []string{"Bearer before", "Bearer after"}) {
		t.Fatalf("credentials after wait: %v", seen)
	}
}

func TestTokenProviderRefreshesGraphQLPages(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		var request graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		page := len(seen)
		if page == 2 && request.Variables["cursor"] != "next" {
			t.Error("GraphQL cursor changed during credential rotation")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{
			"reviewThreads": map[string]any{
				"nodes":    []map[string]any{{"id": fmt.Sprint(page)}},
				"pageInfo": map[string]any{"hasNextPage": page == 1, "endCursor": "next"},
			},
		}}}})
	}))
	defer server.Close()
	client := New(Options{BaseURL: server.URL, TokenProvider: sequenceTokenProvider(t, "first", "second")})
	rows, err := client.ListPullReviewThreads(context.Background(), "owner", "repo", 1, nil)
	if err != nil || len(rows) != 2 || !reflect.DeepEqual(seen, []string{"Bearer first", "Bearer second"}) {
		t.Fatalf("GraphQL pages count=%d credentials=%v err=%v", len(rows), seen, err)
	}
}

func TestTokenProviderBindsQuotaToReplacementCredential(t *testing.T) {
	for _, tc := range []struct {
		name         string
		tokens       []string
		remaining    int
		wantRequests int
		wantError    string
	}{
		{"rotated", []string{"a", "b", "b"}, 11, 1, ""},
		{"new-token-low", []string{"a", "b", "b"}, 10, 0, "reserve"},
		{"unstable", []string{"a", "b", "c"}, 11, 0, "changed during"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var probes, requests, observed []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				if r.URL.Path == "/rate_limit" {
					probes = append(probes, token)
					remaining := 100
					if token == "b" {
						remaining = tc.remaining
					}
					writeRateLimits(t, w, time.Now().Add(time.Hour), remaining, remaining)
					return
				}
				requests = append(requests, token)
				fmt.Fprint(w, `{"id":1}`)
			}))
			defer server.Close()
			client := New(Options{
				BaseURL: server.URL, RateLimitReserve: 10,
				TokenProvider: sequenceTokenProvider(t, tc.tokens...),
				RateLimit:     func(token string, _ RateLimitSnapshot) { observed = append(observed, token) },
			})
			_, err := client.GetRepo(context.Background(), "owner", "repo", nil)
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("error = %v", err)
			}
			if !reflect.DeepEqual(probes, []string{"a", "b"}) || len(requests) != tc.wantRequests {
				t.Fatalf("probes=%v requests=%v", probes, requests)
			}
			if len(requests) > 0 && requests[0] != "b" || len(observed) < 2 || observed[1] != "b" {
				t.Fatalf("wrong quota identity: requests=%v observed=%v", requests, observed)
			}
		})
	}
}

func TestTokenProviderRejectsForeignOriginBeforeExecutionAndRedirects(t *testing.T) {
	calls := 0
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls++ }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer server.Close()
	client := New(Options{
		BaseURL:       server.URL,
		TokenProvider: func(context.Context) (string, error) { calls++; return "managed", nil },
		HTTPClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			t.Fatal("provider redirect bypassed dispatch boundary")
			return nil
		}},
	})
	var out map[string]any
	if err := client.doJSON(context.Background(), http.MethodGet, target.URL, nil, nil, &out); err == nil || calls != 0 {
		t.Fatalf("foreign request error=%v provider calls=%d", err, calls)
	}
	if _, err := client.GetRepo(context.Background(), "owner", "repo", nil); err == nil {
		t.Fatal("redirect succeeded")
	}
	if calls != 1 || targetCalls != 0 {
		t.Fatalf("provider=%d target=%d", calls, targetCalls)
	}
}

func TestTokenProviderFailureDoesNotLeakOrFallBack(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++ }))
	defer server.Close()
	for _, provider := range []func(context.Context) (string, error){
		func(context.Context) (string, error) { return "", errors.New("secret-output-and-path") },
		func(context.Context) (string, error) { return "", nil },
		func(ctx context.Context) (string, error) {
			child, cancel := context.WithCancel(ctx)
			cancel()
			return "", fmt.Errorf("secret-helper-path: %w", child.Err())
		},
		func(ctx context.Context) (string, error) {
			child, cancel := context.WithDeadline(ctx, time.Unix(0, 0))
			defer cancel()
			return "", fmt.Errorf("secret-helper-path: %w", child.Err())
		},
	} {
		client := New(Options{BaseURL: server.URL, Token: "static", TokenProvider: provider})
		_, err := client.GetRepo(context.Background(), "owner", "repo", nil)
		if err == nil || err.Error() != "GitHub token provider failed" || requests != 0 ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("failure = %v requests=%d", err, requests)
		}
	}
}

func TestTokenProviderPreservesParentContextErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{"canceled", func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}, context.Canceled},
		{"deadline", func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Unix(0, 0))
		}, context.DeadlineExceeded},
	} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				ctx, cancel := tc.ctx()
				defer cancel()
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					requests++
				}))
				defer server.Close()
				client := New(Options{
					BaseURL: server.URL,
					TokenProvider: func(providerCtx context.Context) (string, error) {
						cancel()
						<-providerCtx.Done()
						return "", errors.New("secret-provider-output-and-path")
					},
				})
				path := "/repos/owner/repo"
				if method == http.MethodPost {
					path = client.graphQLURL
				}
				var out map[string]any
				err := client.doJSON(ctx, method, path, nil, nil, &out)
				if !errors.Is(err, tc.want) || err.Error() != tc.want.Error() || requests != 0 {
					t.Fatalf("error = %v, want %v; requests=%d", err, tc.want, requests)
				}
			})
		}
	}
}

func TestTokenProviderRateLimitResponseUsesActualTokenAfterWait(t *testing.T) {
	var observed []string
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"resources": map[string]any{"core": map[string]int{"remaining": 100, "limit": 5000}}})
	}))
	defer server.Close()
	client := New(Options{
		BaseURL: server.URL, TokenProvider: sequenceTokenProvider(t, "before", "after"),
		RateLimit: func(token string, _ RateLimitSnapshot) { observed = append(observed, token) },
	})
	if _, err := client.GetRateLimits(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(observed, []string{"after"}) {
		t.Fatalf("quota observation=%v", observed)
	}
}
