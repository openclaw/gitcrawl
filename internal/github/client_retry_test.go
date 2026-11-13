package github

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type contentRetryTransport func(*http.Request) (*http.Response, error)

func (f contentRetryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestContentGETRetryRejectsTransportAndTLSFailures(t *testing.T) {
	for _, cause := range []error{io.ErrUnexpectedEOF, x509.UnknownAuthorityError{Cert: &x509.Certificate{}}} {
		var calls int
		client := New(Options{BaseURL: "https://example.invalid", HTTPClient: &http.Client{
			Transport: contentRetryTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, cause
			}),
		}})
		_, err := client.GetRepo(context.Background(), "fixture", "repo", nil)
		if !errors.Is(err, cause) || calls != 1 {
			t.Fatalf("transport calls=%d err=%v", calls, err)
		}
	}
}

func TestContentGETRetryBounds(t *testing.T) {
	for name, tc := range map[string]struct {
		statuses []int
		wantCode int
	}{
		"reviews recover":        {[]int{502, 200}, 0},
		"two transient failures": {[]int{503, 504, 200}, 0},
		"exhaustion":             {[]int{502, 503, 504}, 504},
		"transient then limited": {[]int{502, 429, 200}, 0},
		"limited then transient": {[]int{403, 502, 200}, 0},
		"one rate limit retry":   {[]int{429, 429}, 429},
		"mixed rate limit bound": {[]int{502, 429, 429}, 429},
		"combined total bound":   {[]int{502, 502, 429}, 429},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(calls.Add(1)) - 1
				if r.Method != http.MethodGet || r.URL.Path != "/repos/fixture/repo/pulls/1/reviews" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				if i >= len(tc.statuses) {
					t.Error("request exceeded retry bound")
					fmt.Fprint(w, `[]`)
					return
				}
				status := tc.statuses[i]
				if status != 200 {
					if status == 403 || status == 429 {
						w.Header().Set("Retry-After", "1")
					}
					w.WriteHeader(status)
					fmt.Fprintf(w, "attempt %d", i+1)
					return
				}
				fmt.Fprint(w, `[{"id":7}]`)
			}))
			defer server.Close()
			rows, err := New(Options{BaseURL: server.URL}).ListPullReviews(context.Background(), "fixture", "repo", 1, nil)
			if tc.wantCode == 0 {
				if err != nil || len(rows) != 1 || fmt.Sprint(rows[0]["id"]) != "7" {
					t.Fatalf("recovery rows=%v err=%v", rows, err)
				}
			} else {
				failure, ok := err.(*RequestError)
				if !ok || failure.Status != tc.wantCode || failure.Body != fmt.Sprintf("attempt %d", len(tc.statuses)) {
					t.Fatalf("final typed failure was not preserved: %v", err)
				}
				if rows != nil {
					t.Fatalf("failed collection returned rows: %v", rows)
				}
			}
			if got := int(calls.Load()); got != len(tc.statuses) {
				t.Fatalf("requests=%d want=%d", got, len(tc.statuses))
			}
		})
	}
}

func TestContentGETRetriesOnlyTheFailedPage(t *testing.T) {
	var first, second atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "" {
			first.Add(1)
			w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next"`)
			fmt.Fprint(w, `[{"id":1}]`)
		} else if second.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
		} else {
			fmt.Fprint(w, `[{"id":2}]`)
		}
	}))
	defer server.Close()
	client := New(Options{BaseURL: server.URL, PageDelay: -1})
	rows, err := client.ListPullReviews(context.Background(), "fixture", "repo", 1, nil)
	if err != nil || len(rows) != 2 || fmt.Sprint(rows[0]["id"]) != "1" || fmt.Sprint(rows[1]["id"]) != "2" || first.Load() != 1 || second.Load() != 2 {
		t.Fatalf("pagination rows=%v first=%d second=%d err=%v", rows, first.Load(), second.Load(), err)
	}
}

func TestContentGETRetryExclusions(t *testing.T) {
	for name, tc := range map[string]struct {
		method, path, body string
		status             int
	}{
		"non GET":            {"POST", "/content", "", 502},
		"GET with body":      {"GET", "/content", "{}", 502},
		"quota probe":        {"GET", "/rate_limit", "", 502},
		"authentication":     {"GET", "/content", "", 401},
		"permission":         {"GET", "/content", "", 403},
		"not found":          {"GET", "/content", "", 404},
		"unqualified limit":  {"GET", "/content", "", 429},
		"other server error": {"GET", "/content", "", 500},
		"malformed success":  {"GET", "/content", "", 200},
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, `{`)
			}))
			defer server.Close()
			client := New(Options{BaseURL: server.URL})
			var out map[string]any
			var err error
			if tc.body == "" {
				err = client.doJSON(context.Background(), tc.method, tc.path, nil, nil, &out)
			} else {
				err = client.doJSON(context.Background(), tc.method, tc.path, strings.NewReader(tc.body), nil, &out)
			}
			if err == nil || calls.Load() != 1 {
				t.Fatalf("excluded request calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestContentGETRetryDoesNotRetryQuotaPreflight(t *testing.T) {
	var probes, content atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			probes.Add(1)
		} else {
			content.Add(1)
		}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	client := New(Options{BaseURL: server.URL, RateLimitReserve: 10})
	_, err := client.ListPullReviews(context.Background(), "fixture", "repo", 1, nil)
	var nested *RequestError
	if !errors.As(err, &nested) || nested.Status != 502 || probes.Load() != 1 || content.Load() != 0 {
		t.Fatalf("preflight probes=%d content=%d err=%v", probes.Load(), content.Load(), err)
	}
}

func TestContentGETRetryRechecksQuotaReserve(t *testing.T) {
	var probes, content atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			remaining := 12
			if probes.Add(1) > 1 {
				remaining = 10
			}
			writeRateLimits(t, w, time.Now().Add(time.Hour), remaining, 100)
			return
		}
		content.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	client := New(Options{BaseURL: server.URL, RateLimitReserve: 10})
	_, err := client.ListPullReviews(context.Background(), "fixture", "repo", 1, nil)
	var reserve *RateLimitReserveError
	if !errors.As(err, &reserve) || probes.Load() != 2 || content.Load() != 1 {
		t.Fatalf("reserve probes=%d content=%d err=%v", probes.Load(), content.Load(), err)
	}
}

func TestContentGETRetryPreservesCredentialSelection(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("fixed=%v", fixed), func(t *testing.T) {
			var seen []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen = append(seen, r.Header.Get("Authorization"))
				if len(seen) == 1 {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			client := New(Options{BaseURL: server.URL, TokenProvider: sequenceTokenProvider(t, "first", "second")})
			var selected *string
			want := []string{"Bearer first", "Bearer second"}
			if fixed {
				token := "bound"
				selected = &token
				want = []string{"Bearer bound", "Bearer bound"}
			}
			response, err := client.doWithToken(context.Background(), http.MethodGet, "/content", nil, nil, selected)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("credential selection changed: %v", seen)
			}
		})
	}
}

func TestContentGETRetryStopsOnCredentialFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	var credentials int
	client := New(Options{BaseURL: server.URL, TokenProvider: func(context.Context) (string, error) {
		credentials++
		if credentials > 1 {
			return "", &RequestError{Method: "GET", Status: 502}
		}
		return "fixture-token", nil
	}})
	_, err := client.GetRepo(context.Background(), "fixture", "repo", nil)
	if err == nil || credentials != 2 || calls.Load() != 1 {
		t.Fatalf("credential failure credentials=%d requests=%d err=%v", credentials, calls.Load(), err)
	}
}

func TestContentGETRetryRejectsForeignRedirect(t *testing.T) {
	var foreignCalls, calls atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		foreignCalls.Add(1)
		fmt.Fprint(w, `{}`)
	}))
	defer foreign.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		http.Redirect(w, r, foreign.URL, http.StatusFound)
	}))
	defer server.Close()
	client := New(Options{BaseURL: server.URL, TokenProvider: sequenceTokenProvider(t, "first", "second")})
	_, err := client.GetRepo(context.Background(), "fixture", "repo", nil)
	if err == nil || calls.Load() != 2 || foreignCalls.Load() != 0 {
		t.Fatalf("redirect calls=%d foreign=%d err=%v", calls.Load(), foreignCalls.Load(), err)
	}
}

func TestContentGETRetryCancellation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := Reporter(func(message string) {
		if strings.Contains(message, "transient content retry") {
			cancel()
		}
	})
	_, err := New(Options{BaseURL: server.URL}).GetRepo(ctx, "fixture", "repo", reporter)
	if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("cancellation calls=%d err=%v", calls.Load(), err)
	}
}

func TestContentGETRetryHints(t *testing.T) {
	for _, hint := range []string{"3", "18446744073709551616", "invalid", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)} {
		t.Run(hint, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", hint)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			_, err := New(Options{BaseURL: server.URL}).GetRepo(context.Background(), "fixture", "repo", nil)
			if failure, ok := err.(*RequestError); !ok || failure.Status != 503 || calls.Load() != 1 {
				t.Fatalf("unusable hint calls=%d err=%v", calls.Load(), err)
			}
		})
	}
	for _, date := range []bool{false, true} {
		t.Run(fmt.Sprintf("honor date=%v", date), func(t *testing.T) {
			var calls atomic.Int32
			var first time.Time
			var earliest time.Time
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					first = time.Now()
					earliest = first.Add(time.Second)
					hint := "1"
					if date {
						earliest = first.Add(2 * time.Second).Truncate(time.Second)
						hint = earliest.UTC().Format(http.TimeFormat)
					}
					w.Header().Set("Retry-After", hint)
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if time.Now().Before(earliest) {
					t.Error("retried before server's requested time")
				}
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			_, err := New(Options{BaseURL: server.URL}).GetRepo(context.Background(), "fixture", "repo", nil)
			if err != nil || calls.Load() != 2 {
				t.Fatalf("hint recovery calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestContentGETRetryDoesNotExtendDeadline(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := New(Options{BaseURL: server.URL}).GetRepo(ctx, "fixture", "repo", nil)
	if failure, ok := err.(*RequestError); !ok || failure.Status != 503 || calls.Load() != 1 {
		t.Fatalf("unaffordable wait calls=%d err=%v", calls.Load(), err)
	}
}
