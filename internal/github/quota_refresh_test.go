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
	"sync/atomic"
	"testing"
	"time"
)

// A fresh REST reply can still contain an expired GraphQL resource. The
// credential-bound GraphQL observation must be refreshed, never invented.
func TestExpiredGraphQLRESTSnapshotRefreshesFromProvider(t *testing.T) {
	rest, probes, content := 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			rest++
			writeRateLimits(t, w, time.Now().Add(-time.Second), 19983, 19983)
			return
		}
		var request graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Query == "query { viewer { id } }" {
			content++
		} else {
			probes++
		}
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"viewer": map[string]any{"id": "fixture"}, "rateLimit": map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}}})
	}))
	defer server.Close()
	c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: func(context.Context) (string, error) { return "fixture", nil }})
	var result map[string]any
	if err := c.doGraphQL(context.Background(), "query { viewer { id } }", nil, nil, &result); err != nil {
		t.Fatal(err)
	}
	if rest != 1 || probes != 1 || content != 1 || historyString(historyMap(result["viewer"])["id"]) != "fixture" {
		t.Fatalf("rest=%d probes=%d content=%d result=%v", rest, probes, content, result)
	}
}

func TestExpiredGraphQLQuotaRefreshFailsClosed(t *testing.T) {
	for _, mode := range []string{"low", "stale", "missing_cost", "missing_remaining", "bad_reset", "missing_limit", "http", "partial", "decode", "rotation", "credential_failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probes, content, credentials := 0, 0, 0
			canary := "fixture-private-value"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/rate_limit" {
					writeRateLimits(t, w, time.Now().Add(-time.Second), 19983, 19983)
					return
				}
				var q graphqlEnvelope
				json.NewDecoder(r.Body).Decode(&q)
				if strings.Contains(q.Query, "viewer") {
					content++
					t.Error("content dispatched after failed refresh")
					return
				}
				probes++
				if r.Header.Get("Authorization") != "Bearer fixture" {
					t.Error("refresh credential changed")
				}
				switch mode {
				case "http":
					http.Error(w, canary, 401)
					return
				case "decode":
					fmt.Fprint(w, "{")
					return
				case "cancel":
					cancel()
					return
				}
				rate := map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}
				switch mode {
				case "low":
					rate["remaining"] = 1500
				case "stale":
					rate["resetAt"] = time.Now().UTC().Add(-time.Second).Format(time.RFC3339)
				case "missing_cost":
					delete(rate, "cost")
				case "missing_remaining":
					delete(rate, "remaining")
				case "bad_reset":
					rate["resetAt"] = canary
				case "missing_limit":
					delete(rate, "limit")
				}
				envelope := map[string]any{"data": map[string]any{"rateLimit": rate}}
				if mode == "partial" {
					envelope["errors"] = []any{map[string]any{"type": "FORBIDDEN", "message": canary, "path": []any{"rateLimit"}}}
				}
				json.NewEncoder(w).Encode(envelope)
			}))
			defer server.Close()
			c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: func(context.Context) (string, error) {
				credentials++
				if credentials == 3 {
					if mode == "rotation" {
						return "replacement", nil
					}
					if mode == "credential_failure" {
						return "", errors.New(canary)
					}
				}
				return "fixture", nil
			}})
			var out map[string]any
			err := c.doGraphQL(ctx, "query { viewer { id } }", nil, nil, &out)
			wantProbes := 1
			if mode == "decode" {
				wantProbes = 3
			}
			if err == nil || probes != wantProbes || content != 0 || out != nil {
				t.Fatalf("err=%v probes=%d content=%d out=%v", err, probes, content, out)
			}
			cause := safeHistoryCause(err)
			code, _ := cause["code"].(string)
			wants := map[string]string{"low": "quota_reserve_reached", "stale": "quota_snapshot_expired", "missing_cost": "quota_cost_missing", "missing_remaining": "quota_remaining_missing", "bad_reset": "quota_reset_invalid", "http": "http_status", "decode": "unexpected_eof", "rotation": "credential_changed", "credential_failure": "credential_provider_failed", "cancel": "cancelled"}
			if want := wants[mode]; want != "" && code != want {
				t.Fatalf("cause=%v want=%s", cause, want)
			}
			_, _, evidence := HistoryFailureDetails(err)
			if strings.Contains(string(evidence), canary) || !strings.Contains(string(evidence), "graphql_quota_refresh") {
				t.Fatalf("unsafe or missing refresh evidence: %s", evidence)
			}
		})
	}
}

func TestExpiredGraphQLQuotaRefreshBindsRotatedCredential(t *testing.T) {
	var probes, content []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		credential := r.Header.Get("Authorization")
		if r.URL.Path == "/rate_limit" {
			probes = append(probes, credential)
			writeRateLimits(t, w, time.Now().Add(-time.Second), 19983, 19983)
			return
		}
		var q graphqlEnvelope
		json.NewDecoder(r.Body).Decode(&q)
		if strings.Contains(q.Query, "viewer") {
			content = append(content, credential)
		} else {
			probes = append(probes, credential)
		}
		fmt.Fprint(w, `{"data":{"rateLimit":{"cost":1,"limit":20000,"remaining":19000,"resetAt":"2099-01-01T00:00:00Z"}}}`)
	}))
	defer server.Close()
	c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: sequenceTokenProvider(t, "first", "second", "second", "second")})
	// Observations from the old credential must not constrain or admit the new one.
	c.reserve.bindGraphQLToken("first")
	c.reserve.observeGraphQL(RateLimitSnapshot{Resource: "graphql", Remaining: 0, ResetAt: time.Now().Add(time.Hour)}, time.Now())
	var out map[string]any
	if err := c.doGraphQL(context.Background(), "query { viewer { id } }", nil, nil, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(probes, []string{"Bearer first", "Bearer second", "Bearer second"}) || !reflect.DeepEqual(content, []string{"Bearer second"}) {
		t.Fatalf("wrong binding: probes=%v content=%v", probes, content)
	}
}

func TestExpiredGraphQLQuotaRefreshConcurrentReserve(t *testing.T) {
	var active, peak, probes, content atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for p := peak.Load(); n > p; p = peak.Load() {
			if peak.CompareAndSwap(p, n) {
				break
			}
		}
		if r.URL.Path == "/rate_limit" {
			writeRateLimits(t, w, time.Now().Add(-time.Second), 19983, 19983)
			return
		}
		var q graphqlEnvelope
		json.NewDecoder(r.Body).Decode(&q)
		balance := 19999
		if strings.Contains(q.Query, "viewer") {
			content.Add(1)
		} else if probes.Add(1) == 1 {
			balance = 1501
		}
		fmt.Fprintf(w, `{"data":{"rateLimit":{"cost":1,"limit":20000,"remaining":%d,"resetAt":"2099-01-01T00:00:00Z"}}}`, balance)
	}))
	defer server.Close()
	c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: func(context.Context) (string, error) { return "fixture", nil }})
	results := make(chan error, 8)
	for range 8 {
		go func() {
			var out map[string]any
			results <- c.doGraphQL(context.Background(), "query { viewer { id } }", nil, nil, &out)
		}()
	}
	success, blocked := 0, 0
	for range 8 {
		err := <-results
		var floor *RateLimitReserveError
		if err == nil {
			success++
		} else if errors.As(err, &floor) {
			blocked++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || blocked != 7 || content.Load() != 1 || probes.Load() != 8 || peak.Load() != 1 {
		t.Fatalf("success=%d blocked=%d content=%d probes=%d simultaneous=%d", success, blocked, content.Load(), probes.Load(), peak.Load())
	}
}

func TestExpiredGraphQLQuotaRefreshRefusesRedirect(t *testing.T) {
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			writeRateLimits(t, w, time.Now().Add(-time.Second), 19983, 19983)
			return
		}
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer server.Close()
	c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: func(context.Context) (string, error) { return "fixture", nil }})
	var out map[string]any
	err := c.doGraphQL(context.Background(), "query { viewer { id } }", nil, nil, &out)
	var req *RequestError
	if !errors.As(err, &req) || req.Status != 302 || leaked.Load() != 0 {
		t.Fatalf("redirect: err=%v target=%d", err, leaked.Load())
	}
}

func TestExpiredGraphQLQuotaRefreshPreservesPaginationValidation(t *testing.T) {
	for _, mode := range []string{"complete", "short_page", "low_page", "stale_refresh"} {
		t.Run(mode, func(t *testing.T) {
			rest, probes, pages := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/rate_limit" {
					rest++
					reset := time.Now().Add(time.Hour)
					if rest == 3 {
						reset = time.Now().Add(-time.Second)
					}
					writeRateLimits(t, w, reset, 19983, 19983)
					return
				}
				var q graphqlEnvelope
				json.NewDecoder(r.Body).Decode(&q)
				rate := map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": "2099-01-01T00:00:00Z"}
				data := map[string]any{"rateLimit": rate}
				if strings.Contains(q.Query, "issueOrPullRequest") {
					n := historyTestNode()
					n["labels"] = map[string]any{"totalCount": 2, "nodes": []any{map[string]any{"id": "L1", "name": "one"}}, "pageInfo": map[string]any{"hasNextPage": true, "endCursor": "first"}}
					data["repository"] = map[string]any{"databaseId": 42, "nameWithOwner": "fixture/repo", "n0": n}
				} else if q.Variables["after"] != nil {
					pages++
					if q.Variables["after"] != "first" || q.Variables["id"] != "PR_fixture" {
						t.Error("continuation identity/cursor changed")
					}
					page := historyTestConnection(map[string]any{"id": "L2", "name": "two"})
					if mode == "short_page" {
						page = historyTestConnection()
					}
					data["node"] = map[string]any{"id": "PR_fixture", "labels": page}
				} else if rest == 3 {
					probes++
					if mode == "low_page" {
						rate["remaining"] = 1501
					}
					if mode == "stale_refresh" {
						rate["resetAt"] = time.Now().UTC().Add(-time.Second).Format(time.RFC3339)
					}
				}
				json.NewEncoder(w).Encode(map[string]any{"data": data})
			}))
			defer server.Close()
			c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: func(context.Context) (string, error) { return "fixture", nil }})
			batch, err := c.FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil)
			if probes != 1 || rest != 3 {
				t.Fatalf("unbounded refresh: probes=%d rest=%d", probes, rest)
			}
			if mode == "complete" {
				if err != nil || pages != 1 || len(batch.Items) != 1 {
					t.Fatalf("items=%d pages=%d err=%v", len(batch.Items), pages, err)
				}
				item := batch.Items[0]
				if item.Thread["id"] != "PR_fixture" || item.Thread["body"] != "body" || item.Pull["id"] != "9007199254740993" || len(historyNodes(historyMap(item.Thread["_graphql"]), "labels")) != 2 {
					t.Fatal("content/identity/membership changed")
				}
			} else {
				if err == nil || len(batch.Items) != 0 {
					t.Fatal("incomplete membership accepted")
				}
				if mode == "low_page" {
					var floor *RateLimitReserveError
					if !errors.As(err, &floor) || pages != 0 {
						t.Fatalf("refreshed quota ignored page estimate: pages=%d err=%v", pages, err)
					}
				}
				if mode == "stale_refresh" && pages != 0 {
					t.Fatal("old valid observation masked invalid refresh")
				}
			}
		})
	}
}

func TestFreshGraphQLQuotaDoesNotTriggerRefresh(t *testing.T) {
	for _, provider := range []bool{false, true} {
		t.Run(fmt.Sprint(provider), func(t *testing.T) {
			rest, content := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/rate_limit" {
					rest++
					writeRateLimits(t, w, time.Now().Add(time.Hour), 19000, 19000)
					return
				}
				content++
				var q graphqlEnvelope
				json.NewDecoder(r.Body).Decode(&q)
				if q.Query != "query { viewer { id } }" {
					t.Error("unexpected refresh")
				}
				fmt.Fprint(w, `{"data":{"viewer":{"id":"fixture"}}}`)
			}))
			defer server.Close()
			opts := Options{BaseURL: server.URL, RateLimitReserve: 1500, Token: "fixture"}
			if provider {
				opts.TokenProvider = func(context.Context) (string, error) { return "fixture", nil }
			}
			c := New(opts)
			var out map[string]any
			if err := c.doGraphQL(context.Background(), "query { viewer { id } }", nil, nil, &out); err != nil || rest != 1 || content != 1 {
				t.Fatalf("rest=%d content=%d err=%v", rest, content, err)
			}
		})
	}
}
