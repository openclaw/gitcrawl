package github

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestHistoryCauseKeepsOnlyBoundedTypedMetadata(t *testing.T) {
	canary := "private-token-host-body-identity"
	cases := []struct {
		name                string
		err                 error
		category, typ, code string
	}{
		{"unknown", errors.New(canary), "unknown", "unclassified", ""},
		{"cancel", context.Canceled, "cancelled", "context", "cancelled"},
		{"deadline", context.DeadlineExceeded, "cancelled", "context", "deadline"},
		{"eof", io.EOF, "transport", "io", "eof"},
		{"short", io.ErrUnexpectedEOF, "transport", "io", "unexpected_eof"},
		{"json syntax", &json.SyntaxError{}, "decode", "json_syntax", "invalid_json"},
		{"json type", &json.UnmarshalTypeError{Value: canary, Field: canary, Struct: canary, Type: reflect.TypeOf(0)}, "decode", "json_type", "invalid_json_type"},
		{"dns", &net.DNSError{Name: canary, Server: canary, Err: canary, IsTimeout: true}, "network", "dns", "dns_error"},
		{"operation", &net.OpError{Op: canary, Net: canary, Err: errors.New(canary)}, "network", "net_operation", "network_error"},
		{"url", &url.Error{Op: canary, URL: "https://" + canary, Err: errors.New(canary)}, "network", "url_request", "network_error"},
		{"tls", &tls.CertificateVerificationError{Err: errors.New(canary)}, "tls", "certificate_verification", "certificate_invalid"},
		{"authority", x509.UnknownAuthorityError{}, "tls", "unknown_authority", "certificate_invalid"},
		{"hostname", x509.HostnameError{Host: canary}, "tls", "hostname", "certificate_invalid"},
		{"certificate", x509.CertificateInvalidError{Detail: canary}, "tls", "certificate", "certificate_invalid"},
		{"http", &RequestError{Method: canary, URL: canary, Status: 401, Body: canary, Headers: http.Header{"Authorization": []string{canary}}}, "http", "github_http", "http_status"},
		{"expired", &rateLimitStatusExpiredError{RateLimit: RateLimitSnapshot{Resource: "graphql", Remaining: 19999, ResetAt: time.Unix(100, 0)}}, "guard", "quota_snapshot", "quota_snapshot_expired"},
		{"reserve", &RateLimitReserveError{RateLimit: RateLimitSnapshot{Resource: "graphql", Remaining: 1500}, Reserve: 1500}, "guard", "quota_reserve", "quota_reserve_reached"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := safeHistoryCause(fmt.Errorf("%s: %w", canary, tc.err))
			if got["category"] != tc.category || got["type"] != tc.typ {
				t.Fatalf("%+v", got)
			}
			if tc.code != "" && got["code"] != tc.code {
				t.Fatalf("code=%v", got["code"])
			}
			b, _ := json.Marshal(got)
			if strings.Contains(string(b), canary) || len(b) > 2048 {
				t.Fatal("unsafe/unbounded diagnostic", string(b))
			}
		})
	}
	for code, category := range map[string]string{"quota_guard_missing": "guard", "quota_snapshot_missing": "guard", "quota_observation_stale": "guard", "credential_changed": "guard", "origin_mismatch": "guard", "credential_provider_failed": "credential", "quota_cost_missing": "validation", "quota_remaining_missing": "validation", "quota_reset_invalid": "validation", "pagination_budget": "validation", "connection_shape": "validation", "connection_cursor": "validation", "connection_identity": "validation", "connection_count": "validation"} {
		base := errors.New(canary)
		wrapped := requestFailureAt("dispatch_guard", code, base)
		got := safeHistoryCause(wrapped)
		if got["code"] != code || got["category"] != category || !errors.Is(wrapped, base) || wrapped.Error() != base.Error() {
			t.Fatalf("diagnostic changed behavior: %s %+v", code, got)
		}
	}
	var long error = errors.New(canary)
	for i := 0; i < 30; i++ {
		stage := "rest_preflight"
		if i%2 == 0 {
			stage = "graphql_request"
		}
		long = requestFailureAt(stage, "", long)
	}
	got := safeHistoryCause(long)
	if got["chain_truncated"] != true || len(got["stages"].([]string)) != 8 {
		t.Fatal("unbounded cause chain", got)
	}
	unknown := safeHistoryCause(requestFailureAt(canary, canary, errors.New(canary)))
	b, _ := json.Marshal(unknown)
	if strings.Contains(string(b), canary) {
		t.Fatal("untrusted label leaked")
	}
	joined := safeHistoryCause(errors.Join(requestFailureAt("rest_preflight", "", io.ErrUnexpectedEOF), errors.New(canary)))
	if joined["code"] != "unexpected_eof" {
		t.Fatal("primary joined cause lost")
	}
	if requestFailureAt("transport", "", nil) != nil {
		t.Fatal("nil error manufactured")
	}
	// Retry eligibility must stay identical after diagnostic wrapping.
	for _, e := range []error{io.EOF, io.ErrUnexpectedEOF, &RequestError{Status: 503}, &RequestError{Status: 401}, context.Canceled} {
		if transientHistoryError(e) != transientHistoryError(requestFailureAt("graphql_request", "", e)) {
			t.Fatal("retry semantics changed")
		}
	}
}

func TestHistoryCauseDiagnosesNativePreflightWithoutBypassingGuards(t *testing.T) {
	for _, mode := range []string{"expired", "missing", "decode", "rotation", "http", "missing_cost", "missing_remaining", "invalid_reset"} {
		t.Run(mode, func(t *testing.T) {
			gql, credentials := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/rate_limit" {
					if mode == "decode" {
						fmt.Fprint(w, `{"resources":private-response`)
						return
					}
					if mode == "missing" {
						fmt.Fprint(w, `{"resources":{}}`)
						return
					}
					if mode == "http" {
						http.Error(w, "private-response", 401)
						return
					}
					reset := time.Now().Add(time.Hour).Unix()
					if mode == "expired" {
						reset = time.Now().Add(-time.Minute).Unix()
					}
					fmt.Fprintf(w, `{"resources":{"graphql":{"limit":20000,"remaining":19000,"reset":%d}}}`, reset)
					return
				}
				gql++
				rate := map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}
				if mode == "expired" {
					rate["resetAt"] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
				}
				if mode == "missing_cost" {
					delete(rate, "cost")
				}
				if mode == "missing_remaining" {
					delete(rate, "remaining")
				}
				if mode == "invalid_reset" {
					rate["resetAt"] = "private-response"
				}
				json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"rateLimit": rate}})
			}))
			defer server.Close()
			c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: func(context.Context) (string, error) {
				credentials++
				if mode == "rotation" {
					return fmt.Sprint("test-token-", credentials), nil
				}
				return "test-token-placeholder", nil
			}})
			batch, err := c.FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil)
			if err == nil || len(batch.Items) != 0 {
				t.Fatal("failed evidence accepted")
			}
			class, _, evidence := HistoryFailureDetails(err)
			var record struct {
				Cause struct {
					Code   string   `json:"code"`
					Status int      `json:"http_status"`
					Stages []string `json:"stages"`
				} `json:"cause"`
			}
			if e := json.Unmarshal(evidence, &record); e != nil {
				t.Fatal(e)
			}
			want := map[string]string{"expired": "quota_snapshot_expired", "missing": "quota_snapshot_missing", "decode": "invalid_json", "rotation": "credential_changed", "http": "http_status", "missing_cost": "quota_cost_missing", "missing_remaining": "quota_remaining_missing", "invalid_reset": "quota_reset_invalid"}[mode]
			if record.Cause.Code != want {
				t.Fatalf("mode=%s class=%s evidence=%s", mode, class, evidence)
			}
			if mode == "http" {
				if class != "http" || record.Cause.Status != 401 {
					t.Fatal("HTTP status lost")
				}
			} else if class != "fetch" {
				t.Fatal("existing error class changed", class)
			}
			if strings.Contains(string(evidence), "private-response") || strings.Contains(string(evidence), "test-token") {
				t.Fatal("private value leaked")
			}
			if mode == "expired" && gql != 1 {
				t.Fatal("expected only the authoritative quota refresh")
			}
			if mode == "missing" || mode == "decode" || mode == "rotation" || mode == "http" {
				if gql != 0 {
					t.Fatal("guard dispatched GraphQL content")
				}
			}
		})
	}
}

func TestHistoryCauseRetainsPaginationGuardAndAllowsNormalRetry(t *testing.T) {
	expired := true
	restCalls, gqlCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			restCalls++
			reset := time.Now().Add(time.Hour).Unix()
			if expired && restCalls == 3 {
				reset = time.Now().Add(-time.Minute).Unix()
			}
			fmt.Fprintf(w, `{"resources":{"graphql":{"limit":20000,"remaining":19000,"reset":%d}}}`, reset)
			return
		}
		gqlCalls++
		var req graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		data := map[string]any{"rateLimit": map[string]any{"cost": 1, "limit": 20000, "remaining": 19000, "resetAt": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}}
		if expired && restCalls == 3 {
			historyMap(data["rateLimit"])["resetAt"] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
		}
		if strings.Contains(req.Query, "issueOrPullRequest") {
			node := historyTestNode()
			node["labels"] = map[string]any{"totalCount": 2, "nodes": []any{map[string]any{"id": "L1", "name": "one"}}, "pageInfo": map[string]any{"hasNextPage": true, "endCursor": "first"}}
			data["repository"] = map[string]any{"databaseId": 42, "nameWithOwner": "fixture/repo", "n0": node}
		}
		if req.Variables["after"] != nil {
			data["node"] = map[string]any{"id": "PR_fixture", "labels": historyTestConnection(map[string]any{"id": "L2", "name": "two"})}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer server.Close()
	c := New(Options{BaseURL: server.URL, RateLimitReserve: 1500, TokenProvider: func(context.Context) (string, error) { return "test-token-placeholder", nil }})
	batch, err := c.FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil)
	if err == nil || len(batch.Items) != 0 || gqlCalls != 3 {
		t.Fatal("partial pagination accepted")
	}
	class, _, evidence := HistoryFailureDetails(err)
	var record map[string]json.RawMessage
	if err = json.Unmarshal(evidence, &record); err != nil {
		t.Fatal(err)
	}
	if class != "validation" || record["structure"] == nil || !strings.Contains(string(record["cause"]), "pagination_labels") || !strings.Contains(string(record["cause"]), "quota_snapshot_expired") {
		t.Fatalf("lost wrapper or cause: %s", evidence)
	}
	expired = false
	batch, err = c.FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil)
	if err != nil || len(batch.Items) != 1 || len(historyNodes(historyMap(batch.Items[0].Thread["_graphql"]), "labels")) != 2 {
		t.Fatalf("normal retry: items=%d err=%v", len(batch.Items), err)
	}
}
