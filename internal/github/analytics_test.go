package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAnalyticsDiscoveryFiltersOldRowsAndRejectsBadCursors(t *testing.T) {
	more := false
	cursor := "end"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": 19000, "limit": 20000, "used": 1000, "resetAt": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}, "repository": map[string]any{"issues": map[string]any{"totalCount": 2, "pageInfo": map[string]any{"hasNextPage": more, "endCursor": cursor}, "nodes": []any{map[string]any{"number": 2, "updatedAt": "2026-09-24T00:00:00Z"}, map[string]any{"number": 1, "updatedAt": "2026-09-01T00:00:00Z"}}}}}})
	}))
	defer server.Close()
	c := New(Options{Token: "fixture", BaseURL: server.URL})
	since, _ := time.Parse(time.RFC3339, "2026-09-23T00:00:00Z")
	p, e := c.UpdatedNumbers(context.Background(), "fixture", "repo", "issues", "", since)
	if e != nil || len(p.Numbers) != 1 || p.Numbers[0] != 2 || p.Total != 2 {
		t.Fatalf("%+v %v", p, e)
	}
	more = true
	cursor = "same"
	if _, e = c.UpdatedNumbers(context.Background(), "fixture", "repo", "issues", "same", since); e == nil {
		t.Fatal("non-advancing cursor accepted")
	}
}
func TestAnalyticsDiscoveryEmptyContinuation(t *testing.T) {
	for _, tc := range []struct {
		name, after     string
		more, wantError bool
	}{
		{"final continuation", "previous", false, false},
		{"incomplete initial page", "", false, true},
		{"empty advancing page", "previous", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": 19000, "resetAt": "2099-01-01T00:00:00Z"}, "repository": map[string]any{"issues": map[string]any{"totalCount": 100, "pageInfo": map[string]any{"hasNextPage": tc.more, "endCursor": "next"}, "nodes": []any{}}}}})
			}))
			defer server.Close()
			c := New(Options{Token: "test-token-placeholder", BaseURL: server.URL})
			p, err := c.UpdatedNumbers(context.Background(), "fixture", "repo", "issues", tc.after, time.Time{})
			if (err != nil) != tc.wantError {
				t.Fatalf("page=%+v error=%v, wantError=%v", p, err, tc.wantError)
			}
			if err == nil && (p.More || len(p.Numbers) != 0 || p.Total != 100) {
				t.Fatalf("incorrect final page: %+v", p)
			}
		})
	}
}

func TestAnalyticsUnavailableNodesAreNotAuthorizationSuccess(t *testing.T) {
	typ := "NOT_FOUND"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"nodes": []any{map[string]any{"id": "one", "__typename": "User", "login": "fixture"}, nil}}, "errors": []any{map[string]any{"type": typ, "message": "fixture unavailable", "path": []any{"nodes", 1}}}})
	}))
	defer server.Close()
	c := New(Options{Token: "fixture", BaseURL: server.URL})
	nodes, e := c.AnalyticsNodes(context.Background(), []string{"one", "two"}, true)
	if e != nil || len(nodes) != 2 || nodes[1]["id"] != "two" || nodes[1]["__typename"] != "Unavailable" {
		t.Fatalf("%+v %v", nodes, e)
	}
	typ = "FORBIDDEN"
	if _, e = c.AnalyticsNodes(context.Background(), []string{"one", "two"}, true); e == nil {
		t.Fatal("authorization failure became missing-data evidence")
	}
}

func TestAnalyticsQuotaAndHistoryReserveUseActualGraphQLBalance(t *testing.T) {
	var contentRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			fmt.Fprint(w, `{"resources":{"graphql":{"limit":20000,"remaining":19999,"reset":4102444800}}}`)
			return
		}
		var req struct{ Query string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(req.Query, "issueOrPullRequest") {
			contentRequests++
		}
		fmt.Fprint(w, `{"data":{"rateLimit":{"cost":1,"limit":20000,"remaining":2990,"resetAt":"2099-01-01T00:00:00Z"}}}`)
	}))
	defer server.Close()
	c := New(Options{BaseURL: server.URL, Token: "test-token-placeholder", RateLimitReserve: 3000})
	quota, err := c.AnalyticsRateLimit(context.Background())
	if err != nil || quota.Remaining != 2990 || quota.Limit != 20000 {
		t.Fatalf("REST counter admitted work: %+v %v", quota, err)
	}
	if _, err = c.FetchGraphQLHistory(context.Background(), "fixture", "repo", []int{1}, nil); err == nil || !strings.Contains(err.Error(), "quota reserve reached") {
		t.Fatalf("actual GraphQL reserve ignored: %v", err)
	}
	if contentRequests != 0 {
		t.Fatal("content dispatched inside actual GraphQL reserve")
	}
}
