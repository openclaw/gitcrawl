package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestObservedGraphQLAdmissionReservesEstimateBeforeResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		remaining := 3017
		if strings.Contains(request.Query, "viewer") {
			remaining -= 16
			if calls.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"viewer": map[string]any{"id": "fixture"}, "rateLimit": map[string]any{"cost": 1, "limit": 5000, "remaining": remaining, "resetAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}}})
	}))
	defer server.Close()
	c := New(Options{Token: "fixture", BaseURL: server.URL, GraphQLQuotaGuard: true, RateLimitReserve: 3000})
	if _, _, err := c.AnalyticsRateLimit(ctx); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		h := historySession{client: c, remaining: 3017}
		_, err := h.request(ctx, "query { viewer { id } }", nil, 16)
		result <- err
	}()
	defer func() {
		close(release)
		if err := <-result; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	h := historySession{client: c, remaining: 3017}
	_, err := h.request(ctx, "query { viewer { id } }", nil, 16)
	var reserve *RateLimitReserveError
	if !errors.As(err, &reserve) {
		t.Fatalf("second estimate admitted before first response completed: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("content requests=%d want 1", calls.Load())
	}
}
