package github

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOctopoolRelayRejectionOnlyExposesKnown424Reason(t *testing.T) {
	const generic = "Octopool relay rejected the request; no direct GitHub fallback"
	const known = `{"error":{"code":"fallback_local","message":"private-message-canary","request_id":"private-request-canary","details":{"reason":"identities_cooling_down","private":"private-details-canary"}}}`
	for _, test := range []struct {
		name, body, want string
		status           int
	}{
		{"known", known, generic + " (fallback_local: identities_cooling_down)", 424},
		{"exact limit", known + strings.Repeat(" ", (16<<10)-len(known)), generic + " (fallback_local: identities_cooling_down)", 424},
		{"over limit", known + strings.Repeat(" ", (16<<10)+1-len(known)), generic, 424},
		{"other status", known, generic, 403},
		{"unknown code", `{"error":{"code":"private-code-canary","details":{"reason":"identities_cooling_down"}}}`, generic, 424},
		{"unknown reason", `{"error":{"code":"fallback_local","details":{"reason":"private-reason-canary"}}}`, generic, 424},
		{"wrong type", `{"error":{"code":"fallback_local","details":{"reason":{"secret":"private-canary"}}}}`, generic, 424},
		{"malformed", `{"error":{"code":"fallback_local"`, generic, 424},
		{"trailing data", known + `{"secret":"private-canary"}`, generic, 424},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "17")
				w.Header().Set("X-RateLimit-Remaining", "3")
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, err := NewOctopool(server.URL, "fixture", "private-token-canary", "fixture/repo")
			if err != nil {
				t.Fatal(err)
			}
			request, _ := http.NewRequest(http.MethodGet, "https://api.github.com/repos/fixture/repo", nil)
			response, err := client.httpClient.Transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != test.want {
				t.Fatalf("unexpected rejection body: %q, %v", body, err)
			}
			if response.StatusCode != test.status || response.Header.Get("Retry-After") != "17" || response.Header.Get("X-RateLimit-Remaining") != "3" {
				t.Fatal("relay status or retry headers changed")
			}
		})
	}
}
