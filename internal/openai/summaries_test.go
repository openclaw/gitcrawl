package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSummarizeUsesResponsesOutputText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization = %q", got)
		}
		var request summaryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.Model != "summary-test" || request.Input != "thread evidence" || request.Store {
			t.Fatalf("request = %+v", request)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": []map[string]any{{
				"type": "message",
				"content": []map[string]any{{
					"type": "output_text",
					"text": "Concise key summary.",
				}},
			}},
		})
	}))
	defer server.Close()

	retry := NoRetry()
	text, err := New(Options{APIKey: "test-key", BaseURL: server.URL, Retry: &retry}).
		Summarize(context.Background(), "summary-test", "be concise", "thread evidence")
	if err != nil {
		t.Fatalf("summarize: %v", err)
	}
	if text != "Concise key summary." {
		t.Fatalf("summary = %q", text)
	}
}

func TestSummarizeRejectsMissingOutputAndAPIError(t *testing.T) {
	retry := NoRetry()
	for _, test := range []struct {
		name    string
		status  int
		payload any
		want    string
	}{
		{
			name:    "missing output",
			status:  http.StatusOK,
			payload: map[string]any{"output": []any{}},
			want:    "no output text",
		},
		{
			name:   "api error",
			status: http.StatusBadRequest,
			payload: map[string]any{"error": map[string]any{
				"message": "bad prompt",
				"type":    "invalid_request_error",
			}},
			want: "bad prompt",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_ = json.NewEncoder(w).Encode(test.payload)
			}))
			defer server.Close()
			_, err := New(Options{APIKey: "test-key", BaseURL: server.URL, Retry: &retry}).
				Summarize(context.Background(), "summary-test", "instructions", "thread")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
