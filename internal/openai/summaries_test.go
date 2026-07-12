package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
		{
			name:   "decoded error",
			status: http.StatusOK,
			payload: map[string]any{"error": map[string]any{
				"message": "response error",
				"type":    "invalid_response_error",
				"code":    "bad_output",
			}},
			want: "response error",
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

func TestSummarizeValidatesInputsAndRetriesTransientErrors(t *testing.T) {
	client := New(Options{APIKey: "test-key"})
	for _, test := range []struct {
		name         string
		client       *Client
		model, input string
		want         string
	}{
		{name: "model", client: client, input: "thread", want: "model is required"},
		{name: "input", client: client, model: "summary-test", want: "input is required"},
		{name: "key", client: New(Options{}), model: "summary-test", input: "thread", want: "API key is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.client.Summarize(context.Background(), test.model, "instructions", test.input)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}

	var calls, sleeps int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"message": "retry me",
				"type":    "overloaded_error",
				"code":    "overloaded",
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"output_text": "Recovered summary."})
	}))
	defer server.Close()
	retry := RetryConfig{
		MaxAttempts:    2,
		BaseDelay:      time.Millisecond,
		OverloadedBase: time.Millisecond,
		MaxDelay:       time.Millisecond,
		MaxElapsed:     time.Second,
	}
	text, err := New(Options{
		APIKey:  "test-key",
		BaseURL: server.URL,
		Retry:   &retry,
		Sleep: func(context.Context, time.Duration) error {
			sleeps++
			return nil
		},
	}).Summarize(context.Background(), "summary-test", "instructions", "thread")
	if err != nil {
		t.Fatalf("summarize after retry: %v", err)
	}
	if text != "Recovered summary." || calls != 2 || sleeps != 1 {
		t.Fatalf("retry result text=%q calls=%d sleeps=%d", text, calls, sleeps)
	}
}

func TestSummarizeStopsOnCancellationAndRetrySleepErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := New(Options{APIKey: "test-key"}).
		Summarize(ctx, "summary-test", "instructions", "thread")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled error = %v", err)
	}

	stopRetry := errors.New("stop retry")
	retry := RetryConfig{
		MaxAttempts:    2,
		BaseDelay:      time.Millisecond,
		OverloadedBase: time.Millisecond,
		MaxDelay:       time.Millisecond,
		MaxElapsed:     time.Second,
	}
	_, err = New(Options{
		APIKey: "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("offline")
		})},
		Retry: &retry,
		Sleep: func(context.Context, time.Duration) error {
			return stopRetry
		},
	}).Summarize(context.Background(), "summary-test", "instructions", "thread")
	if !errors.Is(err, stopRetry) {
		t.Fatalf("retry sleep error = %v", err)
	}

	noRetry := NoRetry()
	_, err = New(Options{
		APIKey: "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("offline")
		})},
		Retry: &noRetry,
	}).Summarize(context.Background(), "summary-test", "instructions", "thread")
	if err == nil || !strings.Contains(err.Error(), "offline") {
		t.Fatalf("terminal transport error = %v", err)
	}

	_, err = New(Options{
		APIKey: "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, context.Canceled
		})},
		Retry: &retry,
	}).Summarize(context.Background(), "summary-test", "instructions", "thread")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("transport cancellation error = %v", err)
	}

	now := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	shortRetry := retry
	shortRetry.MaxElapsed = time.Nanosecond
	serverUnavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "unavailable"}})
	}))
	defer serverUnavailable.Close()
	_, err = New(Options{
		APIKey:  "test-key",
		BaseURL: serverUnavailable.URL,
		Retry:   &shortRetry,
		Now:     func() time.Time { return now },
	}).Summarize(context.Background(), "summary-test", "instructions", "thread")
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("retry deadline error = %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{"))
	}))
	defer server.Close()
	_, err = New(Options{APIKey: "test-key", BaseURL: server.URL, Retry: &noRetry}).
		Summarize(context.Background(), "summary-test", "instructions", "thread")
	if err == nil || !strings.Contains(err.Error(), "decode OpenAI summary response") {
		t.Fatalf("malformed response error = %v", err)
	}

	_, err = New(Options{APIKey: "test-key", BaseURL: "://bad", Retry: &noRetry}).
		Summarize(context.Background(), "summary-test", "instructions", "thread")
	if err == nil || !strings.Contains(err.Error(), "create OpenAI summary request") {
		t.Fatalf("invalid URL error = %v", err)
	}

	_, err = New(Options{
		APIKey: "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(errorReader{}),
			}, nil
		})},
		Retry: &noRetry,
	}).Summarize(context.Background(), "summary-test", "instructions", "thread")
	if err == nil || !strings.Contains(err.Error(), "read OpenAI summary response") {
		t.Fatalf("read response error = %v", err)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}
