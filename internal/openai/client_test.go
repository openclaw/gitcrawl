package openai

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

func TestEmbedAcceptsLargeBatchResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request embeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.Dimensions != 1024 {
			t.Fatalf("dimensions = %d, want 1024", request.Dimensions)
		}
		response := embeddingResponse{}
		for index := range request.Input {
			vector := make([]float64, 1536)
			for dimension := range vector {
				vector[dimension] = float64((index+dimension)%1000) / 1000
			}
			response.Data = append(response.Data, struct {
				Index     int       `json:"index"`
				Embedding []float64 `json:"embedding"`
			}{Index: index, Embedding: vector})
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	inputs := make([]string, 40)
	for index := range inputs {
		inputs[index] = "thread text"
	}
	noRetry := NoRetry()
	vectors, err := New(Options{APIKey: "test", BaseURL: server.URL, Dimensions: 1024, Retry: &noRetry}).Embed(context.Background(), "text-embedding-3-small", inputs)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(vectors) != len(inputs) {
		t.Fatalf("vectors: got %d want %d", len(vectors), len(inputs))
	}
	if len(vectors[0]) != 1536 {
		t.Fatalf("dimensions: got %d want 1536", len(vectors[0]))
	}
}

func TestEmbedCapsOversizedInputsBeforeRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request embeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(request.Input) != 1 {
			t.Fatalf("inputs = %d, want 1", len(request.Input))
		}
		if got := len([]rune(request.Input[0])); got != maxEmbeddingInputRunes {
			t.Fatalf("input runes = %d, want %d", got, maxEmbeddingInputRunes)
		}
		_ = json.NewEncoder(w).Encode(embeddingResponse{Data: []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		}{{Index: 0, Embedding: []float64{1}}}})
	}))
	defer server.Close()

	input := strings.Repeat("x", maxEmbeddingInputRunes+50)
	vectors, err := New(Options{APIKey: "test", BaseURL: server.URL}).Embed(context.Background(), "text-embedding-3-small", []string{input})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(vectors) != 1 || len(vectors[0]) != 1 {
		t.Fatalf("vectors = %#v", vectors)
	}
}

func TestEmbedErrorBranches(t *testing.T) {
	noRetry := NoRetry()
	client := New(Options{APIKey: "test", Retry: &noRetry})
	if _, err := client.Embed(context.Background(), "", []string{"text"}); err == nil {
		t.Fatal("missing model should fail")
	}
	if vectors, err := client.Embed(context.Background(), "model", nil); err != nil || vectors != nil {
		t.Fatalf("empty inputs = %+v err=%v", vectors, err)
	}
	if _, err := New(Options{Retry: &noRetry}).Embed(context.Background(), "model", []string{"text"}); err == nil {
		t.Fatal("missing API key should fail")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "api-error"):
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(embeddingResponse{Error: &struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    string `json:"code"`
			}{Message: "bad input", Type: "invalid_request"}})
		case strings.Contains(r.URL.Path, "wrong-count"):
			_ = json.NewEncoder(w).Encode(embeddingResponse{})
		case strings.Contains(r.URL.Path, "bad-index"):
			_ = json.NewEncoder(w).Encode(embeddingResponse{Data: []struct {
				Index     int       `json:"index"`
				Embedding []float64 `json:"embedding"`
			}{{Index: 4, Embedding: []float64{1}}}})
		case strings.Contains(r.URL.Path, "empty-vector"):
			_ = json.NewEncoder(w).Encode(embeddingResponse{Data: []struct {
				Index     int       `json:"index"`
				Embedding []float64 `json:"embedding"`
			}{{Index: 0, Embedding: nil}}})
		default:
			http.Error(w, "plain failure", http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	for _, suffix := range []string{"/api-error", "/wrong-count", "/bad-index", "/empty-vector", ""} {
		_, err := New(Options{APIKey: "test", BaseURL: server.URL + suffix, Retry: &noRetry}).Embed(context.Background(), "model", []string{"text"})
		if err == nil {
			t.Fatalf("expected error for %q", suffix)
		}
	}
}

func newSingleVectorServer(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(handler)
}

func writeSingleVector(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(embeddingResponse{Data: []struct {
		Index     int       `json:"index"`
		Embedding []float64 `json:"embedding"`
	}{{Index: 0, Embedding: []float64{0.1}}}})
}

func TestEmbedRetriesOn429AndHonorsRetryAfter(t *testing.T) {
	var calls int32
	server := newSingleVectorServer(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(embeddingResponse{Error: &struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    string `json:"code"`
			}{Message: "rate limited", Type: "rate_limit_exceeded"}})
			return
		}
		writeSingleVector(w)
	})
	defer server.Close()

	var slept []time.Duration
	retry := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Hour, MaxElapsed: time.Hour}
	client := New(Options{APIKey: "test", BaseURL: server.URL, Retry: &retry, Sleep: func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}})
	if _, err := client.Embed(context.Background(), "model", []string{"hi"}); err != nil {
		t.Fatalf("embed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	if len(slept) != 1 || slept[0] != 2*time.Second {
		t.Fatalf("expected single sleep of 2s honoring Retry-After, got %v", slept)
	}
}

func TestEmbedDoesNotSleepAfterFinalRetryableError(t *testing.T) {
	var calls int32
	server := newSingleVectorServer(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(embeddingResponse{Error: &struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		}{Message: "rate limited", Type: "rate_limit_exceeded"}})
	})
	defer server.Close()

	var slept []time.Duration
	retry := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Hour, MaxElapsed: time.Hour}
	client := New(Options{APIKey: "test", BaseURL: server.URL, Retry: &retry, Sleep: func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}})
	_, err := client.Embed(context.Background(), "model", []string{"hi"})
	if err == nil {
		t.Fatalf("expected final retryable error")
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
	if len(slept) != 2 {
		t.Fatalf("slept %d times, want 2 before final attempt: %v", len(slept), slept)
	}
}

func TestEmbedDoesNotRetryInsufficientQuota(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(embeddingResponse{Error: &struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		}{Message: "out of money", Type: "insufficient_quota", Code: "insufficient_quota"}})
	}))
	defer server.Close()
	retry := RetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Hour, MaxElapsed: time.Hour}
	client := New(Options{APIKey: "test", BaseURL: server.URL, Retry: &retry})
	_, err := client.Embed(context.Background(), "model", []string{"hi"})
	if err == nil {
		t.Fatalf("expected error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (no retry on insufficient_quota)", calls)
	}
	apiErr := AsAPIError(err)
	if apiErr == nil {
		t.Fatalf("expected typed APIError, got %T: %v", err, err)
	}
	if apiErr.Code != "insufficient_quota" {
		t.Fatalf("code = %q, want insufficient_quota", apiErr.Code)
	}
}

func TestEmbedOverloadedUsesLongerBackoff(t *testing.T) {
	var calls int32
	server := newSingleVectorServer(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(embeddingResponse{Error: &struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    string `json:"code"`
			}{Message: "overloaded", Type: "overloaded_error"}})
			return
		}
		writeSingleVector(w)
	})
	defer server.Close()

	var slept []time.Duration
	retry := RetryConfig{MaxAttempts: 3, BaseDelay: 10 * time.Millisecond, OverloadedBase: 5 * time.Second, MaxDelay: time.Hour, MaxElapsed: time.Hour}
	client := New(Options{APIKey: "test", BaseURL: server.URL, Retry: &retry, Sleep: func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}})
	if _, err := client.Embed(context.Background(), "model", []string{"hi"}); err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(slept) != 1 {
		t.Fatalf("slept = %v, want one entry", slept)
	}
	if slept[0] < 4*time.Second || slept[0] > 6*time.Second {
		t.Fatalf("overloaded backoff = %v, expected ~5s ± jitter", slept[0])
	}
}

func TestEmbedPropagatesContextCancellation(t *testing.T) {
	// Server returns 429 so retry would normally engage. We pre-cancel the
	// context to assert that cancellation short-circuits the retry loop and
	// is not classified as a retryable failure.
	server := newSingleVectorServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retry := RetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Hour, MaxElapsed: time.Hour}
	client := New(Options{APIKey: "test", BaseURL: server.URL, Retry: &retry})
	_, err := client.Embed(ctx, "model", []string{"hi"})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestEmbedRetryAfterDateForm(t *testing.T) {
	var calls int32
	server := newSingleVectorServer(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Retry-After", time.Now().Add(3*time.Second).UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeSingleVector(w)
	})
	defer server.Close()

	var slept []time.Duration
	retry := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Hour, MaxElapsed: time.Hour}
	client := New(Options{APIKey: "test", BaseURL: server.URL, Retry: &retry, Sleep: func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}})
	if _, err := client.Embed(context.Background(), "model", []string{"hi"}); err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(slept) != 1 || slept[0] < time.Second || slept[0] > 4*time.Second {
		t.Fatalf("expected ~3s sleep from HTTP-date Retry-After, got %v", slept)
	}
}
