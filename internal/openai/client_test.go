package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	vectors, err := New(Options{APIKey: "test", BaseURL: server.URL, Dimensions: 1024}).Embed(context.Background(), "text-embedding-3-small", inputs)
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
	client := New(Options{APIKey: "test"})
	if _, err := client.Embed(context.Background(), "", []string{"text"}); err == nil {
		t.Fatal("missing model should fail")
	}
	if vectors, err := client.Embed(context.Background(), "model", nil); err != nil || vectors != nil {
		t.Fatalf("empty inputs = %+v err=%v", vectors, err)
	}
	if _, err := New(Options{}).Embed(context.Background(), "model", []string{"text"}); err == nil {
		t.Fatal("missing API key should fail")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "api-error"):
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(embeddingResponse{Error: &struct {
				Message string `json:"message"`
				Type    string `json:"type"`
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
		_, err := New(Options{APIKey: "test", BaseURL: server.URL + suffix}).Embed(context.Background(), "model", []string{"text"})
		if err == nil {
			t.Fatalf("expected error for %q", suffix)
		}
	}
}
