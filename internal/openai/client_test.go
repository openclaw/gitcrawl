package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
