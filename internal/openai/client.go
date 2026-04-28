package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultBaseURL            = "https://api.openai.com/v1"
	maxEmbeddingResponseBytes = 64 << 20
)

type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

type Options struct {
	APIKey     string
	BaseURL    string
	HTTPClient *http.Client
}

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

func New(options Options) *Client {
	baseURL := strings.TrimRight(strings.TrimSpace(options.BaseURL), "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &Client{
		apiKey:     strings.TrimSpace(options.APIKey),
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

func (c *Client) Embed(ctx context.Context, model string, texts []string) ([][]float64, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, fmt.Errorf("embedding model is required")
	}
	if len(texts) == 0 {
		return nil, nil
	}
	if c.apiKey == "" {
		return nil, fmt.Errorf("OpenAI API key is required")
	}
	payload, err := json.Marshal(embeddingRequest{Model: model, Input: texts})
	if err != nil {
		return nil, fmt.Errorf("marshal embeddings request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embeddings", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "gitcrawl")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai embeddings request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxEmbeddingResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read embeddings response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var parsed embeddingResponse
		if err := json.Unmarshal(body, &parsed); err == nil && parsed.Error != nil && parsed.Error.Message != "" {
			return nil, fmt.Errorf("openai embeddings failed with status %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return nil, fmt.Errorf("openai embeddings failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var parsed embeddingResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decode embeddings response: %w", err)
	}
	if len(parsed.Data) != len(texts) {
		return nil, fmt.Errorf("openai embeddings returned %d vectors for %d inputs", len(parsed.Data), len(texts))
	}
	out := make([][]float64, len(texts))
	for _, item := range parsed.Data {
		if item.Index < 0 || item.Index >= len(texts) {
			return nil, fmt.Errorf("openai embeddings returned invalid index %d", item.Index)
		}
		out[item.Index] = item.Embedding
	}
	for index, vector := range out {
		if len(vector) == 0 {
			return nil, fmt.Errorf("openai embeddings returned empty vector at index %d", index)
		}
	}
	return out, nil
}
