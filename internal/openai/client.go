package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultBaseURL            = "https://api.openai.com/v1"
	maxEmbeddingResponseBytes = 64 << 20
	maxEmbeddingInputRunes    = 6_000
)

type RetryConfig struct {
	MaxAttempts    int
	BaseDelay      time.Duration
	OverloadedBase time.Duration
	MaxDelay       time.Duration
	MaxElapsed     time.Duration
	Jitter         float64
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    6,
		BaseDelay:      time.Second,
		OverloadedBase: 15 * time.Second,
		MaxDelay:       60 * time.Second,
		MaxElapsed:     5 * time.Minute,
		Jitter:         0.2,
	}
}

func NoRetry() RetryConfig {
	return RetryConfig{MaxAttempts: 1}
}

type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	dimensions int
	retry      RetryConfig

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
	rand  *rand.Rand
	randM sync.Mutex
}

type Options struct {
	APIKey     string
	BaseURL    string
	Dimensions int
	HTTPClient *http.Client
	Retry      *RetryConfig

	Now   func() time.Time
	Sleep func(context.Context, time.Duration) error
}

type embeddingRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions,omitempty"`
}

type embeddingResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
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
	retry := DefaultRetryConfig()
	if options.Retry != nil {
		retry = *options.Retry
	}
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 1
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	return &Client{
		apiKey:     strings.TrimSpace(options.APIKey),
		baseURL:    baseURL,
		httpClient: httpClient,
		dimensions: options.Dimensions,
		retry:      retry,
		now:        now,
		sleep:      sleep,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
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
	texts = capEmbeddingInputs(texts)

	deadline := c.now().Add(c.retry.MaxElapsed)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		vectors, apiErr, err := c.embedOnce(ctx, model, texts)
		if err != nil {
			if isContextErr(err) {
				return nil, err
			}
			lastErr = err
			if attempt+1 >= c.retry.MaxAttempts {
				return nil, err
			}
			delay := c.backoff(attempt, c.retry.BaseDelay, 0)
			if !c.canSleep(deadline, delay) {
				return nil, err
			}
			if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}
		if apiErr == nil {
			return vectors, nil
		}
		lastErr = apiErr
		if !apiErr.Retryable() {
			return nil, apiErr
		}
		if attempt+1 >= c.retry.MaxAttempts {
			return nil, apiErr
		}
		base := c.retry.BaseDelay
		if apiErr.IsOverloaded() {
			base = c.retry.OverloadedBase
		}
		delay := c.backoff(attempt, base, apiErr.RetryAfter)
		if !c.canSleep(deadline, delay) {
			return nil, apiErr
		}
		if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
			return nil, sleepErr
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("openai embeddings: exhausted %d attempts", c.retry.MaxAttempts)
	}
	return nil, lastErr
}

func (c *Client) embedOnce(ctx context.Context, model string, texts []string) ([][]float64, *APIError, error) {
	payload, err := json.Marshal(embeddingRequest{Model: model, Input: texts, Dimensions: c.dimensions})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal embeddings request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embeddings", bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "gitcrawl")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("openai embeddings request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxEmbeddingResponseBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("read embeddings response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		apiErr := &APIError{Status: resp.StatusCode}
		var parsed embeddingResponse
		if jerr := json.Unmarshal(body, &parsed); jerr == nil && parsed.Error != nil {
			apiErr.Message = parsed.Error.Message
			apiErr.Type = parsed.Error.Type
			apiErr.Code = parsed.Error.Code
		} else {
			apiErr.Message = strings.TrimSpace(string(body))
		}
		apiErr.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), c.now())
		return nil, apiErr, nil
	}

	var parsed embeddingResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, nil, fmt.Errorf("decode embeddings response: %w", err)
	}
	if len(parsed.Data) != len(texts) {
		return nil, nil, fmt.Errorf("openai embeddings returned %d vectors for %d inputs", len(parsed.Data), len(texts))
	}
	out := make([][]float64, len(texts))
	for _, item := range parsed.Data {
		if item.Index < 0 || item.Index >= len(texts) {
			return nil, nil, fmt.Errorf("openai embeddings returned invalid index %d", item.Index)
		}
		out[item.Index] = item.Embedding
	}
	for index, vector := range out {
		if len(vector) == 0 {
			return nil, nil, fmt.Errorf("openai embeddings returned empty vector at index %d", index)
		}
	}
	return out, nil, nil
}

func (c *Client) backoff(attempt int, base time.Duration, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		if retryAfter > c.retry.MaxDelay {
			return c.retry.MaxDelay
		}
		return retryAfter
	}
	if base <= 0 {
		base = time.Second
	}
	shift := attempt
	if shift > 6 {
		shift = 6
	}
	delay := base * (1 << shift)
	if delay > c.retry.MaxDelay {
		delay = c.retry.MaxDelay
	}
	if c.retry.Jitter > 0 {
		c.randM.Lock()
		offset := (c.rand.Float64()*2 - 1) * c.retry.Jitter * float64(delay)
		c.randM.Unlock()
		delay += time.Duration(offset)
		if delay < 0 {
			delay = 0
		}
	}
	return delay
}

func (c *Client) canSleep(deadline time.Time, delay time.Duration) bool {
	if c.retry.MaxElapsed <= 0 {
		return true
	}
	return c.now().Add(delay).Before(deadline)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func capEmbeddingInputs(texts []string) []string {
	out := make([]string, len(texts))
	for index, text := range texts {
		runes := []rune(text)
		if len(runes) > maxEmbeddingInputRunes {
			text = string(runes[:maxEmbeddingInputRunes])
		}
		out[index] = text
	}
	return out
}
