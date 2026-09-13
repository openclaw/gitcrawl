package openai

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	crawlembed "github.com/openclaw/crawlkit/embed"
)

const (
	defaultBaseURL         = "https://api.openai.com/v1"
	maxEmbeddingInputRunes = 6_000
	maxEmbeddingInputBytes = 7_000
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
	retry = normalizeRetryConfig(retry)
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
	}
}

func normalizeRetryConfig(retry RetryConfig) RetryConfig {
	defaults := DefaultRetryConfig()
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 1
	}
	if retry.BaseDelay <= 0 {
		retry.BaseDelay = defaults.BaseDelay
	}
	if retry.OverloadedBase <= 0 {
		retry.OverloadedBase = defaults.OverloadedBase
	}
	if retry.MaxDelay <= 0 {
		retry.MaxDelay = defaults.MaxDelay
	}
	return retry
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

	return retryRequest(ctx, c, func() ([][]float64, *APIError, error) {
		return c.embedOnce(ctx, model, texts)
	})
}

func (c *Client) embedOnce(ctx context.Context, model string, texts []string) ([][]float64, *APIError, error) {
	provider, err := crawlembed.NewProvider(crawlembed.Config{
		Provider:   crawlembed.ProviderOpenAI,
		Model:      model,
		BaseURL:    c.baseURL,
		Dimensions: c.dimensions,
		UserAgent:  "gitcrawl",
	}, crawlembed.WithHTTPClient(c.httpClient), crawlembed.WithAPIKey(c.apiKey))
	if err != nil {
		return nil, nil, err
	}

	batch, err := provider.Embed(ctx, texts)
	if err != nil {
		if apiErr := apiErrorFromEmbed(err, c.now()); apiErr != nil {
			return nil, apiErr, nil
		}
		return nil, nil, err
	}
	vectors := batch.Vectors64
	if vectors == nil {
		vectors = make([][]float64, len(batch.Vectors))
		for i, vector := range batch.Vectors {
			vectors[i] = make([]float64, len(vector))
			for j, value := range vector {
				vectors[i][j] = float64(value)
			}
		}
	}
	if len(vectors) != len(texts) {
		return nil, nil, fmt.Errorf("openai embeddings returned %d vectors for %d inputs", len(vectors), len(texts))
	}
	return vectors, nil, nil
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
		offset := (rand.Float64()*2 - 1) * c.retry.Jitter * float64(delay)
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
		out[index] = capEmbeddingInput(text)
	}
	return out
}

func capEmbeddingInput(text string) string {
	runes := 0
	bytes := 0
	for end, r := range text {
		runeBytes := len(string(r))
		if runes >= maxEmbeddingInputRunes || bytes+runeBytes > maxEmbeddingInputBytes {
			return text[:end]
		}
		runes++
		bytes += runeBytes
	}
	return text
}
