package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxSummaryResponseBytes = 4 << 20

type summaryRequest struct {
	Model           string `json:"model"`
	Instructions    string `json:"instructions"`
	Input           string `json:"input"`
	MaxOutputTokens int    `json:"max_output_tokens"`
	Store           bool   `json:"store"`
}

type summaryResponse struct {
	OutputText string `json:"output_text,omitempty"`
	Output     []struct {
		Type    string `json:"type"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"output"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error,omitempty"`
}

func (c *Client) Summarize(ctx context.Context, model, instructions, input string) (string, error) {
	model = strings.TrimSpace(model)
	input = strings.TrimSpace(input)
	if model == "" {
		return "", fmt.Errorf("summary model is required")
	}
	if input == "" {
		return "", fmt.Errorf("summary input is required")
	}
	if c.apiKey == "" {
		return "", fmt.Errorf("OpenAI API key is required")
	}

	deadline := c.now().Add(c.retry.MaxElapsed)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		text, apiErr, err := c.summarizeOnce(ctx, model, instructions, input)
		if err != nil {
			if isContextErr(err) {
				return "", err
			}
			lastErr = err
			if attempt+1 >= c.retry.MaxAttempts {
				return "", err
			}
			delay := c.backoff(attempt, c.retry.BaseDelay, 0)
			if !c.canSleep(deadline, delay) {
				return "", err
			}
			if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
				return "", sleepErr
			}
			continue
		}
		if apiErr == nil {
			return text, nil
		}
		lastErr = apiErr
		if !apiErr.Retryable() || attempt+1 >= c.retry.MaxAttempts {
			return "", apiErr
		}
		base := c.retry.BaseDelay
		if apiErr.IsOverloaded() {
			base = c.retry.OverloadedBase
		}
		delay := c.backoff(attempt, base, apiErr.RetryAfter)
		if !c.canSleep(deadline, delay) {
			return "", apiErr
		}
		if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
			return "", sleepErr
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("openai summary: exhausted %d attempts", c.retry.MaxAttempts)
	}
	return "", lastErr
}

func (c *Client) summarizeOnce(ctx context.Context, model, instructions, input string) (string, *APIError, error) {
	body, err := json.Marshal(summaryRequest{
		Model:           model,
		Instructions:    strings.TrimSpace(instructions),
		Input:           input,
		MaxOutputTokens: 256,
		Store:           false,
	})
	if err != nil {
		return "", nil, fmt.Errorf("encode OpenAI summary request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return "", nil, fmt.Errorf("create OpenAI summary request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "gitcrawl")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("send OpenAI summary request: %w", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxSummaryResponseBytes))
	if err != nil {
		return "", nil, fmt.Errorf("read OpenAI summary response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", apiErrorFromHTTP(resp.StatusCode, resp.Header, responseBody, c.now()), nil
	}
	var decoded summaryResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return "", nil, fmt.Errorf("decode OpenAI summary response: %w", err)
	}
	if decoded.Error != nil {
		return "", &APIError{
			Status:  resp.StatusCode,
			Type:    decoded.Error.Type,
			Code:    decoded.Error.Code,
			Message: decoded.Error.Message,
		}, nil
	}
	if text := strings.TrimSpace(decoded.OutputText); text != "" {
		return text, nil, nil
	}
	var parts []string
	for _, output := range decoded.Output {
		for _, content := range output.Content {
			if content.Type != "output_text" {
				continue
			}
			if text := strings.TrimSpace(content.Text); text != "" {
				parts = append(parts, text)
			}
		}
	}
	text := strings.TrimSpace(strings.Join(parts, "\n"))
	if text == "" {
		return "", nil, fmt.Errorf("OpenAI summary response contained no output text")
	}
	return text, nil, nil
}
