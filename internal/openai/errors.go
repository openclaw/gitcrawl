package openai

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type APIError struct {
	Status     int
	Type       string
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	parts := []string{fmt.Sprintf("openai embeddings status=%d", e.Status)}
	if e.Type != "" {
		parts = append(parts, "type="+e.Type)
	}
	if e.Code != "" {
		parts = append(parts, "code="+e.Code)
	}
	if e.Message != "" {
		parts = append(parts, "message="+e.Message)
	}
	return strings.Join(parts, " ")
}

func (e *APIError) Retryable() bool {
	if e == nil {
		return false
	}
	switch e.Status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return e.Type != "insufficient_quota" && e.Code != "insufficient_quota"
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func (e *APIError) IsOverloaded() bool {
	return e != nil && (e.Type == "overloaded_error" || (e.Status == http.StatusServiceUnavailable && e.Code == "overloaded"))
}

func AsAPIError(err error) *APIError {
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return nil
}

func parseRetryAfter(header string, now time.Time) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(header); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if seconds, err := strconv.ParseFloat(header, 64); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds * float64(time.Second))
	}
	if when, err := http.ParseTime(header); err == nil {
		delta := when.Sub(now)
		if delta < 0 {
			return 0
		}
		return delta
	}
	return 0
}
