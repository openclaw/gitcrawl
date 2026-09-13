package openai

import (
	"context"
	"time"
)

func retryRequest[T any](ctx context.Context, c *Client, request func() (T, *APIError, error)) (T, error) {
	var zero T
	deadline := c.now().Add(c.retry.MaxElapsed)
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		value, apiErr, err := request()
		if err == nil && apiErr == nil {
			return value, nil
		}
		base := c.retry.BaseDelay
		var retryAfter time.Duration
		if err != nil {
			if isContextErr(err) {
				return zero, err
			}
		} else {
			err = apiErr
			if !apiErr.Retryable() {
				return zero, err
			}
			if apiErr.IsOverloaded() {
				base = c.retry.OverloadedBase
			}
			retryAfter = apiErr.RetryAfter
		}
		if attempt+1 >= c.retry.MaxAttempts {
			return zero, err
		}
		delay := c.backoff(attempt, base, retryAfter)
		if !c.canSleep(deadline, delay) {
			return zero, err
		}
		if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
			return zero, sleepErr
		}
	}
}
