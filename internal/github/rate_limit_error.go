package github

import "errors"

// graphQLRateLimitedError marks a GraphQL response GitHub rejected as
// RATE_LIMITED, which it reports with HTTP 200 rather than 403 or 429.
type graphQLRateLimitedError struct{ error }

func (e graphQLRateLimitedError) Unwrap() error { return e.error }

// IsRateLimited reports whether err carries a GitHub primary or secondary
// rate-limit response: a REST response the client honors with a wait before
// retrying, or a GraphQL RATE_LIMITED rejection.
func IsRateLimited(err error) bool {
	var limited graphQLRateLimitedError
	if errors.As(err, &limited) {
		return true
	}
	var reqErr *RequestError
	if !errors.As(err, &reqErr) {
		return false
	}
	_, ok := rateLimitWait(reqErr)
	return ok
}
