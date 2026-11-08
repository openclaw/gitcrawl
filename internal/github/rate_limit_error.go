package github

import "errors"

// IsRateLimited reports whether err carries a GitHub primary or secondary
// rate-limit response, which the client honors with a wait before retrying.
func IsRateLimited(err error) bool {
	var reqErr *RequestError
	if !errors.As(err, &reqErr) {
		return false
	}
	_, ok := rateLimitWait(reqErr)
	return ok
}
