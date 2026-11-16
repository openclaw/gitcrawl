package github

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
)

// Keep the cause available for cancellation/error classification without putting
// private relay URLs, credentials or arbitrary transport diagnostics in logs.
type conversationTransportError struct{ cause error }

func (e *conversationTransportError) Unwrap() error { return e.cause }
func (e *conversationTransportError) Error() string {
	reason := "transport failure"
	var dns *net.DNSError
	var certificate *tls.CertificateVerificationError
	var network net.Error
	var errno syscall.Errno
	switch {
	case errors.Is(e.cause, context.Canceled):
		reason = "context canceled"
	case errors.Is(e.cause, context.DeadlineExceeded):
		reason = "context deadline exceeded"
	case errors.As(e.cause, &dns):
		reason = "DNS lookup failed"
	case errors.As(e.cause, &certificate):
		reason = "TLS certificate verification failed"
	case errors.As(e.cause, &errno):
		reason = errno.Error()
	case errors.Is(e.cause, io.ErrUnexpectedEOF):
		reason = "unexpected EOF"
	case errors.Is(e.cause, io.EOF):
		reason = "EOF"
	case errors.As(e.cause, &network) && network.Timeout():
		reason = "network timeout"
	default:
		reason = fmt.Sprintf("transport failure (%T)", e.cause)
	}
	return "Octopool GraphQL request failed: " + reason
}
