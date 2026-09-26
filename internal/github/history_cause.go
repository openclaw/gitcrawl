package github

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/url"
)

// requestFailure adds source-owned diagnostic labels without changing the error
// text, unwrap chain, retry policy or acceptance decision.
type requestFailure struct {
	cause       error
	stage, code string
}

func (e *requestFailure) Error() string { return e.cause.Error() }
func (e *requestFailure) Unwrap() error { return e.cause }
func requestFailureAt(stage, code string, err error) error {
	if err == nil {
		return nil
	}
	return &requestFailure{cause: err, stage: stage, code: code}
}

// All emitted strings are fixed labels. Never serialize Error(), URL, address,
// field/type names from JSON errors, certificate subjects, headers or tokens.
func safeHistoryCause(err error) map[string]any {
	out := map[string]any{"category": "unknown", "type": "unclassified"}
	stages := []string{}
	stage := func(value string) {
		switch value {
		case "rest_preflight", "rest_quota_decode", "dispatch_guard", "credential", "transport", "graphql_request", "graphql_response", "validation", "identity", "partial_response", "response_decode", "response_size", "missing_data", "discovery_validation", "pagination_labels", "pagination_assignees", "pagination_comments", "pagination_reviews", "pagination_reviewThreads":
			if len(stages) < 8 && (len(stages) == 0 || stages[len(stages)-1] != value) {
				stages = append(stages, value)
			}
		}
	}
	set := func(category, typ, code string) { out["category"] = category; out["type"] = typ; out["code"] = code }
	quota := func(q RateLimitSnapshot, reserve int) {
		v := map[string]any{"remaining": q.Remaining, "limit": q.Limit}
		switch q.Resource {
		case "graphql", "core", "search":
			v["resource"] = q.Resource
		}
		if !q.ResetAt.IsZero() {
			v["reset_at"] = q.ResetAt.UTC().Format("2006-01-02T15:04:05Z07:00")
		}
		if reserve > 0 {
			v["reserve"] = reserve
		}
		out["quota"] = v
	}
	for depth := 0; err != nil && depth < 16; depth++ {
		switch e := err.(type) {
		case *requestFailure:
			stage(e.stage)
			switch e.code {
			case "quota_guard_missing", "quota_snapshot_missing", "quota_observation_stale", "credential_changed", "origin_mismatch":
				set("guard", "native_guard", e.code)
			case "credential_provider_failed":
				set("credential", "native_provider", e.code)
			case "quota_cost_missing", "quota_remaining_missing", "quota_reset_invalid", "pagination_budget":
				set("validation", "native_validation", e.code)
			case "connection_shape", "connection_cursor", "connection_identity", "connection_count":
				set("validation", "native_validation", e.code)
			}
		case *HistoryFailure:
			stage(e.Stage)
		case *rateLimitStatusExpiredError:
			set("guard", "quota_snapshot", "quota_snapshot_expired")
			quota(e.RateLimit, 0)
		case *RateLimitReserveError:
			set("guard", "quota_reserve", "quota_reserve_reached")
			quota(e.RateLimit, e.Reserve)
		case *RequestError:
			set("http", "github_http", "http_status")
			if e.Status >= 100 && e.Status <= 599 {
				out["http_status"] = e.Status
			}
		case *json.SyntaxError:
			set("decode", "json_syntax", "invalid_json")
		case *json.UnmarshalTypeError:
			set("decode", "json_type", "invalid_json_type")
		case *net.DNSError:
			set("network", "dns", "dns_error")
			out["timeout"] = e.Timeout()
		case *net.OpError:
			set("network", "net_operation", "network_error")
			out["timeout"] = e.Timeout()
		case *url.Error:
			set("network", "url_request", "network_error")
			out["timeout"] = e.Timeout()
		case *tls.CertificateVerificationError:
			set("tls", "certificate_verification", "certificate_invalid")
		case x509.UnknownAuthorityError:
			set("tls", "unknown_authority", "certificate_invalid")
		case x509.HostnameError:
			set("tls", "hostname", "certificate_invalid")
		case x509.CertificateInvalidError:
			set("tls", "certificate", "certificate_invalid")
		}
		switch err {
		case context.Canceled:
			set("cancelled", "context", "cancelled")
		case context.DeadlineExceeded:
			set("cancelled", "context", "deadline")
		case io.EOF:
			set("transport", "io", "eof")
		case io.ErrUnexpectedEOF:
			set("transport", "io", "unexpected_eof")
		}
		// A receipt describes its primary cause, not an unbounded joined error tree.
		switch e := err.(type) {
		case interface{ Unwrap() error }:
			err = e.Unwrap()
		case interface{ Unwrap() []error }:
			children := e.Unwrap()
			err = nil
			if len(children) > 0 {
				err = children[0]
			}
		default:
			err = nil
		}
	}
	if err != nil {
		out["chain_truncated"] = true
	}
	out["stages"] = stages
	return out
}

func historyEvidenceWithCause(evidence json.RawMessage, err error) json.RawMessage {
	var value map[string]json.RawMessage
	if json.Unmarshal(evidence, &value) != nil || value == nil {
		value = map[string]json.RawMessage{}
	}
	value["cause"], _ = json.Marshal(safeHistoryCause(err))
	out, _ := json.Marshal(value)
	return out
}
