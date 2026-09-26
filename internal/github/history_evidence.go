package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
)

// HistoryFailure retains a bounded structural receipt, never credentials, HTTP
// headers, prose bodies or actor/profile text. Existing accepted raw evidence
// remains in the archive. A rejected body is represented by its length/hash.
type HistoryFailure struct {
	Cause    error
	Stage    string
	Number   int
	Evidence json.RawMessage
}

type historyResponseReader struct {
	reader       io.Reader
	hash         hash.Hash
	read, hashed int64
}

func (r *historyResponseReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	keep := min(int64(n), 128*1024-r.hashed)
	if keep > 0 {
		_, _ = r.hash.Write(p[:keep])
		r.hashed += keep
	}
	return n, err
}

func (r *historyResponseReader) failure(err error) *HistoryFailure {
	evidence, _ := json.Marshal(map[string]any{"version": 1, "response_bytes_read": r.read, "hashed_prefix_bytes": r.hashed, "prefix_sha256": hex.EncodeToString(r.hash.Sum(nil)), "complete_response": false})
	return &HistoryFailure{Cause: err, Stage: "response_decode", Evidence: evidence}
}

func (e *HistoryFailure) Error() string { return e.Cause.Error() }
func (e *HistoryFailure) Unwrap() error { return e.Cause }

// Error messages can contain prose or identities. Retain only bounded provider
// codes and known query path components, separately from the existing receipt.
func graphQLRejection(data any, failures []graphqlResponseError, cause error) error {
	var evidence map[string]json.RawMessage
	// SafeHistoryEvidence always wraps data in an object, including nil data.
	_ = json.Unmarshal(SafeHistoryEvidence(data), &evidence)
	items := make([]map[string]any, 0, min(len(failures), 8))
	for _, failure := range failures[:min(len(failures), 8)] {
		var code any
		switch failure.Type {
		case "NOT_FOUND", "FORBIDDEN", "UNAUTHORIZED", "UNPROCESSABLE", "RATE_LIMITED", "INTERNAL", "INTERNAL_SERVER_ERROR", "SERVICE_UNAVAILABLE", "MAX_NODE_LIMIT_EXCEEDED", "EXCESSIVE_PAGINATION", "RESOURCE_LIMITS_EXCEEDED":
			code = failure.Type
		}
		path := make([]any, 0, min(len(failure.Path), 8))
		for _, component := range failure.Path[:min(len(failure.Path), 8)] {
			var safe any
			switch value := component.(type) {
			case string:
				switch value {
				case "query", "repository", "node", "nodes", "issue", "pullRequest", "issueOrPullRequest", "issues", "pullRequests", "comments", "reviews", "reviewThreads", "labels", "assignees", "edges", "pageInfo", "totalCount", "hasNextPage", "endCursor", "rateLimit", "id", "__typename", "body", "author", "state", "isResolved", "isOutdated":
					safe = value
				}
				if len(value) >= 2 && len(value) <= 3 && value[0] == 'n' && strings.Trim(value[1:], "0123456789") == "" {
					safe = value // Native generated aliases, never entity IDs.
				}
			case json.Number:
				if index, err := value.Int64(); err == nil && index >= 0 && index <= 10000 {
					safe = index
				}
			}
			path = append(path, safe)
		}
		items = append(items, map[string]any{"type": code, "path": path, "path_truncated": len(failure.Path) > 8})
	}
	evidence["graphql_errors"], _ = json.Marshal(map[string]any{"count": len(failures), "items": items, "truncated": len(failures) > 8})
	encoded, _ := json.Marshal(evidence)
	return &HistoryFailure{Cause: cause, Stage: "partial_response", Evidence: encoded}
}

func historyFailure(stage string, number int, data any, err error) error {
	evidence := SafeHistoryEvidence(data)
	var upstream *HistoryFailure
	if errors.As(err, &upstream) {
		stage = upstream.Stage
		evidence, _ = json.Marshal(map[string]json.RawMessage{"context": evidence, "upstream_rejection": upstream.Evidence})
	}
	return &HistoryFailure{Cause: err, Stage: stage, Number: number, Evidence: evidence}
}

func SafeHistoryEvidence(data any) json.RawMessage {
	raw, _ := json.Marshal(data)
	sum := sha256.Sum256(raw)
	var project func(any, int) any
	project = func(value any, depth int) any {
		if depth > 9 {
			return map[string]any{"truncated": true}
		}
		switch v := value.(type) {
		case map[string]any:
			out := map[string]any{}
			for k, child := range v {
				if len(k) > 1 && k[0] == 'n' && strings.Trim(k[1:], "0123456789") == "" {
					out[k] = project(child, depth+1)
					continue
				}
				switch k {
				case "id", "node_id", "fullDatabaseId", "number", "__typename", "totalCount", "hasNextPage", "endCursor", "createdAt", "updatedAt", "publishedAt", "submittedAt", "state", "isResolved", "isOutdated", "type":
					switch scalar := child.(type) {
					case string:
						if len(scalar) <= 512 {
							out[k] = scalar
						} else {
							out[k] = map[string]any{"truncated": true}
						}
					case bool, float64, int, int64, json.Number, nil:
						out[k] = scalar
					default:
						out[k] = map[string]any{"invalid_type": true}
					}
				case "repository", "node", "data", "nodes", "pageInfo", "comments", "reviews", "reviewThreads", "labels", "assignees", "errors", "path":
					out[k] = project(child, depth+1)
				case "body":
					if text, ok := child.(string); ok {
						h := sha256.Sum256([]byte(text))
						out["body_evidence"] = map[string]any{"bytes": len(text), "sha256": hex.EncodeToString(h[:])}
					}
				}
			}
			return out
		case []any:
			items := make([]any, 0, min(len(v), 12))
			for _, item := range v[:min(len(v), 12)] {
				items = append(items, project(item, depth+1))
			}
			return map[string]any{"count": len(v), "sample": items, "truncated": len(v) > 12}
		default:
			// Unknown strings may be provider error messages or private prose.
			return nil
		}
	}
	out, _ := json.Marshal(map[string]any{"version": 1, "response_bytes": len(raw), "response_sha256": hex.EncodeToString(sum[:]), "structure": project(data, 0)})
	if len(out) > 128*1024 {
		out, _ = json.Marshal(map[string]any{"version": 1, "response_bytes": len(raw), "response_sha256": hex.EncodeToString(sum[:]), "structure_truncated": true})
	}
	return out
}

// HistoryFailureDetails is safe to persist or log even if the original error
// included an HTTP body. It deliberately does not return that body/message.
func HistoryFailureDetails(err error) (string, string, json.RawMessage) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "cancelled", "GraphQL attempt cancelled or timed out", json.RawMessage(`{}`)
	}
	var quota *RateLimitReserveError
	if errors.As(err, &quota) {
		return "rate_limit", quota.Error(), json.RawMessage(`{}`)
	}
	var failure *HistoryFailure
	if errors.As(err, &failure) {
		message := fmt.Sprintf("GraphQL %s rejected for item %d", failure.Stage, failure.Number)
		for _, connection := range []string{"comments", "reviews", "reviewThreads", "labels", "assignees"} {
			for _, reason := range []string{"incomplete history " + connection + " count", "nonadvancing " + connection + " cursor", "missing history " + connection} {
				if strings.HasSuffix(failure.Cause.Error(), reason) {
					message += ": " + reason
				}
			}
		}
		return failure.Stage, message, failure.Evidence
	}
	var response *RequestError
	if errors.As(err, &response) {
		return "http", fmt.Sprintf("GitHub HTTP %d", response.Status), json.RawMessage(`{}`)
	}
	return "fetch", "GraphQL collection failed", json.RawMessage(`{}`)
}
