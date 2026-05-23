package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestThreadVectorPreferredTieBreakers(t *testing.T) {
	current := store.ThreadVector{
		ThreadID:  1,
		UpdatedAt: "2026-04-27T10:00:00Z",
		CreatedAt: "2026-04-27T09:00:00Z",
		Basis:     "title",
		Model:     "b",
	}

	if !threadVectorPreferred(store.ThreadVector{UpdatedAt: "2026-04-27T11:00:00Z"}, current) {
		t.Fatal("newer updated_at should win")
	}
	if threadVectorPreferred(store.ThreadVector{UpdatedAt: "2026-04-27T08:00:00Z"}, current) {
		t.Fatal("older updated_at should not win")
	}
	if !threadVectorPreferred(store.ThreadVector{
		UpdatedAt: current.UpdatedAt,
		CreatedAt: "2026-04-27T09:30:00Z",
	}, current) {
		t.Fatal("newer created_at should break updated_at ties")
	}
	if !threadVectorPreferred(store.ThreadVector{
		UpdatedAt: current.UpdatedAt,
		CreatedAt: current.CreatedAt,
		Basis:     "body",
		Model:     "z",
	}, current) {
		t.Fatal("lexically earlier basis should break timestamp ties")
	}
	if !threadVectorPreferred(store.ThreadVector{
		UpdatedAt: current.UpdatedAt,
		CreatedAt: current.CreatedAt,
		Basis:     current.Basis,
		Model:     "a",
	}, current) {
		t.Fatal("lexically earlier model should break basis ties")
	}
	if !threadVectorTimestampAfter("z-not-a-time", "a-not-a-time") {
		t.Fatal("invalid timestamps should fall back to lexical order")
	}
}

func TestGHRateLimitConfigHelpers(t *testing.T) {
	t.Setenv("GH_HOST", "")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING", "")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_MAX_AGE", "")

	if got := ghRateLimitHostForAPIBaseURL("https://api.github.com"); got != "github.com" {
		t.Fatalf("api.github.com host = %q", got)
	}
	if got := ghRateLimitHostForAPIBaseURL("https://github.example.com/api/v3"); got != "github.example.com" {
		t.Fatalf("enterprise host = %q", got)
	}
	if got := ghRateLimitHostForAPIBaseURL("://bad"); got != "" {
		t.Fatalf("invalid base URL host = %q", got)
	}
	if got := ghRateLimitLowRemaining(); got != 250 {
		t.Fatalf("default low remaining = %d", got)
	}
	if got := ghRateLimitStateMaxAge(); got != 30*time.Minute {
		t.Fatalf("default max age = %s", got)
	}

	t.Setenv("GH_HOST", "example.com")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING", "42")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_MAX_AGE", "2m")

	if got := ghRateLimitHostForArgs([]string{"--hostname", "gh.example.com"}); got != "gh.example.com" {
		t.Fatalf("hostname arg = %q", got)
	}
	if got := ghRateLimitHostForArgs([]string{"--hostname=gh2.example.com"}); got != "gh2.example.com" {
		t.Fatalf("hostname=value arg = %q", got)
	}
	if got := ghRateLimitHostForArgs(nil); got != "example.com" {
		t.Fatalf("env host = %q", got)
	}
	if got := ghRateLimitLowRemaining(); got != 42 {
		t.Fatalf("env low remaining = %d", got)
	}
	if got := ghRateLimitStateMaxAge(); got != 2*time.Minute {
		t.Fatalf("env max age = %s", got)
	}
}

func TestGHPRStatusFormattingAndRawHelpers(t *testing.T) {
	rawComments := `[{"id":123,"author":{"login":"dependabot[bot]","__typename":"User"},"body":"b","createdAt":"2026-04-27T00:00:00Z","url":"https://example.com/c"}]`
	comments := decodeThreadComments(rawComments)
	if len(comments) != 1 || comments[0].ID != "123" || !comments[0].IsBot {
		t.Fatalf("decoded comments = %#v", comments)
	}
	if decoded := decodeThreadComments("{"); decoded != nil {
		t.Fatalf("invalid comments decoded to %#v", decoded)
	}

	threads := summarizePRReviewThreads(nil, []store.PullRequestReviewThread{{
		ReviewThreadID:   "thread-1",
		Path:             "file.go",
		Line:             12,
		IsResolved:       false,
		FirstAuthorLogin: "dependabot[bot]",
		FirstAuthorType:  "User",
		FirstCommentBody: "please fix",
		FirstCommentURL:  "https://example.com/thread",
		CommentsJSON:     rawComments,
	}}, true)
	if !threads.KnownResolution || threads.Total != 1 || threads.Unresolved != 1 || len(threads.Threads[0].Comments) != 1 {
		t.Fatalf("threads summary = %#v", threads)
	}

	result := ghPRStatusResult{
		Number:       50,
		Title:        "preserve selection",
		State:        "open",
		IsMergeReady: false,
		Checks:       ghPRStatusChecks{OverallStatus: "failure", Pass: 2, Fail: 1, Pending: 3},
		ReviewThreads: ghPRStatusReviewThreads{
			Unresolved: 1,
			Resolved:   2,
			Unknown:    3,
		},
		Reviews: ghPRStatusReviews{Approvals: 1, ChangesRequested: 1},
		Cache:   ghPRStatusCache{AgeSeconds: 9},
		BlockingReasons: []string{
			"checks failing",
			"checks pending",
			"checks unknown",
			"merge conflicts",
			"not open",
			"unresolved review threads",
			"changes requested",
			"no approval",
		},
	}
	var out bytes.Buffer
	if err := writeGHPRStatusText(&out, result); err != nil {
		t.Fatalf("write status: %v", err)
	}
	if !strings.Contains(out.String(), "checks: failure pass=2 fail=1 pending=3") {
		t.Fatalf("status output = %q", out.String())
	}
	steps := ghPRStatusNextSteps(result)
	if len(steps) < 6 {
		t.Fatalf("next steps = %#v", steps)
	}
	if ghPRStatusExitCode(result) != 1 {
		t.Fatal("non-ready failing PR should exit 1")
	}
	if ghPRStatusExitCode(ghPRStatusResult{Checks: ghPRStatusChecks{OverallStatus: "pending"}, BlockingReasons: []string{"checks pending"}}) != 3 {
		t.Fatal("pending-only PR should exit 3")
	}

	row := map[string]any{"string": "value", "float": 12.5, "int": 7, "bool": "true", "nested": map[string]any{"x": "y"}}
	if rawString(row, "float") != "12.5" || rawInt(row, "int") != 7 || !rawBool(row, "bool") || rawMap(row, "nested")["x"] != "y" {
		t.Fatalf("raw helper mismatch")
	}
	if !isGHBot("renovate[bot]", "User") || !isGHBot("alice", "Bot") {
		t.Fatal("bot detection mismatch")
	}
}
