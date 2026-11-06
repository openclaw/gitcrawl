package cli

import (
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
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING", "")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_MAX_AGE", "")

	if got := ghRateLimitHostForAPIBaseURL("://bad"); got != "" {
		t.Fatalf("invalid base URL host = %q", got)
	}
	if got := ghRateLimitLowRemaining(); got != 250 {
		t.Fatalf("default low remaining = %d", got)
	}
	if got := ghRateLimitStateMaxAge(); got != 30*time.Minute {
		t.Fatalf("default max age = %s", got)
	}

	t.Setenv("GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING", "42")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_MAX_AGE", "2m")

	if got := ghRateLimitLowRemaining(); got != 42 {
		t.Fatalf("env low remaining = %d", got)
	}
	if got := ghRateLimitStateMaxAge(); got != 2*time.Minute {
		t.Fatalf("env max age = %s", got)
	}
}
