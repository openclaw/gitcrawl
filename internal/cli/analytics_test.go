package cli

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAnalyticsEnrichmentContinuesAfterDraining(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	err := maintainAnalyticsEnrichment(ctx, true, time.Millisecond, func() error {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil
	})
	if calls != 2 || !errors.Is(err, context.Canceled) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	calls = 0
	if err := maintainAnalyticsEnrichment(context.Background(), false, time.Millisecond, func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatalf("one-shot calls=%d err=%v", calls, err)
	}
}
