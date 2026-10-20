package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestAnalyticsOwnerCancellationPreservesRetryDeadline(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Date(2026, 10, 20, 19, 46, 9, 0, time.UTC)
	a := AnalyticsAttempt{Repository: "fixture/repo", Number: 177812, Operation: "graphql_history", StartedAt: base.Format(time.RFC3339Nano), Status: "failed", ErrorClass: "fetch", Evidence: json.RawMessage(`{}`)}
	for i := 0; i < 6; i++ {
		a.FinishedAt = base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
		if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	var original string
	if err = s.DB().QueryRow("select next_attempt_at from analytics_retries").Scan(&original); err != nil {
		t.Fatal(err)
	}
	// A repeated 15-minute owner handoff must not perpetually extend a
	// 16-minute provider deadline. All unsuccessful attempts stay recorded.
	a.ErrorClass = "cancelled"
	a.Evidence = json.RawMessage(`{"parent_context_cancelled":true,"cause":{"category":"cancelled","type":"context","code":"cancelled"}}`)
	for i := 1; i <= 3; i++ {
		a.FinishedAt = base.Add(time.Duration(i) * 15 * time.Minute).Format(time.RFC3339Nano)
		if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
			t.Fatal(err)
		}
		var deadline string
		var attempts int
		if err = s.DB().QueryRow("select next_attempt_at,attempts from analytics_retries where resolved_at is null").Scan(&deadline, &attempts); err != nil {
			t.Fatal(err)
		}
		if deadline != original || attempts != 6 {
			t.Fatalf("owner cancellation changed provider backoff: deadline=%s want=%s attempts=%d", deadline, original, attempts)
		}
	}
	var receipts int
	s.DB().QueryRow("select count(*) from analytics_fetch_attempts").Scan(&receipts)
	if receipts != 9 {
		t.Fatalf("lost receipts: %d", receipts)
	}
	if due, err := s.DueAnalyticsRetries(ctx, a.Repository, base.Add(time.Hour).Format(time.RFC3339Nano), 8, "graphql_history"); err != nil || len(due) != 1 || due[0] != a.Number {
		t.Fatalf("interrupted work not due: %v %v", due, err)
	}
	a.Status = "success"
	a.FinishedAt = base.Add(time.Hour).Format(time.RFC3339Nano)
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	a.Status = "failed"
	a.FinishedAt = base.Add(61 * time.Minute).Format(time.RFC3339Nano)
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	var next string
	var attempts int
	if err = s.DB().QueryRow("select next_attempt_at,attempts from analytics_retries where resolved_at is null").Scan(&next, &attempts); err != nil {
		t.Fatal(err)
	}
	if next != base.Add(61*time.Minute+30*time.Second).Format(time.RFC3339Nano) || attempts != 6 {
		t.Fatalf("new interruption lost historical count or reused old resolved deadline: %s %d", next, attempts)
	}
}

func TestAnalyticsCancellationClassificationAndNewObligation(t *testing.T) {
	for _, tc := range []struct {
		name, evidence string
		interrupted    bool
	}{
		{"owner", `{"parent_context_cancelled":true,"cause":{"category":"cancelled","type":"context","code":"cancelled"}}`, true},
		{"deadline", `{"parent_context_cancelled":true,"cause":{"category":"cancelled","type":"context","code":"deadline"}}`, false},
		{"unqualified", `{}`, false},
		{"no_parent_binding", `{"cause":{"category":"cancelled","type":"context","code":"cancelled"}}`, false},
		{"different_type", `{"parent_context_cancelled":true,"cause":{"category":"cancelled","type":"transport","code":"cancelled"}}`, false},
		{"timeout", `{"parent_context_cancelled":true,"cause":{"category":"cancelled","type":"context","code":"cancelled","timeout":true}}`, false},
		{"truncated", `{"parent_context_cancelled":true,"cause":{"category":"cancelled","type":"context","code":"cancelled","chain_truncated":true}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-10-20T19:00:00Z", 1, 1); err != nil {
				t.Fatal(err)
			}
			a := AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "graphql_history", StartedAt: "2026-10-20T19:00:00Z", FinishedAt: "2026-10-20T19:00:01Z", Status: "failed", ErrorClass: "cancelled", Evidence: json.RawMessage(tc.evidence)}
			if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
				t.Fatal(err)
			}
			var next string
			var attempts, complete int
			if err = s.DB().QueryRow("select next_attempt_at,attempts from analytics_retries where resolved_at is null").Scan(&next, &attempts); err != nil {
				t.Fatal(err)
			}
			want := 1
			if tc.interrupted {
				want = 0
			}
			if next != "2026-10-20T19:00:31Z" || attempts != want {
				t.Fatalf("new obligation: %s attempts=%d", next, attempts)
			}
			s.DB().QueryRow("select complete from analytics_coverage").Scan(&complete)
			if complete != 0 {
				t.Fatal("cancellation claimed complete")
			}
		})
	}
}
