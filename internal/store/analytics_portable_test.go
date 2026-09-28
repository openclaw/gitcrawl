package store

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPortablePruneScrubsAnalyticsWithoutOptionalVacuum(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const sentinel = "synthetic-private-analytics-free-page"
	if err := st.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "review_state", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", ErrorText: sentinel, Evidence: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	// Even an earlier receipt update must not survive in SQLite free pages.
	if _, err := st.DB().Exec("UPDATE analytics_fetch_attempts SET error_text='replaced'"); err != nil {
		t.Fatal(err)
	}
	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{Vacuum: false})
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Vacuumed {
		t.Fatal("private receipt cleanup skipped secure rewrite")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(sentinel)) {
		t.Fatal("private receipt bytes survived no-vacuum prune")
	}
}
