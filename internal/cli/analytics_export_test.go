package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestCloudSnapshotExcludesNativeAnalyticsEvidence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.db")
	st, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const sentinel = "synthetic-private-analytics-receipt"
	if err := st.RecordAnalyticsAttempt(ctx, store.AnalyticsAttempt{Repository: "private/repository", Number: 1, Operation: "review_state", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", ErrorText: sentinel, Evidence: json.RawMessage(`{"body":"` + sentinel + `"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetAnalyticsState(ctx, "private-cursor", sentinel); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveActorProfiles(ctx, []map[string]any{{"id": "actor", "login": "fixture", "bio": sentinel}}, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	snapshot, _, cleanup, err := cloudSQLiteSnapshotPath(ctx, st.DB(), path, gitcrawlCloudPublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	db, err := sql.Open("sqlite", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range store.AnalyticsSourceTables() {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("snapshot retained %s: %d %v", table, count, err)
		}
	}
	data, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(sentinel)) {
		t.Fatal("private analytics bytes survived snapshot")
	}
	var count int
	if err := st.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts").Scan(&count); err != nil || count != 1 {
		t.Fatalf("source receipts changed: %d %v", count, err)
	}
}
