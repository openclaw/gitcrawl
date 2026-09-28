package portable

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestExportExcludesNativeAnalyticsEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, path)
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
	result, err := Export(ctx, ExportOptions{SourceDBPath: path, OutputDir: filepath.Join(dir, "out"), DatabaseName: "archive.db", PublicPath: "archive.db", Profile: CurrentStateV1})
	if err != nil {
		t.Fatal(err)
	}
	db := openRawDB(t, result.DatabasePath)
	defer db.Close()
	for _, table := range store.AnalyticsSourceTables() {
		if tableExists(t, db, table) {
			t.Fatalf("export retained native analytics table %s", table)
		}
	}
	if bytes.Contains(readFile(t, result.DatabasePath), []byte(sentinel)) {
		t.Fatal("private analytics bytes survived export")
	}
	var count int
	if err := st.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts").Scan(&count); err != nil || count != 1 {
		t.Fatalf("source receipts changed: %d %v", count, err)
	}
}
