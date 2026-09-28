package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyticsUpgradePopulatedShippedV13(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "upgrade"
		if interrupted {
			name = "resume_failed_migration"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "archive.db")
			fixture, err := os.ReadFile("testdata/analytics-v13.sql")
			if err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err = db.ExecContext(ctx, string(fixture)); err != nil {
				t.Fatal(err)
			}
			queries := map[string]string{
				"repositories":    "SELECT json_group_array(json_array(id,github_repo_id,raw_json)) FROM repositories",
				"threads":         "SELECT json_group_array(json_array(id,github_id,body,raw_json,observation_sequence)) FROM threads",
				"comments":        "SELECT json_group_array(json_array(id,github_id,body,raw_json,created_at_gh)) FROM comments",
				"comment history": "SELECT json_group_array(json_array(id,comment_id,body,raw_json,recorded_at)) FROM comment_revisions",
				"review state":    "SELECT json_group_array(json_array(thread_id,review_thread_id,is_resolved,comments_json,raw_json,fetched_at)) FROM pull_request_review_threads",
				"review history":  "SELECT json_group_array(json_array(id,thread_id,review_thread_id,is_resolved,comments_json,raw_json,recorded_at)) FROM pull_request_review_thread_revisions",
				"review coverage": "SELECT json_group_array(json_array(thread_id,fetched_at)) FROM pull_request_review_thread_syncs",
				"sync coverage":   "SELECT json_group_array(json_array(id,repo_id,scope,status,started_at,finished_at,stats_json)) FROM sync_runs",
			}
			before := map[string]string{}
			for key, query := range queries {
				var value string
				if err = db.QueryRowContext(ctx, query).Scan(&value); err != nil {
					t.Fatal(err)
				}
				if value == "[]" {
					t.Fatalf("fixture has no %s evidence", key)
				}
				before[key] = value
			}
			if interrupted {
				if _, err = db.Exec(`CREATE VIEW analytics_fetch_attempts AS SELECT 1 AS id, 'fixture/repo' AS repository, 1 AS number`); err != nil {
					t.Fatal(err)
				}
				if st, err := Open(ctx, path); err == nil {
					st.Close()
					t.Fatal("expected migration failure on conflicting view")
				}
				var version int
				if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 13 {
					t.Fatalf("failed migration advanced version: %d %v", version, err)
				}
				// The earlier additive column succeeded; retry must tolerate partial DDL.
				var columns int
				if err = db.QueryRow("SELECT count(*) FROM pragma_table_info('comments') WHERE name='publication_at_gh'").Scan(&columns); err != nil || columns != 1 {
					t.Fatalf("failure did not exercise partial migration: %d %v", columns, err)
				}
				if _, err = db.Exec("DROP VIEW analytics_fetch_attempts"); err != nil {
					t.Fatal(err)
				}
			}
			for pass := 0; pass < 2; pass++ {
				st, err := Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				var version int
				if err = st.DB().QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 15 {
					t.Fatalf("version=%d err=%v", version, err)
				}
				for key, query := range queries {
					var value string
					if err = st.DB().QueryRowContext(ctx, query).Scan(&value); err != nil {
						t.Fatal(err)
					}
					if value != before[key] {
						t.Fatalf("migration changed %s evidence", key)
					}
				}
				var membership sql.NullString
				if err = st.DB().QueryRow("SELECT review_thread_ids_json FROM pull_request_review_thread_syncs").Scan(&membership); err != nil || membership.Valid {
					t.Fatalf("migration inferred membership: %v %v", membership, err)
				}
				for _, table := range AnalyticsSourceTables() {
					var count int
					if err = st.DB().QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("new %s rows=%d err=%v", table, count, err)
					}
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// The same forward-version guard used by old writers/readers fails closed.
			if _, err = db.Exec("PRAGMA user_version=16"); err != nil {
				t.Fatal(err)
			}
			for _, open := range []func(context.Context, string) (*Store, error){Open, OpenReadOnly} {
				st, err := open(ctx, path)
				if err == nil {
					st.Close()
					t.Fatal("newer schema accepted")
				}
				if !strings.Contains(err.Error(), "newer than supported") {
					t.Fatal(err)
				}
			}
		})
	}
}
