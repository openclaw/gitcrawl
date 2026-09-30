package cli

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestAnalyticsExportsExcludePrivateCollectorState(t *testing.T) {
	for _, mode := range []string{"cloud", "portable", "portable-compatibility"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "source.db")
			st, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			_, err = st.DB().ExecContext(ctx, `
    INSERT INTO analytics_fetch_attempts(repository,number,operation,started_at,finished_at,status,error_text,evidence_json)
    VALUES('private/repo',1,'graphql_history','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z','failed','synthetic-private-analytics-attempt','{"id":"synthetic-private-analytics-evidence"}');
    INSERT INTO analytics_retries(repository,number,operation,first_seen_at,last_seen_at,next_attempt_at)
    VALUES('synthetic-private-analytics-retry',1,'graphql_history','','','');
    INSERT INTO analytics_pending_nodes(node_id,kind) VALUES('synthetic-private-analytics-queue','profile');
    INSERT INTO analytics_collection_state(name,value,updated_at) VALUES('updates:private/repo','synthetic-private-analytics-cursor','');
    INSERT INTO analytics_repair_receipts(name,cursor,updated_at) VALUES('synthetic-private-analytics-repair',1,'');
    INSERT INTO actor_profiles(node_id,login,actor_type,observed_at,raw_json)
    VALUES('actor','fixture','User','','{"diagnostic":"synthetic-private-analytics-profile"}');
    INSERT INTO actor_identity_evidence(node_id,actor_node_id,observed_at,raw_json)
    VALUES('comment','actor','','{"diagnostic":"synthetic-private-analytics-identity"}');
   `)
			if err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			source, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			var snapshot string
			if mode == "cloud" {
				var cleanup func()
				snapshot, _, cleanup, err = cloudSQLiteSnapshotPath(ctx, source, path, gitcrawlCloudPublishOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer cleanup()
			} else {
				var cleanup func()
				snapshot, cleanup, err = sqliteSnapshotPath(ctx, source, path)
				if err != nil {
					t.Fatal(err)
				}
				defer cleanup()
				derived, err := store.Open(ctx, snapshot)
				if err != nil {
					t.Fatal(err)
				}
				// Even --no-vacuum pruning must remove discarded diagnostic bytes.
				_, err = derived.PrunePortablePayloads(ctx, store.PortablePruneOptions{BodyChars: 100, RetainSanitizedPayloadColumns: mode == "portable-compatibility"})
				closeErr := derived.Close()
				if err != nil {
					t.Fatal(err)
				}
				if closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			published, err := sql.Open("sqlite", snapshot)
			if err != nil {
				t.Fatal(err)
			}
			defer published.Close()
			for _, table := range []string{"analytics_fetch_attempts", "analytics_retries", "analytics_pending_nodes", "analytics_collection_state", "analytics_repair_receipts"} {
				exists, err := sqliteTableExists(ctx, published, table)
				if err != nil {
					t.Fatal(err)
				}
				if exists {
					t.Errorf("export retained private collector table %s", table)
				}
			}
			if err = published.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte("synthetic-private-analytics-")) {
				t.Error("export retained private analytics bytes")
			}
			if err = source.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("export changed source archive")
			}
		})
	}
}
