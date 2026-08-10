package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func TestPreparePortableThreadPayloadsDefersOnlyDisposableWrites(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name              string
		deferRewrite      bool
		wantBody          string
		wantThreadRaw     string
		wantRepositoryRaw string
	}{
		{name: "legacy no-vacuum", wantBody: "abcdefgh", wantThreadRaw: "", wantRepositoryRaw: ""},
		{name: "derived final rewrite", deferRewrite: true, wantBody: "abcdefghijklmnopqrstuvwxyz", wantThreadRaw: `{"thread":true}`, wantRepositoryRaw: `{"repository":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := seedPortableDeferredStore(t, ctx, filepath.Join(t.TempDir(), "payloads.db"))
			defer st.Close()
			stats := PortablePruneStats{}
			if err := st.preparePortableThreadPayloads(ctx, PortablePruneOptions{BodyChars: 8, DeferSecureRewrite: tc.deferRewrite}, &stats); err != nil {
				t.Fatalf("prepare portable thread payloads: %v", err)
			}
			var body, excerpt, threadRaw, repositoryRaw string
			var length int
			if err := st.DB().QueryRowContext(ctx, `select body, body_excerpt, body_length, raw_json from threads where id = 1`).Scan(&body, &excerpt, &length, &threadRaw); err != nil {
				t.Fatalf("read prepared thread: %v", err)
			}
			if err := st.DB().QueryRowContext(ctx, `select raw_json from repositories where id = 1`).Scan(&repositoryRaw); err != nil {
				t.Fatalf("read prepared repository: %v", err)
			}
			if body != tc.wantBody || excerpt != "abcdefgh" || length != 26 || threadRaw != tc.wantThreadRaw || repositoryRaw != tc.wantRepositoryRaw {
				t.Fatalf("prepared body=%q excerpt=%q length=%d thread_raw=%q repository_raw=%q", body, excerpt, length, threadRaw, repositoryRaw)
			}
		})
	}
}

func TestPortablePruneDeferredRewriteVisibleEquivalence(t *testing.T) {
	ctx := context.Background()
	legacy := seedPortableDeferredStore(t, ctx, filepath.Join(t.TempDir(), "legacy.db"))
	deferred := seedPortableDeferredStore(t, ctx, filepath.Join(t.TempDir(), "deferred.db"))
	retained := seedPortableDeferredStore(t, ctx, filepath.Join(t.TempDir(), "retained.db"))
	defer legacy.Close()
	defer deferred.Close()
	defer retained.Close()
	if _, err := legacy.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 8, Vacuum: false}); err != nil {
		t.Fatalf("legacy prune: %v", err)
	}
	if _, err := deferred.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 8, Vacuum: false, DeferSecureRewrite: true}); err != nil {
		t.Fatalf("deferred prune: %v", err)
	}
	retainedStats, err := retained.PrunePortablePayloads(ctx, PortablePruneOptions{
		BodyChars:                     8,
		DeferSecureRewrite:            true,
		RetainSanitizedPayloadColumns: true,
	})
	if err != nil {
		t.Fatalf("retained-column prune: %v", err)
	}
	for _, st := range []*Store{legacy, deferred} {
		if st.hasColumn(ctx, "threads", "body") || st.hasColumn(ctx, "threads", "raw_json") || st.hasColumn(ctx, "repositories", "raw_json") {
			t.Fatal("portable shaping retained removed payload columns")
		}
	}
	legacyVisible := portableVisibleState(t, ctx, legacy)
	deferredVisible := portableVisibleState(t, ctx, deferred)
	if !reflect.DeepEqual(legacyVisible, deferredVisible) {
		t.Fatalf("visible portable state differs:\nlegacy=%#v\ndeferred=%#v", legacyVisible, deferredVisible)
	}
	retainedVisible := portableVisibleState(t, ctx, retained)
	if !reflect.DeepEqual(legacyVisible, retainedVisible) {
		t.Fatalf("retained-column visible state differs:\nlegacy=%#v\nretained=%#v", legacyVisible, retainedVisible)
	}
	if !retained.hasColumn(ctx, "repositories", "raw_json") || !retained.hasColumn(ctx, "threads", "raw_json") || !retained.hasColumn(ctx, "threads", "body") {
		t.Fatal("sanitized compatibility columns were physically dropped")
	}
	wantDroppedColumns := []string{"comments.raw_json_blob_id", "thread_revisions.raw_json_blob_id"}
	if !slices.Equal(retainedStats.DroppedColumns, wantDroppedColumns) {
		t.Fatalf("retained-column dropped stats = %v, want %v", retainedStats.DroppedColumns, wantDroppedColumns)
	}
	var repositoryRaw, threadRaw, body, excerpt, columnProfile string
	if err := retained.DB().QueryRowContext(ctx, `select raw_json from repositories where id = 1`).Scan(&repositoryRaw); err != nil {
		t.Fatal(err)
	}
	if err := retained.DB().QueryRowContext(ctx, `select raw_json, body, body_excerpt from threads where id = 1`).Scan(&threadRaw, &body, &excerpt); err != nil {
		t.Fatal(err)
	}
	if err := retained.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'column_profile'`).Scan(&columnProfile); err != nil {
		t.Fatal(err)
	}
	if repositoryRaw != "" || threadRaw != "" || body != "abcdefgh" || excerpt != "abcdefgh" || columnProfile != PortableColumnProfileSanitizedCompatibility {
		t.Fatalf("sanitized columns repository_raw=%q thread_raw=%q body=%q excerpt=%q profile=%q", repositoryRaw, threadRaw, body, excerpt, columnProfile)
	}
}

func TestPortablePruneProgressStagesOrdered(t *testing.T) {
	ctx := context.Background()
	st := seedPortableDeferredStore(t, ctx, filepath.Join(t.TempDir(), "progress.db"))
	defer st.Close()
	var stages []PortablePruneStage
	if _, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{
		BodyChars:          8,
		DeferSecureRewrite: true,
		Progress: func(stage PortablePruneStage) {
			stages = append(stages, stage)
		},
	}); err != nil {
		t.Fatalf("prune with progress: %v", err)
	}
	want := []PortablePruneStage{
		PortablePruneStageThreadBodies,
		PortablePruneStageCommentReviewBodies,
		PortablePruneStageMetadataRawPayloads,
		PortablePruneStageFingerprintsSummaries,
		PortablePruneStageDiscardedData,
		PortablePruneStageCanonicalSchemaFinalization,
	}
	if !slices.Equal(stages, want) {
		t.Fatalf("portable prune stages = %v, want %v", stages, want)
	}
}

func TestPortablePruneRetainedColumnProgressStagesOrdered(t *testing.T) {
	ctx := context.Background()
	st := seedPortableDeferredStore(t, ctx, filepath.Join(t.TempDir(), "retained-progress.db"))
	defer st.Close()
	var stages []PortablePruneStage
	if _, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{
		BodyChars:                     8,
		DeferSecureRewrite:            true,
		RetainSanitizedPayloadColumns: true,
		Progress: func(stage PortablePruneStage) {
			stages = append(stages, stage)
		},
	}); err != nil {
		t.Fatalf("retained-column prune with progress: %v", err)
	}
	want := []PortablePruneStage{
		PortablePruneStageThreadBodies,
		PortablePruneStageCommentReviewBodies,
		PortablePruneStageMetadataRawPayloads,
		PortablePruneStageFingerprintsSummaries,
		PortablePruneStageDiscardedData,
		PortablePruneStageCanonicalSchemaFinalization,
		PortablePruneStageDisposableTableDrop,
		PortablePruneStageThreadsRebuildPreflight,
		PortablePruneStageThreadsRebuildForeignKeys,
		PortablePruneStageThreadsRebuildCompactCopy,
		PortablePruneStageThreadsRebuildSchemaSwap,
		PortablePruneStageThreadsRebuildSchemaRestore,
	}
	if !slices.Equal(stages, want) {
		t.Fatalf("retained-column prune stages = %v, want %v", stages, want)
	}
}

func TestPortablePrunePhysicalDropClearsCompatibilityColumnProfile(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "column-profile.db")
	st := seedPortableDeferredStore(t, ctx, dbPath)
	if _, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{
		BodyChars:                     8,
		DeferSecureRewrite:            true,
		RetainSanitizedPayloadColumns: true,
	}); err != nil {
		t.Fatalf("retained-column prune: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close retained-column store: %v", err)
	}
	var err error
	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen retained-column store: %v", err)
	}
	defer st.Close()
	if _, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 8}); err != nil {
		t.Fatalf("physical-drop prune: %v", err)
	}
	if st.hasColumn(ctx, "repositories", "raw_json") || st.hasColumn(ctx, "threads", "raw_json") || st.hasColumn(ctx, "threads", "body") {
		t.Fatal("physical-drop prune retained compatibility columns")
	}
	var value string
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'column_profile'`).Scan(&value); err != sql.ErrNoRows {
		t.Fatalf("column_profile after physical drop = %q, err=%v", value, err)
	}
}

type portableVisible struct {
	BodyExcerpt string
	BodyLength  int
	CommentBody string
	Metadata    map[string]string
	Tables      []string
}

func portableVisibleState(t *testing.T, ctx context.Context, st *Store) portableVisible {
	t.Helper()
	var visible portableVisible
	if err := st.DB().QueryRowContext(ctx, `select body_excerpt, body_length from threads where id = 1`).Scan(&visible.BodyExcerpt, &visible.BodyLength); err != nil {
		t.Fatalf("read portable thread state: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select body from comments where id = 1`).Scan(&visible.CommentBody); err != nil {
		t.Fatalf("read portable comment state: %v", err)
	}
	visible.Metadata = make(map[string]string)
	for _, key := range []string{"schema", "body_chars", "capabilities", "includes", "excluded", "thread_author_profile"} {
		var value string
		if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = ?`, key).Scan(&value); err != nil {
			t.Fatalf("read portable metadata %s: %v", key, err)
		}
		visible.Metadata[key] = value
	}
	rows, err := st.DB().QueryContext(ctx, `select name from sqlite_schema where type = 'table' and name not like 'sqlite_%' order by name`)
	if err != nil {
		t.Fatalf("list portable tables: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scan portable table: %v", err)
		}
		visible.Tables = append(visible.Tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read portable tables: %v", err)
	}
	return visible
}

func seedPortableDeferredStore(t *testing.T, ctx context.Context, dbPath string) *Store {
	t.Helper()
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open portable test store: %v", err)
	}
	_, err = st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{"repository":true}', '2026-08-09T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, 1, 'T1', 1, 'issue', 'open', 'portable', 'abcdefghijklmnopqrstuvwxyz', 'https://github.com/openclaw/gitcrawl/issues/1', '[]', '[]', '{"thread":true}', 'hash', '2026-08-09T00:00:00Z');
		insert into comments(id, thread_id, github_id, comment_type, body, raw_json)
		values(1, 1, 'C1', 'issue_comment', 'comment-payload', '{}');
	`)
	if err != nil {
		_ = st.Close()
		t.Fatalf("seed portable test store: %v", err)
	}
	return st
}
