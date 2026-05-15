package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"
)

func TestUpsertAndListThreadVectors(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "download stalls", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, ThreadVector{
		ThreadID: threadID, Basis: "title_original", Model: "test", Dimensions: 3,
		ContentHash: "hash", Vector: []float64{1, 0, 0}, CreatedAt: "2026-04-26T00:00:00Z", UpdatedAt: "2026-04-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("vector: %v", err)
	}
	if err := st.UpsertThreadVector(ctx, ThreadVector{
		ThreadID: threadID, Basis: "llm_key_summary", Model: "test", Dimensions: 3,
		ContentHash: "summary-hash", Vector: []float64{0, 1, 0}, CreatedAt: "2026-04-26T00:00:00Z", UpdatedAt: "2026-04-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("summary vector: %v", err)
	}

	vectors, err := st.ListThreadVectors(ctx, repoID)
	if err != nil {
		t.Fatalf("list vectors: %v", err)
	}
	if len(vectors) != 2 {
		t.Fatalf("unexpected vectors: %#v", vectors)
	}

	thread, vector, err := st.ThreadVectorByNumber(ctx, ThreadVectorQuery{RepoID: repoID, Model: "test", Basis: "title_original"}, 1)
	if err != nil {
		t.Fatalf("thread vector by number: %v", err)
	}
	if thread.ID != threadID || vector.ThreadID != threadID {
		t.Fatalf("unexpected thread/vector: %#v %#v", thread, vector)
	}
	if len(vector.Vector) != 3 || vector.Vector[0] != 1 {
		t.Fatalf("title vector was overwritten: %#v", vector)
	}
	_, vector, err = st.ThreadVectorByNumber(ctx, ThreadVectorQuery{RepoID: repoID, Model: "test", Basis: "llm_key_summary"}, 1)
	if err != nil {
		t.Fatalf("summary thread vector by number: %v", err)
	}
	if len(vector.Vector) != 3 || vector.Vector[1] != 1 {
		t.Fatalf("summary vector missing: %#v", vector)
	}
}

func TestOpenMigratesThreadVectorsToCompositeKey(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "download stalls", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		drop index if exists idx_thread_vectors_basis_model;
		drop table thread_vectors;
		create table thread_vectors (
			thread_id integer primary key references threads(id) on delete cascade,
			basis text not null,
			model text not null,
			dimensions integer not null,
			content_hash text not null,
			vector_json text not null,
			vector_backend text not null,
			created_at text not null,
			updated_at text not null
		);
		insert into thread_vectors(thread_id, basis, model, dimensions, content_hash, vector_json, vector_backend, created_at, updated_at)
		values(?, 'title_original', 'test', 3, 'hash', '[1,0,0]', 'exact', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z');
	`, threadID)
	if err != nil {
		t.Fatalf("seed legacy vector table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	defer st.Close()
	if !st.threadVectorsHaveCompositeKey(ctx) {
		t.Fatal("thread_vectors primary key was not migrated")
	}
	var version int
	if err := st.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("user_version = %d, want %d", version, schemaVersion)
	}
	if err := st.UpsertThreadVector(ctx, ThreadVector{
		ThreadID: threadID, Basis: "llm_key_summary", Model: "test", Dimensions: 3,
		ContentHash: "summary-hash", Vector: []float64{0, 1, 0}, CreatedAt: "2026-04-26T00:00:00Z", UpdatedAt: "2026-04-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("summary vector: %v", err)
	}
	vectors, err := st.ListThreadVectors(ctx, repoID)
	if err != nil {
		t.Fatalf("list vectors: %v", err)
	}
	if len(vectors) != 2 {
		t.Fatalf("migrated vector table still overwrites rows: %#v", vectors)
	}
}

func TestListThreadVectorsDecodesBinaryPayloads(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "download stalls", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload[0:4], math.Float32bits(0.25))
	binary.LittleEndian.PutUint32(payload[4:8], math.Float32bits(-0.5))
	_, err = st.DB().ExecContext(ctx, `
		insert into thread_vectors(thread_id, basis, model, dimensions, content_hash, vector_json, vector_backend, created_at, updated_at)
		values(?, 'llm_key_summary', 'text-embedding-3-large', 2, 'hash', ?, 'vectorlite', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z')
	`, threadID, payload)
	if err != nil {
		t.Fatalf("seed vector: %v", err)
	}

	_, vector, err := st.ThreadVectorByNumber(ctx, ThreadVectorQuery{RepoID: repoID}, 1)
	if err != nil {
		t.Fatalf("thread vector by number: %v", err)
	}
	if len(vector.Vector) != 2 || vector.Vector[0] != 0.25 || vector.Vector[1] != -0.5 {
		t.Fatalf("unexpected vector: %#v", vector.Vector)
	}
}
