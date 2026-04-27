package store

import (
	"context"
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

	vectors, err := st.ListThreadVectors(ctx, repoID)
	if err != nil {
		t.Fatalf("list vectors: %v", err)
	}
	if len(vectors) != 1 || vectors[0].Vector[0] != 1 {
		t.Fatalf("unexpected vectors: %#v", vectors)
	}

	thread, vector, err := st.ThreadVectorByNumber(ctx, ThreadVectorQuery{RepoID: repoID, Model: "test", Basis: "title_original"}, 1)
	if err != nil {
		t.Fatalf("thread vector by number: %v", err)
	}
	if thread.ID != threadID || vector.ThreadID != threadID {
		t.Fatalf("unexpected thread/vector: %#v %#v", thread, vector)
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
