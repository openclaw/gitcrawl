package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	clusterer "github.com/openclaw/gitcrawl/internal/cluster"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func clusterBenchmarkData(count int) ([]clusterer.Node, map[int64]store.Thread, map[int64][]float64) {
	rng := rand.New(rand.NewPCG(7, 11))
	dims := config.Default().OpenAI.EmbedDimensions
	nodes := make([]clusterer.Node, count)
	threads := make(map[int64]store.Thread, count)
	vectors := make(map[int64][]float64, count)
	var center []float64
	for i := range nodes {
		if i%8 == 0 {
			center = make([]float64, dims)
			for j := range center {
				center[j] = rng.NormFloat64()
			}
		}
		values := make([]float64, dims)
		for j := range values {
			values[j] = center[j] + 0.15*rng.NormFloat64()
		}
		id := int64(i + 1)
		title := fmt.Sprintf("Synthetic embedding group %d", i/8)
		nodes[i] = clusterer.Node{ThreadID: id, Number: i + 1, Title: title}
		kind := "issue"
		if i%3 == 0 {
			kind = "pull_request"
		}
		threads[id] = store.Thread{Number: i + 1, Kind: kind, State: "open", Title: title}
		vectors[id] = values
	}
	return nodes, threads, vectors
}

var benchmarkEdges map[string]clusterer.Edge

func BenchmarkClusterEdges2000(b *testing.B) {
	nodes, threads, vectors := clusterBenchmarkData(2000)
	opts := clusterBuildOptions{Threshold: 0.90, CrossKindThreshold: defaultCrossKindMinScore}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkEdges = scoreClusterEdges(nodes, threads, vectors, opts)
	}
}

func BenchmarkClusterCommand2000(b *testing.B) {
	ctx := context.Background()
	b.Setenv("GITCRAWL_NO_UPDATE_CHECK", "1")
	dir := b.TempDir()
	configPath, dbPath := filepath.Join(dir, "config.toml"), filepath.Join(dir, "gitcrawl.db")
	app := New()
	app.Stdout, app.Stderr = io.Discard, io.Discard
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		b.Fatal(err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "synthetic", Name: "bench", FullName: "synthetic/bench", UpdatedAt: now})
	if err != nil {
		b.Fatal(err)
	}
	nodes, threads, vectors := clusterBenchmarkData(2000)
	if err := st.WithTx(ctx, func(st *store.Store) error {
		for _, node := range nodes {
			thread := threads[node.ThreadID]
			thread.RepoID, thread.GitHubID = repoID, fmt.Sprint(node.ThreadID)
			thread.LabelsJSON, thread.AssigneesJSON, thread.RawJSON = "[]", "[]", "{}"
			thread.ContentHash, thread.UpdatedAt = fmt.Sprint(node.ThreadID), now
			if _, err := st.UpsertThread(ctx, thread); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{RepoID: repoID, Basis: "title_original", Model: "text-embedding-3-small", Force: true})
	if err != nil {
		b.Fatal(err)
	}
	if len(tasks) != len(nodes) {
		b.Fatalf("tasks: %d want %d", len(tasks), len(nodes))
	}
	// Bulk fixture insertion stays outside the timed command and avoids per-row fsync.
	tx, err := st.DB().BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	for _, task := range tasks {
		values := vectors[task.ThreadID]
		data, err := json.Marshal(values)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `insert into thread_vectors(thread_id,basis,model,dimensions,content_hash,vector_json,vector_backend,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)`, task.ThreadID, "title_original", "text-embedding-3-small", len(values), task.ContentHash, string(data), "exact", now, now); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	if err := st.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := app.Run(ctx, []string{"--config", configPath, "cluster", "synthetic/bench", "--threshold", "0.90", "--json"}); err != nil {
			b.Fatal(err)
		}
	}
}
