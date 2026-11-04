package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

// This opt-in differential oracle exercises the unchanged command and writes
// synthetic evidence only. scripts/cluster_simd_parity.py builds and compares
// scalar, SIMD-enabled and forced-fallback arms without providers or real data.
func TestClusterCommand2000DurableOracle(t *testing.T) {
	output := os.Getenv("GITCRAWL_DURABLE_ORACLE_OUTPUT")
	if output == "" {
		t.Skip("requires explicit GITCRAWL_DURABLE_ORACLE_OUTPUT ending in .json.gz")
	}
	if !filepath.IsAbs(output) || !strings.HasSuffix(output, ".json.gz") {
		t.Fatal("oracle output must be an absolute .json.gz path")
	}
	ctx := context.Background()
	dir := t.TempDir()
	home := filepath.Join(dir, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	for _, name := range []string{"XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"} {
		t.Setenv(name, filepath.Join(home, name))
	}
	t.Setenv("GITCRAWL_NO_UPDATE_CHECK", "1")
	configPath, dbPath := filepath.Join(dir, "config.toml"), filepath.Join(dir, "gitcrawl.db")
	// Keep direct opt-in execution isolated as well as the sanitized collector.
	// These are the exact override keys consumed by Config.ApplyRuntimeEnv,
	// plus the config selector and CLI remote selector.
	for _, name := range []string{
		"GITCRAWL_SUMMARY_MODEL", "GITCRAWL_EMBED_MODEL", "GITCRAWL_EMBED_BASE_URL",
		"GITCRAWL_VECTOR_BACKEND", "GITCRAWL_TUI_LAYOUT", "GITCRAWL_REMOTE",
		"GITCRAWL_REMOTE_MODE", "GITCRAWL_REMOTE_ENDPOINT", "GITCRAWL_REMOTE_ARCHIVE", "GITCRAWL_REMOTE_TOKEN_ENV",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("GITCRAWL_CONFIG", configPath)
	t.Setenv("GITCRAWL_DB_PATH", dbPath)
	run := func(args ...string) ([]byte, error) {
		app := New()
		var stdout, stderr bytes.Buffer
		app.Stdout, app.Stderr = &stdout, &stderr
		err := app.Run(ctx, append([]string{"--config", configPath}, args...))
		return stdout.Bytes(), err
	}
	if _, err := run("init", "--db", dbPath); err != nil {
		t.Fatal(err)
	}
	open := func() *store.Store {
		st, err := store.Open(ctx, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	closeStore := func(st *store.Store) {
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
	const fixtureTime = "2026-01-01T00:00:00Z"
	const basis = "title_original"
	const smallModel = "text-embedding-3-small"
	const largeModel = "text-embedding-3-large"
	nodes, threads, vectors := clusterBenchmarkData(2000)
	st := open()
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "synthetic", Name: "bench", FullName: "synthetic/bench", UpdatedAt: fixtureTime})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx *store.Store) error {
		for _, node := range nodes {
			thread := threads[node.ThreadID]
			thread.RepoID, thread.GitHubID = repoID, fmt.Sprint(node.ThreadID)
			thread.LabelsJSON, thread.AssigneesJSON, thread.RawJSON = "[]", "[]", "{}"
			thread.ContentHash, thread.UpdatedAt = fmt.Sprint(node.ThreadID), fixtureTime
			id, err := tx.UpsertThread(ctx, thread)
			if err != nil {
				return err
			}
			if id != node.ThreadID {
				return fmt.Errorf("fixture thread ID: got %d, want %d", id, node.ThreadID)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	seedVectors := func(st *store.Store, model string, selected map[int64]bool) {
		t.Helper()
		tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{RepoID: repoID, Basis: basis, Model: model, Force: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) != len(nodes) {
			t.Fatalf("embedding tasks: got %d, want %d", len(tasks), len(nodes))
		}
		tx, err := st.DB().BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		inserted := 0
		for _, task := range tasks {
			if selected != nil && !selected[task.ThreadID] {
				continue
			}
			values, ok := vectors[task.ThreadID]
			if !ok || task.ContentHash == "" {
				t.Fatalf("invalid vector task for thread %d", task.ThreadID)
			}
			data, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `insert into thread_vectors(thread_id,basis,model,dimensions,content_hash,vector_json,vector_backend,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)`, task.ThreadID, basis, model, len(values), task.ContentHash, string(data), "exact", fixtureTime, fixtureTime); err != nil {
				t.Fatal(err)
			}
			inserted++
		}
		want := len(nodes)
		if selected != nil {
			want = len(selected)
		}
		if inserted != want {
			t.Fatalf("inserted vectors: got %d, want %d", inserted, want)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	seedVectors(st, smallModel, nil)
	threadInput := durableOracleQuery(t, ctx, st, `select id,repo_id,github_id,number,kind,state,title,content_hash,labels_json,assignees_json,raw_json from threads order by id`)
	threadJSON, err := json.Marshal(threadInput)
	if err != nil {
		t.Fatal(err)
	}
	threadHash := sha256.Sum256(threadJSON)
	closeStore(st)
	allIDs := make([]int64, len(nodes))
	limitedIDs := make([]int64, 1997)
	selected := make(map[int64]bool, len(limitedIDs))
	for i, node := range nodes {
		allIDs[i] = node.ThreadID
		if i < len(limitedIDs) {
			limitedIDs[i] = node.ThreadID
			selected[node.ThreadID] = true
		}
	}
	artifact := durableOracleArtifact{Schema: 1, Fixture: "clusterBenchmarkData(2000);PCG(7,11)", ThreadInputSHA256: hex.EncodeToString(threadHash[:]), Phases: []durableOraclePhase{}}
	clusterArgs := []string{"cluster", "synthetic/bench", "--threshold", "0.90", "--json"}
	phase := func(name, model string, ids []int64, extra ...string) durableOraclePhase {
		t.Helper()
		stdout, err := run(append(append([]string{}, clusterArgs...), extra...)...)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var result struct {
			RunID    int64                 `json:"run_id"`
			Coverage clusterVectorCoverage `json:"vector_coverage"`
		}
		if err := json.Unmarshal(stdout, &result); err != nil {
			t.Fatal(err)
		}
		covered := len(nodes)
		if model == largeModel {
			covered = len(limitedIDs)
		}
		want := clusterVectorCoverage{Supported: true, Eligible: len(nodes), Covered: covered, Fresh: covered, Processed: len(ids), Missing: len(nodes) - covered, Partial: len(ids) < len(nodes), Complete: len(ids) == len(nodes)}
		if result.Coverage != want || result.RunID <= 0 {
			t.Fatalf("%s coverage/run: got %+v run=%d, want %+v", name, result.Coverage, result.RunID, want)
		}
		cfg, err := config.Load(configPath)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.OpenAI.EmbedModel != model || cfg.EmbeddingBasis != basis || cfg.OpenAI.EmbedDimensions != len(vectors[allIDs[0]]) {
			t.Fatalf("%s configured vector identity differs from fixture", name)
		}
		st := open()
		defer closeStore(st)
		p := durableOraclePhase{Name: name, Output: json.RawMessage(bytes.TrimSpace(stdout)), SelectedThreadIDs: ids, Config: map[string]any{"model": cfg.OpenAI.EmbedModel, "basis": cfg.EmbeddingBasis, "dimensions": cfg.OpenAI.EmbedDimensions}, Snapshot: durableOracleSnapshot(t, ctx, st), Vectors: durableOracleVectors(t, ctx, st)}
		var previous map[string][]map[string]any
		if len(artifact.Phases) > 0 {
			previous = artifact.Phases[len(artifact.Phases)-1].Snapshot
		}
		durableOracleValidateLineage(t, p.Snapshot, previous, selected, result.RunID, allIDs)
		artifact.Phases = append(artifact.Phases, p)
		return p
	}
	phase("full", smallModel, allIDs)
	phase("limited", smallModel, limitedIDs, "--limit", "1997")
	if _, err := run("configure", "--embed-model", largeModel); err != nil {
		t.Fatal(err)
	}
	st = open()
	seedVectors(st, largeModel, selected)
	closeStore(st)
	beforeStrict := phase("model_partial", largeModel, limitedIDs)
	stdout, strictErr := run(append(append([]string{}, clusterArgs...), "--strict-vectors")...)
	if strictErr == nil || !strings.Contains(strictErr.Error(), "vector coverage is incomplete") || len(bytes.TrimSpace(stdout)) != 0 {
		t.Fatalf("strict rejection: error=%v stdout=%q", strictErr, stdout)
	}
	st = open()
	afterStrict := durableOracleSnapshot(t, ctx, st)
	if !reflect.DeepEqual(beforeStrict.Snapshot, afterStrict) {
		t.Fatal("strict rejection mutated durable persistence")
	}
	strictVectors := durableOracleVectors(t, ctx, st)
	closeStore(st)
	if !reflect.DeepEqual(beforeStrict.Vectors, strictVectors) {
		t.Fatal("strict rejection mutated fixture vectors")
	}
	artifact.Phases = append(artifact.Phases, durableOraclePhase{Name: "strict_rejected", Error: strictErr.Error(), Config: beforeStrict.Config, SelectedThreadIDs: limitedIDs, Snapshot: afterStrict, Vectors: strictVectors})
	artifact.Status = "pass"
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if err := json.NewEncoder(zw).Encode(artifact); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if compressed.Len() > 2<<20 {
		t.Fatalf("compressed oracle exceeds 2 MiB arm budget: %d", compressed.Len())
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(compressed.Bytes()); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("durable oracle: 4 phases, %d compressed bytes", compressed.Len())
}

type durableOracleArtifact struct {
	Schema            int                  `json:"schema"`
	Fixture           string               `json:"fixture"`
	ThreadInputSHA256 string               `json:"thread_input_sha256"`
	Status            string               `json:"status"`
	Phases            []durableOraclePhase `json:"phases"`
}

type durableOraclePhase struct {
	Name              string                      `json:"name"`
	Output            json.RawMessage             `json:"output,omitempty"`
	Error             string                      `json:"error,omitempty"`
	Config            map[string]any              `json:"config"`
	SelectedThreadIDs []int64                     `json:"selected_thread_ids"`
	Snapshot          map[string][]map[string]any `json:"snapshot"`
	Vectors           []durableOracleVector       `json:"vectors"`
}

type durableOracleVector struct {
	ThreadID     int64  `json:"thread_id"`
	Basis        string `json:"basis"`
	Model        string `json:"model"`
	Dimensions   int    `json:"dimensions"`
	ContentHash  string `json:"content_hash"`
	Backend      string `json:"backend"`
	VectorSHA256 string `json:"vector_sha256"`
}

func TestDurableOracleQueryPreservesStoredJSON(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "json-oracle.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows := durableOracleQuery(t, ctx, st, `select NULL as absent_json, 'null' as null_json, '{"x":1,"x":2}' as duplicate_json, '{"x":1} trailing' as trailing_json`)
	if len(rows) != 1 || rows[0]["absent_json"] != nil {
		t.Fatal("SQL NULL was not preserved")
	}
	for column, raw := range map[string]string{
		"null_json":      "null",
		"duplicate_json": `{"x":1,"x":2}`,
		"trailing_json":  `{"x":1} trailing`,
	} {
		if !reflect.DeepEqual(rows[0][column], map[string]string{"stored_json": raw}) {
			t.Fatalf("%s lost raw JSON identity: %#v", column, rows[0][column])
		}
	}
}

func durableOracleVectors(t *testing.T, ctx context.Context, st *store.Store) []durableOracleVector {
	t.Helper()
	rows, err := st.DB().QueryContext(ctx, `select thread_id,basis,model,dimensions,content_hash,vector_backend,vector_json from thread_vectors order by thread_id,basis,model`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := []durableOracleVector{}
	for rows.Next() {
		var v durableOracleVector
		var raw string
		if err := rows.Scan(&v.ThreadID, &v.Basis, &v.Model, &v.Dimensions, &v.ContentHash, &v.Backend, &raw); err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256([]byte(raw))
		v.VectorSHA256 = hex.EncodeToString(hash[:])
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func durableOracleSnapshot(t *testing.T, ctx context.Context, st *store.Store) map[string][]map[string]any {
	t.Helper()
	return map[string][]map[string]any{
		"cluster_groups":      durableOracleQuery(t, ctx, st, "select * from cluster_groups order by id"),
		"cluster_memberships": durableOracleQuery(t, ctx, st, "select * from cluster_memberships order by cluster_id,thread_id"),
		"cluster_runs":        durableOracleQuery(t, ctx, st, "select * from cluster_runs order by id"),
		"cluster_events":      durableOracleQuery(t, ctx, st, "select * from cluster_events order by id"),
	}
}

func durableOracleQuery(t *testing.T, ctx context.Context, st *store.Store, query string) []map[string]any {
	t.Helper()
	rows, err := st.DB().QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	result := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			value := values[i]
			if b, ok := value.([]byte); ok {
				value = string(b)
			}
			if value != nil && strings.HasSuffix(column, "_json") {
				raw, ok := value.(string)
				if !ok {
					t.Fatalf("%s is not JSON text", column)
				}
				// Preserve storage identity, including SQL NULL versus JSON null,
				// duplicate keys, trailing bytes and exact number spelling.
				value = map[string]string{"stored_json": raw}
			}
			if f, ok := value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
				t.Fatalf("non-finite %s", column)
			}
			row[column] = value
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func durableOracleValidateLineage(t *testing.T, current, previous map[string][]map[string]any, selected map[int64]bool, runID int64, allIDs []int64) {
	t.Helper()
	groups := map[int64]map[string]any{}
	for _, row := range current["cluster_groups"] {
		if row["status"] != "active" || row["closed_at"] != nil {
			t.Fatal("partial fixture retired a durable group")
		}
		groups[row["id"].(int64)] = row
	}
	members := map[int64]map[string]any{}
	for _, row := range current["cluster_memberships"] {
		id := row["thread_id"].(int64)
		if row["state"] != "active" || row["removed_at"] != nil || members[id] != nil {
			t.Fatalf("inactive or duplicate membership: %d", id)
		}
		if groups[row["cluster_id"].(int64)] == nil {
			t.Fatal("membership has no durable group")
		}
		members[id] = row
	}
	if len(members) != len(allIDs) {
		t.Fatalf("durable membership count: got %d, want %d", len(members), len(allIDs))
	}
	for _, id := range allIDs {
		if members[id] == nil {
			t.Fatalf("missing durable member %d", id)
		}
	}
	for _, group := range groups {
		rep, ok := group["representative_thread_id"].(int64)
		if !ok || members[rep] == nil || members[rep]["cluster_id"] != group["id"] {
			t.Fatal("representative is not an active member of its group")
		}
	}
	if previous == nil {
		for id, member := range members {
			if member["first_seen_run_id"] != runID || member["last_seen_run_id"] != runID {
				t.Fatalf("initial membership lineage differs for %d", id)
			}
		}
		return
	}
	if len(current["cluster_groups"]) != len(previous["cluster_groups"]) {
		t.Fatal("partial run changed durable group count")
	}
	for _, before := range previous["cluster_groups"] {
		after := groups[before["id"].(int64)]
		for _, key := range []string{"id", "repo_id", "stable_key", "stable_slug", "representative_thread_id", "cluster_type", "title", "status", "closed_at", "created_at"} {
			if after == nil || !reflect.DeepEqual(before[key], after[key]) {
				t.Fatalf("partial run changed group identity field %s", key)
			}
		}
	}
	for _, before := range previous["cluster_memberships"] {
		id := before["thread_id"].(int64)
		after := members[id]
		if before["cluster_id"] != after["cluster_id"] || before["first_seen_run_id"] != after["first_seen_run_id"] {
			t.Fatalf("partial run changed member identity/first seen: %d", id)
		}
		if before["created_at"] != after["created_at"] {
			t.Fatalf("partial run changed member creation time: %d", id)
		}
		wantLast := before["last_seen_run_id"]
		if selected[id] {
			wantLast = runID
		}
		if after["last_seen_run_id"] != wantLast {
			t.Fatalf("partial last-seen lineage differs for %d", id)
		}
	}
}
