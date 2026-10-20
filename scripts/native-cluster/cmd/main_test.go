//go:build native_cluster_overlay

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/openclaw/gitcrawl/internal/store"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestNativeCombinedCommandAndRestartNoop(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "source.db")
	s, e := store.Open(ctx, db)
	if e != nil {
		t.Fatal(e)
	}
	repo, e := s.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	for i := 1; i <= 3; i++ {
		_, e = s.UpsertThread(ctx, store.Thread{RepoID: repo, GitHubID: strconv.Itoa(i), Number: i, Kind: "issue", State: "open", Title: "native", Body: "body", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"})
		if e != nil {
			t.Fatal(e)
		}
	}
	p := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 10, p)
	if e != nil {
		t.Fatal(e)
	}
	rawReq, _ := json.Marshal(req)
	res := store.NativeEmbeddingResponse{Format: "clawmmunity.native-embeddings.v1", RequestSHA256: store.NativeSHA256(rawReq), Model: req.Model, Dimensions: 1536, Tasks: len(req.Tasks), Ready: len(req.Tasks), Status: "complete"}
	for _, task := range req.Tasks {
		v := make([]float64, 1536)
		v[0] = 1
		res.Results = append(res.Results, struct {
			TaskID      string    `json:"task_id"`
			InputSHA256 string    `json:"input_sha256"`
			Vector      []float64 `json:"vector"`
		}{task.TaskID, task.InputSHA256, v})
	}
	rawRes, _ := json.Marshal(res)
	reqFile := filepath.Join(dir, "request.json")
	resFile := filepath.Join(dir, "response.json")
	os.WriteFile(reqFile, rawReq, 0600)
	os.WriteFile(resFile, rawRes, 0600)

	s.DB().Exec(`create table if not exists analytics_coverage(repository text primary key,through text,issues integer,pull_requests integer,complete integer,observed_at text)`)
	if _, e = s.DB().Exec("insert into analytics_coverage values('openclaw/openclaw','now',3,0,1,?)", time.Now().UTC().Format(time.RFC3339Nano)); e != nil {
		t.Fatal(e)
	}
	s.Close()
	policy := filepath.Join(dir, "policy.json")
	os.WriteFile(policy, p, 0600)
	os.WriteFile(filepath.Join(dir, "runner.lock"), nil, 0600)
	gate := filepath.Join(dir, "gate")
	cache := filepath.Join(dir, "cache.json")
	origArgs, origOut := os.Args, os.Stdout
	defer func() { os.Args = origArgs; os.Stdout = origOut }()
	for round := 0; round < 2; round++ {
		// Native stats, not the host candidate cache, prove no-op.
		os.Args = []string{"producer", "combined", "--db", db, "--consumer-exclusions", policy, "--pair-cache-dir", cache, "--request", reqFile, "--response", resFile, "--receipt", filepath.Join(dir, "receipt"+strconv.Itoa(round)), "--commit-gate", gate}
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		os.Stdout = w
		done := make(chan []string, 1)
		go func() {
			var phases []string
			scan := bufio.NewScanner(r)
			for scan.Scan() {
				var msg map[string]any
				if json.Unmarshal(scan.Bytes(), &msg) != nil {
					continue
				}
				phase, _ := msg["phase"].(string)
				phases = append(phases, phase)
				if phase == "prepared" {
					os.WriteFile(gate, []byte(msg["content_digest"].(string)), 0600)
				}
			}
			r.Close()
			done <- phases
		}()
		e = run()
		w.Close()
		os.Stdout = origOut
		phases := <-done
		if e != nil {
			t.Fatal(e)
		}
		if round == 0 && (len(phases) != 2 || phases[1] != "committed") {
			t.Fatal(phases)
		}
		if round == 1 && (len(phases) != 1 || phases[0] != "unchanged") {
			t.Fatal("restart churn", phases)
		}
	}
	s, e = store.OpenReadOnly(ctx, db)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var count int
	s.DB().QueryRow("select count(*) from cluster_runs").Scan(&count)
	if count != 1 {
		t.Fatal("duplicate run")
	}
}
