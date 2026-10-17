package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

var updateDefaultOutputGolden = flag.Bool("update-default-output", false, "rewrite testdata/default_output golden files")

// TestNeighborsDefaultOutputGolden pins what neighbors prints without any
// time filter, so a new filter or field cannot silently change the rows,
// order, or scores existing scripts and agents read.
func TestNeighborsDefaultOutputGolden(t *testing.T) {
	configPath := seedDefaultOutputStore(t)
	run := func(t *testing.T, args ...string) string {
		t.Helper()
		app := New()
		var stdout, stderr bytes.Buffer
		app.Stdout = &stdout
		app.Stderr = &stderr
		if err := app.Run(context.Background(), append([]string{"--config", configPath}, args...)); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, stderr.String())
		}
		return stdout.String()
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"neighbors.json", []string{"neighbors", "openclaw/openclaw", "--number", "301", "--json"}},
		{"neighbors.txt", []string{"neighbors", "openclaw/openclaw", "--number", "301"}},
		{"neighbors-limit-threshold.json", []string{"neighbors", "openclaw/openclaw", "--number", "301", "--limit", "2", "--threshold", "0.95", "--json"}},
		{"neighbors-include-closed.json", []string{"neighbors", "openclaw/openclaw", "--number", "301", "--include-closed", "--json"}},
		{"neighbors-closed-source.json", []string{"neighbors", "openclaw/openclaw", "--number", "#303", "--include-closed", "--json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeDefaultOutput(t, run(t, tc.args...))
			path := filepath.Join("testdata", "default_output", tc.name)
			if *updateDefaultOutputGolden {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run with -update-default-output to create): %v", err)
			}
			if got != string(want) {
				t.Fatalf("%v output changed\n--- want %s\n%s\n--- got\n%s", tc.args, path, want, got)
			}
		})
	}
}

// Local bookkeeping clocks change on every run; GitHub clocks are fixed by the seed.
var defaultOutputVolatileKeys = map[string]bool{
	"first_pulled_at": true, "last_pulled_at": true, "updated_at": true,
}

var defaultOutputLocalClock = regexp.MustCompile(`"(` + strings.Join(func() []string {
	keys := make([]string, 0, len(defaultOutputVolatileKeys))
	for key := range defaultOutputVolatileKeys {
		keys = append(keys, key)
	}
	return keys
}(), "|") + `)": ("[^"]*"|[0-9.]+)`)

func normalizeDefaultOutput(t *testing.T, out string) string {
	t.Helper()
	return defaultOutputLocalClock.ReplaceAllString(out, `"$1": "<local clock>"`)
}

// seedDefaultOutputStore seeds two topics with fixed GitHub clocks: gateway
// rows (open, merged, and closed) and unrelated cache rows.
func seedDefaultOutputStore(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}

	embeddings := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode embeddings request: %v", err)
		}
		data := make([]map[string]any, 0, len(payload.Input))
		for index := range payload.Input {
			data = append(data, map[string]any{"index": index, "embedding": []float64{1, 0, 0}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	t.Cleanup(embeddings.Close)
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", embeddings.URL)

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	const clock = "2026-01-01T00:00:00Z"
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:        "openclaw",
		Name:         "openclaw",
		FullName:     "openclaw/openclaw",
		GitHubRepoID: "12345",
		RawJSON:      `{"full_name":"openclaw/openclaw"}`,
		UpdatedAt:    clock,
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	// A complete open sync, then a targeted one that must not move the state clock.
	for _, run := range []store.RunRecord{
		{Scope: "open", StartedAt: "2026-03-20T09:00:00Z", FinishedAt: "2026-03-20T09:02:00Z", StatsJSON: `{"closed_sweep_through":"2026-03-20T09:00:00Z"}`},
		{Scope: "numbers:301", StartedAt: "2026-03-21T09:00:00Z", FinishedAt: "2026-03-21T09:00:05Z", StatsJSON: `{}`},
	} {
		run.RepoID, run.Kind, run.Status = repoID, "sync", "success"
		if _, err := st.RecordRun(ctx, run); err != nil {
			t.Fatalf("seed sync run: %v", err)
		}
	}
	type seed struct {
		number                  int
		kind, state, title      string
		author, association     string
		created, closed, merged string
		draft                   bool
		vector                  []float64
	}
	seeds := []seed{
		{301, "issue", "open", "Gateway websocket stalls on reconnect", "alice", "NONE", "2026-03-01T10:00:00Z", "", "", false, []float64{1, 0, 0}},
		{302, "pull_request", "open", "Fix gateway websocket reconnect stall", "bob", "CONTRIBUTOR", "2026-03-02T10:00:00Z", "", "", true, []float64{0.99, 0.1, 0}},
		{303, "pull_request", "closed", "Gateway websocket reconnect backoff", "carol", "MEMBER", "2026-02-20T10:00:00Z", "2026-03-05T10:00:00Z", "2026-03-05T10:00:00Z", false, []float64{0.98, 0.15, 0.1}},
		{304, "issue", "closed", "Gateway websocket drops messages", "dave", "NONE", "2026-01-10T10:00:00Z", "2026-02-01T10:00:00Z", "", false, []float64{0.97, 0.2, 0.1}},
		{305, "issue", "open", "Gateway websocket timeout after sleep", "erin", "", "2026-03-10T10:00:00Z", "", "", false, []float64{0.96, 0.25, 0.1}},
		{306, "issue", "open", "Portable cache grows without bound", "frank", "NONE", "2026-03-03T10:00:00Z", "", "", false, []float64{0, 1, 0}},
		{307, "pull_request", "open", "Prune the portable cache", "grace", "CONTRIBUTOR", "2026-03-04T10:00:00Z", "", "", false, []float64{0.1, 0.99, 0}},
	}
	for _, s := range seeds {
		path := "issues"
		if s.kind == "pull_request" {
			path = "pull"
		}
		body := s.title + " in the default output fixture."
		id, err := st.UpsertThread(ctx, store.Thread{
			RepoID:            repoID,
			GitHubID:          "gh-" + strconv.Itoa(s.number),
			Number:            s.number,
			Kind:              s.kind,
			State:             s.state,
			Title:             s.title,
			Body:              body,
			AuthorLogin:       s.author,
			AuthorType:        "User",
			AuthorAssociation: s.association,
			HTMLURL:           "https://github.com/openclaw/openclaw/" + path + "/" + strconv.Itoa(s.number),
			LabelsJSON:        "[]",
			AssigneesJSON:     "[]",
			RawJSON:           "{}",
			ContentHash:       "thread-" + strconv.Itoa(s.number),
			IsDraft:           s.draft,
			CreatedAtGitHub:   s.created,
			UpdatedAtGitHub:   s.created,
			ClosedAtGitHub:    s.closed,
			MergedAtGitHub:    s.merged,
			FirstPulledAt:     clock,
			LastPulledAt:      clock,
			UpdatedAt:         clock,
		})
		if err != nil {
			t.Fatalf("seed thread %d: %v", s.number, err)
		}
		if _, err := st.UpsertDocument(ctx, store.Document{
			ThreadID:   id,
			Title:      s.title,
			RawText:    s.title + "\n\n" + body,
			DedupeText: strings.ToLower(s.title + " " + body),
			UpdatedAt:  clock,
		}); err != nil {
			t.Fatalf("seed document %d: %v", s.number, err)
		}
		tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
			RepoID: repoID, Basis: "title_original", Model: "text-embedding-3-small",
			Number: s.number, Limit: 1, IncludeClosed: true,
		})
		if err != nil || len(tasks) != 1 {
			t.Fatalf("embedding task for %d = %#v, %v", s.number, tasks, err)
		}
		if err := st.UpsertThreadVector(ctx, store.ThreadVector{
			ThreadID:    id,
			Basis:       "title_original",
			Model:       "text-embedding-3-small",
			Dimensions:  3,
			ContentHash: tasks[0].ContentHash,
			Vector:      s.vector,
			CreatedAt:   clock,
			UpdatedAt:   clock,
		}); err != nil {
			t.Fatalf("seed vector %d: %v", s.number, err)
		}
	}
	return configPath
}
