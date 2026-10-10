package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

type neighborsTestOutput struct {
	Neighbors []map[string]any `json:"neighbors"`
}

func TestNeighborsRowsCarryThreadStateAndTimes(t *testing.T) {
	configPath := seedNeighborTimelineStore(t)

	out := runNeighborsTest(t, configPath, 200, "--include-closed")
	rows := map[int]map[string]any{}
	for _, row := range out.Neighbors {
		rows[int(row["number"].(float64))] = row
	}
	open := rows[201]
	if open["state"] != "open" || open["html_url"] != "https://github.com/openclaw/openclaw/pull/201" ||
		open["author_login"] != "carol" || open["author_association"] != "CONTRIBUTOR" ||
		open["created_at_gh"] != "2026-09-01T00:00:00Z" || open["is_draft"] != false {
		t.Fatalf("open neighbor row = %#v", open)
	}
	if _, ok := open["merged_at_gh"]; ok {
		t.Fatalf("open neighbor row should omit merged_at_gh: %#v", open)
	}
	merged := rows[202]
	if merged["state"] != "closed" || merged["closed_at_gh"] != "2026-09-05T00:00:00Z" || merged["merged_at_gh"] != "2026-09-05T00:00:00Z" {
		t.Fatalf("merged neighbor row = %#v", merged)
	}
	closed := rows[204]
	if closed["state"] != "closed" || closed["closed_at_gh"] != "2026-09-12T00:00:00Z" {
		t.Fatalf("closed neighbor row = %#v", closed)
	}
	if _, ok := closed["merged_at_gh"]; ok {
		t.Fatalf("closed issue row should omit merged_at_gh: %#v", closed)
	}
}

func TestNeighborsTimeFilters(t *testing.T) {
	configPath := seedNeighborTimelineStore(t)

	for _, tc := range []struct {
		name   string
		number int
		args   []string
		want   []int
	}{
		{name: "default open now", want: []int{201, 205}},
		{name: "open at includes rows closed since", args: []string{"--open-at", "2026-09-10T12:00:00Z"}, want: []int{201, 204}},
		// 2026-09-11T23:00Z: #204 closes an hour later, #203 is already open.
		{name: "open at compares instants across offsets", args: []string{"--open-at", "2026-09-12T08:00:00+09:00"}, want: []int{201, 204, 203}},
		{name: "open at includes a row created at T", args: []string{"--open-at", "2026-09-15"}, want: []int{201, 205, 203}},
		{name: "open at excludes a row closed at T", args: []string{"--open-at", "2026-09-12"}, want: []int{201, 203}},
		// Half a second after #205 was created and #204 closed; as text, "…00Z" sorts after "…00.5Z".
		{name: "open at compares creation instants, not text", args: []string{"--open-at", "2026-09-15T00:00:00.5Z"}, want: []int{201, 205, 203}},
		{name: "open at compares closure instants, not text", args: []string{"--open-at", "2026-09-12T00:00:00.5Z"}, want: []int{201, 203}},
		{name: "created after open rows", args: []string{"--created-after", "2026-09-10"}, want: []int{205}},
		{name: "created after with closed rows", args: []string{"--created-after", "2026-09-10", "--include-closed"}, want: []int{205, 203}},
		{name: "created after excludes a row created at T", args: []string{"--created-after", "2026-09-11", "--include-closed"}, want: []int{205}},
		{name: "created after compares instants, not text", args: []string{"--created-after", "2026-09-11T00:00:00.5Z", "--include-closed"}, want: []int{205}},
		{name: "created after number", args: []string{"--created-after", "202"}, want: []int{205}},
		// Numbers, not clocks: #203 and #204 rank after #202 whatever their created_at.
		{name: "created after number with closed rows", args: []string{"--created-after", "#202", "--include-closed"}, want: []int{204, 205, 203}},
		{name: "created after thread url", args: []string{"--created-after", "https://github.com/openclaw/openclaw/pull/202", "--include-closed"}, want: []int{204, 205, 203}},
		{name: "merged after", args: []string{"--merged-after", "2026-09-10"}, want: []int{203}},
		{name: "merged after excludes a merge at T", args: []string{"--merged-after", "2026-09-05"}, want: []int{203}},
		// Half a second after #202 merged; as text, "…00Z" sorts after "…00.5Z".
		{name: "merged after compares instants across offsets", args: []string{"--merged-after", "2026-09-05T09:00:00.5+09:00"}, want: []int{203}},
		{name: "filters apply before limit", args: []string{"--merged-after", "2026-09-10", "--limit", "1"}, want: []int{203}},
		{name: "closed source with open at", number: 204, args: []string{"--open-at", "2026-09-01T12:00:00Z"}, want: []int{202, 201}},
		{name: "closed source with merged after", number: 202, args: []string{"--merged-after", "2026-09-10"}, want: []int{203}},
		{name: "filters combine", args: []string{"--open-at", "2026-09-10T12:00:00Z", "--created-after", "2026-08-15"}, want: []int{201}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			number := tc.number
			if number == 0 {
				number = 200
			}
			out := runNeighborsTest(t, configPath, number, tc.args...)
			var got []int
			for _, row := range out.Neighbors {
				got = append(got, int(row["number"].(float64)))
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("neighbors %v = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}

func TestNeighborsRejectsInvalidTimeFilters(t *testing.T) {
	configPath := seedNeighborTimelineStore(t)
	for _, flagName := range []string{"--open-at", "--created-after", "--merged-after"} {
		app := New()
		app.Stdout = &bytes.Buffer{}
		app.Stderr = &bytes.Buffer{}
		err := app.Run(context.Background(), []string{"--config", configPath, "neighbors", "openclaw/openclaw", "--number", "200", flagName, "last week", "--json"})
		var cliErr *cliError
		if !errors.As(err, &cliErr) || cliErr.code != 2 || !strings.Contains(err.Error(), "RFC 3339") {
			t.Fatalf("%s invalid time error = %v, want usage error", flagName, err)
		}
	}
}

func TestNeighborsRejectsCreatedAfterFromAnotherRepository(t *testing.T) {
	configPath := seedNeighborTimelineStore(t)
	app := New()
	app.Stdout = &bytes.Buffer{}
	app.Stderr = &bytes.Buffer{}
	err := app.Run(context.Background(), []string{"--config", configPath, "neighbors", "openclaw/openclaw", "--number", "200", "--created-after", "openclaw/gitcrawl#202", "--json"})
	var cliErr *cliError
	if !errors.As(err, &cliErr) || cliErr.code != 2 || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("cross-repository --created-after error = %v, want usage error", err)
	}
}

func TestNeighborsOmitsStateAsOfWithoutCompleteSync(t *testing.T) {
	configPath := seedNeighborTimelineStore(t)
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(context.Background(), []string{"--config", configPath, "neighbors", "openclaw/openclaw", "--number", "200", "--json"}); err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode neighbors output: %v", err)
	}
	if _, ok := out["state_as_of"]; ok {
		t.Fatalf("state_as_of without a complete sync = %v, want omitted", out["state_as_of"])
	}
}

func runNeighborsTest(t *testing.T, configPath string, number int, extra ...string) neighborsTestOutput {
	t.Helper()
	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	args := append([]string{"--config", configPath, "neighbors", "openclaw/openclaw", "--number", strconv.Itoa(number), "--threshold", "0.1", "--json"}, extra...)
	if err := app.Run(context.Background(), args); err != nil {
		t.Fatalf("neighbors %v: %v", extra, err)
	}
	var out neighborsTestOutput
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode neighbors output %q: %v", stdout.String(), err)
	}
	return out
}

// seedNeighborTimelineStore seeds issue #200 (filed 2026-09-10) and similar
// rows whose GitHub timestamps place them before, around, and after it.
func seedNeighborTimelineStore(t *testing.T) string {
	t.Helper()
	t.Setenv("OPENAI_API_KEY", "")
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:        "openclaw",
		Name:         "openclaw",
		FullName:     "openclaw/openclaw",
		GitHubRepoID: "12345",
		RawJSON:      `{"full_name":"openclaw/openclaw"}`,
		UpdatedAt:    now,
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	type seed struct {
		number          int
		kind, state     string
		created, closed string
		merged          string
		vector          []float64
	}
	seeds := []seed{
		{number: 200, kind: "issue", state: "open", created: "2026-09-10T00:00:00Z", vector: []float64{1, 0}},
		{number: 201, kind: "pull_request", state: "open", created: "2026-09-01T00:00:00Z", vector: []float64{0.99, 0.14}},
		{number: 202, kind: "pull_request", state: "closed", created: "2026-09-01T00:00:00Z", closed: "2026-09-05T00:00:00Z", merged: "2026-09-05T00:00:00Z", vector: []float64{0.98, 0.2}},
		{number: 204, kind: "issue", state: "closed", created: "2026-08-01T00:00:00Z", closed: "2026-09-12T00:00:00Z", vector: []float64{0.97, 0.24}},
		{number: 205, kind: "pull_request", state: "open", created: "2026-09-15T00:00:00Z", vector: []float64{0.96, 0.28}},
		{number: 203, kind: "pull_request", state: "closed", created: "2026-09-11T00:00:00Z", closed: "2026-09-20T00:00:00Z", merged: "2026-09-20T00:00:00Z", vector: []float64{0.8, 0.6}},
	}
	for _, s := range seeds {
		path := "issues"
		if s.kind == "pull_request" {
			path = "pull"
		}
		id, err := st.UpsertThread(ctx, store.Thread{
			RepoID:            repoID,
			GitHubID:          "gh-" + strconv.Itoa(s.number),
			Number:            s.number,
			Kind:              s.kind,
			State:             s.state,
			Title:             "Gateway websocket stalls " + strconv.Itoa(s.number),
			Body:              "Gateway websocket stalls when messages arrive.",
			AuthorLogin:       "carol",
			AuthorType:        "User",
			AuthorAssociation: "CONTRIBUTOR",
			HTMLURL:           "https://github.com/openclaw/openclaw/" + path + "/" + strconv.Itoa(s.number),
			LabelsJSON:        "[]",
			AssigneesJSON:     "[]",
			RawJSON:           "{}",
			ContentHash:       "thread-" + strconv.Itoa(s.number),
			CreatedAtGitHub:   s.created,
			UpdatedAtGitHub:   s.created,
			ClosedAtGitHub:    s.closed,
			MergedAtGitHub:    s.merged,
			UpdatedAt:         now,
		})
		if err != nil {
			t.Fatalf("seed thread %d: %v", s.number, err)
		}
		if _, err := st.UpsertDocument(ctx, store.Document{
			ThreadID:   id,
			Title:      "Gateway websocket stalls " + strconv.Itoa(s.number),
			RawText:    "Gateway websocket stalls when messages arrive.",
			DedupeText: "gateway websocket stalls when messages arrive.",
			UpdatedAt:  now,
		}); err != nil {
			t.Fatalf("seed document %d: %v", s.number, err)
		}
		tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
			RepoID:        repoID,
			Basis:         "title_original",
			Model:         "text-embedding-3-small",
			Number:        s.number,
			Limit:         1,
			IncludeClosed: true,
		})
		if err != nil || len(tasks) != 1 {
			t.Fatalf("embedding task for %d = %#v, %v", s.number, tasks, err)
		}
		if err := st.UpsertThreadVector(ctx, store.ThreadVector{
			ThreadID:    id,
			Basis:       "title_original",
			Model:       "text-embedding-3-small",
			Dimensions:  2,
			ContentHash: tasks[0].ContentHash,
			Vector:      s.vector,
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			t.Fatalf("seed vector %d: %v", s.number, err)
		}
	}
	return configPath
}
