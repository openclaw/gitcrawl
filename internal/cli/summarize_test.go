package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitcrawlopenai "github.com/openclaw/gitcrawl/internal/openai"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestSummarizeCommandPersistsCurrentRevisionSummary(t *testing.T) {
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
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := store.Thread{
		RepoID:          repoID,
		GitHubID:        "42",
		Number:          42,
		Kind:            "issue",
		State:           "open",
		Title:           "Gateway reconnect loop",
		Body:            "Reconnect loops after websocket timeout.",
		HTMLURL:         "https://github.com/openclaw/openclaw/issues/42",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}
	threadID, err := st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	thread.ID = threadID
	if _, err := st.UpsertDocument(ctx, store.Document{
		ThreadID:   threadID,
		Title:      thread.Title,
		Body:       thread.Body,
		RawText:    thread.Title + "\n\n" + thread.Body,
		DedupeText: thread.Title + " " + thread.Body,
		UpdatedAt:  thread.UpdatedAt,
	}); err != nil {
		t.Fatalf("document: %v", err)
	}
	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, store.ThreadEvidence{Thread: thread}, "2026-07-12T00:00:00Z"); err != nil {
		t.Fatalf("enrichment: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/responses" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		var payload struct {
			Input string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if !strings.Contains(payload.Input, "Gateway reconnect loop") {
			t.Fatalf("input = %q", payload.Input)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": []map[string]any{{
				"type": "message",
				"content": []map[string]any{{
					"type": "output_text",
					"text": "Gateway reconnect loops after websocket timeout.",
				}},
			}},
		})
	}))
	defer server.Close()
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)
	t.Setenv("GITCRAWL_OPENAI_RETRY_DISABLED", "1")

	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "summarize", "openclaw/openclaw", "--json"}); err != nil {
		t.Fatalf("summarize: %v", err)
	}
	var result summaryResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v\n%s", err, stdout.String())
	}
	if result.Selected != 1 || result.Summarized != 1 || result.Status != "success" {
		t.Fatalf("result = %+v", result)
	}

	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	coverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	if len(coverage.Rows) != 1 || coverage.Rows[0].Enrichment.Summaries.Fresh != 1 {
		t.Fatalf("summary coverage = %+v", coverage.Rows)
	}
	runs, err := st.ListRuns(ctx, repoID, "summary", 1)
	if err != nil {
		t.Fatalf("summary runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "success" {
		t.Fatalf("summary runs = %+v", runs)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close before rerun: %v", err)
	}
	stdout.Reset()
	if err := app.Run(ctx, []string{"--config", configPath, "summarize", "openclaw/openclaw", "--json"}); err != nil {
		t.Fatalf("summarize current: %v", err)
	}
	if calls != 1 {
		t.Fatalf("current summary triggered another API call: %d", calls)
	}
}

func TestSummaryInputIncludesThreadIdentity(t *testing.T) {
	input := summaryInput(store.SummaryTask{
		Number: 7,
		Kind:   "pull_request",
		Title:  "Fix reconnect",
		Text:   "Evidence body",
	})
	for _, want := range []string{"kind: pull_request", "number: #7", "title: Fix reconnect", "Evidence body"} {
		if !strings.Contains(input, want) {
			t.Fatalf("input missing %q: %s", want, input)
		}
	}
}

func TestSummarizeCommandRecordsPartialAPIResults(t *testing.T) {
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
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	for _, number := range []int{42, 43} {
		thread := store.Thread{
			RepoID:          repoID,
			GitHubID:        fmt.Sprintf("%d", number),
			Number:          number,
			Kind:            "issue",
			State:           "open",
			Title:           fmt.Sprintf("Summary case %d", number),
			Body:            "Distinct evidence.",
			HTMLURL:         fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", number),
			LabelsJSON:      "[]",
			AssigneesJSON:   "[]",
			RawJSON:         "{}",
			ContentHash:     fmt.Sprintf("thread-%d", number),
			UpdatedAtGitHub: "2026-07-12T00:00:00Z",
			UpdatedAt:       "2026-07-12T00:00:00Z",
		}
		threadID, err := st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("thread %d: %v", number, err)
		}
		thread.ID = threadID
		if _, err := st.UpsertDocument(ctx, store.Document{
			ThreadID:   threadID,
			Title:      thread.Title,
			Body:       thread.Body,
			RawText:    thread.Title + "\n\n" + thread.Body,
			DedupeText: thread.Title + " " + thread.Body,
			UpdatedAt:  thread.UpdatedAt,
		}); err != nil {
			t.Fatalf("document %d: %v", number, err)
		}
		if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, store.ThreadEvidence{Thread: thread}, "2026-07-12T00:00:00Z"); err != nil {
			t.Fatalf("enrichment %d: %v", number, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	failAll := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Input string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if failAll || strings.Contains(payload.Input, "number: #43") {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"message": "summary rejected",
				"type":    "invalid_request_error",
				"code":    "bad_summary",
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"output_text": "Stable summary."})
	}))
	defer server.Close()
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)
	t.Setenv("GITCRAWL_OPENAI_RETRY_DISABLED", "1")

	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "summarize", "openclaw/openclaw", "--json"}); err != nil {
		t.Fatalf("summarize: %v", err)
	}
	var result summaryResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Selected != 2 || result.Summarized != 1 || result.Failed != 1 || result.Status != "partial" {
		t.Fatalf("partial result = %+v", result)
	}
	if len(result.Failures) != 1 || result.Failures[0].Status != http.StatusBadRequest ||
		result.Failures[0].Type != "invalid_request_error" || result.Failures[0].Code != "bad_summary" {
		t.Fatalf("failure metadata = %+v", result.Failures)
	}

	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	runs, err := st.ListRuns(ctx, repoID, "summary", 1)
	if err != nil {
		t.Fatalf("summary runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "partial" || !strings.Contains(runs[0].ErrorText, "summary rejected") {
		t.Fatalf("summary runs = %+v", runs)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close before failed rerun: %v", err)
	}
	failAll = true
	stdout.Reset()
	err = app.Run(ctx, []string{"--config", configPath, "summarize", "openclaw/openclaw", "--force", "--json"})
	if err == nil || !strings.Contains(err.Error(), "summary rejected") {
		t.Fatalf("failed summarize error = %v", err)
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen after failed run: %v", err)
	}
	defer st.Close()
	runs, err = st.ListRuns(ctx, repoID, "summary", 1)
	if err != nil {
		t.Fatalf("failed summary runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "error" {
		t.Fatalf("failed summary run = %+v", runs)
	}
}

func TestSummaryFailurePreservesGenericErrors(t *testing.T) {
	failure := summaryFailure(7, fmt.Errorf("network unavailable"))
	if failure.Number != 7 || failure.Message != "network unavailable" || failure.Status != 0 {
		t.Fatalf("generic failure = %+v", failure)
	}
	apiFailure := summaryFailure(8, &gitcrawlopenai.APIError{
		Status:  http.StatusTooManyRequests,
		Type:    "rate_limit_error",
		Code:    "rate_limit",
		Message: "slow down",
	})
	if apiFailure.Status != http.StatusTooManyRequests || apiFailure.Type != "rate_limit_error" ||
		apiFailure.Code != "rate_limit" || !strings.Contains(apiFailure.Message, "slow down") {
		t.Fatalf("API failure = %+v", apiFailure)
	}
}

func TestSummarizeCancelsWorkersAfterPersistenceFailure(t *testing.T) {
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
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	for _, number := range []int{42, 43} {
		thread := store.Thread{
			RepoID: repoID, GitHubID: fmt.Sprintf("%d", number), Number: number, Kind: "issue", State: "open",
			Title: fmt.Sprintf("Persistence failure %d", number), Body: "evidence",
			HTMLURL:    fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", number),
			LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: fmt.Sprintf("thread-%d", number),
			UpdatedAtGitHub: "2026-07-12T00:00:00Z", UpdatedAt: "2026-07-12T00:00:00Z",
		}
		thread.ID, err = st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("thread %d: %v", number, err)
		}
		if _, err := st.UpsertDocument(ctx, store.Document{
			ThreadID: thread.ID, Title: thread.Title, Body: thread.Body,
			RawText: thread.Title + "\n\n" + thread.Body, DedupeText: thread.Title + " " + thread.Body,
			UpdatedAt: thread.UpdatedAt,
		}); err != nil {
			t.Fatalf("document %d: %v", number, err)
		}
		if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, store.ThreadEvidence{Thread: thread}, thread.UpdatedAt); err != nil {
			t.Fatalf("enrichment %d: %v", number, err)
		}
	}
	if _, err := st.DB().ExecContext(ctx, `
		create trigger reject_summary_insert
		before insert on thread_key_summaries
		begin
			select raise(abort, 'summary persistence rejected');
		end
	`); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	blockedStarted := make(chan struct{})
	blockedCancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Input string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if strings.Contains(payload.Input, "number: #42") {
			close(blockedStarted)
			<-r.Context().Done()
			close(blockedCancelled)
			return
		}
		select {
		case <-blockedStarted:
		case <-time.After(2 * time.Second):
			t.Error("second summary worker did not start")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "output_text": "Fast summary."})
	}))
	defer server.Close()
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)
	t.Setenv("GITCRAWL_OPENAI_RETRY_DISABLED", "1")

	err = New().Run(ctx, []string{"--config", configPath, "summarize", "openclaw/openclaw", "--json"})
	if err == nil || !strings.Contains(err.Error(), "summary persistence rejected") {
		t.Fatalf("summarize error = %v", err)
	}
	select {
	case <-blockedCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked summary worker was not cancelled and joined")
	}
}
