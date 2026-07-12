package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
)

func TestRemoteResultHelpersMapRowsAndCoerceTypes(t *testing.T) {
	result := crawlremote.QueryResult{
		Columns: []string{"thread_id", "github_id", "number", "kind", "state", "title", "author", "url", "isDraft", "score"},
		Rows: [][]any{{
			json.Number("42"),
			"gh-42",
			"7",
			"pull",
			"open",
			"remote row",
			"alice",
			"https://example.test/pull/7",
			"true",
			json.Number("0.75"),
		}},
	}

	threads := remoteThreads(result)
	if len(threads) != 1 {
		t.Fatalf("threads len = %d", len(threads))
	}
	if threads[0].ID != 42 || threads[0].Number != 7 || !threads[0].IsDraft || threads[0].LabelsJSON != "[]" {
		t.Fatalf("thread coercion = %#v", threads[0])
	}
	if threads[0].AuthorLogin != "alice" || threads[0].HTMLURL != "https://example.test/pull/7" {
		t.Fatalf("thread string fallbacks = %#v", threads[0])
	}

	hits := remoteSearchHits(result)
	if len(hits) != 1 {
		t.Fatalf("hits len = %d", len(hits))
	}
	if hits[0].ThreadID != 42 || hits[0].Number != 7 || hits[0].Score != 0.75 {
		t.Fatalf("hit coercion = %#v", hits[0])
	}
	if hits[0].Snippet != "remote row" {
		t.Fatalf("hit snippet fallback = %q", hits[0].Snippet)
	}
}

func TestPollRemoteLoginStates(t *testing.T) {
	t.Run("pending then complete", func(t *testing.T) {
		polls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/auth/github/poll" {
				http.NotFound(w, r)
				return
			}
			polls++
			w.Header().Set("content-type", "application/json")
			if polls == 1 {
				_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "pending"})
				return
			}
			_ = json.NewEncoder(w).Encode(crawlremote.LoginPollResult{Status: "complete", Token: "session-token", Login: "alice"})
		}))
		defer server.Close()

		client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{})
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		result, err := pollRemoteLogin(context.Background(), client, "login-1", "secret", time.Second, time.Millisecond)
		if err != nil {
			t.Fatalf("poll login: %v", err)
		}
		if result.Token != "session-token" || polls != 2 {
			t.Fatalf("result=%#v polls=%d", result, polls)
		}
	})

	for _, tc := range []struct {
		name   string
		result crawlremote.LoginPollResult
		want   string
	}{
		{name: "complete missing token", result: crawlremote.LoginPollResult{Status: "complete"}, want: "without token"},
		{name: "error with message", result: crawlremote.LoginPollResult{Status: "error", Error: "denied"}, want: "denied"},
		{name: "error without message", result: crawlremote.LoginPollResult{Status: "error"}, want: "remote login failed"},
		{name: "unknown status", result: crawlremote.LoginPollResult{Status: "confused"}, want: `status "confused"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("content-type", "application/json")
				_ = json.NewEncoder(w).Encode(tc.result)
			}))
			defer server.Close()

			client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{})
			if err != nil {
				t.Fatalf("client: %v", err)
			}
			_, err = pollRemoteLogin(context.Background(), client, "login-1", "secret", time.Second, time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSendIngestRowsBatchesAndFinalizes(t *testing.T) {
	ctx := context.Background()
	var requests []crawlremote.IngestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/v1/apps/gitcrawl/archives/gitcrawl%2Fopenclaw/ingest" {
			http.NotFound(w, r)
			return
		}
		var body crawlremote.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests = append(requests, body)
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{RowsAccepted: int64(len(body.Rows)), Complete: body.Final})
	}))
	defer server.Close()

	client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{
		TokenProvider: crawlremote.StaticToken("publish-token"),
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	rows := make([][]any, gitcrawlCloudBatchSize+1)
	for i := range rows {
		rows[i] = []any{i}
	}
	accepted, err := sendIngestRows(ctx, client, "gitcrawl", "gitcrawl/openclaw", crawlremote.IngestManifest{App: "gitcrawl"}, "threads", []string{"id"}, rows, true)
	if err != nil {
		t.Fatalf("send ingest: %v", err)
	}
	if accepted != int64(len(rows)) {
		t.Fatalf("accepted = %d", accepted)
	}
	if len(requests) != 2 {
		t.Fatalf("requests len = %d", len(requests))
	}
	if requests[0].Cursor != "" || requests[0].Final || len(requests[0].Rows) != gitcrawlCloudBatchSize {
		t.Fatalf("first request = %#v", requests[0])
	}
	if requests[1].Cursor != "250" || !requests[1].Final || len(requests[1].Rows) != 1 {
		t.Fatalf("second request = %#v", requests[1])
	}
}

func TestSendSnapshotIngestRowsRotatesMutationTokens(t *testing.T) {
	ctx := context.Background()
	var requests []crawlremote.IngestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body crawlremote.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests = append(requests, body)
		token := fmt.Sprintf("generation-%d", len(requests))
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{
			SnapshotID:    body.Manifest.SnapshotID,
			MutationToken: token,
			RowsAccepted:  int64(len(body.Rows)),
			Complete:      body.Final,
		})
	}))
	defer server.Close()

	client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{
		TokenProvider: crawlremote.StaticToken("publish-token"),
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	rows := make([][]any, gitcrawlCloudBatchSize+1)
	for i := range rows {
		rows[i] = []any{i}
	}
	progress, err := sendSnapshotIngestRows(
		ctx,
		client,
		"gitcrawl",
		"gitcrawl/openclaw",
		crawlremote.IngestManifest{App: "gitcrawl", SnapshotID: strings.Repeat("a", 64)},
		"threads",
		[]string{"id"},
		rows,
		"previous-dataset",
		true,
	)
	if err != nil {
		t.Fatalf("send snapshot ingest: %v", err)
	}
	if progress.RowsAccepted != int64(len(rows)) || progress.MutationToken != "generation-2" {
		t.Fatalf("progress = %#v", progress)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %#v", requests)
	}
	if requests[0].Cursor != "" || requests[0].MutationToken != "previous-dataset" {
		t.Fatalf("first request = %#v", requests[0])
	}
	if requests[1].Cursor != "250" || requests[1].MutationToken != "generation-1" || !requests[1].Final {
		t.Fatalf("second request = %#v", requests[1])
	}
}

func TestSendIngestRowsDrainsRemoteResetBeforeRetry(t *testing.T) {
	ctx := context.Background()
	var requests []crawlremote.IngestRequest
	resetCalls := 0
	rejectedFirstBatch := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/v1/apps/gitcrawl/archives/gitcrawl%2Fopenclaw/ingest" {
			http.NotFound(w, r)
			return
		}
		var body crawlremote.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("content-type", "application/json")
		if len(body.Rows) == 0 {
			resetCalls++
			_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{ResetIncomplete: resetCalls == 1, ResetDeleted: 10000})
			return
		}
		if body.Cursor == "" && !rejectedFirstBatch {
			rejectedFirstBatch = true
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":   "reset_incomplete",
				"message": "archive table reset is still in progress",
			})
			return
		}
		requests = append(requests, body)
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{RowsAccepted: int64(len(body.Rows)), Complete: body.Final})
	}))
	defer server.Close()

	client, err := crawlremote.NewClientFromConfig(crawlremote.Config{Endpoint: server.URL}, crawlremote.Options{
		TokenProvider: crawlremote.StaticToken("publish-token"),
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	accepted, err := sendIngestRows(ctx, client, "gitcrawl", "gitcrawl/openclaw", crawlremote.IngestManifest{App: "gitcrawl"}, "threads", []string{"id"}, [][]any{{1}}, true)
	if err != nil {
		t.Fatalf("send ingest: %v", err)
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d", accepted)
	}
	if resetCalls != 2 {
		t.Fatalf("resetCalls = %d", resetCalls)
	}
	if len(requests) != 1 || requests[0].Cursor != "" || !requests[0].Final {
		t.Fatalf("data requests = %#v", requests)
	}
}

func TestRemoteAndCloudCommandDispatchErrors(t *testing.T) {
	app := New()
	if err := app.runRemote(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "requires a subcommand") {
		t.Fatalf("remote no args err = %v", err)
	}
	if err := app.runRemote(context.Background(), []string{"unknown"}); err == nil || !strings.Contains(err.Error(), "unknown remote subcommand") {
		t.Fatalf("remote unknown err = %v", err)
	}
	if err := app.runCloud(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "requires a subcommand") {
		t.Fatalf("cloud no args err = %v", err)
	}
	if err := app.runCloud(context.Background(), []string{"unknown"}); err == nil || !strings.Contains(err.Error(), "unknown cloud subcommand") {
		t.Fatalf("cloud unknown err = %v", err)
	}
}

func TestRemoteClientRejectsBearerAuthOverRemoteHTTP(t *testing.T) {
	const tokenEnv = "GITCRAWL_TEST_REMOTE_HTTP_TOKEN"
	t.Setenv(tokenEnv, "test-token")

	cfg := config.Default()
	cfg.Remote = crawlremote.Config{
		Mode:     crawlremote.ModeCloud,
		Endpoint: "http://remote.example.test",
		Archive:  "gitcrawl/example",
		TokenEnv: tokenEnv,
	}

	_, err := New().remoteClient(cfg)
	if err == nil || !strings.Contains(err.Error(), "cannot use bearer auth over http") {
		t.Fatalf("remoteClient error = %v", err)
	}
}
