package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
)

func threadListTestPage(number string) remote.QueryResult {
	columns := strings.Fields("id repo_id github_id number kind state title body author_login author_type author_association html_url labels_json assignees_json content_hash is_draft created_at_gh updated_at_gh closed_at_gh merged_at_gh first_pulled_at last_pulled_at updated_at closed_at_local close_reason_local")
	values := map[string]any{"id": number, "repo_id": "1", "number": number, "is_draft": "0", "title": "full\x00body"}
	row := make([]any, len(columns))
	for i, col := range columns {
		row[i] = values[col]
	}
	return remote.QueryResult{Columns: columns, Rows: [][]any{row}, Stats: remote.QueryStats{SnapshotID: "fixed-pin"}}
}

func TestRemoteThreadsSplitsSelectionAtOnePin(t *testing.T) {
	t.Setenv("CRAWL_REMOTE_TOKEN", "fixture")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request remote.QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		numbers, ok := request.Args["numbers"].([]any)
		if !ok || len(numbers) > 500 || request.Args["repository"] != "owner/repo" || request.Args["includeClosed"] != true {
			t.Error("invalid selection")
		}
		if calls == 1 && request.SnapshotID != "" || calls > 1 && request.SnapshotID != "fixed-pin" {
			t.Error("lost fixed pin")
		}
		json.NewEncoder(w).Encode(threadListTestPage(numbers[0].(string)))
	}))
	defer server.Close()
	cfg := config.Default()
	cfg.Remote.Endpoint = server.URL
	cfg.Remote.Archive = "fixture"
	cfg.Remote.Mode = remote.ModeCloud
	a := New()
	var out bytes.Buffer
	a.Stdout = &out
	a.format = FormatJSON
	numbers := []int{1}
	for n := 501; n >= 1; n-- {
		numbers = append(numbers, n)
	}
	if err := a.runRemoteThreads(context.Background(), cfg, "owner/repo", true, numbers, 0); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !strings.Contains(out.String(), `"number": 1`) {
		t.Fatalf("split/limit differs: %d %s", calls, out.String())
	}
	calls = 0
	out.Reset()
	if err := a.runRemoteThreads(context.Background(), cfg, "owner/repo", true, numbers, 1); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("read beyond global limit", calls)
	}
}

func TestRemoteThreadsRejectsChangedPinAndMalformedRows(t *testing.T) {
	t.Setenv("CRAWL_REMOTE_TOKEN", "fixture")
	for _, broken := range []string{"pin", "row", "cursor"} {
		t.Run(broken, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				page := threadListTestPage(strconv.Itoa(calls))
				page.Cursor = "next"
				if calls == 2 {
					switch broken {
					case "pin":
						page.Stats.SnapshotID = "changed"
					case "row":
						page.Rows[0][0] = float64(9007199254740993)
					case "cursor":
						page.Rows = nil
					}
				}
				json.NewEncoder(w).Encode(page)
			}))
			defer server.Close()
			cfg := config.Default()
			cfg.Remote.Endpoint = server.URL
			cfg.Remote.Archive = "fixture"
			cfg.Remote.Mode = remote.ModeCloud
			a := New()
			var out bytes.Buffer
			a.Stdout = &out
			a.format = FormatJSON
			if err := a.runRemoteThreads(context.Background(), cfg, "owner/repo", false, nil, 0); err == nil {
				t.Fatal("accepted invalid page")
			}
			if calls != 2 {
				t.Fatalf("expected two real requests, got %d", calls)
			}
			if out.Len() != 0 {
				t.Fatal("printed partial result")
			}
		})
	}
}

func TestRemoteStatusUsesAdvertisedSchema(t *testing.T) {
	cfg := config.Default()
	status := remoteControlStatus("fixture.toml", cfg, remote.Status{SchemaName: "postgres-gitcrawl-current"})
	if len(status.Databases) != 1 || status.Databases[0].Kind != "postgres-gitcrawl-current" {
		t.Fatal("remote schema was relabeled", status.Databases)
	}
}
