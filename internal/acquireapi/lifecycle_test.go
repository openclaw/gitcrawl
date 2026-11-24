package acquireapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
)

const lifecycleColumns = "id repo_id github_id number kind state title body author_login author_type author_association html_url labels_json assignees_json raw_json content_hash is_draft created_at_gh updated_at_gh closed_at_gh merged_at_gh closed_at_local close_reason_local first_pulled_at last_pulled_at observation_sequence evidence_observation_sequence evidence_source_updated_at updated_at sequence ordinal"
const lifecycleOriginalEvent = "{\n \"__typename\":\"ClosedEvent\", \"id\":\"close-7\", \"createdAt\":\"2026-01-02T00:00:00Z\", \"actor\":null, \"extra\":\"λ\\u0000\"\n}"

type lifecycleFixture struct {
	api              *fixtureAPI
	archiveHandler   http.Handler
	client           *Client
	provider         *github.OctopoolConversations
	providerCursors  []string
	parentDrift      bool
	missingParent    bool
	queryPins        []string
	terminalFirst    bool
	expectedSince    string
	windowNumbers    []int
	failCandidates   bool
	candidateToken   string
	windowPin        string
	candidateQueries []remote.QueryRequest
	providerNumbers  []int
}

func newLifecycleFixture(t *testing.T) *lifecycleFixture {
	t.Helper()
	horizon := time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)
	f := &lifecycleFixture{expectedSince: "2026-01-01T00:00:00Z", api: &fixtureAPI{t: t, state: State{Revision: 1, Sequence: 20, CompletedThrough: &horizon, Checkpoint: json.RawMessage(`{"version":1,"phase":"previous-mode"}`)}, pages: map[string][]byte{}, receipts: map[string]Receipt{}}}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/query") {
			active := f.api.state.ActiveRun
			f.api.serve(w, r)
			if active != nil && f.api.state.ActiveRun == nil {
				f.api.state.CompletedRun = active
			}
			return
		}
		var q remote.QueryRequest
		if json.NewDecoder(r.Body).Decode(&q) != nil {
			t.Error("invalid query")
		}
		token := "reader"
		if q.Name == "gitcrawl.lifecycle.candidates" && f.candidateToken != "" {
			token = f.candidateToken
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "reader credential refused", http.StatusUnauthorized)
			return
		}
		pin := "pg-gitcrawl-current-v1:seed:20"
		if f.windowPin != "" {
			pin = f.windowPin
		}
		if q.Name == "gitcrawl.repositories.current" {
			latest := pin
			if f.windowPin != "" {
				latest = fmt.Sprintf("pg-gitcrawl-current-v1:seed:%d", f.api.state.Sequence)
			}
			json.NewEncoder(w).Encode(remote.QueryResult{Columns: []string{"id", "full_name", "github_repo_id"}, Rows: [][]any{{"3", "EXAMPLE/PROJECT", "42"}}, Stats: remote.QueryStats{SnapshotID: latest}})
			return
		}
		if q.Name == "gitcrawl.lifecycle.candidates" {
			f.candidateQueries = append(f.candidateQueries, q)
			if f.failCandidates {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			if q.SnapshotID != pin || q.Limit < 1 || q.Limit > 500 || q.Cursor != "" || q.Args["since"] != f.expectedSince || q.Args["until"] != "2026-01-04T00:00:00Z" {
				t.Error("changed lifecycle window query", q)
			}
			after, _ := q.Args["after_number"].(string)
			minimum, _ := strconv.Atoi(after)
			result := remote.QueryResult{Columns: []string{"number"}, Stats: remote.QueryStats{SnapshotID: pin, SchemaVersion: 1, ObservationOrder: "completed-publication/thread-number"}}
			for _, number := range f.windowNumbers {
				if number > minimum {
					result.Rows = append(result.Rows, []any{strconv.Itoa(number)})
					if len(result.Rows) == q.Limit {
						break
					}
				}
			}
			json.NewEncoder(w).Encode(result)
			return
		}
		if q.Name != "gitcrawl.threads.list" || q.Args["repository"] != "EXAMPLE/PROJECT" || q.Args["includeClosed"] != true || q.SnapshotID != pin {
			t.Error("not a selected pinned parent query", q)
		}
		f.queryPins = append(f.queryPins, q.SnapshotID)
		result := remote.QueryResult{Columns: strings.Fields(lifecycleColumns), Rows: [][]any{}, Stats: remote.QueryStats{SnapshotID: q.SnapshotID}}
		if !f.missingParent {
			for _, v := range q.Args["numbers"].([]any) {
				n, _ := strconv.Atoi(v.(string))
				change, err := remotedelivery.ProjectAcquiredThread(3, int64(100+n), "example/project", map[string]any{"id": json.Number(strconv.Itoa(500 + n)), "number": json.Number(strconv.Itoa(n)), "node_id": fmt.Sprint("PR-", n), "title": "retained", "body": "retained\x00body", "pull_request": map[string]any{}}, nil, "2026-01-01T00:00:00Z", nil)
				if err != nil {
					t.Fatal(err)
				}
				row := make([]any, 0, len(change.Row)+2)
				for _, cell := range change.Row {
					if cell.Value == nil {
						row = append(row, nil)
					} else {
						row = append(row, *cell.Value)
					}
				}
				seq := "5"
				if f.parentDrift {
					seq = "6"
				}
				row = append(row, seq, "0")
				result.Rows = append(result.Rows, row)
			}
		}
		json.NewEncoder(w).Encode(result)
	}))
	f.archiveHandler = api.Config.Handler
	t.Cleanup(api.Close)
	var err error
	f.client, err = NewClient(api.URL, "fixture", "publisher", "reader", nil)
	if err != nil {
		t.Fatal(err)
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/github/request" || r.Header.Get("Authorization") != "Bearer relay" {
			t.Error("wrong Octopool boundary")
		}
		var request struct {
			Method, Path, Pool string
			GraphQL            struct {
				Query     string
				Variables map[string]any
			}
			Headers map[string]string
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Method != "POST" || request.Path != "/graphql" || request.Headers["cache-control"] != "max-age=0" {
			t.Error("not fresh native GraphQL")
		}
		cursor, _ := request.GraphQL.Variables["cursor"].(string)
		f.providerCursors = append(f.providerCursors, cursor)
		if request.GraphQL.Variables["since"] != f.expectedSince {
			t.Error("changed since", request.GraphQL.Variables)
		}
		selected, ok := request.GraphQL.Variables["number"].(float64)
		number := int(selected)
		f.providerNumbers = append(f.providerNumbers, number)
		if !ok || number < 1 || request.GraphQL.Variables["owner"] != "example" || request.GraphQL.Variables["repo"] != "project" {
			t.Error("invalid repository-scoped lifecycle selection")
		}
		node := fmt.Sprintf("PR-%d", number)
		events, page := lifecycleOriginalEvent, `{"hasNextPage":true,"endCursor":"next"}`
		if f.terminalFirst {
			page = `{"hasNextPage":false,"endCursor":"last"}`
		}
		if cursor != "" {
			events = `{"__typename":"MergedEvent","id":"merge-7","createdAt":"2026-01-03T00:00:00Z","actor":{"login":"merger"}}, {"__typename":"ClosedEvent","id":"later","createdAt":"2026-01-05T00:00:00Z","actor":null}`
			page = `{"hasNextPage":false,"endCursor":"last"}`
		}
		fmt.Fprintf(w, `{"status":200,"body_encoding":"json","relay":{"route_kind":"graphql_read","cache":"miss"},"identity":{"kind":"github_app"},"body":{"data":{"repository":{"nameWithOwner":"example/project","issueOrPullRequest":{"id":%q,"__typename":"PullRequest","number":%d,"repository":{"nameWithOwner":"example/project"},"timelineItems":{"nodes":[%s],"pageInfo":%s}}}}}}`, node, number, events, page)
	}))
	t.Cleanup(provider.Close)
	f.provider, err = github.NewOctopoolConversations(provider.URL, "fixture", "relay", "example/project")
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func lifecycleOptions() LifecycleOptions {
	return LifecycleOptions{Repository: "example/project", Numbers: []int{7}, Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), MaxPages: 10}
}

func TestLifecycleHTTPPreservesRawAndResumesLostACK(t *testing.T) {
	f := newLifecycleFixture(t)
	f.api.drop = 2
	ctx, cancel := runContext()
	defer cancel()
	if _, err := RunLifecycle(ctx, f.client, f.provider, lifecycleOptions()); err == nil {
		t.Fatal("lost ACK must surface")
	}
	if len(f.api.pages) != 1 || f.api.state.ActiveRun == nil || len(f.providerCursors) != 1 {
		t.Fatal("first page not retained")
	}
	o := lifecycleOptions()
	o.Since = time.Time{}
	result, err := RunLifecycle(ctx, f.client, f.provider, o)
	if err != nil || !result.Complete || result.Threads != 1 || f.api.begins != 1 || !reflect.DeepEqual(f.providerCursors, []string{"", "next"}) {
		t.Fatal("did not resume original cursor", result, err, f.providerCursors)
	}
	if !f.api.state.CompletedThrough.Equal(time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)) || len(f.api.changes) != 0 {
		t.Fatal("lifecycle advanced source horizon or wrote parents")
	}
	var first PageRequest
	json.Unmarshal(f.api.pages["lifecycle-1"], &first)
	if first.Lifecycle.Events[0].RawJSON != lifecycleOriginalEvent || first.ChildObservation != nil || len(first.Changes) != 0 {
		t.Fatal("lost raw bytes or fabricated child facts")
	}
	var last PageRequest
	json.Unmarshal(f.api.pages["lifecycle-2"], &last)
	if !last.Final || !last.Lifecycle.Terminal || last.Lifecycle.NextCursor != "" || len(last.Lifecycle.Events) != 1 || strings.Contains(last.Lifecycle.Events[0].RawJSON, "later") {
		t.Fatal("horizon or terminal mapping", last)
	}
}

func TestLifecyclePageLimitAndChangedSelection(t *testing.T) {
	f := newLifecycleFixture(t)
	ctx, cancel := runContext()
	defer cancel()
	o := lifecycleOptions()
	o.MaxPages = 1
	result, err := RunLifecycle(ctx, f.client, f.provider, o)
	if err != nil || result.Complete || len(f.api.pages) != 1 {
		t.Fatal("page bound", result, err)
	}
	for _, mode := range []string{"selection", "since", "parent"} {
		o = lifecycleOptions()
		if mode == "selection" {
			o.Numbers = []int{8}
		}
		if mode == "since" {
			o.Since = o.Since.Add(time.Hour)
		}
		f.parentDrift = mode == "parent"
		if _, err = RunLifecycle(ctx, f.client, f.provider, o); err == nil {
			t.Fatal("resumed changed", mode)
		}
	}
	if len(f.providerCursors) != 1 || len(f.api.pages) != 1 {
		t.Fatal("mismatch fetched or wrote")
	}
}

func TestLifecycleMissingParentAndBoundsBeforeBegin(t *testing.T) {
	f := newLifecycleFixture(t)
	f.missingParent = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := RunLifecycle(ctx, f.client, f.provider, lifecycleOptions()); err == nil {
		t.Fatal("missing parent accepted")
	}
	f.missingParent = false
	for _, numbers := range [][]int{nil, {7, 7}, {0}, make([]int, 501)} {
		o := lifecycleOptions()
		o.Numbers = numbers
		if _, err := RunLifecycle(ctx, f.client, f.provider, o); err == nil {
			t.Fatal("invalid selection")
		}
	}
	o := lifecycleOptions()
	o.Since = time.Time{}
	if _, err := RunLifecycle(ctx, f.client, f.provider, o); err == nil {
		t.Fatal("missing initial since")
	}
	if f.api.begins != 0 || len(f.providerCursors) != 0 {
		t.Fatal("invalid request made progress")
	}
}

func TestLifecycleLostFinalACKRecoversWithoutRecollection(t *testing.T) {
	f := newLifecycleFixture(t)
	f.terminalFirst = true
	f.api.drop = 2
	ctx, cancel := runContext()
	defer cancel()
	if _, err := RunLifecycle(ctx, f.client, f.provider, lifecycleOptions()); err == nil {
		t.Fatal("final ACK unexpectedly delivered")
	}
	if f.api.state.ActiveRun != nil || f.api.state.CompletedRun == nil || len(f.api.pages) != 1 {
		t.Fatal("final page not completed")
	}
	result, err := RunLifecycle(ctx, f.client, f.provider, lifecycleOptions())
	if err != nil || !result.Complete || result.RunID != f.api.state.CompletedRun.ID || f.api.begins != 1 || len(f.providerCursors) != 1 {
		t.Fatal("final response loss caused recollection", result, err)
	}
	completed := f.api.state.CompletedRun
	f.api.state.CompletedRun = nil
	if _, err = RunLifecycle(ctx, f.client, f.provider, lifecycleOptions()); err == nil {
		t.Fatal("old API without completion metadata silently recollected")
	}
	f.api.state.CompletedRun = completed
	// A changed explicit window is a new request, not inferred completion.
	o := lifecycleOptions()
	o.Since = o.Since.Add(time.Hour)
	f.expectedSince = o.Since.Format(time.RFC3339Nano)
	if _, err = RunLifecycle(ctx, f.client, f.provider, o); err != nil {
		t.Fatal(err)
	}
	if f.api.begins != 2 || len(f.providerCursors) != 2 {
		t.Fatal("changed window silently reused old completion")
	}
}

func TestLegacyLifecycleResumesAndAcknowledgesWithoutParentProgress(t *testing.T) {
	f := newLifecycleFixture(t)
	ctx, cancel := runContext()
	defer cancel()
	o := lifecycleOptions()
	o.MaxPages = 1
	if result, err := RunLifecycle(ctx, f.client, f.provider, o); err != nil || result.Complete {
		t.Fatal(result, err)
	}
	var scope map[string]json.RawMessage
	if err := json.Unmarshal(f.api.state.ActiveRun.Scope, &scope); err != nil {
		t.Fatal(err)
	}
	delete(scope, "parentCheckpoint")
	f.api.state.ActiveRun.Scope, _ = json.Marshal(scope)
	original := string(f.api.state.ActiveRun.Scope)
	horizon := f.api.state.ActiveRun.Horizon
	f.api.drop = 2
	o.Since = time.Time{}
	if _, err := RunLifecycle(ctx, f.client, f.provider, o); err == nil {
		t.Fatal("expected lost final acknowledgement")
	}
	if f.api.state.ActiveRun != nil || f.api.state.CompletedRun == nil || string(f.api.state.CompletedRun.Scope) != original || !f.api.state.CompletedThrough.Equal(horizon) {
		t.Fatal("legacy scope or completion changed")
	}
	calls := len(f.providerCursors)
	result, err := RunLifecycle(ctx, f.client, f.provider, o)
	if err != nil || !result.Complete || result.RunID != f.api.state.CompletedRun.ID || len(f.providerCursors) != calls || f.api.begins != 1 {
		t.Fatal("legacy completion recollected", result, err)
	}
	if _, err := parentProgressCheckpoint(f.api.state, o.Repository); err == nil {
		t.Fatal("legacy run invented parent progress")
	}
	if !reflect.DeepEqual(f.providerCursors, []string{"", "next"}) {
		t.Fatal("legacy cursor changed", f.providerCursors)
	}
}
