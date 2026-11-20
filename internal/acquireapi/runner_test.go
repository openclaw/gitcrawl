package acquireapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
)

type fixtureAPI struct {
	t        *testing.T
	state    State
	pages    map[string][]byte
	receipts map[string]Receipt
	changes  []remote.Change
	requests int
	drop     int
	begins   int
	horizons []time.Time
}

func newFixtureAPI(t *testing.T) (*fixtureAPI, *Client, func()) {
	f := &fixtureAPI{t: t, state: State{Revision: 1, Sequence: 2, Checkpoint: json.RawMessage(`{}`)}, pages: map[string][]byte{}, receipts: map[string]Receipt{}}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	client, err := NewClient(server.URL, "fixture", "publisher", "reader", nil)
	if err != nil {
		t.Fatal(err)
	}
	return f, client, server.Close
}
func (f *fixtureAPI) serve(w http.ResponseWriter, r *http.Request) {
	f.requests++
	if strings.HasSuffix(r.URL.Path, "/query") {
		if r.Header.Get("Authorization") != "Bearer reader" {
			f.t.Error("wrong query credential")
		}
		var q remote.QueryRequest
		json.NewDecoder(r.Body).Decode(&q)
		if q.Name == "gitcrawl.repositories.current" {
			json.NewEncoder(w).Encode(remote.QueryResult{Columns: []string{"id", "full_name", "github_repo_id"}, Rows: [][]any{{"3", "example/project", "42"}}})
			return
		}
		if q.Name != "gitcrawl.threads.current" || q.Limit != 1 || len(q.Args) != 1 {
			f.t.Errorf("wrong exact parent lookup: %+v", q)
		}
		json.NewEncoder(w).Encode(remote.QueryResult{Rows: [][]any{}})
		return
	}
	if r.Header.Get("Authorization") != "Bearer publisher" {
		f.t.Error("wrong publisher credential")
	}
	body, _ := io.ReadAll(r.Body)
	switch {
	case strings.HasSuffix(r.URL.Path, "/state"):
		json.NewEncoder(w).Encode(f.state)
	case strings.HasSuffix(r.URL.Path, "/runs"):
		var request RunRequest
		if err := json.Unmarshal(body, &request); err != nil {
			f.t.Fatal(err)
		}
		if request.ExpectedRevision != f.state.Revision {
			f.t.Error("wrong run revision")
		}
		f.state.ActiveRun = &request
		f.begins++
		f.horizons = append(f.horizons, request.Horizon)
		json.NewEncoder(w).Encode(map[string]string{"runId": request.ID, "status": "accepted"})
	case strings.HasSuffix(r.URL.Path, "/resolve-ids"):
		var request ResolveRequest
		json.Unmarshal(body, &request)
		if len(request.Threads)+len(request.Comments) == 0 {
			http.Error(w, "empty identity request", http.StatusBadRequest)
			return
		}
		out := ResolvedIDs{RepositoryID: "3", Threads: []string{}, Comments: []string{}}
		for _, thread := range request.Threads {
			n, _ := strconv.Atoi(thread.Number)
			out.Threads = append(out.Threads, strconv.Itoa(100+n))
		}
		for _, comment := range request.Comments {
			n, _ := strconv.Atoi(comment.GitHubID)
			out.Comments = append(out.Comments, strconv.Itoa(1000+n))
		}
		json.NewEncoder(w).Encode(out)
	case strings.HasSuffix(r.URL.Path, "/pages"):
		var request PageRequest
		if err := json.Unmarshal(body, &request); err != nil {
			f.t.Fatal(err)
		}
		if len(body) > MaxPageBytes || len(request.Changes) > 500 {
			f.t.Error("oversize page dispatched")
		}
		receipt, ok := f.receipts[request.ID]
		if ok {
			if !bytes.Equal(f.pages[request.ID], body) {
				f.t.Fatal("retry changed exact request bytes")
			}
			receipt.Replayed = true
		} else {
			if request.ExpectedRevision != f.state.Revision {
				f.t.Fatal("stale page")
			}
			sum := sha256.Sum256(body)
			f.state.Revision++
			f.state.Sequence++
			receipt = Receipt{RunID: request.RunID, PageID: request.ID, SHA256: hex.EncodeToString(sum[:]), Revision: f.state.Revision, Sequence: f.state.Sequence, Final: request.Final}
			f.pages[request.ID] = body
			f.receipts[request.ID] = receipt
			f.state.Checkpoint = request.Checkpoint
			f.changes = append(f.changes, request.Changes...)
			if request.Final {
				h := f.state.ActiveRun.Horizon
				f.state.CompletedThrough = &h
				f.state.ActiveRun = nil
			}
		}
		if f.drop > 0 {
			f.drop--
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		json.NewEncoder(w).Encode(receipt)
	default:
		f.t.Errorf("unexpected route %s", r.URL.Path)
		http.NotFound(w, r)
	}
}

type fixtureProvider struct {
	requested []int
	parents   int
	body      string
	empty     bool
}

func (p *fixtureProvider) GetRepo(context.Context, string, string, github.Reporter) (map[string]any, error) {
	return map[string]any{"id": json.Number("42")}, nil
}
func (p *fixtureProvider) parent(n int) map[string]any {
	return map[string]any{"id": json.Number(strconv.Itoa(500 + n)), "number": json.Number(strconv.Itoa(n)), "title": "full title", "body": "full parent\x00λ", "updated_at": "2026-01-02T00:00:00Z", "created_at": "2026-01-01T00:00:00Z", "pull_request": map[string]any{}, "user": map[string]any{"login": "author", "type": "User"}}
}
func (p *fixtureProvider) GetIssue(_ context.Context, _, _ string, n int, _ github.Reporter) (map[string]any, error) {
	p.requested = append(p.requested, n)
	return p.parent(n), nil
}
func (p *fixtureProvider) ReadRESTPage(_ context.Context, r github.RESTPageRequest, _ github.Reporter) (github.RESTPage, error) {
	if r.Family == "issues" {
		p.parents++
		if p.empty {
			return github.RESTPage{Rows: []map[string]any{}}, nil
		}
		return github.RESTPage{Rows: []map[string]any{p.parent(7), p.parent(8)}}, nil
	}
	if r.Family == "issue_comment" {
		return github.RESTPage{Rows: []map[string]any{{"id": json.Number("9"), "body": p.body, "created_at": "2026-01-02T00:00:00Z"}}}, nil
	}
	if r.Family == "pull_review" {
		return github.RESTPage{Rows: []map[string]any{{"id": json.Number("10"), "body": "review", "state": "APPROVED", "submitted_at": "2026-01-02T12:00:00Z"}}}, nil
	}
	return github.RESTPage{Rows: []map[string]any{}}, nil
}
func options() Options {
	return Options{Repository: "example/project", Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), MaxThreads: 100, ParentPageSize: 100, ChildPageSize: 20, Now: func() time.Time { return time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC) }}
}
func runContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Minute)
}

func TestRunHTTPCompleteAndNoNormalParentRefetch(t *testing.T) {
	f, client, close := newFixtureAPI(t)
	defer close()
	p := &fixtureProvider{body: "full comment\x00λ"}
	ctx, cancel := runContext()
	defer cancel()
	result, err := Run(ctx, client, p, options())
	if err != nil || !result.Complete || result.Threads != 2 {
		t.Fatal(result, err)
	}
	if p.parents != 1 || len(p.requested) != 0 {
		t.Fatal("normal parent page was refetched", p)
	}
	if f.state.ActiveRun != nil || !f.state.CompletedThrough.Equal(options().Now()) {
		t.Fatal("horizon not completed")
	}
	if len(f.changes) != 6 {
		t.Fatal("lost native rows", len(f.changes))
	}
	for _, change := range f.changes {
		if change.Table == "comments" && change.Row[3].Value != nil && *change.Row[3].Value == "pull_review" {
			if change.Row[15].Value == nil || *change.Row[15].Value != "2026-01-02T12:00:00Z" {
				t.Fatal("review submission omitted")
			}
		}
	}
}
func TestRunHTTPAmbiguousPageReplayAndProcessResume(t *testing.T) {
	f, client, close := newFixtureAPI(t)
	defer close()
	f.drop = 2
	p := &fixtureProvider{body: "body"}
	ctx, cancel := runContext()
	defer cancel()
	if _, err := Run(ctx, client, p, options()); err == nil {
		t.Fatal("two lost responses did not stop")
	}
	if len(f.pages) != 1 || f.state.ActiveRun == nil {
		t.Fatal("first page not committed exactly once")
	}
	resumed := options()
	resumed.Now = func() time.Time { return time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC) }
	resumed.Since = time.Time{}
	result, err := Run(ctx, client, p, resumed)
	if err != nil || !result.Complete {
		t.Fatal(result, err)
	}
	if f.begins != 1 {
		t.Fatal("resume replaced original run")
	}
	// Only the second queued parent needs a fresh read after process restart.
	if fmt.Sprint(p.requested) != "[8]" {
		t.Fatal("unexpected resume parent reads", p.requested)
	}
	for _, change := range f.changes {
		if change.Table == "threads" && (change.Row[23].Value == nil || *change.Row[23].Value != "2026-01-03T00:00:00Z") {
			t.Fatal("retry changed first capture time")
		}
	}
}
func TestHistoricalMicroRunsSurviveIncrementalWithoutFalseHorizon(t *testing.T) {
	f, client, close := newFixtureAPI(t)
	defer close()
	h := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	f.state.CompletedThrough = &h
	ctx, cancel := runContext()
	defer cancel()
	o := options()
	o.Numbers = []int{7, 8}
	o.MaxThreads = 1
	p := &fixtureProvider{body: "body"}
	result, err := Run(ctx, client, p, o)
	if err != nil || !result.Complete || result.Threads != 1 {
		t.Fatal(result, err)
	}
	if !f.state.CompletedThrough.Equal(h) {
		t.Fatal("historical work advanced freshness")
	}
	inc := options()
	p.empty = true
	if _, err = Run(ctx, client, p, inc); err != nil {
		t.Fatal(err)
	}
	latest := *f.state.CompletedThrough
	result, err = Run(ctx, client, p, o)
	if err != nil || !result.Complete {
		t.Fatal(result, err)
	}
	if fmt.Sprint(p.requested) != "[7 8]" || !f.state.CompletedThrough.Equal(latest) {
		t.Fatal("selection progress or horizon lost", p.requested)
	}
}
func TestOversizePageCannotAdvanceCheckpoint(t *testing.T) {
	f, client, close := newFixtureAPI(t)
	defer close()
	ctx, cancel := runContext()
	defer cancel()
	_, err := Run(ctx, client, &fixtureProvider{body: strings.Repeat("x", MaxPageBytes)}, options())
	if err == nil || len(f.pages) != 1 || f.state.ActiveRun == nil {
		t.Fatal("oversize child advanced", err, len(f.pages))
	}
	var cp checkpoint
	json.Unmarshal(f.state.Checkpoint, &cp)
	if cp.Phase != "issue_comment" {
		t.Fatal(cp.Phase)
	}
}
