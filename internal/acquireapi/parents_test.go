package acquireapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
)

type parentProvider struct {
	batches   [][]int
	body      string
	wrong     bool
	failAbove int
}

func (p *parentProvider) ReadParentRepository(context.Context, string, string, github.Reporter) (map[string]any, error) {
	return map[string]any{"id": json.Number("42")}, nil
}
func (p *parentProvider) ReadParents(_ context.Context, numbers []int, _ github.Reporter) ([]github.ParentSnapshot, error) {
	p.batches = append(p.batches, append([]int(nil), numbers...))
	if p.failAbove > 0 && len(numbers) > p.failAbove {
		return nil, fmt.Errorf("fixture response exceeds bound")
	}
	out := make([]github.ParentSnapshot, len(numbers))
	for i, n := range numbers {
		out[i] = github.ParentSnapshot{Kind: "pull_request", Number: n, NodeID: fmt.Sprint("PR_", n), GraphQLDatabaseID: "9007199254740997", ProviderFields: map[string]any{"id": fmt.Sprint("PR_", n), "title": "title", "body": p.body, "pullState": "OPEN", "author": nil, "authorAssociation": "NONE", "url": "https://github.com/example/project/pull/7", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "closedAt": nil, "mergedAt": nil, "isDraft": true}}
		if p.wrong {
			out[i].NodeID = "wrong"
		}
	}
	return out, nil
}
func parentOptions(n int) ParentOptions {
	s := ParentSelection{Schema: "gitcrawl-parent-selection/v1", Repository: "example/project"}
	for i := 1; i <= n; i++ {
		s.Parents = append(s.Parents, SelectedParent{Number: i, Kind: "pull_request", BackingIssueID: strconv.Itoa(500 + i), NodeID: fmt.Sprint("PR_", i)})
	}
	return ParentOptions{Selection: s, MaxParents: 100, BatchSize: 50, Now: options().Now}
}
func parentFixture(t *testing.T) (*fixtureAPI, *Client, *int, func()) {
	f, _, oldClose := newFixtureAPI(t)
	oldClose()
	h := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	f.state.CompletedThrough = &h
	calls := new(int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/query") {
			var q remote.QueryRequest
			json.NewDecoder(r.Body).Decode(&q)
			if q.Name == "gitcrawl.repositories.current" {
				json.NewEncoder(w).Encode(remote.QueryResult{Columns: []string{"id", "full_name", "github_repo_id"}, Rows: [][]any{{"3", "example/project", "42"}}})
				return
			}
			if r.Header.Get("Authorization") != "Bearer reader" || q.Name != "gitcrawl.threads.current" {
				t.Error("wrong batch reader")
			}
			ids, ok := q.Args["ids"].([]any)
			if !ok || q.Limit != len(ids) || len(ids) > 50 {
				t.Error("not exact batch lookup", q)
			}
			*calls++
			json.NewEncoder(w).Encode(remote.QueryResult{Rows: [][]any{}})
			return
		}
		f.serve(w, r)
	}))
	c, err := NewClient(server.URL, "fixture", "publisher", "reader", nil)
	if err != nil {
		t.Fatal(err)
	}
	return f, c, calls, server.Close
}
func TestRunParentsBatchesWithoutChildrenOrNewHorizon(t *testing.T) {
	f, c, reads, close := parentFixture(t)
	defer close()
	p := &parentProvider{body: "full\x00λ"}
	ctx, cancel := runContext()
	defer cancel()
	result, err := RunParents(ctx, c, p, parentOptions(52))
	if err != nil || !result.Complete || result.Threads != 52 {
		t.Fatal(result, err)
	}
	if len(p.batches) != 2 || len(p.batches[0]) != 50 || len(p.batches[1]) != 2 || *reads != 2 {
		t.Fatal("not batched", p.batches, *reads)
	}
	if f.begins != 1 || f.state.ActiveRun != nil || !f.state.CompletedThrough.Equal(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("changed horizon")
	}
	for _, raw := range f.pages {
		var page PageRequest
		json.Unmarshal(raw, &page)
		if page.ChildObservation != nil {
			t.Fatal("invented child traversal")
		}
	}
	for i, row := range f.changes {
		if row.Table != "threads" || *row.Row[2].Value != strconv.Itoa(501+i) || *row.Row[7].Value != "full\x00λ" {
			t.Fatal("lost selected identity/body", row)
		}
	}
}
func TestRunParentsLostAckResumesOriginalOffsetAndTime(t *testing.T) {
	f, c, _, close := parentFixture(t)
	defer close()
	f.drop = 2
	p := &parentProvider{body: "body"}
	o := parentOptions(3)
	o.BatchSize = 2
	ctx, cancel := runContext()
	defer cancel()
	if _, err := RunParents(ctx, c, p, o); err == nil {
		t.Fatal("lost acknowledgements not surfaced")
	}
	if len(f.changes) != 2 || f.state.ActiveRun == nil {
		t.Fatal("first batch not retained")
	}
	changed := o
	changed.Selection.Parents = slices.Clone(o.Selection.Parents)
	changed.Selection.Parents[0].BackingIssueID = "999"
	if _, err := RunParents(ctx, c, p, changed); err == nil {
		t.Fatal("changed selection resumed")
	}
	o.Now = func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }
	result, err := RunParents(ctx, c, p, o)
	if err != nil || !result.Complete || result.Threads != 1 {
		t.Fatal(result, err)
	}
	if len(p.batches) != 2 || fmt.Sprint(p.batches[1]) != "[3]" || f.begins != 1 {
		t.Fatal("refetched committed batch", p.batches)
	}
	for _, row := range f.changes {
		if *row.Row[23].Value != "2026-01-03T00:00:00Z" {
			t.Fatal("changed first pulled time")
		}
	}
}
func TestRunParentsBoundsAndIdentityStopBeforeFacts(t *testing.T) {
	for _, mode := range []string{"wrong", "oversize", "no-horizon"} {
		t.Run(mode, func(t *testing.T) {
			f, c, _, close := parentFixture(t)
			defer close()
			p := &parentProvider{body: "body"}
			switch mode {
			case "wrong":
				p.wrong = true
			case "oversize":
				p.body = strings.Repeat("x", (2<<20)+1000)
			case "no-horizon":
				f.state.CompletedThrough = nil
			}
			ctx, cancel := runContext()
			defer cancel()
			if _, err := RunParents(ctx, c, p, parentOptions(1)); err == nil {
				t.Fatal("accepted", mode)
			}
			if len(f.changes) != 0 {
				t.Fatal("invalid input wrote facts")
			}
			if mode == "no-horizon" && f.begins != 0 {
				t.Fatal("reserved run without horizon")
			}
		})
	}
	f, c, _, close := parentFixture(t)
	defer close()
	p := &parentProvider{body: strings.Repeat("x", 1100000)}
	ctx, cancel := runContext()
	defer cancel()
	r, err := RunParents(ctx, c, p, parentOptions(2))
	if err != nil || !r.Complete || len(f.pages) != 2 {
		t.Fatal("failed bounded page split", r, len(f.pages), err)
	}
}
func TestParentSelectionIsTypedAndScoped(t *testing.T) {
	o := parentOptions(1)
	raw, _ := json.Marshal(o.Selection)
	if _, err := DecodeParentSelection(raw, "example/project"); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeParentSelection(raw, "other/project"); err == nil {
		t.Fatal("cross repository selection")
	}
	for _, raw := range []string{`{"schema":"gitcrawl-parent-selection/v1","repository":"example/project","parents":[{"number":1,"kind":"pr","backingIssueId":"5"}]}`, string(raw) + ` {}`, `{"schema":"gitcrawl-parent-selection/v1","repository":"example/project","parents":[],"unexpected":true}`} {
		if _, err := DecodeParentSelection([]byte(raw), "example/project"); err == nil {
			t.Fatal("accepted invalid selection")
		}
	}
}

func TestRunParentsCanReduceBatchAfterBoundedFailure(t *testing.T) {
	f, c, _, close := parentFixture(t)
	defer close()
	p := &parentProvider{body: "body", failAbove: 1}
	o := parentOptions(2)
	ctx, cancel := runContext()
	defer cancel()
	if _, err := RunParents(ctx, c, p, o); err == nil || !strings.Contains(err.Error(), "smaller --parent-page-size") {
		t.Fatal("missing actionable bounded failure", err)
	}
	if len(f.pages) != 0 || f.begins != 1 {
		t.Fatal("failed fetch advanced progress")
	}
	o.BatchSize = 1
	r, err := RunParents(ctx, c, p, o)
	if err != nil || !r.Complete || r.Threads != 2 || f.begins != 1 {
		t.Fatal("smaller batch failed to resume original run", r, err)
	}
}
