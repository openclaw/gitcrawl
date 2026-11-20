package acquireapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
)

func TestCommitsHTTPResumeAndExactACK(t *testing.T) {
	horizon := time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)
	f := &fixtureAPI{t: t, state: State{Revision: 1, Sequence: 20, CompletedThrough: &horizon, Checkpoint: json.RawMessage(`{}`)}, pages: map[string][]byte{}, receipts: map[string]Receipt{}, drop: 2}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		old := f.state.ActiveRun
		f.serve(w, r)
		if old != nil && f.state.ActiveRun == nil {
			f.state.CompletedRun = old
		}
	}))
	defer api.Close()
	c, err := NewClient(api.URL, "fixture", "publisher", "reader", nil)
	if err != nil {
		t.Fatal(err)
	}
	oid := strings.Repeat("a", 40)
	cursors := []string{}
	broken := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			GraphQL struct {
				Query     string
				Variables map[string]any
			}
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		var data string
		switch {
		case strings.Contains(req.GraphQL.Query, "defaultBranchRef"):
			data = `{"repository":{"id":"R1","nameWithOwner":"fixture/repo","defaultBranchRef":{"name":"main","target":{"oid":"` + oid + `"}}}}`
		case strings.Contains(req.GraphQL.Query, "history(first:1"):
			cursor, _ := req.GraphQL.Variables["cursor"].(string)
			cursors = append(cursors, cursor)
			terminal, next := "true", `"next"`
			commitOID := oid
			id := "C1"
			if cursor != "" {
				terminal, next, id = "false", "null", "C2"
				commitOID = strings.Repeat("b", 40)
			}
			data = fmt.Sprintf(`{"repository":{"id":"R1","nameWithOwner":"fixture/repo","object":{"__typename":"Commit","oid":%q,"history":{"nodes":[{ "id":%q,"oid":%q,"committedDate":"2026-01-02T00:00:00Z","message":"body λ\u0000","repository":{"id":"R1","nameWithOwner":"fixture/repo"},"author":null,"url":"https://example.invalid/c"}],"pageInfo":{"hasNextPage":%s,"endCursor":%s}}}}}`, oid, id, commitOID, terminal, next)
		default:
			id := req.GraphQL.Variables["id"].(string)
			if broken {
				data = `{"node":null}`
			} else {
				data = fmt.Sprintf(`{"node":{"id":%q,"oid":%q,"associatedPullRequests":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`, id, map[string]string{"C1": oid, "C2": strings.Repeat("b", 40)}[id])
			}
		}
		fmt.Fprint(w, `{"status":200,"body_encoding":"json","relay":{"route_kind":"graphql_read","cache":"miss"},"identity":{"kind":"github_app"},"body":{"data":`+data+`}}`)
	}))
	defer provider.Close()
	p, _ := github.NewOctopoolConversations(provider.URL, "fixture", "relay", "fixture/repo")
	ctx, cancel := runContext()
	defer cancel()
	o := CommitOptions{Repository: "fixture/repo", Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), MaxPages: 10}
	if _, err = RunCommits(ctx, c, p, o); err == nil {
		t.Fatal("lost ACK must surface")
	}
	if len(f.pages) != 1 {
		t.Fatal("did not preserve first page")
	}
	broken = true
	if _, err = RunCommits(ctx, c, p, o); err == nil || len(f.pages) != 1 {
		t.Fatal("partial attribution advanced", err, len(f.pages), cursors)
	}
	broken = false
	f.drop = 2 // Lose both acknowledgements for the final committed page.
	o.Since = time.Time{}
	got, err := RunCommits(ctx, c, p, o)
	if err == nil || f.state.ActiveRun != nil || f.state.CompletedRun == nil {
		t.Fatal("final ACK loss was not retained", got, err)
	}
	got, err = RunCommits(ctx, c, p, o)
	if err != nil || !got.Complete || f.begins != 1 || !f.state.CompletedThrough.Equal(horizon) || len(f.changes) != 0 {
		t.Fatal(got, err)
	}
	var first PageRequest
	_ = json.Unmarshal(f.pages["commits-1"], &first)
	if first.Commits == nil || first.Commits.Attributions[0].RawJSON == "" || !strings.Contains(first.Commits.RawJSON, `\u0000`) {
		t.Fatal("raw proof lost")
	}
	if strings.Join(cursors, ",") != ",next,next" {
		t.Fatal("did not resume exact page", cursors)
	}
	o.Since = time.Time{}
	got, err = RunCommits(ctx, c, p, o)
	if err != nil || !got.Complete || f.begins != 1 {
		t.Fatal("completed replay", got, err)
	}
}

type emptyCommitProvider struct{ calls int }

func (p *emptyCommitProvider) ReadCommitHead(context.Context, github.Reporter) (github.CommitHead, error) {
	p.calls++
	return github.CommitHead{Repository: "example/project", NodeID: "R1", Branch: "main", OID: strings.Repeat("a", 40)}, nil
}
func (p *emptyCommitProvider) ReadCommit(context.Context, github.CommitQuery, github.Reporter) (github.CommitRead, error) {
	p.calls++
	return github.CommitRead{Terminal: true}, nil
}
func (*emptyCommitProvider) ReadCommitAttribution(context.Context, string, string, string, github.Reporter) (github.CommitAttributionRead, error) {
	panic("empty history has no attribution")
}

func TestCommitsPreserveHistoricalProgressAcrossModes(t *testing.T) {
	for _, mode := range []string{"parents", "historical", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			f, c, _, close := parentFixture(t)
			defer close()
			o := parentOptions(3)
			sha, err := parentSelectionHash(o.Selection, o.Selection.Repository)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "parents" {
				sha, err = selectionHash([]int{7, 8})
				if err != nil {
					t.Fatal(err)
				}
			}
			prior := checkpoint{Version: 1, RunID: "previous", Phase: "done", History: history{SHA: sha, Offset: 1}}
			f.state.Checkpoint, _ = json.Marshal(prior)
			ctx, cancel := runContext()
			defer cancel()
			provider := &emptyCommitProvider{}
			result, err := RunCommits(ctx, c, provider, CommitOptions{Repository: "example/project", Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), MaxPages: 1})
			if err != nil || !result.Complete {
				t.Fatal(result, err)
			}
			if mode == "parents" {
				p := &parentProvider{body: "full"}
				o.MaxParents = 2
				next, e := RunParents(ctx, c, p, o)
				if e != nil || !next.Complete || len(p.batches) != 1 || fmt.Sprint(p.batches[0]) != "[2 3]" {
					t.Fatal("parent offset lost", next, e, p.batches)
				}
			} else {
				o := options()
				p := &fixtureProvider{body: "full", empty: mode == "incremental"}
				if mode == "historical" {
					o.Numbers = []int{7, 8}
				}
				next, e := Run(ctx, c, p, o)
				if e != nil || !next.Complete {
					t.Fatal("parent mode rejected commit checkpoint", next, e)
				}
				if mode == "historical" && fmt.Sprint(p.requested) != "[8]" {
					t.Fatal("historical offset restarted", p.requested)
				}
				var cp checkpoint
				_ = json.Unmarshal(f.state.Checkpoint, &cp)
				if cp.History.SHA != sha || cp.History.Offset < 1 {
					t.Fatal("historical progress discarded", cp)
				}
			}
		})
	}
}

func TestLifecycleCommitsModeCycleRetainsParentProgress(t *testing.T) {
	f := newLifecycleFixture(t)
	prior := append(json.RawMessage(nil), f.api.state.Checkpoint...)
	ctx, cancel := runContext()
	defer cancel()
	life := LifecycleOptions{Repository: "example/project", Numbers: []int{7}, Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), MaxPages: 10}
	for i := 0; i < 2; i++ {
		result, err := RunLifecycle(ctx, f.client, f.provider, life)
		if err != nil || !result.Complete {
			t.Fatal("lifecycle mode failed", result, err)
		}
		provider := &emptyCommitProvider{}
		result, err = RunCommits(ctx, f.client, provider, CommitOptions{Repository: life.Repository, Since: life.Since, MaxPages: 1})
		if err != nil || !result.Complete {
			t.Fatal("commit mode failed", result, err)
		}
		restored, err := parentProgressCheckpoint(f.api.state, life.Repository)
		if err != nil || string(restored) != string(prior) {
			t.Fatal("mode cycle lost parent progress", string(restored), err)
		}
	}
	// An old completed lifecycle without the saved parent state is not evidence
	// that there was no historical progress. Do not invent an empty checkpoint.
	if _, err := RunLifecycle(ctx, f.client, f.provider, life); err != nil {
		t.Fatal(err)
	}
	var scope map[string]json.RawMessage
	if err := json.Unmarshal(f.api.state.CompletedRun.Scope, &scope); err != nil {
		t.Fatal(err)
	}
	delete(scope, "parentCheckpoint")
	f.api.state.CompletedRun.Scope, _ = json.Marshal(scope)
	provider := &emptyCommitProvider{}
	if _, err := RunCommits(ctx, f.client, provider, CommitOptions{Repository: life.Repository, Since: life.Since, MaxPages: 1}); err == nil || provider.calls != 0 {
		t.Fatal("legacy progress fabricated", err, provider.calls)
	}
}
