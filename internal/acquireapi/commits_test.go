package acquireapi

import (
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
	o.Since = time.Time{}
	got, err := RunCommits(ctx, c, p, o)
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
	o.Since = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err = RunCommits(ctx, c, p, o)
	if err != nil || !got.Complete || f.begins != 1 {
		t.Fatal("completed replay", got, err)
	}
}
