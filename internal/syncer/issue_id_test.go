package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/portable"
	"github.com/openclaw/gitcrawl/internal/store"
)

type identityGitHub struct {
	metadataGitHub
	calls int
	badID any
	fail  bool
}

func (f *identityGitHub) GetIssue(ctx context.Context, owner, repo string, n int, report gh.Reporter) (map[string]any, error) {
	f.calls++
	row := identityRow(n)
	if f.badID != nil {
		row["id"] = f.badID
	}
	return row, nil
}
func identityRow(n int) map[string]any {
	row := map[string]any{"id": json.Number(fmt.Sprint(1000 + n)), "number": n, "title": "identity fixture", "body": "body", "state": "closed", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z", "node_id": fmt.Sprintf("NODE_%d", n)}
	if n%2 == 0 {
		row["pull_request"] = map[string]any{}
	}
	return row
}
func (f *identityGitHub) ListRepositoryIssues(context.Context, string, string, gh.ListIssuesOptions, gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{identityRow(7), identityRow(8)}, nil
}
func (f *identityGitHub) ListIssueComments(ctx context.Context, owner, repo string, n int, r gh.Reporter) ([]map[string]any, error) {
	if f.fail {
		return nil, errors.New("fixture child failure")
	}
	return []map[string]any{{"id": 3000 + n, "body": "child", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z"}}, nil
}
func (f *identityGitHub) FetchGraphQLHistory(ctx context.Context, owner, repo string, numbers []int, r gh.Reporter) (gh.HistoryBatch, error) {
	raw, _ := f.GetRepo(ctx, owner, repo, r)
	b := gh.HistoryBatch{Repository: raw}
	for _, n := range numbers {
		row := identityRow(n)
		row["_gitcrawl_source"] = "graphql"
		row["_graphql"] = map[string]any{"id": fmt.Sprintf("NODE_%d", n), "fullDatabaseId": 2000 + n}
		item := gh.HistoryItem{Thread: row}
		item.Comments, _ = f.ListIssueComments(ctx, owner, repo, n, r)
		if n%2 == 0 {
			row["id"] = nil
			item.Pull, _ = f.GetPull(ctx, owner, repo, n, r)
			item.Pull["id"] = json.Number(fmt.Sprint(2000 + n))
		}
		b.Items = append(b.Items, item)
	}
	return b, nil
}

func TestCanonicalIssueIdentityMixedModesAndPortableRoundtrip(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "source.db")
			st, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			client := &identityGitHub{}
			raw, _ := client.GetRepo(ctx, "fixture", "repo", nil)
			repoID, err := New(client, st).upsertRepository(ctx, st, Options{Owner: "fixture", Repo: "repo"}, raw)
			if err != nil {
				t.Fatal(err)
			}
			ids := map[int]int64{}
			if legacy {
				for _, n := range []int{7, 8} {
					row := identityRow(n)
					row["id"] = fmt.Sprintf("LEGACY_NODE_%d", n)
					id, err := st.UpsertThread(ctx, mapIssueToThread(repoID, row, "2026-01-01T00:00:00Z"))
					if err != nil {
						t.Fatal(err)
					}
					ids[n] = id
				}
			}
			base := Options{Owner: "fixture", Repo: "repo", State: "all", IncludeComments: true, IncludePRMetadata: true}
			graph := base
			graph.GraphQLHistory = true
			graph.Numbers = []int{7, 8}
			targeted := base
			targeted.Numbers = []int{7, 8}
			// Begin with GraphQL so both new and legacy rows must resolve the backing Issue.
			for _, options := range []Options{graph, base, targeted, graph, base} {
				if _, err := New(client, st).Sync(ctx, options); err != nil {
					t.Fatal(err)
				}
				rows, err := st.ListThreads(ctx, repoID, true)
				if err != nil || len(rows) != 2 {
					t.Fatalf("rows=%+v err=%v", rows, err)
				}
				for _, row := range rows {
					if row.GitHubID != fmt.Sprint(1000+row.Number) {
						t.Fatalf("wrong identity %+v", row)
					}
					if ids[row.Number] != 0 && ids[row.Number] != row.ID {
						t.Fatal("local ID changed")
					}
					ids[row.Number] = row.ID
				}
				assertTableRowCount(t, st, "comments", 2)
			}
			// Portable import reopens the exported SQLite image, rebuilding runtime schema.
			exported, err := portable.Export(ctx, portable.ExportOptions{SourceDBPath: path, OutputDir: filepath.Join(t.TempDir(), "export"), DatabaseName: "archive.db", PublicPath: "data/archive.db", Repository: "fixture/repo", Profile: "current-state-v1"})
			if err != nil {
				t.Fatal(err)
			}
			imported, err := store.Open(ctx, exported.DatabasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer imported.Close()
			if _, err := New(client, imported).Sync(ctx, graph); err != nil {
				t.Fatal(err)
			}
			rows, err := imported.ListThreads(ctx, repoID, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if ids[row.Number] != row.ID || row.GitHubID != fmt.Sprint(1000+row.Number) {
					t.Fatalf("roundtrip identity %+v", row)
				}
			}
			assertTableRowCount(t, imported, "comments", 2)
		})
	}
}

func TestCanonicalIssueIdentityFailureAndClosedSweep(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &identityGitHub{fail: true}
	if _, err := New(client, st).Sync(ctx, Options{Owner: "fixture", Repo: "repo", State: "all", Numbers: []int{8}, IncludeComments: true}); err == nil {
		t.Fatal("child failure accepted")
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := st.ListThreads(ctx, repo.ID, true)
	if len(rows) != 1 || rows[0].GitHubID != "1008" {
		t.Fatalf("failure parent: %+v", rows)
	}
	client.fail = false
	if _, err := New(client, st).Sync(ctx, Options{Owner: "fixture", Repo: "repo", State: "open"}); err != nil {
		t.Fatal(err)
	}
	rows, _ = st.ListThreads(ctx, repo.ID, true)
	for _, row := range rows {
		if row.GitHubID != fmt.Sprint(1000+row.Number) {
			t.Fatalf("closed sweep: %+v", row)
		}
	}
	client.badID = "PR_opaque"
	if _, err := New(client, st).Sync(ctx, Options{Owner: "fixture", Repo: "repo", State: "all", Numbers: []int{10}}); err == nil {
		t.Fatal("unresolved parent accepted")
	}
	assertTableRowCount(t, st, "threads", 2)
}

func TestIssueIDResolutionCacheAndValidation(t *testing.T) {
	client := &identityGitHub{}
	r := issueIDResolver{client: client, options: Options{Owner: "fixture", Repo: "repo"}}
	ctx := context.Background()
	for i := 0; i < issueIDCacheLimit+1; i++ {
		row := identityRow(2*i + 2)
		row["_gitcrawl_source"] = "graphql"
		if _, err := r.resolve(ctx, row); err != nil {
			t.Fatal(err)
		}
		if _, err := r.resolve(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	if client.calls != issueIDCacheLimit+1 || len(r.cache) != issueIDCacheLimit {
		t.Fatal("unbounded or ineffective cache")
	}
	for _, bad := range []any{nil, "", "0", "01", "PR_node", "-1", "1.5", float64(9007199254740992), 1.5} {
		if _, err := canonicalIssueID(bad); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
}

func TestGraphQLIssueIDResolutionHonorsRESTQuota(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rate_limit" {
			calls++
			http.Error(w, "unexpected Issue request", 500)
			return
		}
		fmt.Fprint(w, `{"resources":{"core":{"limit":5000,"remaining":500,"reset":4070908800}}}`)
	}))
	defer server.Close()
	r := issueIDResolver{client: gh.New(gh.Options{BaseURL: server.URL, RateLimitReserve: 500}), options: Options{Owner: "fixture", Repo: "repo"}}
	row := identityRow(8)
	row["_gitcrawl_source"] = "graphql"
	_, err := r.resolve(context.Background(), row)
	var quota *gh.RateLimitReserveError
	if !errors.As(err, &quota) || calls != 0 || len(r.cache) != 0 {
		t.Fatalf("quota bypass err=%v calls=%d", err, calls)
	}
}
