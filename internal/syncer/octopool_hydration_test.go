package syncer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestOctopoolHydrationUsesNativePersistenceWithRESTIdentity(t *testing.T) {
	ctx := context.Background()
	row, err := (fakeGitHub{}).GetIssue(ctx, "openclaw", "gitcrawl", 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	row["id"] = json.Number("9007199254740993")
	row["node_id"] = "PR_fixture"
	row["body"] = strings.Repeat("full λ body ", 1000)
	connection := func(nodes ...any) map[string]any {
		if nodes == nil {
			nodes = []any{}
		}
		return map[string]any{"totalCount": len(nodes), "nodes": nodes, "pageInfo": map[string]any{"hasNextPage": false}}
	}
	node := map[string]any{"id": "PR_fixture", "__typename": "PullRequest", "number": 8, "updatedAt": row["updated_at"], "repository": map[string]any{"nameWithOwner": "openclaw/gitcrawl"}, "comments": connection(map[string]any{"id": "IC_fixture", "fullDatabaseId": "9007199254740994", "body": ""}), "reviews": connection(), "reviewThreads": connection()}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		var body any
		switch request.Path {
		case "/repos/openclaw/gitcrawl":
			body = map[string]any{"id": 123}
		case "/repos/openclaw/gitcrawl/issues":
			body = []any{row}
		case "/graphql":
			body = map[string]any{"data": map[string]any{"repository": map[string]any{"n0": node}}}
		default:
			t.Errorf("unexpected request path %s", request.Path)
			w.WriteHeader(400)
			return
		}
		raw, _ := json.Marshal(body)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "text", "body": string(raw), "relay": map[string]any{"route_kind": "graphql_read", "cache": "miss"}, "identity": map[string]any{"kind": "github_app"}})
	}))
	defer server.Close()
	client, err := gh.NewOctopoolConversations(server.URL, "fixture", "fixture-token", "openclaw/gitcrawl")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Inventory(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = client.Prepare(ctx, []int{8}, nil); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "native.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stats, err := New(client, st).Sync(ctx, Options{Octopool: true, Owner: "openclaw", Repo: "gitcrawl", State: "all", Numbers: []int{8}, IncludeComments: true, Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ThreadsSynced != 1 || stats.CommentsSynced != 1 || stats.RevisionsCreated != 0 {
		t.Fatalf("unexpected native coverage: %+v", stats)
	}
	var id, body string
	if err = st.DB().QueryRowContext(ctx, "select github_id,body from threads where number=8").Scan(&id, &body); err != nil {
		t.Fatal(err)
	}
	if id != "9007199254740993" || body != row["body"] {
		t.Fatal("native thread lost REST identity or full body")
	}
	if err = st.DB().QueryRowContext(ctx, "select github_id,body from comments").Scan(&id, &body); err != nil {
		t.Fatal(err)
	}
	if id != "9007199254740994" || body != "" {
		t.Fatal("native comment lost exact identity or empty body")
	}
}
