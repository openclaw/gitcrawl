package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestStreamPersistsBeforeFailureAndResumesWithoutRefetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection := func(nodes ...any) map[string]any {
		if nodes == nil {
			nodes = []any{}
		}
		return map[string]any{"nodes": nodes, "totalCount": len(nodes), "pageInfo": map[string]any{"hasNextPage": false}}
	}
	row := func(n int) map[string]any {
		return map[string]any{"id": n + 100, "node_id": fmt.Sprint("I_", n), "number": n, "state": "open", "title": fmt.Sprint("title ", n), "body": strings.Repeat("full λ body ", 1000), "updated_at": "2026-01-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z", "repository_url": "https://api.github.com/repos/fixture/repo", "html_url": fmt.Sprintf("https://github.com/fixture/repo/issues/%d", n)}
	}
	var lock sync.Mutex
	calls := map[int]int{}
	pagesFetched := 0
	secondPage := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path    string              `json:"path"`
			Query   map[string][]string `json:"query"`
			GraphQL struct {
				Query string `json:"query"`
			} `json:"graphql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		var body any
		headers := map[string]string{}
		switch request.Path {
		case "/repos/fixture/repo":
			body = map[string]any{"id": 123}
		case "/repos/fixture/repo/issues":
			lock.Lock()
			pagesFetched++
			lock.Unlock()
			if len(request.Query["after"]) == 0 {
				body = []any{row(7), row(8)}
				headers["link"] = `<https://api.github.com/repositories/123/issues?state=all&sort=created&direction=asc&per_page=100&page=100&after=provider-cursor>; rel="next"`
			} else {
				body = []any{row(9)}
				close(secondPage)
			}
		case "/graphql":
			n := 7
			if strings.Contains(request.GraphQL.Query, "number:8") {
				n = 8
			}
			if strings.Contains(request.GraphQL.Query, "number:9") {
				n = 9
			}
			lock.Lock()
			calls[n]++
			attempt := calls[n]
			lock.Unlock()
			select {
			case <-secondPage:
			case <-ctx.Done():
				t.Error("inventory acquisition did not overlap hydration")
				return
			}
			if n == 9 && attempt == 1 {
				body = map[string]any{"errors": []any{map[string]any{"message": "fixture interrupted acquisition"}}}
				break
			}
			node := map[string]any{"id": fmt.Sprint("I_", n), "number": n, "__typename": "Issue", "updatedAt": "2026-01-01T00:00:00Z", "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "comments": connection(map[string]any{"id": fmt.Sprint("IC_", n), "fullDatabaseId": fmt.Sprint(n + 1000), "body": ""})}
			body = map[string]any{"data": map[string]any{"repository": map[string]any{"n0": node}}}
		default:
			t.Errorf("unexpected path %s", request.Path)
			w.WriteHeader(400)
			return
		}
		raw, _ := json.Marshal(body)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "text", "body": string(raw), "headers": headers, "relay": map[string]any{"route_kind": "graphql_read", "cache": "miss"}, "identity": map[string]any{"kind": "github_app"}})
	}))
	defer server.Close()
	dir := t.TempDir()
	nativePath := filepath.Join(dir, "native.sqlite")
	st, err := store.Open(ctx, nativePath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	identity, err := store.NativeReportFileIdentity(nativePath)
	if err != nil {
		t.Fatal(err)
	}
	identityJSON, _ := json.Marshal(identity)
	scope := github.InventoryScope{Checkpoint: filepath.Join(dir, "inventory.sqlite"), NativeIdentity: string(identityJSON), SelectionSHA256: "fixture-selected", IncludeDiscovered: true, StartedAt: time.Now().UTC()}
	client := func() *github.OctopoolConversations {
		c, err := github.NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	var first bytes.Buffer
	err = hydrateStream(ctx, client(), st, "fixture/repo", []int{7, 8, 9, 10}, 1, 2, scope, "", nil, json.NewEncoder(&first))
	if err == nil || !strings.Contains(err.Error(), "GraphQL returned errors") {
		t.Fatalf("first run error = %v", err)
	}
	var count int
	if err = st.DB().QueryRowContext(ctx, "select count(*) from threads").Scan(&count); err != nil || count != 2 {
		t.Fatalf("durable threads=%d err=%v", count, err)
	}
	if strings.Contains(first.String(), `"complete":true`) {
		t.Fatal("failed traversal claimed complete")
	}
	// A newly excluded historical key absent upstream must neither invalidate
	// this checkpoint's original input nor be reported as missing on resume.
	if _, err = st.DB().ExecContext(ctx, "insert into thread_exclusions values('fixture/repo',10,'issue',0,'110','2026-01-01T00:00:00Z','owner_requested','fixture')"); err != nil {
		t.Fatal(err)
	}
	scope.StartedAt = time.Now().UTC()
	var resumed bytes.Buffer
	if err = hydrateStream(ctx, client(), st, "fixture/repo", []int{7, 8, 9, 10}, 1, 2, scope, filepath.Join(dir, "selected.txt"), nil, json.NewEncoder(&resumed)); err != nil {
		t.Fatal(err)
	}
	lock.Lock()
	defer lock.Unlock()
	if pagesFetched != 2 || calls[7] != 1 || calls[8] != 1 || calls[9] != 2 {
		t.Fatalf("repeated completed work: pages=%d calls=%v", pagesFetched, calls)
	}
	if err = st.DB().QueryRowContext(ctx, "select count(*) from threads").Scan(&count); err != nil || count != 3 {
		t.Fatalf("resumed threads=%d err=%v", count, err)
	}
	if err = st.DB().QueryRowContext(ctx, "select count(*) from comments where body=''").Scan(&count); err != nil || count != 3 {
		t.Fatalf("full empty comment records=%d err=%v", count, err)
	}
	var body string
	if err = st.DB().QueryRowContext(ctx, "select body from threads where number=9").Scan(&body); err != nil || body != row(9)["body"] {
		t.Fatal("resumed native body lost bytes", err)
	}
	if !strings.Contains(resumed.String(), `"complete":true`) || !strings.Contains(resumed.String(), `"numbers":[9]`) || strings.Contains(resumed.String(), `"numbers":[7]`) {
		t.Fatalf("bad resume receipts: %s", resumed.String())
	}
}

func TestStreamRejectsCanonicalIdentityCollisionBeforeHydration(t *testing.T) {
	for _, collision := range []string{"id", "node_id"} {
		t.Run(collision, func(t *testing.T) {
			ctx := context.Background()
			rows := []map[string]any{{"id": 101, "node_id": "I_1", "number": 1, "updated_at": "2026-01-01T00:00:00Z"}, {"id": 102, "node_id": "I_2", "number": 2, "updated_at": "2026-01-01T00:00:00Z"}}
			rows[1][collision] = rows[0][collision]
			graphqlCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Path string `json:"path"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				var body any = map[string]any{"id": 123}
				if request.Path == "/repos/fixture/repo/issues" {
					body = rows
				} else if request.Path == "/graphql" {
					graphqlCalls++
					t.Error("hydrated conflicting identity")
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "relay": map[string]any{"cache": "miss"}})
			}))
			defer server.Close()
			dir := t.TempDir()
			st, err := store.Open(ctx, filepath.Join(dir, "native.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			client, err := github.NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
			if err != nil {
				t.Fatal(err)
			}
			scope := github.InventoryScope{Checkpoint: filepath.Join(dir, "checkpoint.sqlite"), NativeIdentity: "fixture-native", SelectionSHA256: "fixture-input", StartedAt: time.Now()}
			var output bytes.Buffer
			err = hydrateStream(ctx, client, st, "fixture/repo", []int{1, 2}, 1, 1, scope, "", nil, json.NewEncoder(&output))
			if err == nil || !strings.Contains(err.Error(), "belongs to both") || graphqlCalls != 0 {
				t.Fatalf("collision result: %v; GraphQL=%d", err, graphqlCalls)
			}
		})
	}
}
