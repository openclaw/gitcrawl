package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOctopoolCommitRawAndPinnedPagination(t *testing.T) {
	oid := strings.Repeat("a", 40)
	commit := `{ "id":"C_9007199254740993", "oid":"` + oid + `", "committedDate":"2026-01-02T00:00:00Z", "message":"full λ\u0000 text", "repository":{"id":"R1","nameWithOwner":"fixture/repo"},"author":{"name":"someone","user":null},"future":true }`
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var p struct {
			GraphQL struct {
				Query     string
				Variables map[string]any
			}
			Path, Method string
		}
		if json.NewDecoder(r.Body).Decode(&p) != nil || p.Method != "POST" || p.Path != "/graphql" || r.Header.Get("Authorization") != "Bearer token" {
			t.Error("wrong transport")
		}
		var data string
		switch {
		case strings.Contains(p.GraphQL.Query, "defaultBranchRef"):
			data = `{"repository":{"id":"R1","nameWithOwner":"fixture/repo","defaultBranchRef":{"name":"main","target":{"oid":"` + oid + `"}}}}`
		case strings.Contains(p.GraphQL.Query, "history(first:1"):
			if p.GraphQL.Variables["oid"] != oid || strings.Contains(p.GraphQL.Query, "comments(") {
				t.Error("unpinned or expanded query")
			}
			data = `{"repository":{"id":"R1","nameWithOwner":"fixture/repo","object":{"__typename":"Commit","oid":"` + oid + `","history":{"nodes":[` + commit + `],"pageInfo":{"hasNextPage":false,"endCursor":"last"}}}}}`
		default:
			data = `{"node":{"id":"C_9007199254740993","oid":"` + oid + `","associatedPullRequests":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":"last"}}}}`
		}
		fmt.Fprint(w, `{"status":200,"body_encoding":"json","relay":{"route_kind":"graphql_read","cache":"miss"},"identity":{"kind":"github_app"},"body":{"data":`+data+`}}`)
	}))
	defer server.Close()
	c, err := NewOctopoolConversations(server.URL, "fixture", "token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.ReadCommitHead(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.ReadCommit(context.Background(), CommitQuery{Head: h, Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Until: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)}, nil)
	if err != nil || string(p.RawJSON) != commit || !p.Terminal || p.NextCursor != "" {
		t.Fatal(p, err)
	}
	a, err := c.ReadCommitAttribution(context.Background(), "C_9007199254740993", oid, "", nil)
	if err != nil || !a.Terminal || a.NextCursor != "" || calls != 3 {
		t.Fatal(a, err, calls)
	}
	boundary, err := c.ReadCommit(context.Background(), CommitQuery{Head: h, Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Until: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}, nil)
	if err != nil || len(boundary.RawJSON) != 0 || !boundary.Terminal {
		t.Fatal("inclusive provider upper bound leaked", boundary, err)
	}
	if c.metadata != nil || len(c.issues) != 0 {
		t.Fatal("opened native inventory")
	}
}
func TestOctopoolCommitRejectsPartialAndWrongHead(t *testing.T) {
	oid := strings.Repeat("a", 40)
	base := `{"status":200,"body_encoding":"json","relay":{"route_kind":"graphql_read","cache":"miss"},"identity":{"kind":"github_app"},"body":{"data":{"repository":{"id":"R1","nameWithOwner":"fixture/repo","object":{"__typename":"Commit","oid":"` + oid + `","history":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}}`
	for _, mode := range []string{"partial", "head", "cursor", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			body := base
			switch mode {
			case "partial":
				body = strings.Replace(body, `"data":`, `"errors":[{"message":"partial"}],"data":`, 1)
			case "head":
				body = strings.Replace(body, oid, strings.Repeat("b", 40), 1)
			case "cursor":
				body = strings.Replace(body, `"hasNextPage":false`, `"hasNextPage":true`, 1)
			case "oversize":
				body = strings.Repeat("x", 3<<20+1)
			}
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer s.Close()
			c, _ := NewOctopoolConversations(s.URL, "fixture", "token", "fixture/repo")
			_, err := c.ReadCommit(context.Background(), CommitQuery{Head: CommitHead{Repository: "fixture/repo", NodeID: "R1", OID: oid}, Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Until: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)}, nil)
			if err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}
