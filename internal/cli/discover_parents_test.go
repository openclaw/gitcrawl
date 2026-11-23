package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/acquireapi"
)

func TestDiscoverParentsOctopoolResumeWithoutSQLite(t *testing.T) {
	for _, badLastPage := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "invalid page preserves cursor"}[badLastPage], func(t *testing.T) {
			root := t.TempDir()
			numbers := filepath.Join(root, "numbers.txt")
			if err := os.WriteFile(numbers, []byte("7, 8, 9"), 0600); err != nil {
				t.Fatal(err)
			}
			db := filepath.Join(root, "untouched", "native.sqlite")
			t.Setenv("GITCRAWL_DB", db)
			t.Setenv("OCTOPOOL_TOKEN", "relay-fixture")
			t.Setenv("OCTOPOOL_POOL", "fixture")
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var q struct {
					Pool, Method, Path string
					Query              map[string][]string
				}
				if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
					t.Error(err)
				}
				if r.Header.Get("Authorization") != "Bearer relay-fixture" || q.Pool != "fixture" || q.Method != "GET" {
					t.Errorf("wrong relay request: %+v", q)
				}
				if q.Path == "/repos/example/project" {
					json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": map[string]any{"id": 42}})
					return
				}
				if q.Path != "/repos/example/project/issues" || q.Query["sort"][0] != "created" || q.Query["direction"][0] != "asc" || q.Query["state"][0] != "all" || len(q.Query["since"]) != 0 {
					t.Errorf("wrong inventory scope: %+v", q)
				}
				headers := map[string]string{}
				var rows any
				if len(q.Query["page"]) == 0 {
					headers["Link"] = `<https://api.github.com/repositories/42/issues?state=all&sort=created&direction=asc&per_page=100&page=2>; rel="next"`
					rows = []map[string]any{{"number": 7, "id": json.Number("9007199254740993"), "pull_request": map[string]any{"url": "https://api.github.com/repos/example/project/pulls/7"}}, {"number": 6, "id": 6}}
				} else {
					rows = []map[string]any{{"number": 8, "id": 80}, {"number": 7, "id": json.Number("9007199254740993"), "pull_request": map[string]any{}}}
					if badLastPage {
						rows = []map[string]any{{"number": 8, "id": 80}, {"number": 9, "id": "invalid"}}
					}
				}
				json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": rows, "headers": headers})
			}))
			defer server.Close()
			t.Setenv("OCTOPOOL_URL", server.URL)
			run := func(extra ...string) (parentDiscovery, error) {
				a := New()
				var out bytes.Buffer
				a.Stdout = &out
				a.Stderr = io.Discard
				a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal credential lookup"); return "", nil }
				args := []string{"discover-parents", "--repo", "example/project", "--numbers-file", numbers, "--json"}
				args = append(args, extra...)
				err := a.Run(context.Background(), args)
				var result parentDiscovery
				if e := json.Unmarshal(out.Bytes(), &result); e != nil {
					t.Fatalf("invalid output %q: %v", out.String(), e)
				}
				return result, err
			}
			first, err := run("--max-pages", "1")
			if err != nil || first.Complete || first.NextCursor == "" || first.Pages != 1 || len(first.Selection.Parents) != 1 || first.Selection.Parents[0].BackingIssueID != "9007199254740993" || first.Selection.Parents[0].Kind != "pull_request" {
				t.Fatalf("first page: %+v %v", first, err)
			}
			second, err := run("--cursor", first.NextCursor)
			if second.NumbersSHA256 != first.NumbersSHA256 {
				t.Fatal("selection changed")
			}
			if badLastPage {
				if err == nil || second.Pages != 0 || second.NextCursor != first.NextCursor || len(second.Selection.Parents) != 0 {
					t.Fatalf("invalid page advanced: %+v %v", second, err)
				}
			} else {
				if err != nil || !second.Complete || second.NextCursor != "" || second.Pages != 1 || len(second.Selection.Parents) != 2 {
					t.Fatalf("terminal: %+v %v", second, err)
				}
				b, _ := json.Marshal(second.Selection)
				if _, e := acquireapi.DecodeParentSelection(b, "example/project"); e != nil {
					t.Fatal(e)
				}
			}
			if calls != 4 {
				t.Fatalf("unexpected calls %d", calls)
			}
			if _, e := os.Stat(filepath.Dir(db)); !os.IsNotExist(e) {
				t.Fatal("SQLite path touched", e)
			}
		})
	}
}

func TestDiscoverParentsRejectsSelectionBeforeCredentials(t *testing.T) {
	t.Setenv("OCTOPOOL_TOKEN", "")
	file := filepath.Join(t.TempDir(), "numbers")
	if err := os.WriteFile(file, []byte("7,7"), 0600); err != nil {
		t.Fatal(err)
	}
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	err := a.Run(context.Background(), []string{"discover-parents", "--repo", "example/project", "--numbers-file", file})
	if err == nil || !strings.Contains(err.Error(), "duplicate parent") {
		t.Fatal(err)
	}
	if releaseNotificationAllowed([]string{"discover-parents"}) {
		t.Fatal("unrelated release read enabled")
	}
}

func TestDiscoverUpdatedParentsKeepsOldThreadsAndResumeScope(t *testing.T) {
	t.Setenv("OCTOPOOL_TOKEN", "fixture")
	t.Setenv("OCTOPOOL_POOL", "fixture")
	since := "2026-11-19T00:00:00Z"
	calls := 0
	oversized := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var q struct {
			Method, Path string
			Query        map[string][]string
		}
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Fatal(err)
		}
		if q.Method != "GET" {
			t.Errorf("method: %s", q.Method)
		}
		if q.Path == "/repos/example/project" {
			json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": map[string]any{"id": 42}})
			return
		}
		if q.Path != "/repos/example/project/issues" || strings.Join(q.Query["since"], "") != since || strings.Join(q.Query["sort"], "") != "updated" || strings.Join(q.Query["direction"], "") != "desc" || strings.Join(q.Query["per_page"], "") != "20" {
			t.Errorf("wrong updated inventory: %+v", q)
		}
		row := map[string]any{"number": 7, "id": json.Number("9007199254740993"), "created_at": "2020-01-01T00:00:00Z", "updated_at": "2026-11-22T00:00:00Z", "body": "must not be exported"}
		headers := map[string]string{}
		if len(q.Query["page"]) == 0 {
			headers["Link"] = `<https://api.github.com/repositories/42/issues?state=all&sort=updated&direction=desc&per_page=20&since=2026-11-19T00%3A00%3A00Z&page=2>; rel="next"`
		} else {
			row["number"], row["id"], row["pull_request"] = 8, 80, map[string]any{}
		}
		rows := []any{row}
		if oversized {
			for len(rows) <= 20 {
				rows = append(rows, row)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": rows, "headers": headers})
	}))
	defer server.Close()
	t.Setenv("OCTOPOOL_URL", server.URL)
	type observedDiscovery struct {
		Selection                                           acquireapi.ParentSelection
		NumbersSHA256, UpdatedSince, ObservedAt, NextCursor string
		Complete                                            bool
	}
	run := func(cursor string) observedDiscovery {
		a := New()
		var out bytes.Buffer
		a.Stdout, a.Stderr = &out, io.Discard
		a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal auth lookup"); return "", nil }
		if err := a.Run(context.Background(), []string{"discover-parents", "--repo", "example/project", "--updated-since", since, "--cursor", cursor, "--page-size", "20", "--max-pages", "1", "--json"}); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "must not be exported") || strings.Contains(out.String(), "body") {
			t.Fatal("raw provider fields escaped")
		}
		var result observedDiscovery
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.UpdatedSince != since || result.ObservedAt == "" || result.NumbersSHA256 != "" || len(result.Selection.Parents) != 1 {
			t.Fatalf("invalid receipt: %+v", result)
		}
		return result
	}
	first := run("")
	if first.Complete || first.NextCursor == "" || first.Selection.Parents[0].Number != 7 {
		t.Fatalf("old updated parent missing: %+v", first)
	}
	second := run(first.NextCursor)
	if !second.Complete || second.NextCursor != "" || second.Selection.Parents[0].Kind != "pull_request" || calls != 4 {
		t.Fatalf("resume: %+v calls=%d", second, calls)
	}
	oversized = true
	a := New()
	var out bytes.Buffer
	a.Stdout, a.Stderr = &out, io.Discard
	err := a.Run(context.Background(), []string{"discover-parents", "--repo", "example/project", "--updated-since", since, "--page-size", "20", "--json"})
	var invalid parentDiscovery
	if json.Unmarshal(out.Bytes(), &invalid) != nil || err == nil || !strings.Contains(err.Error(), "exceeds requested size") || invalid.Complete || invalid.Pages != 0 || invalid.Scanned != 0 || invalid.NextCursor != "" || len(invalid.Selection.Parents) != 0 {
		t.Fatalf("oversized page advanced: %+v %v", invalid, err)
	}
}
