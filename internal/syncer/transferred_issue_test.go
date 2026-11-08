package syncer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

// GitHub answers GET /repos/{owner}/{repo}/issues/{n} for a transferred issue
// with 301 to the issue's new repository and number, and the default HTTP
// client follows it. An exact selection must not persist that other issue under
// the requested repository, where it would replace an unrelated local thread.
func TestSyncNumbersRejectsIssueTransferredToAnotherRepository(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requested int
		target    string
	}{
		{name: "new number", requested: 5, target: "/repos/fixture/other/issues/9"},
		{name: "same number", requested: 9, target: "/repos/fixture/other/issues/9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			transferred := false
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/repos/fixture/repo":
					fmt.Fprintf(w, `{"id":100,"name":"repo","full_name":"fixture/repo","owner":{"login":"fixture"},"url":%q}`, server.URL+"/repos/fixture/repo")
				case r.URL.Path == "/repos/fixture/repo/issues/9" && !transferred:
					fmt.Fprintf(w, `{"id":909,"number":9,"title":"local nine","state":"open","body":"keep","updated_at":"2026-01-01T00:00:00Z","html_url":"https://github.com/fixture/repo/issues/9","repository_url":%q}`, server.URL+"/repos/fixture/repo")
				case r.URL.Path == fmt.Sprintf("/repos/fixture/repo/issues/%d", tc.requested):
					w.Header().Set("Location", server.URL+tc.target)
					w.WriteHeader(http.StatusMovedPermanently)
					fmt.Fprintf(w, `{"message":"Moved Permanently","url":%q}`, server.URL+tc.target)
				case r.URL.Path == tc.target:
					fmt.Fprintf(w, `{"id":9909,"number":9,"title":"moved elsewhere","state":"open","body":"other repository","updated_at":"2026-02-01T00:00:00Z","html_url":"https://github.com/fixture/other/issues/9","repository_url":%q}`, server.URL+"/repos/fixture/other")
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.Error(w, "unexpected", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client := gh.New(gh.Options{BaseURL: server.URL})
			if _, err := New(client, s).Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{9}}); err != nil {
				t.Fatal(err)
			}

			transferred = true
			stats, err := New(client, s).Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{tc.requested}})
			var count int
			var title, htmlURL, githubID string
			if err := s.DB().QueryRow(`select count(*), max(title), max(html_url), max(github_id) from threads`).Scan(&count, &title, &htmlURL, &githubID); err != nil {
				t.Fatal(err)
			}
			if count != 1 || title != "local nine" || htmlURL != "https://github.com/fixture/repo/issues/9" || githubID != "909" {
				t.Fatalf("local thread replaced: count=%d title=%q url=%q id=%q", count, title, htmlURL, githubID)
			}
			if err == nil || !strings.Contains(err.Error(), "https://github.com/fixture/other/issues/9") {
				t.Fatalf("transferred issue accepted: %+v %v", stats, err)
			}
			if stats.ThreadsSynced != 0 {
				t.Fatalf("transferred issue persisted: %+v", stats)
			}
			var failures int
			if err := s.DB().QueryRow(`select count(*) from sync_attempt_failures where number = ? and operation = 'issue'`, tc.requested).Scan(&failures); err != nil {
				t.Fatal(err)
			}
			if failures != 1 {
				t.Fatalf("transferred selection failure not recorded: %d", failures)
			}
		})
	}
}

// A renamed repository redirects both the repository and its issues to the new
// name, so the requested issue still belongs to the synced repository.
func TestSyncNumbersFollowsRenamedRepositoryRedirect(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/fixture/old":
			w.Header().Set("Location", server.URL+"/repositories/100")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/repos/fixture/old/issues/5":
			w.Header().Set("Location", server.URL+"/repositories/100/issues/5")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/repositories/100":
			fmt.Fprintf(w, `{"id":100,"name":"repo","full_name":"fixture/repo","owner":{"login":"fixture"},"url":%q}`, server.URL+"/repos/fixture/repo")
		case "/repositories/100/issues/5":
			fmt.Fprintf(w, `{"id":505,"number":5,"title":"renamed home","state":"open","updated_at":"2026-01-01T00:00:00Z","html_url":"https://github.com/fixture/repo/issues/5","repository_url":%q}`, server.URL+"/repos/fixture/repo")
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	stats, err := New(gh.New(gh.Options{BaseURL: server.URL}), s).Sync(ctx, Options{Owner: "fixture", Repo: "old", Numbers: []int{5}})
	if err != nil || stats.ThreadsSynced != 1 {
		t.Fatalf("renamed repository selection: %+v %v", stats, err)
	}
}
