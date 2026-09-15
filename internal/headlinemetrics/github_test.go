package headlinemetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/github"
)

func fixtureCollector(t *testing.T, override func(http.ResponseWriter, *http.Request) bool, traffic bool) Collector {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("unexpected mutation %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("missing normal client authorization")
		}
		if override != nil && override(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/search/issues":
			if !strings.Contains(r.URL.Query().Get("q"), "is:pr is:open") {
				t.Error("incorrect pull query")
			}
			fmt.Fprint(w, `{"total_count":3,"incomplete_results":false}`)
		case strings.HasSuffix(r.URL.Path, "/traffic/clones"):
			fmt.Fprint(w, `{"clones":[{"timestamp":"2026-09-14T00:00:00Z","count":0},{"timestamp":"2026-09-15T00:00:00Z","count":7}]}`)
		case strings.HasSuffix(r.URL.Path, "/releases"):
			fmt.Fprint(w, `[{"id":42,"published_at":"2026-09-14T15:00:00Z","name":"Stable","tag_name":"v1","html_url":"https://example.test/v1"},{"id":43,"draft":true},{"id":44,"prerelease":true}]`)
		case strings.HasPrefix(r.URL.Path, "/repos/"):
			fmt.Fprint(w, `{"stargazers_count":100,"forks_count":5,"watchers_count":100,"subscribers_count":7,"open_issues_count":11}`)
		default:
			t.Errorf("unexpected API %s", r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return GitHubCollector(github.New(github.Options{BaseURL: server.URL, Token: "fixture-token", HTTPClient: server.Client()}), traffic)
}

func TestGitHubMetricsUseActualWatchersSeparateIssuesAndCompletedDays(t *testing.T) {
	c := testConfig(t)
	rows, err := fixtureCollector(t, nil, true)(context.Background(), c, "2026-09-15T01:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 14 {
		t.Fatalf("rows=%d", len(rows))
	}
	for _, target := range c.Targets {
		values := map[string]float64{}
		events := 0
		for _, r := range rows {
			if r.Target != target.Target {
				continue
			}
			if r.Type == "event" {
				events++
				if r.Label != "Stable" {
					t.Fatal(r)
				}
				continue
			}
			if r.Value == nil {
				t.Fatal("unexpected unknown", r)
			}
			values[r.Metric] = *r.Value
			if r.Kind == "daily" && (r.TS != "2026-09-14T23:59:59.999Z" || r.ObservedAt != "2026-09-15T01:00:00Z") {
				t.Fatal("incorrect UTC day", r)
			}
		}
		if values["stars"] != 100 || values["forks"] != 5 || values["watchers"] != 7 || values["open_prs"] != 3 || values["open_issues"] != 8 || values["clones"] != 0 || events != 1 {
			t.Fatalf("%s values=%v events=%d", target.Target, values, events)
		}
	}
}

func TestMissingOrIncompletePRCountNeverFallsBackToCombinedIssues(t *testing.T) {
	for _, body := range []string{`{}`, `{"total_count":3,"incomplete_results":true}`, `{"total_count":-1}`, `{"total_count":30}`, `{"total_count":3,"incomplete_results":"invalid"}`} {
		t.Run(body, func(t *testing.T) {
			collect := fixtureCollector(t, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path == "/search/issues" {
					fmt.Fprint(w, body)
					return true
				}
				return false
			}, false)
			rows, err := collect(context.Background(), testConfig(t), "2026-09-15T01:00:00Z")
			if err == nil {
				t.Fatal("missing count reported healthy")
			}
			for _, r := range rows {
				if r.Metric == "open_issues" && r.Value != nil {
					t.Fatal("combined count used", r)
				}
			}
		})
	}
}

func TestOptionalCloneTrafficAndOtherPartialFailures(t *testing.T) {
	for _, status := range []int{403, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			collect := fixtureCollector(t, func(w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "/traffic/clones") {
					w.WriteHeader(status)
					fmt.Fprint(w, "private response")
					return true
				}
				return false
			}, true)
			rows, err := collect(context.Background(), testConfig(t), "2026-09-15T01:00:00Z")
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 12 {
				t.Fatalf("unexpected optional traffic rows %d", len(rows))
			}
		})
	}
	cases := []struct {
		name    string
		match   func(*http.Request) bool
		body    string
		missing string
	}{
		{"missing watchers", func(r *http.Request) bool { return r.URL.Path == "/repos/openclaw/openclaw" }, `{"stargazers_count":1,"forks_count":1,"watchers_count":900,"open_issues_count":10}`, "watchers"},
		{"malformed repo", func(r *http.Request) bool { return r.URL.Path == "/repos/openclaw/openclaw" }, `{"stargazers_count":12,`, "stars"},
		{"malformed traffic", func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, "/traffic/clones") }, `{"clones":[{"timestamp":"bad","count":3}]}`, ""},
		{"missing clone count", func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, "/traffic/clones") }, `{"clones":[{"timestamp":"2026-09-14T00:00:00Z"}]}`, "clones"},
		{"invalid stable release", func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, "/releases") }, `[{"id":9,"published_at":"bad","name":"v1"}]`, ""},
		{"malformed releases", func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, "/releases") }, `{`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collect := fixtureCollector(t, func(w http.ResponseWriter, r *http.Request) bool {
				if tc.match(r) {
					fmt.Fprint(w, tc.body)
					return true
				}
				return false
			}, true)
			rows, err := collect(context.Background(), testConfig(t), "2026-09-15T01:00:00Z")
			if err == nil || len(rows) < 10 {
				t.Fatalf("partial=%d,%v", len(rows), err)
			}
			if tc.missing != "" {
				found := false
				for _, r := range rows {
					if r.Target == "openclaw/openclaw" && r.Metric == tc.missing {
						found = true
						if r.Value != nil {
							t.Fatal("invalid value retained", r)
						}
					}
				}
				if !found {
					t.Fatal("missing unknown observation")
				}
			}
		})
	}
}

func TestReleasePaginationBeyondFivePagesAndNoUnauthenticatedTraffic(t *testing.T) {
	pages := 0
	collect := fixtureCollector(t, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/traffic/clones") {
			t.Error("unauthenticated traffic request")
			return true
		}
		if !strings.HasSuffix(r.URL.Path, "/releases") {
			return false
		}
		pages++
		size := 100
		if pages == 6 {
			size = 1
		}
		releases := make([]github.Release, size)
		for i := range releases {
			releases[i] = github.Release{ID: int64(pages*100 + i), Created: "2026-09-14T00:00:00Z", Tag: "v1"}
		}
		if err := json.NewEncoder(w).Encode(releases); err != nil {
			t.Error(err)
		}
		return true
	}, false)
	c := testConfig(t)
	c.Targets = c.Targets[:1]
	rows, err := collect(context.Background(), c, "2026-09-15T01:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if pages != 6 || len(rows) != 506 {
		t.Fatalf("pages=%d rows=%d", pages, len(rows))
	}
	if _, err := collect(context.Background(), c, "bad"); err == nil {
		t.Fatal("invalid clock accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := collect(ctx, c, "2026-09-15T01:00:00Z"); err == nil {
		t.Fatal("cancellation accepted")
	}
}
