package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

const oversizedRejection = `{"error":{"code":"fallback_local","details":{"reason":"github_response_too_large"}}}`

func TestOctopoolOversizedPagesRestartWithoutLosingRows(t *testing.T) {
	for _, test := range []struct {
		laterPage bool
		finalSize int
	}{{false, 10}, {true, 10}, {false, 1}} {
		t.Run(fmt.Sprintf("later_page=%t/final_size=%d", test.laterPage, test.finalSize), func(t *testing.T) {
			var requests []string
			body := strings.Repeat("complete λ body ", 100)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Pool, Method, Path string
					Query              map[string][]string
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer fixture-token" || request.Pool != "fixture" || request.Method != "GET" || request.Path != "/repos/fixture/repo/pulls/7/comments" {
					t.Error("request changed pool, identity or resource")
				}
				size, _ := strconv.Atoi(request.Query["per_page"][0])
				page := 1
				if values := request.Query["page"]; len(values) > 0 {
					page, _ = strconv.Atoi(values[0])
				}
				requests = append(requests, fmt.Sprintf("%d/%d", size, page))
				if page > 1 && (len(request.Query["marker"]) != 1 || request.Query["marker"][0] != "relay-next") {
					t.Error("did not follow the provider's complete next link")
				}
				if size > test.finalSize && (!test.laterPage || page == 2) {
					w.WriteHeader(http.StatusFailedDependency)
					_, _ = w.Write([]byte(oversizedRejection))
					return
				}
				start, end := (page-1)*size, min(page*size, 101)
				rows := []map[string]any{}
				for i := start; i < end; i++ {
					rows = append(rows, map[string]any{"id": json.Number(strconv.FormatInt(9007199254740993+int64(i), 10)), "body": body, "diff_hunk": body})
				}
				headers := map[string]string{}
				if end < 101 {
					headers["Link"] = fmt.Sprintf(`<https://api.github.com/repos/fixture/repo/pulls/7/comments?per_page=%d&page=%d&marker=relay-next>; rel="next"`, size, page+1)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": rows, "headers": headers, "relay": map[string]any{"cache": "miss"}})
			}))
			defer server.Close()
			client, err := NewOctopool(server.URL, "fixture", "fixture-token", "fixture/repo")
			if err != nil {
				t.Fatal(err)
			}
			rows, err := client.ListPullReviewComments(context.Background(), "fixture", "repo", 7, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 101 {
				t.Fatalf("lost or duplicated rows: %d", len(rows))
			}
			for i, row := range rows {
				if fmt.Sprint(row["id"]) != strconv.FormatInt(9007199254740993+int64(i), 10) || row["body"] != body || row["diff_hunk"] != body {
					t.Fatalf("row %d lost order, identity or full content", i)
				}
			}
			want := []string{"100/1"}
			if test.laterPage {
				want = append(want, "100/2")
			}
			if test.finalSize == 1 {
				want = append(want, "10/1")
			}
			for page := 1; page <= (101+test.finalSize-1)/test.finalSize; page++ {
				want = append(want, fmt.Sprintf("%d/%d", test.finalSize, page))
			}
			if strings.Join(requests, ",") != strings.Join(want, ",") {
				t.Fatalf("unexpected restart or pagination: %v", requests)
			}
		})
	}
}

func TestOctopoolOversizedSingleRecordStopsAtOne(t *testing.T) {
	var sizes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Query map[string][]string }
		_ = json.NewDecoder(r.Body).Decode(&request)
		sizes = append(sizes, request.Query["per_page"][0])
		w.WriteHeader(http.StatusFailedDependency)
		_, _ = w.Write([]byte(oversizedRejection))
	}))
	defer server.Close()
	client, err := NewOctopool(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := client.ListPullReviewComments(context.Background(), "fixture", "repo", 7, nil)
	if err == nil || rows != nil || strings.Join(sizes, ",") != "100,10,1" {
		t.Fatalf("single-record failure did not stop: sizes=%v rows=%d error=%v", sizes, len(rows), err)
	}
}

func TestPaginationDoesNotReduceForOtherFailures(t *testing.T) {
	for _, test := range []struct {
		name     string
		pooled   bool
		status   int
		response string
	}{
		{"authentication", true, 401, oversizedRejection},
		{"other reason", true, 424, strings.ReplaceAll(oversizedRejection, "github_response_too_large", "owner_denied")},
		{"not a typed relay", false, 424, oversizedRejection},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.response))
			}))
			defer server.Close()
			client := New(Options{BaseURL: server.URL})
			if test.pooled {
				var err error
				client, err = NewOctopool(server.URL, "fixture", "fixture-token", "fixture/repo")
				if err != nil {
					t.Fatal(err)
				}
			}
			rows, err := client.ListPullReviewComments(context.Background(), "fixture", "repo", 7, nil)
			if err == nil || rows != nil || calls != 1 {
				t.Fatalf("unrelated failure retried: calls=%d rows=%d error=%v", calls, len(rows), err)
			}
		})
	}
}
