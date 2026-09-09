package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestProviderIDsPersistLosslesslyThroughSync(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ids := []string{"9007199254740991", "9007199254740992", "9007199254740993", "9223372036854775807"}
	const timestamp = "2026-09-01T12:00:00Z"
	const repoJSON = `{"id":9223372036854775807,"open_issues_count":2,"owner":{"id":9007199254740993}}`
	const issueJSON = `{"id":9007199254740992,"number":1,"state":"open","title":"Issue","body":"Issue body","user":{"login":"author","type":"User"},"created_at":"2026-09-01T12:00:00Z","updated_at":"2026-09-01T12:00:00Z"}`
	const pullJSON = `{"id":9007199254740993,"number":2,"state":"open","title":"Pull request","body":"Pull body","draft":true,"pull_request":{},"user":{"login":"author","type":"User"},"created_at":"2026-09-01T12:00:00Z","updated_at":"2026-09-01T12:00:00Z"}`
	requests := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests[r.URL.Path+"?"+r.URL.Query().Get("page")]++
		body := ""
		pageIDs := ids[:2]
		switch r.URL.Query().Get("page") {
		case "":
		case "2":
			pageIDs = ids[2:]
		default:
			http.Error(w, "unexpected page", http.StatusBadRequest)
			return
		}
		nextPage := func() {
			if r.URL.Query().Get("page") == "" {
				w.Header().Set("Link", `<http://`+r.Host+r.URL.Path+`?page=2>; rel="next"`)
			}
		}
		switch r.URL.Path {
		case "/repos/owner/repo":
			body = repoJSON
		case "/repos/owner/repo/issues":
			nextPage()
			body = "[" + issueJSON + "]"
			if r.URL.Query().Get("page") == "2" {
				body = "[" + pullJSON + "]"
			}
		case "/repos/owner/repo/issues/1/comments":
			body = `[]`
		case "/repos/owner/repo/issues/2/comments", "/repos/owner/repo/pulls/2/reviews", "/repos/owner/repo/pulls/2/comments":
			nextPage()
			var rows []string
			for _, id := range pageIDs {
				rows = append(rows, fmt.Sprintf(`{"id":%s,"body":"Comment %s","state":"APPROVED","user":{"login":"reviewer","type":"User","id":%s},"created_at":%q,"updated_at":%q}`,
					id, id, id, timestamp, timestamp))
			}
			body = "[" + strings.Join(rows, ",") + "]"
		case "/repos/owner/repo/pulls/2":
			body = `{"id":9007199254740993,"draft":true,"head":{"sha":"head","ref":"branch","repo":{"id":9223372036854775807,"full_name":"owner/repo"}},"base":{"sha":"base"},"additions":7,"deletions":3,"changed_files":1}`
		case "/repos/owner/repo/pulls/2/files":
			body = `[{"filename":"file.go","status":"modified","additions":7,"deletions":3,"changes":10}]`
		case "/repos/owner/repo/pulls/2/commits":
			body = `[{"sha":"head","author":{"id":9007199254740993,"login":"author"},"commit":{"message":"Change"}}]`
		case "/repos/owner/repo/commits/head/check-runs":
			body = `{"check_runs":[{"id":9007199254740993,"name":"test","status":"completed","conclusion":"success","check_suite":{"id":9223372036854775807}}]}`
		case "/repos/owner/repo/actions/runs":
			nextPage()
			var rows []string
			for _, id := range pageIDs {
				rows = append(rows, fmt.Sprintf(`{"id":%s,"run_number":17,"head_sha":"head","name":"test","status":"completed","conclusion":"success","created_at":%q,"updated_at":%q}`,
					id, timestamp, timestamp))
			}
			body = `{"total_count":4,"workflow_runs":[` + strings.Join(rows, ",") + `]}`
		case "/graphql":
			body = `{"data":{"repository":{"pullRequest":{"reviewThreads":{
				"nodes":[{"id":"PRRT_opaque","path":"file.go","line":42,"startLine":40,
					"isResolved":false,"isOutdated":true,"viewerCanResolve":true,"viewerCanUnresolve":false,"viewerCanReply":true,
					"comments":{"nodes":[
						{"id":"PRRC_first","databaseId":9007199254740991,"body":"First","author":{"login":"reviewer","__typename":"User"}},
						{"id":"PRRC_second","databaseId":9007199254740992},
						{"id":"PRRC_third","databaseId":9007199254740993},
						{"id":"PRRC_fourth","databaseId":9223372036854775807}
					],"pageInfo":{"hasNextPage":false,"endCursor":null}}
				}],"pageInfo":{"hasNextPage":false,"endCursor":null}
			}}}}}`
		default:
			t.Errorf("unexpected request: %s", r.URL)
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		_, _ = fmt.Fprint(w, body)
	}))
	defer server.Close()

	client := gh.New(gh.Options{BaseURL: server.URL})
	syncer := New(client, st)
	var firstRowIDs map[string]int64
	for pass := 1; pass <= 2; pass++ {
		stats, err := syncer.Sync(ctx, Options{
			Owner: "owner", Repo: "repo", State: "all",
			IncludeComments: true, IncludePRDetails: true,
		})
		if err != nil {
			t.Fatalf("sync %d: %v", pass, err)
		}
		if stats.ThreadsSynced != 2 || stats.IssuesSynced != 1 || stats.PullRequestsSynced != 1 ||
			stats.CommentsSynced != 12 || stats.ReviewThreadsSynced != 1 || stats.PRDetailsSynced != 1 ||
			stats.PRFilesSynced != 1 || stats.PRCommitsSynced != 1 || stats.PRChecksSynced != 1 || stats.WorkflowRunsSynced != 4 {
			t.Fatalf("sync %d counts = %+v", pass, stats)
		}
		repo, err := st.RepositoryByFullName(ctx, "owner/repo")
		if err != nil || repo.GitHubRepoID != ids[3] {
			t.Fatalf("repository = %+v, err=%v", repo, err)
		}
		requireRawProviderID(t, repo.RawJSON, ids[3], "id")
		requireRawProviderID(t, repo.RawJSON, ids[2], "owner", "id")
		if total := expectedIssueTotal(mustProviderObject(t, repo.RawJSON), "open", "", 0); total != 2 {
			t.Errorf("open issue count = %d", total)
		}
		threads, err := st.ListThreads(ctx, repo.ID, true)
		if err != nil || len(threads) != 2 {
			t.Fatalf("threads = %+v, err=%v", threads, err)
		}
		rowIDs := map[string]int64{"repository": repo.ID}
		var pull store.Thread
		for _, thread := range threads {
			wantID := ids[thread.Number]
			if thread.GitHubID != wantID {
				t.Errorf("thread %d ID = %q, want %q", thread.Number, thread.GitHubID, wantID)
			}
			requireRawProviderID(t, thread.RawJSON, wantID, "id")
			rowIDs[fmt.Sprintf("thread:%d", thread.Number)] = thread.ID
			if thread.Number == 2 {
				pull = thread
			}
		}
		if pull.ID == 0 || !pull.IsDraft {
			t.Fatalf("pull request = %+v", pull)
		}
		comments, err := st.ListComments(ctx, pull.ID)
		if err != nil || len(comments) != 12 {
			t.Fatalf("comments = %+v, err=%v", comments, err)
		}
		wantComments := make(map[string]bool)
		for _, family := range []string{"issue_comment", "pull_review", "pull_review_comment"} {
			for _, id := range ids {
				wantComments[family+":"+id] = true
			}
		}
		for _, comment := range comments {
			key := comment.CommentType + ":" + comment.GitHubID
			if !wantComments[key] || comment.Body != "Comment "+comment.GitHubID {
				t.Errorf("unexpected comment = %+v", comment)
			}
			delete(wantComments, key)
			rowIDs[key] = comment.ID
			requireRawProviderID(t, comment.RawJSON, comment.GitHubID, "id")
			requireRawProviderID(t, comment.RawJSON, comment.GitHubID, "user", "id")
		}
		if len(wantComments) != 0 {
			t.Errorf("missing comments = %v", wantComments)
		}
		detail, found, err := st.PullRequestDetailByThread(ctx, pull.ID)
		if err != nil || !found || detail.Additions != 7 || detail.Deletions != 3 || detail.ChangedFiles != 1 {
			t.Fatalf("detail = %+v, found=%t, err=%v", detail, found, err)
		}
		requireRawProviderID(t, detail.RawJSON, ids[2], "id")
		requireRawProviderID(t, detail.RawJSON, ids[3], "head", "repo", "id")
		files, err := st.PullRequestFiles(ctx, pull.ID)
		if err != nil || len(files) != 1 || files[0].Additions != 7 || files[0].Deletions != 3 || files[0].Changes != 10 {
			t.Fatalf("files = %+v, err=%v", files, err)
		}
		commits, err := st.PullRequestCommits(ctx, pull.ID)
		if err != nil || len(commits) != 1 {
			t.Fatalf("commits = %+v, err=%v", commits, err)
		}
		requireRawProviderID(t, commits[0].RawJSON, ids[2], "author", "id")
		checks, err := st.PullRequestChecks(ctx, pull.ID)
		if err != nil || len(checks) != 1 {
			t.Fatalf("checks = %+v, err=%v", checks, err)
		}
		requireRawProviderID(t, checks[0].RawJSON, ids[2], "id")
		requireRawProviderID(t, checks[0].RawJSON, ids[3], "check_suite", "id")
		runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{HeadSHA: "head", Limit: -1})
		if err != nil || len(runs) != len(ids) {
			t.Fatalf("workflow runs = %+v, err=%v", runs, err)
		}
		wantRuns := make(map[string]bool)
		for _, id := range ids {
			wantRuns[id] = true
		}
		for _, run := range runs {
			if !wantRuns[run.RunID] || run.RunNumber != 17 {
				t.Errorf("unexpected run = %+v", run)
			}
			delete(wantRuns, run.RunID)
			requireRawProviderID(t, run.RawJSON, run.RunID, "id")
		}
		if len(wantRuns) != 0 {
			t.Errorf("missing runs = %v", wantRuns)
		}
		reviewThreads, err := st.PullRequestReviewThreads(ctx, pull.ID)
		if err != nil || len(reviewThreads) != 1 {
			t.Fatalf("review threads = %+v, err=%v", reviewThreads, err)
		}
		review := reviewThreads[0]
		if review.ReviewThreadID != "PRRT_opaque" || review.Line != 42 || review.StartLine != 40 ||
			review.IsResolved || !review.IsOutdated || !review.ViewerCanResolve || review.ViewerCanUnresolve || !review.ViewerCanReply {
			t.Errorf("review thread fields = %+v", review)
		}
		var graphComments []json.RawMessage
		if err := json.Unmarshal([]byte(review.CommentsJSON), &graphComments); err != nil || len(graphComments) != len(ids) {
			t.Fatalf("graph comments = %s, err=%v", review.CommentsJSON, err)
		}
		rawComments := mapValue(mustProviderObject(t, review.RawJSON)["comments"])
		rawNodes := mapAnySlice(rawComments, "nodes")
		if len(rawNodes) != len(ids) {
			t.Fatalf("raw graph comments = %#v", rawComments)
		}
		for i, id := range ids {
			requireRawProviderID(t, string(graphComments[i]), id, "databaseId")
			if jsonID(rawNodes[i]["databaseId"]) != id {
				t.Errorf("raw graph ID = %v, want %s", rawNodes[i]["databaseId"], id)
			}
		}
		if pass == 1 {
			firstRowIDs = rowIDs
		} else if !reflect.DeepEqual(firstRowIDs, rowIDs) {
			t.Errorf("repeated sync changed row identities: first=%v, second=%v", firstRowIDs, rowIDs)
		}
		for table, want := range map[string]int{
			"repositories": 1, "threads": 2, "comments": 12, "github_workflow_runs": 4,
			"pull_request_review_threads": 1, "comment_revisions": 12, "pull_request_review_thread_revisions": 1,
		} {
			var count int
			if err := st.DB().QueryRowContext(ctx, "select count(*) from "+table).Scan(&count); err != nil || count != want {
				t.Errorf("sync %d %s count = %d, want %d, err=%v", pass, table, count, want, err)
			}
		}
	}
	for _, path := range []string{
		"/repos/owner/repo/issues", "/repos/owner/repo/issues/2/comments",
		"/repos/owner/repo/pulls/2/reviews", "/repos/owner/repo/pulls/2/comments", "/repos/owner/repo/actions/runs",
	} {
		if requests[path+"?"] != 2 || requests[path+"?2"] != 2 {
			t.Errorf("pagination requests for %s = %v", path, requests)
		}
	}
}

func requireRawProviderID(t *testing.T, raw, want string, path ...string) {
	t.Helper()
	value := json.RawMessage(raw)
	for _, key := range path {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(value, &object); err != nil {
			t.Fatalf("decode raw JSON at %q: %v", key, err)
		}
		value = object[key]
	}
	if string(value) != want {
		t.Errorf("raw %s = %s, want numeric %s", strings.Join(path, "."), value, want)
	}
}

func mustProviderObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		t.Fatal(err)
	}
	return object
}
