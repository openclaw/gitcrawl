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
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestMapPullChecksUsesProviderIdentity(t *testing.T) {
	rows := []map[string]any{
		{"id": json.Number("9007199254740992"), "name": "same", "details_url": "https://example.invalid/check", "status": "queued", "old": true},
		{"id": json.Number("9007199254740993"), "name": "same", "details_url": "https://example.invalid/check"},
		{"name": "anonymous"},
		{"name": "anonymous"},
		{"id": json.Number("9007199254740992"), "name": "renamed", "status": "completed", "conclusion": "success", "new": true},
		{"id": json.Number("9223372036854775807")},
	}
	got := mapPullChecks(7, rows, "2026-09-01T12:00:00Z")
	if len(got) != 4 {
		t.Fatalf("mapped checks = %+v, want four independent identities", got)
	}
	if got[0].Name != "renamed" || got[0].Status != "completed" || got[0].Conclusion != "success" ||
		got[0].DetailsURL != "" || got[0].RawJSON != mustJSON(rows[4]) {
		t.Fatalf("later fetched observation was not replaced in place: %+v", got[0])
	}
	requireRawProviderID(t, got[0].RawJSON, "9007199254740992", "id")
	requireRawProviderID(t, got[1].RawJSON, "9007199254740993", "id")
	for _, check := range got {
		if check.ThreadID != 7 || check.FetchedAt != "2026-09-01T12:00:00Z" {
			t.Fatalf("check attribution = %+v", check)
		}
	}
	if got[1].Name != "same" || got[2].Name != "anonymous" || got[3].Name != "anonymous" {
		t.Fatalf("distinct or missing provider IDs collapsed: %+v", got)
	}
}

func TestSyncCheckRunPageOverlap(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "archive.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	mode, checkRequests := "overlap", 0
	var cancelPage context.CancelFunc
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := ""
		switch r.URL.Path {
		case "/repos/fixture/repo":
			body = `{"id":1}`
		case "/repos/fixture/repo/issues/2", "/repos/fixture/repo/issues/3":
			number := 2
			if strings.HasSuffix(r.URL.Path, "/3") {
				number = 3
			}
			body = fmt.Sprintf(`{"id":%d,"number":%d,"state":"open","title":"Pull request","body":"Body","pull_request":{},"created_at":"2026-09-01T00:00:00Z","updated_at":"2026-09-01T00:00:00Z"}`, number, number)
		case "/repos/fixture/repo/pulls/2", "/repos/fixture/repo/pulls/3":
			head := "head"
			if strings.HasSuffix(r.URL.Path, "/3") {
				head = "neighbor"
			}
			body = fmt.Sprintf(`{"head":{"sha":%q},"base":{"sha":"base"},"changed_files":2}`, head)
		case "/repos/fixture/repo/pulls/2/files", "/repos/fixture/repo/pulls/3/files":
			body = `[{"filename":"same.go","status":"removed"},{"filename":"same.go","status":"added"}]`
		case "/repos/fixture/repo/pulls/2/commits", "/repos/fixture/repo/pulls/3/commits":
			body = `[{"sha":"commit","commit":{"message":"Retained"}}]`
		case "/repos/fixture/repo/commits/neighbor/check-runs":
			body = `{"check_runs":[{"id":7,"name":"neighbor","status":"completed","details_url":"https://example.invalid/neighbor"}]}`
		case "/repos/fixture/repo/commits/head/check-runs":
			checkRequests++
			if r.URL.Query().Get("page") == "" {
				w.Header().Set("Link", `<http://`+r.Host+r.URL.Path+`?page=2>; rel="next"`)
				body = `{"check_runs":[
					{"id":9007199254740992,"name":"test","details_url":"https://example.invalid/check","status":"queued","old":true},
					{"id":9007199254740992,"name":"test","details_url":"https://example.invalid/check","status":"in_progress"},
					{"id":9007199254740993,"name":"other","details_url":"https://example.invalid/other"},
					{"name":"anonymous"},{"name":"anonymous"}
				]}`
			} else {
				switch mode {
				case "page failure":
					http.Error(w, "fixture page unavailable", http.StatusBadRequest)
					return
				case "cancel":
					cancelPage()
					return
				case "distinct collision":
					body = `{"check_runs":[{"id":9223372036854775807,"name":"test","details_url":"https://example.invalid/check"}]}`
				default:
					body = `{"check_runs":[{"id":9007199254740992,"name":"test","details_url":"https://example.invalid/check","status":"completed","conclusion":"success","check_suite":{"app":{"name":"CI"}},"new":true}]}`
				}
			}
		case "/repos/fixture/repo/actions/runs":
			body = `{"workflow_runs":[]}`
		case "/graphql":
			body = `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		_, _ = fmt.Fprint(w, body)
	}))
	defer server.Close()

	client := gh.New(gh.Options{BaseURL: server.URL})
	service := New(client, st)
	service.now = func() time.Time { return now }
	options := Options{Owner: "fixture", Repo: "repo", Numbers: []int{2}, IncludePRDetails: true}
	var repo store.Repository
	var before store.PullRequestCache
	var beforeChecks []store.PullRequestCheck
	for pass := 1; pass <= 2; pass++ {
		stats, err := service.Sync(ctx, options)
		if err != nil || stats.ThreadsSynced != 1 || stats.PRDetailsSynced != 1 || stats.PRChecksSynced != 4 {
			t.Fatalf("overlap sync %d: stats=%+v err=%v", pass, stats, err)
		}
		repo, err = st.RepositoryByFullName(ctx, "fixture/repo")
		if err != nil {
			t.Fatal(err)
		}
		before, err = st.PullRequestCache(ctx, repo.ID, 2)
		if err != nil {
			t.Fatal(err)
		}
		beforeChecks, err = st.PullRequestChecksAPIOrder(ctx, before.Detail.ThreadID)
		if err != nil || len(beforeChecks) != 4 {
			t.Fatalf("stored check count: %+v err=%v", beforeChecks, err)
		}
		check := beforeChecks[0]
		if check.Name != "test" || check.Status != "completed" || check.Conclusion != "success" ||
			check.WorkflowName != "CI" || check.FetchedAt != now.Format(time.RFC3339Nano) ||
			!strings.Contains(check.RawJSON, `"new":true`) || strings.Contains(check.RawJSON, `"old"`) {
			t.Fatalf("later fetched check not retained: %+v", check)
		}
		requireRawProviderID(t, check.RawJSON, "9007199254740992", "id")
		requireRawProviderID(t, beforeChecks[1].RawJSON, "9007199254740993", "id")
		if beforeChecks[1].Name != "other" || beforeChecks[2].Name != "anonymous" || beforeChecks[3].Name != "anonymous" ||
			len(before.Files) != 2 || before.Files[0].Status != "removed" || before.Files[1].Status != "added" || len(before.Commits) != 1 {
			t.Fatalf("API order or sibling identities changed: checks=%+v files=%+v commits=%+v", beforeChecks, before.Files, before.Commits)
		}
		assertChildReservation(t, ctx, st, before.Detail.ThreadID, store.ThreadChildPullRequestChecks, int64(pass))
		if pass == 1 {
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			service = New(client, st)
			service.now = func() time.Time { return now }
			now = now.Add(time.Hour)
		}
	}

	metadata := options
	metadata.IncludePRDetails, metadata.IncludePRMetadata = false, true
	now = now.Add(time.Hour)
	if stats, err := service.Sync(ctx, metadata); err != nil || stats.PRChecksSynced != 0 || checkRequests != 4 {
		t.Fatalf("metadata-only refresh fetched checks: %+v requests=%d err=%v", stats, checkRequests, err)
	}
	before, err = st.PullRequestCache(ctx, repo.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	unchangedChecks := func() {
		t.Helper()
		got, err := st.PullRequestChecksAPIOrder(ctx, before.Detail.ThreadID)
		if err != nil || !reflect.DeepEqual(got, beforeChecks) {
			t.Fatalf("unselected or failed check snapshot changed: %+v err=%v", got, err)
		}
		assertChildReservation(t, ctx, st, before.Detail.ThreadID, store.ThreadChildPullRequestChecks, 2)
	}
	unchangedChecks()
	lastSuccess, err := st.LastSuccessfulSyncAt(ctx, repo.ID)
	if err != nil {
		t.Fatal(err)
	}
	unchangedSuccess := func() {
		t.Helper()
		got, err := st.LastSuccessfulSyncAt(ctx, repo.ID)
		if err != nil || !got.Equal(lastSuccess) {
			t.Fatalf("failed check hydration advanced success: got=%s want=%s err=%v", got, lastSuccess, err)
		}
	}

	// Distinct IDs are not interchangeable, even when the legacy SQL key
	// collides. The failed item must roll back without losing its neighbor.
	mode = "distinct collision"
	now = now.Add(time.Hour)
	options.Numbers = []int{2, 3}
	stats, err := service.Sync(ctx, options)
	if err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed: pull_request_checks") ||
		stats.ThreadsSynced != 1 || stats.PRChecksSynced != 1 {
		t.Fatalf("distinct check collision was hidden or lost neighbor: %+v err=%v", stats, err)
	}
	after, err := st.PullRequestCache(ctx, repo.ID, 2)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("failed replacement did not roll back: %+v err=%v", after, err)
	}
	unchangedChecks()
	unchangedSuccess()
	neighbor, err := st.PullRequestCache(ctx, repo.ID, 3)
	if err != nil || len(neighbor.Checks) != 1 || neighbor.Checks[0].Name != "neighbor" {
		t.Fatalf("neighbor snapshot lost: %+v err=%v", neighbor, err)
	}
	failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
	if err != nil || len(failures) == 0 {
		t.Fatalf("rollback failure ledger missing: %+v err=%v", failures, err)
	}
	detailsFailed := false
	for _, failure := range failures {
		if failure.Number != 2 || failure.ThreadID != before.Detail.ThreadID || failure.ResolvedAt != "" {
			t.Fatalf("wrong failed identity: %+v", failure)
		}
		detailsFailed = detailsFailed || failure.Operation == "pull_request_details"
	}
	if !detailsFailed {
		t.Fatal("check persistence failure did not retain the details retry")
	}

	options.Numbers = []int{2}
	for _, failureMode := range []string{"page failure", "cancel"} {
		mode = failureMode
		now = now.Add(time.Hour)
		attemptCtx := ctx
		if mode == "cancel" {
			attemptCtx, cancelPage = context.WithCancel(ctx)
		}
		stats, err := service.Sync(attemptCtx, options)
		if cancelPage != nil {
			cancelPage()
		}
		if err == nil || stats.PRDetailsSynced != 0 || stats.PRChecksSynced != 0 {
			t.Fatalf("%s accepted partial pagination: %+v err=%v", mode, stats, err)
		}
		unchangedChecks()
		unchangedSuccess()
	}
	mode = "overlap"
	now = now.Add(time.Hour)
	if stats, err := service.Sync(ctx, options); err != nil || stats.PRChecksSynced != 4 {
		t.Fatalf("completed retry: %+v err=%v", stats, err)
	}
	failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
	if err != nil || len(failures) != 0 {
		t.Fatalf("completed retry left failures: %+v err=%v", failures, err)
	}
}
