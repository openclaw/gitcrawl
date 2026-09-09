package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

var providerIDFixtures = []string{
	"9007199254740991",
	"9007199254740992",
	"9007199254740993",
	"9223372036854775807",
}

func requireProviderNumber(t *testing.T, value any, want string) {
	t.Helper()
	number, ok := value.(json.Number)
	if !ok || number.String() != want {
		t.Errorf("number = %v (%T), want json.Number(%q)", value, value, want)
	}
}

func TestProviderIDsSingleResources(t *testing.T) {
	for _, id := range providerIDFixtures {
		t.Run(id, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"id":%s,"owner":{"id":%s},"number":7,"draft":false,"body":null}`, id, id)
			}))
			defer server.Close()
			client := New(Options{BaseURL: server.URL})
			row, err := client.GetRepo(context.Background(), "owner", "repo", nil)
			if err != nil {
				t.Fatal(err)
			}
			requireProviderNumber(t, row["id"], id)
			requireProviderNumber(t, row["owner"].(map[string]any)["id"], id)
			if intValue(row["number"]) != 7 || row["draft"] != false || row["body"] != nil {
				t.Errorf("ordinary fields = %#v", row)
			}
			encoded, err := json.Marshal(row)
			if err != nil || !strings.Contains(string(encoded), `"id":`+id) {
				t.Errorf("re-encoded ID = %s, err=%v", encoded, err)
			}
		})
	}
}

func TestProviderIDsPaginatedCollections(t *testing.T) {
	for _, field := range []string{"", "check_runs", "workflow_runs"} {
		t.Run("field="+field, func(t *testing.T) {
			var pages []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				pages = append(pages, page)
				ids := providerIDFixtures[:2]
				if page == "" {
					w.Header().Set("Link", `<`+serverURL(r)+`?page=2>; rel="next"`)
				} else if page == "2" {
					ids = providerIDFixtures[2:]
				} else {
					http.Error(w, "unexpected page", http.StatusBadRequest)
					return
				}
				rows := fmt.Sprintf(`[{"id":%s,"nested":{"id":%s}},{"id":%s,"nested":{"id":%s}}]`, ids[0], ids[0], ids[1], ids[1])
				if field != "" {
					rows = fmt.Sprintf(`{"total_count":4,%q:%s}`, field, rows)
				}
				_, _ = fmt.Fprint(w, rows)
			}))
			defer server.Close()
			client := New(Options{BaseURL: server.URL})
			ctx := context.Background()
			var rows []map[string]any
			var err error
			switch field {
			case "":
				rows, err = client.ListIssueComments(ctx, "owner", "repo", 1, nil)
			case "check_runs":
				rows, err = client.ListCommitCheckRuns(ctx, "owner", "repo", "head", nil)
			case "workflow_runs":
				rows, err = client.ListWorkflowRuns(ctx, "owner", "repo", ListWorkflowRunsOptions{}, nil)
			}
			if err != nil || len(rows) != len(providerIDFixtures) {
				t.Fatalf("rows = %#v, err=%v", rows, err)
			}
			if !reflect.DeepEqual(pages, []string{"", "2"}) {
				t.Errorf("pages = %v", pages)
			}
			for i, row := range rows {
				requireProviderNumber(t, row["id"], providerIDFixtures[i])
				requireProviderNumber(t, row["nested"].(map[string]any)["id"], providerIDFixtures[i])
			}
		})
	}
}

func TestProviderIDsGraphQLPagination(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request graphqlEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		switch calls {
		case 1:
			if request.Variables["cursor"] != nil || request.Variables["pr"] != float64(7) {
				t.Errorf("first variables = %#v", request.Variables)
			}
			_, _ = fmt.Fprint(w, `{"data":{"repository":{"pullRequest":{"reviewThreads":{
				"nodes":[{"id":"PRRT_first","isResolved":false,"isOutdated":true,"line":42,"startLine":null,
					"viewerCanResolve":true,"viewerCanUnresolve":false,"viewerCanReply":true,
					"comments":{"nodes":[
						{"id":"PRRC_first","databaseId":9007199254740991},
						{"id":"PRRC_second","databaseId":9007199254740992}
					],"pageInfo":{"hasNextPage":true,"endCursor":"comments-next"}}}],
				"pageInfo":{"hasNextPage":true,"endCursor":"threads-next"}
			}}}}}`)
		case 2:
			if request.Variables["threadID"] != "PRRT_first" || request.Variables["cursor"] != "comments-next" {
				t.Errorf("comment variables = %#v", request.Variables)
			}
			_, _ = fmt.Fprint(w, `{"data":{"node":{"comments":{
				"nodes":[{"id":"PRRC_third","databaseId":9007199254740993}],
				"pageInfo":{"hasNextPage":false,"endCursor":"comments-done"}
			}}}}`)
		case 3:
			if request.Variables["cursor"] != "threads-next" || request.Variables["threadID"] != nil {
				t.Errorf("thread variables = %#v", request.Variables)
			}
			_, _ = fmt.Fprint(w, `{"data":{"repository":{"pullRequest":{"reviewThreads":{
				"nodes":[{"id":"PRRT_second","line":8,"comments":{
					"nodes":[{"id":"PRRC_fourth","databaseId":9223372036854775807}],
					"pageInfo":{"hasNextPage":false,"endCursor":null}
				}}],"pageInfo":{"hasNextPage":false,"endCursor":null}
			}}}}}`)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client := New(Options{BaseURL: server.URL})
	threads, err := client.ListPullReviewThreads(context.Background(), "owner", "repo", 7, nil)
	if err != nil || len(threads) != 2 || calls != 3 {
		t.Fatalf("threads = %#v, calls=%d, err=%v", threads, calls, err)
	}
	first := threads[0]
	if first["id"] != "PRRT_first" || first["isResolved"] != false || first["isOutdated"] != true ||
		intValue(first["line"]) != 42 || first["startLine"] != nil ||
		first["viewerCanResolve"] != true || first["viewerCanUnresolve"] != false || first["viewerCanReply"] != true {
		t.Errorf("first thread = %#v", first)
	}
	comments := reviewThreadCommentsFromMap(first["comments"])
	if len(comments.Nodes) != 3 || comments.PageInfo.HasNextPage || comments.PageInfo.EndCursor != "comments-done" {
		t.Fatalf("completed comments = %#v", comments)
	}
	for i, row := range comments.Nodes {
		requireProviderNumber(t, row["databaseId"], providerIDFixtures[i])
	}
	if comments.Nodes[2]["id"] != "PRRC_third" || threads[1]["id"] != "PRRT_second" || intValue(threads[1]["line"]) != 8 {
		t.Errorf("opaque IDs or line changed: %#v", threads)
	}
	last := reviewThreadCommentsFromMap(threads[1]["comments"])
	if len(last.Nodes) != 1 || last.Nodes[0]["id"] != "PRRC_fourth" {
		t.Fatalf("last comments = %#v", last)
	}
	requireProviderNumber(t, last.Nodes[0]["databaseId"], providerIDFixtures[3])
}

func TestProviderIDsDecodeBehavior(t *testing.T) {
	for _, tc := range []struct {
		name, surface, body, wantError string
	}{
		{"object malformed", "object", `{"id":`, "decode github response:"},
		{"list malformed", "list", `[{"id":`, "decode github page:"},
		{"envelope malformed", "envelope", `{"workflow_runs":[`, "decode github page:"},
		{"envelope wrong shape", "envelope", `{"workflow_runs":{}}`, `decode github page "workflow_runs":`},
		{"envelope missing", "envelope", `{}`, `decode github page: missing "workflow_runs"`},
		{"graphql malformed", "graphql", `{"data":`, "decode github response:"},
		{"graphql wrong shape", "graphql", `{"data":true}`, "decode github graphql data:"},
		{"graphql null", "graphql", `{"data":null}`, "github graphql response missing data"},
		{"object null", "object", `null`, ""},
		{"list null", "list", `null`, ""},
		{"envelope null", "envelope", `{"workflow_runs":null}`, ""},
		{"object trailing", "object", `{"id":9007199254740993} trailing`, ""},
		{"list trailing", "list", `[{"id":9007199254740993}] trailing`, ""},
		{"envelope trailing", "envelope", `{"workflow_runs":[{"id":9007199254740993}]} trailing`, ""},
		{"graphql trailing", "graphql", `{"data":{"id":9007199254740993}} trailing`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			client := New(Options{BaseURL: server.URL})
			ctx := context.Background()
			var rows []map[string]any
			var err error
			switch tc.surface {
			case "object":
				var row map[string]any
				row, err = client.GetRepo(ctx, "owner", "repo", nil)
				if row != nil {
					rows = append(rows, row)
				}
			case "list":
				rows, err = client.ListIssueComments(ctx, "owner", "repo", 1, nil)
			case "envelope":
				rows, err = client.ListWorkflowRuns(ctx, "owner", "repo", ListWorkflowRunsOptions{}, nil)
			case "graphql":
				var row map[string]any
				err = client.doGraphQL(ctx, "query { id }", nil, nil, &row)
				if row != nil {
					rows = append(rows, row)
				}
			}
			if tc.wantError != "" {
				if err == nil || !strings.HasPrefix(err.Error(), tc.wantError) {
					t.Fatalf("error = %v, want prefix %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(tc.name, "trailing") {
				if len(rows) != 1 {
					t.Fatalf("rows = %#v", rows)
				}
				requireProviderNumber(t, rows[0]["id"], "9007199254740993")
			} else if len(rows) != 0 {
				t.Errorf("null rows = %#v", rows)
			}
		})
	}
}
