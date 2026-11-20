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

func parentFixture(number int, kind string) map[string]any {
	stateField := "issueState"
	if kind == "PullRequest" {
		stateField = "pullState"
	}
	return map[string]any{"__typename": kind, "id": fmt.Sprintf("node-%d", number),
		"fullDatabaseId": json.Number("9007199254740993"), "number": json.Number(fmt.Sprint(number)),
		"repository": map[string]any{"nameWithOwner": "fixture/repo"}, "title": "full title λ\x00",
		"body": strings.Repeat("full body λ\x00", 1000), "author": nil, "authorAssociation": "NONE",
		"createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "closedAt": nil,
		"url": fmt.Sprintf("https://github.com/fixture/repo/issues/%d", number), stateField: "OPEN",
		"locked": false, "activeLockReason": nil, "milestone": nil}
}

func parentTestClient(t *testing.T, response func(map[string]any) map[string]any) (*OctopoolConversations, *int) {
	t.Helper()
	calls := new(int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		if r.URL.Path != "/v1/github/request" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("wrong relay or credential")
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request["path"] != "/graphql" || request["method"] != "POST" || request["pool"] != "fixture" {
			t.Error("non-GraphQL request")
		}
		if historyMap(request["headers"])["cache-control"] != "max-age=0" {
			t.Error("request was not fresh")
		}
		query := historyString(historyMap(request["graphql"])["query"])
		if !strings.Contains(query, "fragment ParentIssue on Issue { "+parentFields+" issueState: state") || !strings.Contains(query, "fragment ParentPull on PullRequest { "+parentFields+" pullState: state") {
			t.Error("distinct schema enum fields share a response name")
		}
		for _, connection := range []string{"comments(", "reviews(", "reviewThreads(", "labels(", "assignees("} {
			if strings.Contains(query, connection) {
				t.Errorf("parent query included %s", connection)
			}
		}
		if len(query) > 16<<10 {
			t.Error("query exceeded relay bound")
		}
		_ = json.NewEncoder(w).Encode(response(request))
	}))
	t.Cleanup(server.Close)
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	return client, calls
}

func parentEnvelope(repository map[string]any) map[string]any {
	return map[string]any{"status": 200, "body_encoding": "json", "body": map[string]any{
		"data": map[string]any{"repository": repository}}, "relay": map[string]any{"route_kind": "graphql_read", "cache": "miss"},
		"identity": map[string]any{"kind": "github_app"}}
}

func TestOctopoolParentsFullFieldsAndSeparatePRIdentity(t *testing.T) {
	issue, pull := parentFixture(9, "Issue"), parentFixture(7, "PullRequest")
	issue["body"] = "" // An empty full body is different from a missing body.
	pull["body"] = strings.Repeat("full body λ\x00", 60000)
	pull["fullDatabaseId"] = "9223372036854775807"
	pull["mergedAt"], pull["isDraft"], pull["merged"] = nil, true, false
	client, calls := parentTestClient(t, func(request map[string]any) map[string]any {
		variables := historyMap(historyMap(request["graphql"])["variables"])
		if variables["owner"] != "fixture" || variables["repo"] != "repo" {
			t.Error("wrong repository variables")
		}
		return parentEnvelope(map[string]any{"nameWithOwner": "fixture/repo", "n0": pull, "n1": issue})
	})
	parents, err := client.ReadParents(context.Background(), []int{7, 9}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 2 || *calls != 1 || parents[0].Kind != "pull_request" || parents[0].Number != 7 ||
		parents[0].NodeID != "node-7" || parents[0].GraphQLDatabaseID != "9223372036854775807" || parents[0].BackingIssueID != "" ||
		parents[1].BackingIssueID != "9007199254740993" || !reflect.DeepEqual(parents[0].ProviderFields, pull) ||
		!reflect.DeepEqual(parents[1].ProviderFields, issue) {
		t.Fatal("parent fields, order or exact identities changed")
	}
	if client.metadata != nil || len(client.issues) != 0 || len(client.conversations) != 0 {
		t.Fatal("parent read mutated conversation inventory")
	}
}

func TestOctopoolParentsCancelledBeforeDispatch(t *testing.T) {
	client, calls := parentTestClient(t, func(map[string]any) map[string]any { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := client.ReadParents(ctx, []int{7}, nil); err == nil || rows != nil || *calls != 0 {
		t.Fatalf("cancelled batch dispatched: rows=%d calls=%d err=%v", len(rows), *calls, err)
	}
}

func TestOctopoolParentsFiftyAliasesOneRequest(t *testing.T) {
	numbers := make([]int, 50)
	repository := map[string]any{"nameWithOwner": "fixture/repo"}
	for i := range numbers {
		numbers[i] = i + 1
		repository[fmt.Sprintf("n%d", i)] = parentFixture(i+1, "Issue")
	}
	client, calls := parentTestClient(t, func(request map[string]any) map[string]any {
		query := historyString(historyMap(request["graphql"])["query"])
		if strings.Count(query, "issueOrPullRequest(") != 50 {
			t.Error("wrong alias count")
		}
		return parentEnvelope(repository)
	})
	if rows, err := client.ReadParents(context.Background(), numbers, nil); err != nil || len(rows) != 50 || *calls != 1 {
		t.Fatalf("batch: rows=%d calls=%d err=%v", len(rows), *calls, err)
	}
}

func TestOctopoolParentsRejectsInvalidSelectionBeforeRequest(t *testing.T) {
	client, calls := parentTestClient(t, func(map[string]any) map[string]any { return nil })
	for _, numbers := range [][]int{nil, make([]int, 51), {0}, {-1}, {7, 7}} {
		if rows, err := client.ReadParents(context.Background(), numbers, nil); err == nil || rows != nil {
			t.Fatal("accepted invalid selection")
		}
	}
	if *calls != 0 {
		t.Fatal("invalid selection made a provider request")
	}
}

func TestOctopoolParentsRejectsIncompleteOrUnqualifiedBatch(t *testing.T) {
	for _, mode := range []string{"missing", "null", "wrong-number", "wrong-repository", "outer-repository", "duplicate-node", "kind",
		"missing-body", "null-body", "null-title", "invalid-id", "missing-time", "invalid-time", "state", "errors", "stale", "identity", "route", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			first, second := parentFixture(7, "Issue"), parentFixture(8, "PullRequest")
			repository := map[string]any{"nameWithOwner": "fixture/repo", "n0": first, "n1": second}
			client, calls := parentTestClient(t, func(map[string]any) map[string]any {
				envelope := parentEnvelope(repository)
				switch mode {
				case "missing":
					delete(repository, "n1")
				case "null":
					repository["n1"] = nil
				case "wrong-number":
					second["number"] = 10
				case "wrong-repository":
					second["repository"] = map[string]any{"nameWithOwner": "other/repo"}
				case "outer-repository":
					repository["nameWithOwner"] = "other/repo"
				case "duplicate-node":
					second["id"] = first["id"]
				case "kind":
					second["__typename"] = "Discussion"
				case "missing-body":
					delete(second, "body")
				case "null-body":
					second["body"] = nil
				case "null-title":
					second["title"] = nil
				case "invalid-id":
					second["fullDatabaseId"] = "090"
				case "missing-time":
					delete(second, "updatedAt")
				case "invalid-time":
					second["createdAt"] = "not a timestamp"
				case "state":
					second["pullState"] = "UNKNOWN"
				case "errors":
					historyMap(envelope["body"])["errors"] = []any{map[string]any{"message": "partial result"}}
				case "stale":
					historyMap(envelope["relay"])["cache"] = "stale"
				case "identity":
					historyMap(envelope["identity"])["kind"] = "oauth_user"
				case "route":
					historyMap(envelope["relay"])["route_kind"] = "rest_read"
				case "oversized":
					second["body"] = strings.Repeat("x", 3<<20)
				}
				return envelope
			})
			if rows, err := client.ReadParents(context.Background(), []int{7, 8}, nil); err == nil || rows != nil {
				t.Fatalf("accepted incomplete batch: rows=%d err=%v", len(rows), err)
			}
			if *calls != 1 || len(client.issues) != 0 || len(client.conversations) != 0 {
				t.Fatal("failure retried/fell back or published partial state")
			}
		})
	}
}
