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

func lifecycleClient(t *testing.T, response func() string) (*OctopoolConversations, *int) {
	t.Helper()
	calls := new(int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		var request map[string]any
		if json.NewDecoder(r.Body).Decode(&request) != nil || r.URL.Path != "/v1/github/request" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("wrong relay request")
		}
		graphql := historyMap(request["graphql"])
		query := historyString(graphql["query"])
		if request["method"] != "POST" || request["path"] != "/graphql" || request["pool"] != "fixture" || historyMap(request["headers"])["cache-control"] != "max-age=0" ||
			!strings.Contains(query, "CLOSED_EVENT,REOPENED_EVENT,MERGED_EVENT") || !strings.Contains(query, "closer {") || strings.Contains(query, "comments(") {
			t.Error("wrong lifecycle query or acquisition boundary")
		}
		fmt.Fprint(w, response())
	}))
	t.Cleanup(server.Close)
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	return client, calls
}

func lifecycleEnvelope(events, page string) string {
	return `{"status":200,"body_encoding":"json","relay":{"route_kind":"graphql_read","cache":"miss"},"identity":{"kind":"github_app"},"body":{"data":{"node":{"id":"PR-node","__typename":"PullRequest","number":7,"repository":{"nameWithOwner":"fixture/repo"},"timelineItems":{"nodes":` + events + `,"pageInfo":` + page + `}}}}}`
}

func lifecycleQuery() LifecycleQuery {
	return LifecycleQuery{NodeID: "PR-node", Kind: "pull_request", Number: 7, Since: time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)}
}

func TestOctopoolLifecyclePreservesRawEventsAndCursor(t *testing.T) {
	raw := `{"__typename": "ClosedEvent", "id":"close-1", "createdAt":"2026-09-08T01:00:00Z", "actor":null, "futureField":"λ\u0000"}`
	client, calls := lifecycleClient(t, func() string {
		return lifecycleEnvelope("["+raw+"]", `{"hasNextPage":true,"endCursor":"next-page"}`)
	})
	page, err := client.ReadLifecycle(context.Background(), lifecycleQuery(), nil)
	if err != nil || page.Terminal || page.NextCursor != "next-page" || len(page.Events) != 1 || string(page.Events[0]) != raw || *calls != 1 {
		t.Fatalf("raw lifecycle page changed: %#v %v", page, err)
	}
	if client.metadata != nil || len(client.issues) != 0 || len(client.conversations) != 0 {
		t.Fatal("lifecycle page mutated local conversation inventory")
	}
}

func TestOctopoolLifecycleRejectsIncompletePage(t *testing.T) {
	base := lifecycleEnvelope(`[{"__typename":"MergedEvent","id":"merge-1","createdAt":"2026-09-08T02:00:00Z","actor":null,"commit":null}]`, `{"hasNextPage":true,"endCursor":"next-page"}`)
	for _, mode := range []string{"parent", "repository", "kind", "number", "cursor", "empty", "event-kind", "event-id", "event-time", "old-event", "duplicate", "errors", "stale"} {
		t.Run(mode, func(t *testing.T) {
			body := base
			switch mode {
			case "parent":
				body = strings.Replace(body, `"id":"PR-node"`, `"id":"other"`, 1)
			case "repository":
				body = strings.Replace(body, "fixture/repo", "other/repo", 1)
			case "kind":
				body = strings.Replace(body, "PullRequest", "Issue", 1)
			case "number":
				body = strings.Replace(body, `"number":7`, `"number":8`, 1)
			case "cursor":
				body = strings.Replace(body, `"next-page"`, `""`, 1)
			case "empty":
				body = lifecycleEnvelope("[]", `{"hasNextPage":true,"endCursor":"next-page"}`)
			case "event-kind":
				body = strings.Replace(body, "MergedEvent", "IssueComment", 1)
			case "event-id":
				body = strings.Replace(body, "merge-1", "", 1)
			case "event-time":
				body = strings.Replace(body, "2026-09-08T02:00:00Z", "invalid", 1)
			case "old-event":
				body = strings.Replace(body, "2026-09-08T02:00:00Z", "2026-09-07T02:00:00Z", 1)
			case "duplicate":
				body = lifecycleEnvelope(`[{"__typename":"ClosedEvent","id":"same","createdAt":"2026-09-08T02:00:00Z"},{"__typename":"ClosedEvent","id":"same","createdAt":"2026-09-08T02:00:00Z"}]`, `{"hasNextPage":false,"endCursor":"last"}`)
			case "errors":
				body = strings.Replace(body, `"data":`, `"errors":[{"message":"partial"}],"data":`, 1)
			case "stale":
				body = strings.Replace(body, `"cache":"miss"`, `"cache":"stale"`, 1)
			}
			client, calls := lifecycleClient(t, func() string { return body })
			if _, err := client.ReadLifecycle(context.Background(), lifecycleQuery(), nil); err == nil || *calls != 1 {
				t.Fatalf("incomplete page accepted or retried: %v calls=%d", err, *calls)
			}
		})
	}
}

func TestOctopoolLifecycleEmptyTerminalAndInvalidSelection(t *testing.T) {
	client, calls := lifecycleClient(t, func() string { return lifecycleEnvelope("[]", `{"hasNextPage":false,"endCursor":null}`) })
	page, err := client.ReadLifecycle(context.Background(), lifecycleQuery(), nil)
	if err != nil || !page.Terminal || len(page.Events) != 0 {
		t.Fatal(page, err)
	}
	q := lifecycleQuery()
	q.Number = 0
	if _, err = client.ReadLifecycle(context.Background(), q, nil); err == nil || *calls != 1 {
		t.Fatal("invalid selection dispatched")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = client.ReadLifecycle(ctx, lifecycleQuery(), nil); err == nil || *calls != 1 {
		t.Fatal("cancelled page dispatched")
	}
}

func TestOctopoolLifecycleTerminalDropsProviderEndCursor(t *testing.T) {
	raw := `{"__typename":"MergedEvent","id":"merge-1","createdAt":"2026-09-08T02:00:00Z","actor":null,"commit":null}`
	client, _ := lifecycleClient(t, func() string {
		return lifecycleEnvelope("["+raw+"]", `{"hasNextPage":false,"endCursor":"last-edge"}`)
	})
	page, err := client.ReadLifecycle(context.Background(), lifecycleQuery(), nil)
	if err != nil || !page.Terminal || page.NextCursor != "" || len(page.Events) != 1 || string(page.Events[0]) != raw {
		t.Fatalf("terminal page retained a continuation cursor: %#v %v", page, err)
	}
}
