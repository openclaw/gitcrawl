package remotedelivery

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
)

func graphParent() github.ParentSnapshot {
	return github.ParentSnapshot{Kind: "pull_request", Number: 7, NodeID: "PR_node", GraphQLDatabaseID: "9007199254740997", ProviderFields: map[string]any{
		"id": "PR_node", "fullDatabaseId": json.Number("9007199254740997"), "title": "full title\x00λ", "body": "full body\x00λ", "author": nil, "authorAssociation": "CONTRIBUTOR", "url": "https://github.com/example/project/pull/7", "pullState": "MERGED", "merged": true, "number": 7, "__typename": "PullRequest", "repository": map[string]any{"nameWithOwner": "example/project"}, "mergeCommit": map[string]any{"oid": strings.Repeat("a", 40)}, "mergedBy": map[string]any{"login": "merger", "__typename": "User"}, "isDraft": false, "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-03T00:00:00Z", "closedAt": "2026-01-03T00:00:00Z", "mergedAt": "2026-01-03T00:00:00Z", "headRefName": "feature", "additions": json.Number("23"),
	}}
}
func TestParentProjectionPreservesUnobservedFieldsAndNativeIdentity(t *testing.T) {
	old, err := ProjectAcquiredThread(3, 107, "example/project", map[string]any{"id": json.Number("9007199254740993"), "number": 7, "title": "old", "body": "old", "state": "open", "pull_request": map[string]any{"diff_url": "kept"}, "labels": []any{map[string]any{"name": "kept"}}, "assignees": []any{map[string]any{"login": "kept"}}, "custom": map[string]any{"large": json.Number("9007199254740999")}, "updated_at": "2026-01-02T00:00:00Z"}, nil, "2026-01-02T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"closed_at_local", "close_reason_local", "evidence_source_updated_at"} {
		v := "retained"
		old.Row[deliveryColumn("threads", field)] = remote.ChangeValue{Kind: "text", Value: &v}
	}
	evidence := "12"
	old.Row[deliveryColumn("threads", "evidence_observation_sequence")] = remote.ChangeValue{Kind: "integer", Value: &evidence}
	before, _ := json.Marshal(old)
	got, err := ProjectAcquiredParent(3, 107, "9007199254740993", graphParent(), &old, "2026-01-04T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"labels_json", "assignees_json", "closed_at_local", "close_reason_local", "first_pulled_at", "evidence_source_updated_at", "evidence_observation_sequence", "observation_sequence"} {
		if !reflect.DeepEqual(got.Row[deliveryColumn("threads", field)], old.Row[deliveryColumn("threads", field)]) {
			t.Fatalf("lost %s", field)
		}
	}
	for field, want := range map[string]string{"github_id": "9007199254740993", "title": "full title\x00λ", "body": "full body\x00λ", "state": "closed", "is_draft": "0", "merged_at_gh": "2026-01-03T00:00:00Z"} {
		v := got.Row[deliveryColumn("threads", field)]
		if v.Value == nil || *v.Value != want {
			t.Fatalf("%s=%+v", field, v)
		}
	}
	if got.Row[deliveryColumn("threads", "author_login")].Kind != "null" {
		t.Fatal("observed null author retained stale author")
	}
	raw := *got.Row[deliveryColumn("threads", "raw_json")].Value
	for _, part := range []string{"diff_url", "9007199254740999", "_gitcrawl_graphql_parent", "headRefName"} {
		if !strings.Contains(raw, part) {
			t.Fatal("lost raw metadata", part)
		}
	}
	after, _ := json.Marshal(old)
	if string(before) != string(after) {
		t.Fatal("mutated prior")
	}
	same := graphParent()
	same.ProviderFields["updatedAt"] = "2025-01-01T00:00:00Z"
	if _, err := ProjectAcquiredParent(3, 107, "9007199254740993", same, &old, "2026-01-04T00:00:00Z"); err == nil {
		t.Fatal("accepted older parent")
	}
	if _, err := ProjectAcquiredParent(3, 107, "9007199254740997", graphParent(), &old, "2026-01-04T00:00:00Z"); err == nil {
		t.Fatal("replaced backing issue ID with PR ID")
	}
}
func TestParentProjectionRejectsDifferentNodeOnFirstMerge(t *testing.T) {
	const at = "2026-01-02T00:00:00Z"
	old, err := ProjectAcquiredThread(3, 107, "example/project", map[string]any{"id": json.Number("9007199254740993"), "number": 7, "node_id": "PR_original", "pull_request": map[string]any{}, "updated_at": at}, nil, at, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ProjectAcquiredParent(3, 107, "9007199254740993", graphParent(), &old, "2026-01-04T00:00:00Z"); err == nil {
		t.Fatal("different provider node replaced existing unmerged PR")
	}
}

func TestParentProjectionNewUnknownCollectionsAndEmptyBody(t *testing.T) {
	p := graphParent()
	p.ProviderFields["body"] = ""
	got, err := ProjectAcquiredParent(3, 107, "9007199254740993", p, nil, "2026-01-04T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"labels_json", "assignees_json"} {
		if v := got.Row[deliveryColumn("threads", field)]; v.Value == nil || *v.Value != "null" {
			t.Fatal("unobserved collection claimed empty", field, v)
		}
	}
	if v := got.Row[deliveryColumn("threads", "body")]; v.Kind != "text" || v.Value == nil || *v.Value != "" {
		t.Fatal("empty body became unknown", v)
	}
	for _, field := range []string{"observation_sequence", "evidence_observation_sequence"} {
		if *got.Row[deliveryColumn("threads", field)].Value != "0" {
			t.Fatal("invented child observation")
		}
	}
}

func TestParentProjectionKeepsOnlyProvenAuthorMetadata(t *testing.T) {
	raw := map[string]any{"id": json.Number("9007199254740993"), "number": 7, "title": "old", "body": "old", "pull_request": map[string]any{}, "updated_at": "2026-01-02T00:00:00Z", "user": map[string]any{"node_id": "U_original", "id": json.Number("9007199254740999"), "login": "old-name", "type": "User", "custom": "kept"}}
	old, err := ProjectAcquiredThread(3, 107, "example/project", raw, nil, "2026-01-02T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []string{"U_original", "U_changed", ""} {
		t.Run(node, func(t *testing.T) {
			parent := graphParent()
			if node != "" {
				parent.ProviderFields["author"] = map[string]any{"id": node, "login": "new-name", "__typename": "User", "url": "https://github.com/new-name"}
			}
			got, err := ProjectAcquiredParent(3, 107, "9007199254740993", parent, &old, "2026-01-04T00:00:00Z")
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			d := json.NewDecoder(strings.NewReader(*got.Row[deliveryColumn("threads", "raw_json")].Value))
			d.UseNumber()
			if err = d.Decode(&decoded); err != nil {
				t.Fatal(err)
			}
			if node == "" {
				if decoded["user"] != nil {
					t.Fatal("null author retained old metadata")
				}
				return
			}
			user := decoded["user"].(map[string]any)
			if user["login"] != "new-name" || user["node_id"] != node {
				t.Fatal("queried author fields not updated", user)
			}
			if node == "U_original" {
				if user["id"] != json.Number("9007199254740999") || user["custom"] != "kept" {
					t.Fatal("matching author lost unqueried metadata", user)
				}
			} else {
				if _, ok := user["id"]; ok {
					t.Fatal("new author inherited old numeric ID")
				}
				if _, ok := user["custom"]; ok {
					t.Fatal("new author inherited metadata")
				}
			}
		})
	}
}

func TestMergedPullFactsSurviveIssuesRefresh(t *testing.T) {
	p := graphParent()
	const backing = "9007199254740993"
	const merged = "2026-01-03T00:00:00Z"
	fromGraph, err := ProjectAcquiredParent(3, 107, backing, p, nil, merged)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(c remote.Change) map[string]any {
		t.Helper()
		var m map[string]any
		d := json.NewDecoder(strings.NewReader(*c.Row[deliveryColumn("threads", "raw_json")].Value))
		d.UseNumber()
		if err := d.Decode(&m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	graph := decode(fromGraph)
	check := func(m map[string]any) {
		t.Helper()
		actor, _ := m["merged_by"].(map[string]any)
		if m["merge_commit_sha"] != strings.Repeat("a", 40) || m["merged_at"] != merged || actor["login"] != "merger" || m["id"] != json.Number(backing) {
			t.Fatal("canonical merge or backing ID missing")
		}
	}
	check(graph)
	// Old producers retained only the namespaced full observation.
	delete(graph, "merge_commit_sha")
	delete(graph, "merged_at")
	delete(graph, "merged_by")
	b, _ := json.Marshal(graph)
	old := string(b)
	fromGraph.Row[deliveryColumn("threads", "raw_json")].Value = &old
	issue := map[string]any{"number": 7, "id": json.Number(backing), "node_id": p.NodeID, "state": "closed", "updated_at": "2026-01-04T00:00:00Z", "title": "edited", "body": "latest full body", "pull_request": map[string]any{"merged_at": merged}}
	refreshed, err := ProjectAcquiredThread(3, 107, "example/project", issue, &fromGraph, "2026-01-05T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	m := decode(refreshed)
	check(m)
	if !reflect.DeepEqual(m["_gitcrawl_graphql_parent"], graph["_gitcrawl_graphql_parent"]) || m["body"] != "latest full body" || m["title"] != "edited" {
		t.Fatal("lost provider fields")
	}
	again, err := ProjectAcquiredThread(3, 107, "example/project", issue, &refreshed, "2026-01-06T00:00:00Z", nil)
	if err != nil || !reflect.DeepEqual(decode(again), m) {
		t.Fatal("second Issues refresh changed merge evidence", err)
	}
	other := maps.Clone(issue)
	other["id"] = json.Number("9007199254740994")
	if _, err = ProjectAcquiredThread(3, 107, "example/project", other, &refreshed, merged, nil); err == nil {
		t.Fatal("accepted mismatched backing Issue ID")
	}
	other = maps.Clone(issue)
	other["pull_request"] = map[string]any{"merged_at": "2026-01-04T00:00:00Z"}
	if _, err = ProjectAcquiredThread(3, 107, "example/project", other, &refreshed, merged, nil); !errors.Is(err, ErrFullPullRequired) {
		t.Fatal("reused merge at a different time", err)
	}
	other = maps.Clone(issue)
	other["pull_request"] = map[string]any{"merged_at": nil}
	if _, err := ProjectAcquiredThread(3, 107, "example/project", other, &refreshed, merged, nil); !errors.Is(err, ErrFullPullRequired) {
		t.Fatal("sparse Issues response erased known merge", err)
	}
	full := map[string]any{"merged_at": merged, "merge_commit_sha": strings.Repeat("a", 40), "merged_by": nil}
	noActor := maps.Clone(full)
	delete(noActor, "merged_by")
	withPriorActor, err := ProjectAcquiredThread(3, 107, "example/project", other, &refreshed, merged, noActor)
	if err != nil || decode(withPriorActor)["merged_by"].(map[string]any)["login"] != "merger" {
		t.Fatal("unobserved actor erased previous observation", err)
	}
	sparse, err := ProjectAcquiredThread(3, 107, "example/project", other, &refreshed, merged, full)
	if err != nil {
		t.Fatal(err)
	}
	sparseRaw := decode(sparse)
	if sparseRaw["merge_commit_sha"] != strings.Repeat("a", 40) || sparseRaw["pull_request"].(map[string]any)["merged_at"] != nil || *sparse.Row[deliveryColumn("threads", "merged_at_gh")].Value != merged {
		t.Fatal("sparse Issues/full Pull evidence was lost")
	}
	// The fresh full Pull observed an unavailable merger. An older GraphQL
	// actor must not reappear on the next Issues refresh.
	afterSparse, err := ProjectAcquiredThread(3, 107, "example/project", issue, &sparse, merged, nil)
	if err != nil || decode(afterSparse)["merged_by"] != nil {
		t.Fatal("older graph actor superseded the current canonical actor", err)
	}
	p.ProviderFields["mergeCommit"] = nil
	if _, err = ProjectAcquiredParent(3, 107, backing, p, &fromGraph, merged); err == nil {
		t.Fatal("older namespace-only known OID erased")
	}
	if _, err = ProjectAcquiredParent(3, 107, backing, p, &refreshed, merged); err == nil {
		t.Fatal("known immutable merge OID erased")
	}

}
