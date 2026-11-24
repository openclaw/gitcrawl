package github

import (
	"maps"
	"strings"
	"testing"
)

func TestPullMergeRequiresMatchingProviderIdentityAndObservedFields(t *testing.T) {
	const at = "2026-01-02T00:00:00Z"
	issue := map[string]any{"number": 7, "node_id": "PR_7", "pull_request": map[string]any{"merged_at": at}}
	pull := map[string]any{"number": 7, "node_id": "PR_7", "merged": true, "merged_at": at, "merge_commit_sha": strings.Repeat("b", 40), "merged_by": nil, "base": map[string]any{"repo": map[string]any{"id": 42, "full_name": "example/repo"}}}
	verify := func(p map[string]any) (map[string]any, error) {
		return VerifiedPullMerge("example/repo", "42", 7, issue, p)
	}
	got, err := verify(pull)
	if err != nil || len(got) != 3 || got["merge_commit_sha"] != strings.Repeat("b", 40) || got["merged_by"] != nil {
		t.Fatal("explicitly unavailable merge facts", got, err)
	}
	unknown := maps.Clone(pull)
	unknown["merge_commit_sha"] = nil
	if _, err := verify(unknown); err == nil {
		t.Fatal("unknown merge OID became a proven absence")
	}
	missing := maps.Clone(pull)
	delete(missing, "merge_commit_sha")
	if _, err := verify(missing); err == nil {
		t.Fatal("missing OID became an observed fact")
	}
	missing = maps.Clone(pull)
	delete(missing, "merged_by")
	partial, err := verify(missing)
	if err != nil {
		t.Fatal("missing merger blocked an observed commit", err)
	}
	if _, present := partial["merged_by"]; present {
		t.Fatal("invented merger absence")
	}

	for key, value := range map[string]any{"number": 8, "node_id": "PR_other", "merged": false, "merged_at": "2026-01-03T00:00:00Z", "merge_commit_sha": "abc123", "merged_by": map[string]any{}, "base": map[string]any{"repo": map[string]any{"id": 43, "full_name": "example/repo"}}} {
		p := maps.Clone(pull)
		p[key] = value
		if _, err := verify(p); err == nil {
			t.Fatal("accepted invalid full Pull", key)
		}
	}
	graph := map[string]any{"pullState": "MERGED", "merged": true, "mergedAt": at, "mergeCommit": map[string]any{"oid": strings.Repeat("a", 40)}}
	g, err := ParentMergeFields(graph)
	if err != nil || len(g) != 2 {
		t.Fatal("unobserved actor became an asserted null", g, err)
	}
	mixed := maps.Clone(graph)
	mixed["merge_commit_sha"] = strings.Repeat("b", 40)
	if _, err = PullMergeFields(mixed); err == nil {
		t.Fatal("conflicting GraphQL/REST merge aliases")
	}
	graph["mergeCommit"] = nil
	g, err = ParentMergeFields(graph)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := g["merge_commit_sha"]; present {
		t.Fatal("missing OID became an authoritative null")
	}
	graph["mergeCommit"] = map[string]any{"oid": "abcdef0"}
	if _, err = ParentMergeFields(graph); err == nil {
		t.Fatal("invalid GraphQL OID")
	}
	graph["pullState"] = "CLOSED"
	if _, err = ParentMergeFields(graph); err == nil {
		t.Fatal("contradictory GraphQL merge state")
	}
}
