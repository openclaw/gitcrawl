package syncer

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReusableMappingsPreserveProviderFields(t *testing.T) {
	body := strings.Repeat("full λ\x00 text", 1000)
	row := map[string]any{"id": json.Number("9007199254740993"), "number": json.Number("7"), "title": "full title", "body": body, "state": "closed", "pull_request": map[string]any{"merged_at": "2026-01-02T03:04:05Z"}, "user": map[string]any{"login": "writer", "type": "User"}, "author_association": "MEMBER", "draft": true, "labels": []any{map[string]any{"name": "bug"}}, "assignees": []any{map[string]any{"login": "reviewer"}}, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z", "closed_at": "2026-01-02T01:00:00Z", "html_url": "https://github.com/example/project/pull/7"}
	got := MapIssueToThread(10, row, "2026-01-03T00:00:00Z")
	if got.GitHubID != "9007199254740993" || got.Kind != "pull_request" || got.Body != body || got.Title != "full title" || got.RepoID != 10 || got.Number != 7 || !got.IsDraft || got.AuthorAssociation != "MEMBER" || got.MergedAtGitHub != "2026-01-02T03:04:05Z" {
		t.Fatalf("thread mapping lost fields: %+v", got)
	}
	var raw map[string]any
	if json.Unmarshal([]byte(got.RawJSON), &raw) != nil || raw["body"] != body || !strings.Contains(got.LabelsJSON, "bug") || !strings.Contains(got.AssigneesJSON, "reviewer") || got.ContentHash != contentHash("full title", body, got.LabelsJSON) {
		t.Fatal("thread source bytes/hash lost")
	}
}

func TestReusableCommentMappingUsesNativePublicationOwner(t *testing.T) {
	for _, test := range []struct{ kind, state, submitted, published string }{
		{"pull_review", "APPROVED", "2026-01-02T03:04:05Z", "2026-01-02T03:04:05Z"},
		{"pull_review", "PENDING", "2026-01-02T03:04:05Z", ""},
		{"pull_review_comment", "", "", ""},
		{"issue_comment", "", "", "2026-01-01T00:00:00Z"},
	} {
		t.Run(test.kind+test.state, func(t *testing.T) {
			row := map[string]any{"id": json.Number("9007199254740993"), "body": "full\x00λ", "user": map[string]any{"login": "actor[bot]", "type": "Bot"}, "state": test.state, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z", "submitted_at": test.submitted, "deleted_at": "2026-01-03T00:00:00Z", "deletion_reason": "provider"}
			got := MapComment(17, test.kind, row)
			if got.ThreadID != 17 || got.GitHubID != "9007199254740993" || got.Body != "full\x00λ" || !got.IsBot || got.ReviewState != test.state || got.Published != test.published || got.DeletedAt != "2026-01-03T00:00:00Z" || got.DeletionReason != "provider" {
				t.Fatalf("comment mapping lost fields: %+v", got)
			}
			if test.kind == "pull_review" && test.state == "APPROVED" && got.Submitted != test.submitted {
				t.Fatal("review submission lost")
			}
		})
	}
}
