package remotedelivery

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/remote"
)

func TestAcquisitionProjectionPreservesLocalAndNativeFields(t *testing.T) {
	raw := map[string]any{"id": json.Number("9007199254740993"), "number": json.Number("7"), "title": "title", "body": "body\x00λ", "state": "closed", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-03T00:00:00Z", "pull_request": map[string]any{}, "labels": []any{}, "assignees": []any{}}
	original, err := ProjectAcquiredThread(3, 107, "example/project", raw, nil, "2026-01-04T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	set := func(name, value string) {
		i := deliveryColumn("threads", name)
		original.Row[i].Value = &value
		original.Row[i].Kind = "text"
	}
	set("closed_at_local", "2026-01-02T00:00:00Z")
	set("close_reason_local", "retained decision")
	set("first_pulled_at", "2025-12-01T00:00:00Z")
	draft := "1"
	original.Row[deliveryColumn("threads", "is_draft")] = remote.ChangeValue{Kind: "integer", Value: &draft}
	updated, err := ProjectAcquiredThread(3, 107, "example/project", raw, &original, "2026-01-05T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]string{"closed_at_local": "2026-01-02T00:00:00Z", "close_reason_local": "retained decision", "first_pulled_at": "2025-12-01T00:00:00Z", "last_pulled_at": "2026-01-05T00:00:00Z", "github_id": "9007199254740993", "body": "body\x00λ", "is_draft": "1"} {
		got := updated.Row[deliveryColumn("threads", field)]
		if got.Value == nil || *got.Value != want {
			t.Fatalf("%s changed", field)
		}
	}
	result := remote.QueryResult{Columns: append(append([]string{}, strings.Fields(privateLayouts[1].columns)...), "sequence", "ordinal")}
	row := make([]any, 0, len(updated.Row)+2)
	for _, value := range updated.Row {
		if value.Value == nil {
			row = append(row, nil)
		} else {
			row = append(row, *value.Value)
		}
	}
	row = append(row, "10", "0")
	result.Rows = [][]any{row}
	got, err := AcquisitionParent(result, 107)
	if err != nil || got == nil || *got.Row[deliveryColumn("threads", "body")].Value != "body\x00λ" {
		t.Fatal("full parent decode", err)
	}
	raw["updated_at"] = "2025-01-01T00:00:00Z"
	if _, err = ProjectAcquiredThread(3, 107, "example/project", raw, &original, "2026-01-05T00:00:00Z", nil); err == nil {
		t.Fatal("older provider metadata accepted")
	}
}

func TestAcquisitionProjectionMatchesNativeEmptyText(t *testing.T) {
	parent, err := ProjectAcquiredThread(3, 107, "example/project", map[string]any{"id": json.Number("7"), "number": json.Number("7")}, nil, "2026-01-01T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"github_id", "kind", "state", "title", "html_url", "labels_json", "assignees_json", "raw_json", "content_hash", "evidence_source_updated_at", "updated_at"} {
		v := parent.Row[deliveryColumn("threads", name)]
		if v.Kind != "text" || v.Value == nil {
			t.Fatalf("native required text %s became NULL", name)
		}
	}
	for _, name := range []string{"body", "author_login", "author_type", "created_at_gh", "updated_at_gh"} {
		if parent.Row[deliveryColumn("threads", name)].Kind != "null" {
			t.Fatalf("native optional %s did not retain NULL", name)
		}
	}
	comment, err := ProjectAcquiredComment(107, 9, "issue_comment", map[string]any{"id": json.Number("9")})
	if err != nil {
		t.Fatal(err)
	}
	body := comment.Row[deliveryColumn("comments", "body")]
	if body.Kind != "text" || body.Value == nil || *body.Value != "" {
		t.Fatal("native empty comment body became NULL")
	}
}
