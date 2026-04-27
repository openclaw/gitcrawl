package documents

import (
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestBuildIncludesTitleBodyAndLabels(t *testing.T) {
	doc := BuildWithComments(store.Thread{
		ID:         12,
		Title:      "Download stalls",
		Body:       "Large artifacts hang near the end.",
		LabelsJSON: `[{"name":"bug"},{"name":"downloads"}]`,
		UpdatedAt:  "2026-04-26T00:00:00Z",
	}, []store.Comment{{AuthorLogin: "vincentkoc", Body: "same failure here"}})
	if doc.ThreadID != 12 {
		t.Fatalf("thread id: got %d want 12", doc.ThreadID)
	}
	if !strings.Contains(doc.RawText, "Labels: bug, downloads") {
		t.Fatalf("raw text missing labels: %q", doc.RawText)
	}
	if !strings.Contains(doc.RawText, "vincentkoc: same failure here") {
		t.Fatalf("raw text missing comment: %q", doc.RawText)
	}
	if doc.DedupeText != "download stalls large artifacts hang near the end. bug downloads same failure here" {
		t.Fatalf("dedupe text: %q", doc.DedupeText)
	}
}

func TestBuildToleratesBadLabelJSON(t *testing.T) {
	doc := Build(store.Thread{Title: "A", LabelsJSON: `nope`})
	if doc.DedupeText != "a" {
		t.Fatalf("dedupe text: %q", doc.DedupeText)
	}
}
