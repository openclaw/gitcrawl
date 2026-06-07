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
		Body:       "Large downloads hang near the end.",
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
	if doc.DedupeText != "download stalls large downloads hang near the end. bug downloads same failure here" {
		t.Fatalf("dedupe text: %q", doc.DedupeText)
	}
}

func TestBuildToleratesBadLabelJSON(t *testing.T) {
	doc := Build(store.Thread{Title: "A", LabelsJSON: `nope`})
	if doc.DedupeText != "a" {
		t.Fatalf("dedupe text: %q", doc.DedupeText)
	}
}

func TestBuildSupportsLegacyStringLabels(t *testing.T) {
	doc := Build(store.Thread{Title: "A", LabelsJSON: `["bug","help wanted"]`})
	if doc.DedupeText != "a bug help wanted" {
		t.Fatalf("dedupe text: %q", doc.DedupeText)
	}
}

func TestBuildIncludesPullRequestPathsAndCommitSubjects(t *testing.T) {
	doc := BuildWithContext(
		store.Thread{ID: 8, Title: "Refresh cache", LabelsJSON: "[]"},
		nil,
		[]store.PullRequestFile{
			{Path: "internal/cache/store.go"},
			{Path: "docs/cache.md", PreviousPath: "docs/old-cache.md"},
		},
		[]store.PullRequestCommit{
			{Message: "fix: refresh manifest cache\n\nLong body"},
			{Message: "test: cover stale entries"},
		},
	)
	for _, want := range []string{"Changed files:", "internal/cache/store.go", "docs/old-cache.md", "Commits:", "fix: refresh manifest cache"} {
		if !strings.Contains(doc.RawText, want) {
			t.Fatalf("raw text missing %q: %s", want, doc.RawText)
		}
	}
	if strings.Contains(doc.RawText, "Long body") {
		t.Fatalf("commit body leaked into document: %s", doc.RawText)
	}
	if !strings.Contains(doc.DedupeText, "internal/cache/store.go") || !strings.Contains(doc.DedupeText, "test: cover stale entries") {
		t.Fatalf("dedupe text missing pull context: %s", doc.DedupeText)
	}
}
