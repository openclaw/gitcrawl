package cli

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestAnalyticsBaselineAcceptsCompleteEmptyLanes(t *testing.T) {
	for _, counts := range [][2]int{{0, 0}, {0, 5}, {5, 0}, {5, 6}} {
		data := fmt.Sprintf(`{"phase":"complete","discovery":[{"kind":"issues","done":1,"total":%d,"updated_at":"2026-01-01T01:00:00+01:00"},{"kind":"pullRequests","done":1,"total":%d,"updated_at":"2026-01-01T00:00:01Z"}]}`, counts[0], counts[1])
		through, issues, prs, ok := analyticsBaseline([]byte(data))
		if !ok || issues != counts[0] || prs != counts[1] || through != "2026-01-01T00:00:00Z" {
			t.Fatalf("baseline counts=%v: %s %d %d %t", counts, through, issues, prs, ok)
		}
	}
}
func TestAnalyticsBaselineRejectsIncompleteEvidence(t *testing.T) {
	for _, broken := range []map[string]any{
		{"kind": "issues", "done": 1, "total": 0, "updated_at": "2026-01-01T00:00:00Z"}, // duplicate lane
		{"kind": "pullRequests", "done": 1, "updated_at": "2026-01-01T00:00:00Z"},
		{"kind": "pullRequests", "done": 0, "total": 0, "updated_at": "2026-01-01T00:00:00Z"},
		{"kind": "pullRequests", "done": 1, "total": -1, "updated_at": "2026-01-01T00:00:00Z"},
		{"kind": "pullRequests", "done": 1, "total": 0, "updated_at": "not-a-date"},
	} {
		data, _ := json.Marshal(map[string]any{"phase": "complete", "discovery": []any{map[string]any{"kind": "issues", "done": 1, "total": 0, "updated_at": "2026-01-01T00:00:00Z"}, broken}})
		if _, _, _, ok := analyticsBaseline(data); ok {
			t.Fatalf("accepted incomplete evidence: %s", data)
		}
	}
}
