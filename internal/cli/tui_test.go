package cli

import (
	"context"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestTUILayoutStacksNarrowTerminals(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 80
	model.height = 24

	layout := model.layout()
	if !layout.stacked {
		t.Fatal("expected narrow terminal to use stacked layout")
	}
	if layout.members.x != 0 || layout.members.y <= layout.clusters.y {
		t.Fatalf("expected members pane below clusters, got clusters=%+v members=%+v", layout.clusters, layout.members)
	}

	view := model.View()
	for _, label := range []string{"[*] Clusters", "[ ] Members", "[ ] Detail"} {
		if !strings.Contains(view, label) {
			t.Fatalf("expected view to contain %q", label)
		}
	}
}

func TestTUIMouseSelectsClusterRows(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	if model.selected != 0 {
		t.Fatalf("first row click selected %d, want 0", model.selected)
	}

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	if model.selected != 1 {
		t.Fatalf("second row click selected %d, want 1", model.selected)
	}
}

func TestTUIMouseHeaderSortsClusterRows(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 2,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})

	if model.payload.Sort != "size" {
		t.Fatalf("header click sort = %q, want size", model.payload.Sort)
	}
	if model.payload.Clusters[0].ID != 2 {
		t.Fatalf("size sort first cluster id = %d, want 2", model.payload.Clusters[0].ID)
	}
}

func TestTUIWideLayoutToggle(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 160
	model.height = 40

	columns := model.layout()
	model.toggleWideLayout()
	rightStack := model.layout()

	if columns.detail.y != columns.members.y {
		t.Fatalf("columns layout should align detail and members: %+v", columns)
	}
	if rightStack.detail.y <= rightStack.members.y {
		t.Fatalf("right-stack detail should sit below members: %+v", rightStack)
	}
	if rightStack.clusters.w <= columns.clusters.w {
		t.Fatalf("right-stack should give clusters more width, columns=%+v rightStack=%+v", columns.clusters, rightStack.clusters)
	}
}

func TestTUIMouseIgnoresRightClick(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	layout := model.layout()
	model.selected = 1

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonRight,
	})
	if model.selected != 1 {
		t.Fatalf("right click changed selected cluster to %d", model.selected)
	}
}

func TestTUIFiltersClusters(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	model.search = "second"
	model.applyClusterFilters()
	if len(model.payload.Clusters) != 1 {
		t.Fatalf("filtered clusters: got %d want 1", len(model.payload.Clusters))
	}
	if model.payload.Clusters[0].ID != 2 {
		t.Fatalf("filtered cluster id: got %d want 2", model.payload.Clusters[0].ID)
	}

	model.search = ""
	model.minSize = 4
	model.applyClusterFilters()
	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 2 {
		t.Fatalf("min-size filter mismatch: %+v", model.payload.Clusters)
	}
}

func TestTUIRightClickOpensActionMenu(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonRight,
	})

	if !model.menuOpen {
		t.Fatal("expected right click to open action menu")
	}
	if model.selected != 1 {
		t.Fatalf("right click selected %d, want 1", model.selected)
	}
	if len(model.menuItems) < 3 {
		t.Fatalf("expected action menu items, got %+v", model.menuItems)
	}
}

func TestTUIActionMenuIncludesBodyLinkActions(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.hasDetail = true
	model.memberIndex = 0
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{
			Thread: store.Thread{
				Number:  42,
				Kind:    "issue",
				State:   "open",
				Title:   "Thread with links",
				HTMLURL: "https://github.com/openclaw/openclaw/issues/42",
			},
			BodySnippet: "See [the repro](https://example.com/repro).",
		},
	}}

	model.openActionMenu()

	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joined := strings.Join(labels, "\n")
	for _, want := range []string{"Copy title", "Copy cluster summary", "Open first body link", "Copy first body link"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("menu labels missing %q in:\n%s", want, joined)
		}
	}
}

func TestTUIMemberRowsGroupAndSkipHeaders(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.detail = store.ClusterDetail{Members: []store.ClusterMemberDetail{
		{Thread: store.Thread{ID: 1, Number: 10, Kind: "pull_request", State: "open", Title: "PR"}},
		{Thread: store.Thread{ID: 2, Number: 11, Kind: "issue", State: "open", Title: "Issue"}},
	}}
	model.memberSort = memberSortKind
	model.sortMembers()

	if len(model.memberRows) != 4 {
		t.Fatalf("member rows = %d, want grouped headers plus two members", len(model.memberRows))
	}
	if model.memberRows[0].selectable || model.memberRows[0].label != "ISSUES (1)" {
		t.Fatalf("first row should be issue header, got %+v", model.memberRows[0])
	}
	if model.memberIndex != 1 {
		t.Fatalf("member index = %d, want first selectable row 1", model.memberIndex)
	}
	model.focus = focusMembers
	model.memberIndex = 0
	model.move(1)
	if model.memberIndex != 1 {
		t.Fatalf("move from header selected %d, want 1", model.memberIndex)
	}
}

func TestTUIMemberMovementHonorsStepSize(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.detail = store.ClusterDetail{Members: []store.ClusterMemberDetail{
		{Thread: store.Thread{ID: 1, Number: 10, Kind: "issue", State: "open", Title: "First"}},
		{Thread: store.Thread{ID: 2, Number: 11, Kind: "issue", State: "open", Title: "Second"}},
		{Thread: store.Thread{ID: 3, Number: 12, Kind: "issue", State: "open", Title: "Third"}},
		{Thread: store.Thread{ID: 4, Number: 13, Kind: "pull_request", State: "open", Title: "Fourth"}},
	}}
	model.memberSort = memberSortKind
	model.sortMembers()
	model.focus = focusMembers

	model.move(3)
	if got := model.memberRows[model.memberIndex].thread().Number; got != 13 {
		t.Fatalf("move(3) selected #%d, want #13", got)
	}
	model.move(-2)
	if got := model.memberRows[model.memberIndex].thread().Number; got != 11 {
		t.Fatalf("move(-2) selected #%d, want #11", got)
	}
}

func TestTUICompactDetailLimitsBody(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.hasDetail = true
	model.compactDetail = true
	model.memberIndex = 0
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{
			Thread:      store.Thread{Number: 42, Kind: "issue", State: "open", Title: "Long body", HTMLURL: "https://github.com/openclaw/openclaw/issues/42"},
			BodySnippet: strings.Repeat("line\n", 30),
		},
	}}

	lines := strings.Join(model.detailLines(80), "\n")
	if !strings.Contains(lines, "Press d for full detail") {
		t.Fatalf("compact detail did not include truncation hint:\n%s", lines)
	}
}

func TestTUIRefreshWithoutStoreReportsUnavailable(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	model.refreshFromStore()

	if model.status != "Refresh unavailable for this view" {
		t.Fatalf("refresh status = %q", model.status)
	}
}

func TestTUIEmptyStateSuggestsRecoveryActions(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   nil,
	})

	detail := strings.Join(model.detailLines(80), "\n")
	if !strings.Contains(detail, "Try f to lower the minimum size") {
		t.Fatalf("detail empty state missing recovery actions:\n%s", detail)
	}
	rows := model.clusterRows()
	if len(rows) != 1 || !strings.Contains(rows[0][2], "No clusters visible") {
		t.Fatalf("cluster empty row mismatch: %+v", rows)
	}
}

func sampleTUIClusters() []store.ClusterSummary {
	return []store.ClusterSummary{
		{
			ID:                   1,
			StableSlug:           "alpha-bravo-charlie",
			Status:               "active",
			RepresentativeKind:   "issue",
			RepresentativeTitle:  "First issue",
			RepresentativeNumber: 11,
			MemberCount:          3,
			UpdatedAt:            "2026-04-27T10:00:00Z",
		},
		{
			ID:                   2,
			StableSlug:           "delta-echo-foxtrot",
			Status:               "active",
			RepresentativeKind:   "pull_request",
			RepresentativeTitle:  "Second PR",
			RepresentativeNumber: 12,
			MemberCount:          5,
			UpdatedAt:            "2026-04-27T11:00:00Z",
		},
	}
}
