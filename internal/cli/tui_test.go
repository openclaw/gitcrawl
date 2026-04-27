package cli

import (
	"context"
	"fmt"
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

func TestTUIViewShowsRowsInDefaultTerminal(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 80
	model.height = 24

	view := model.View()
	if !strings.Contains(view, "alpha-bravo") {
		t.Fatalf("expected default terminal view to render cluster rows:\n%s", view)
	}
	if model.clusterTable.Height() < 1 {
		t.Fatalf("cluster table viewport height = %d, want at least 1", model.clusterTable.Height())
	}
}

func TestTUIViewKeepsEssentialFooterHintsNarrow(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 80
	model.height = 24

	view := model.View()
	for _, want := range []string{"right-click menu", "? help", "q quit"} {
		if !strings.Contains(view, want) {
			t.Fatalf("narrow footer missing %q:\n%s", want, view)
		}
	}
}

func TestTUIInAppHelpMentionsMenuMouse(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	help := strings.Join(model.helpLines(80), "\n")
	for _, want := range []string{"left click menu row", "wheel in menu", "open link picker"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %q:\n%s", want, help)
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

func TestTUIMouseSelectsVisibleClusterWindow(t *testing.T) {
	clusters := make([]store.ClusterSummary, 0, 30)
	for i := 0; i < 30; i++ {
		clusters = append(clusters, store.ClusterSummary{
			ID:                   int64(i + 1),
			StableSlug:           fmt.Sprintf("cluster-%02d", i+1),
			Status:               "active",
			RepresentativeKind:   "issue",
			RepresentativeTitle:  fmt.Sprintf("Issue %02d", i+1),
			RepresentativeNumber: 100 + i,
			MemberCount:          3,
			UpdatedAt:            fmt.Sprintf("2026-04-27T%02d:00:00Z", i%24),
		})
	}
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   clusters,
	})
	model.width = 140
	model.height = 24
	model.selected = 20
	model.syncComponents()
	start := model.clusterVisibleStart()
	if start == 0 {
		t.Fatalf("expected selected row to force a scrolled cluster window")
	}
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})

	if model.selected != start {
		t.Fatalf("visible first row click selected %d, want %d", model.selected, start)
	}
}

func TestTUIMouseSelectsVisibleMemberWindow(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 24
	model.focus = focusMembers
	model.memberRows = make([]memberRow, 0, 30)
	for i := 0; i < 30; i++ {
		model.memberRows = append(model.memberRows, memberRow{
			selectable: true,
			member: store.ClusterMemberDetail{
				Thread: store.Thread{
					ID:              int64(i + 1),
					Number:          200 + i,
					Kind:            "issue",
					State:           "open",
					Title:           fmt.Sprintf("Member %02d", i+1),
					UpdatedAtGitHub: fmt.Sprintf("2026-04-27T%02d:00:00Z", i%24),
				},
			},
		})
	}
	model.memberIndex = 20
	model.syncComponents()
	start := model.memberVisibleStart()
	if start == 0 {
		t.Fatalf("expected selected row to force a scrolled member window")
	}
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.members.x + 2,
		Y:      layout.members.y + 3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})

	if model.memberIndex != start {
		t.Fatalf("visible first member row click selected %d, want %d", model.memberIndex, start)
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

func TestTUIClusterRowsShowClusterIDs(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	rows := model.clusterRows()
	if len(rows) == 0 || rows[0][0] != "C2" {
		t.Fatalf("cluster id cell = %q, want C2 in first row: %+v", rows[0][0], rows)
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

func TestTUIFiltersUseLoadedWorkingSet(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    5,
		Limit:      20,
		Clusters:   sampleTUIClusters(),
	})

	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 2 {
		t.Fatalf("default min-size view mismatch: %+v", model.payload.Clusters)
	}
	model.minSize = 1
	model.applyClusterFilters()
	if len(model.payload.Clusters) != 2 {
		t.Fatalf("lowered min-size should use loaded working set, got %+v", model.payload.Clusters)
	}
}

func TestMergeClusterSummariesKeepsPrimaryView(t *testing.T) {
	primary := []store.ClusterSummary{{ID: 20}, {ID: 10}}
	secondary := []store.ClusterSummary{{ID: 10}, {ID: 30}}
	merged := mergeClusterSummaries(primary, secondary)

	if len(merged) != 3 {
		t.Fatalf("merged length = %d, want 3", len(merged))
	}
	if merged[0].ID != 20 || merged[1].ID != 10 || merged[2].ID != 30 {
		t.Fatalf("merged order mismatch: %+v", merged)
	}
}

func TestTUIHideClosedUsesLoadedWorkingSet(t *testing.T) {
	clusters := sampleTUIClusters()
	clusters[0].Status = "closed"
	clusters[0].ClosedAt = "2026-04-27T00:00:00Z"
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		HideClosed: true,
		Clusters:   clusters,
	})

	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 2 {
		t.Fatalf("hide-closed view mismatch: %+v", model.payload.Clusters)
	}
	model.showClosed = true
	model.applyClusterFilters()
	if len(model.payload.Clusters) != 2 {
		t.Fatalf("showing closed should use loaded working set, got %+v", model.payload.Clusters)
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
	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joinedLabels := strings.Join(labels, "\n")
	for _, want := range []string{"Copy cluster ID", "Copy cluster name", "Copy cluster title", "Copy cluster summary"} {
		if !strings.Contains(joinedLabels, want) {
			t.Fatalf("expected cluster action %q, got %+v", want, model.menuItems)
		}
	}
	if !strings.Contains(joinedLabels, "Copy visible clusters") {
		t.Fatalf("expected visible cluster action menu item, got %+v", model.menuItems)
	}
}

func TestTUIMouseCanClickActionMenuItems(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.openActionMenu()
	layout := model.layout()
	closeIndex := len(model.menuItems) - 1

	model.handleMouse(tea.MouseMsg{
		X:      layout.detail.x + 2,
		Y:      layout.detail.y + 4 + closeIndex,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})

	if model.menuOpen {
		t.Fatal("expected menu click to close action menu")
	}
	if model.status != "Menu closed" {
		t.Fatalf("menu click status = %q, want Menu closed", model.status)
	}
}

func TestTUIMouseWheelMovesActionMenu(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.openActionMenu()
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.detail.x + 2,
		Y:      layout.detail.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelDown,
	})
	if model.menuIndex != 1 {
		t.Fatalf("wheel down menu index = %d, want 1", model.menuIndex)
	}

	model.handleMouse(tea.MouseMsg{
		X:      layout.detail.x + 2,
		Y:      layout.detail.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelUp,
	})
	if model.menuIndex != 0 {
		t.Fatalf("wheel up menu index = %d, want 0", model.menuIndex)
	}
}

func TestTUIActionMenuKeepsSelectionVisible(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 16
	model.syncComponents()
	model.detailView.Height = 6
	model.openActionMenu()

	model.menuIndex = 12
	model.keepMenuVisible()

	if model.menuOff == 0 {
		t.Fatalf("expected long menu to scroll selected action into view")
	}
	lines := strings.Join(model.menuLines(80), "\n")
	if !strings.Contains(lines, model.menuItems[model.menuIndex].label) {
		t.Fatalf("visible menu lines do not include selected item %q:\n%s", model.menuItems[model.menuIndex].label, lines)
	}
	if !strings.Contains(lines, "/") {
		t.Fatalf("expected menu footer to show visible range:\n%s", lines)
	}
}

func TestTUIMouseClickUsesMenuOffset(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 16
	model.syncComponents()
	model.menuOpen = true
	model.menuOff = 5
	model.menuItems = make([]tuiMenuItem, 8)
	for index := range model.menuItems {
		model.menuItems[index] = tuiMenuItem{label: fmt.Sprintf("Item %d", index), action: "close-menu"}
	}
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.detail.x + 2,
		Y:      layout.detail.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})

	if model.menuIndex != 5 {
		t.Fatalf("menu click selected %d, want offset row 5", model.menuIndex)
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
			BodySnippet: "See [the repro](https://example.com/repro) and https://example.com/log.",
		},
	}}

	model.openActionMenu()

	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joined := strings.Join(labels, "\n")
	for _, want := range []string{"Copy title", "Copy cluster summary", "Open first body link", "Copy first body link", "Open body link...", "Copy body link...", "Copy all body links"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("menu labels missing %q in:\n%s", want, joined)
		}
	}
}

func TestTUILinkPickerKeepsMenuOpen(t *testing.T) {
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
			Thread:      store.Thread{Number: 42, Kind: "issue", State: "open", Title: "Thread with links", HTMLURL: "https://github.com/openclaw/openclaw/issues/42"},
			BodySnippet: "See https://example.com/run and https://example.com/raw.",
		},
	}}
	model.openActionMenu()

	if closeMenu := model.runAction("open-link-picker"); closeMenu {
		t.Fatal("link picker action should keep menu open")
	}
	if model.menuTitle != "Open Link" {
		t.Fatalf("menu title = %q, want Open Link", model.menuTitle)
	}
	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joined := strings.Join(labels, "\n")
	for _, want := range []string{" 1  https://example.com/run", " 2  https://example.com/raw", "Back to actions"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("link picker missing %q in:\n%s", want, joined)
		}
	}
}

func TestTUIActionMenuIncludesViewControls(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    5,
		Clusters:   sampleTUIClusters(),
	})

	model.openActionMenu()

	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joined := strings.Join(labels, "\n")
	for _, want := range []string{"Sort clusters by size", "Member sort recent", "Min size 1+", "Hide closed", "Help"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("menu missing view control %q in:\n%s", want, joined)
		}
	}

	model.runAction("min-size-1")
	if model.minSize != 1 {
		t.Fatalf("min-size menu action set %d, want 1", model.minSize)
	}
	model.runAction("sort-size")
	if model.payload.Sort != "size" {
		t.Fatalf("sort menu action set %q, want size", model.payload.Sort)
	}
	model.runAction("member-sort-recent")
	if model.memberSort != memberSortRecent {
		t.Fatalf("member sort menu action set %q, want recent", model.memberSort)
	}
	model.runAction("show-help")
	if !model.showHelp {
		t.Fatal("help menu action did not show help")
	}
}

func TestTUIReferenceLinksAreUniqueAndOrdered(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.memberIndex = 0
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{
			BodySnippet: "See [run](https://example.com/run), https://example.com/run, and https://example.com/raw.",
			Summaries:   map[string]string{"key_summary": "Summary link https://example.com/summary."},
		},
	}}

	links := model.referenceLinks()
	want := []string{"https://example.com/run", "https://example.com/raw", "https://example.com/summary"}
	if strings.Join(links, "\n") != strings.Join(want, "\n") {
		t.Fatalf("reference links = %+v, want %+v", links, want)
	}
}

func TestTUIVisibleClustersClipboardText(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	text := model.visibleClustersClipboardText()
	for _, want := range []string{"C1 [active] 3 items alpha-bravo-charlie", "C2 [active] 5 items delta-echo-foxtrot"} {
		if !strings.Contains(text, want) {
			t.Fatalf("visible clusters clipboard missing %q in:\n%s", want, text)
		}
	}
}

func TestTUIMemberListClipboardText(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.memberRows = []memberRow{
		{label: "ISSUES (1)"},
		{
			selectable: true,
			member: store.ClusterMemberDetail{Thread: store.Thread{
				Number:  42,
				Kind:    "issue",
				State:   "open",
				Title:   "A useful bug",
				HTMLURL: "https://github.com/openclaw/openclaw/issues/42",
			}},
		},
	}

	text := model.memberListClipboardText()
	want := "#42 [open] Issue A useful bug https://github.com/openclaw/openclaw/issues/42"
	if text != want {
		t.Fatalf("member list clipboard = %q, want %q", text, want)
	}
}

func TestTUIActionMenuOmitsThreadActionsWithoutSelectedThread(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.hasDetail = true
	model.memberIndex = 0
	model.memberRows = []memberRow{{label: "ISSUES (1)"}}

	model.openActionMenu()

	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joined := strings.Join(labels, "\n")
	if strings.Contains(joined, "Open selected thread") || strings.Contains(joined, "Copy selected URL") {
		t.Fatalf("menu should omit thread actions without a selected thread:\n%s", joined)
	}
	if !strings.Contains(joined, "Copy cluster summary") {
		t.Fatalf("menu should keep cluster action:\n%s", joined)
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
	if len(rows) != 1 || !strings.Contains(rows[0][3], "No clusters visible") {
		t.Fatalf("cluster empty row mismatch: %+v", rows)
	}
}

func TestTUIPanePositionLabels(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.detail = store.ClusterDetail{Members: []store.ClusterMemberDetail{
		{Thread: store.Thread{ID: 1, Number: 10, Kind: "issue", State: "open", Title: "First"}},
		{Thread: store.Thread{ID: 2, Number: 11, Kind: "issue", State: "open", Title: "Second"}},
	}}
	model.sortMembers()
	model.selected = 1
	model.memberIndex = 2

	if got := model.clusterPositionLabel(); got != "2/2" {
		t.Fatalf("cluster position = %q, want 2/2", got)
	}
	if got := model.memberPositionLabel(); got != "2/2" {
		t.Fatalf("member position = %q, want 2/2", got)
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
