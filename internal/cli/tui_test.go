package cli

import (
	"context"
	"fmt"
	"path/filepath"
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

func TestTUIHeaderShowsDetailMode(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 160
	model.height = 32

	header := model.renderHeader(160)
	if !strings.Contains(header, "detail:full") {
		t.Fatalf("header missing full detail mode:\n%s", header)
	}
	model.compactDetail = true
	header = model.renderHeader(160)
	if !strings.Contains(header, "detail:compact") {
		t.Fatalf("header missing compact detail mode:\n%s", header)
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
	for _, want := range []string{"left click menu row", "wheel in menu", "a: open action menu", "#: jump to issue/PR number", "p: switch repository", "n: load neighbors", "open selected thread or representative", "open link picker", "repos, filter, jump, sort"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %q:\n%s", want, help)
		}
	}
}

func TestTUIActionShortcutOpensMenu(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	updated, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'a'}})
	model = updated.(clusterBrowserModel)

	if !model.menuOpen || model.menuTitle != "Actions" {
		t.Fatalf("action shortcut state menu=%v title=%q", model.menuOpen, model.menuTitle)
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

func TestTUIFilterCancelRestoresPreviousSearch(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.search = "first"
	model.applyClusterFilters()
	model.startFilterInput()

	model.searchInput.SetValue("second")
	model.search = "second"
	model.applyClusterFilters()
	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 2 {
		t.Fatalf("live filter setup mismatch: %+v", model.payload.Clusters)
	}

	updated, _ := model.handleSearchKey(tea.KeyMsg{Type: tea.KeyEsc})
	model = updated

	if model.search != "first" || model.status != "Filter cancelled" {
		t.Fatalf("cancel search/status = %q/%q", model.search, model.status)
	}
	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 1 {
		t.Fatalf("cancel did not restore previous filtered clusters: %+v", model.payload.Clusters)
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
	if model.menuIndex != 2 {
		t.Fatalf("wheel down menu index = %d, want 2", model.menuIndex)
	}

	model.handleMouse(tea.MouseMsg{
		X:      layout.detail.x + 2,
		Y:      layout.detail.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelUp,
	})
	if model.menuIndex != 1 {
		t.Fatalf("wheel up menu index = %d, want 1", model.menuIndex)
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
	if !strings.Contains(lines, "Pg page") {
		t.Fatalf("expected menu footer to mention paging:\n%s", lines)
	}
	if !strings.Contains(lines, "1.") {
		t.Fatalf("expected menu lines to show number shortcuts:\n%s", lines)
	}
}

func TestTUIActionMenuPagesWithKeyboard(t *testing.T) {
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

	updated, _ := model.updateMenu(tea.KeyMsg{Type: tea.KeyPgDown})
	model = updated.(clusterBrowserModel)
	if model.menuIndex != 3 {
		t.Fatalf("page down menu index = %d, want 3", model.menuIndex)
	}
	if model.menuOff == 0 {
		t.Fatalf("expected page down to scroll menu offset")
	}

	updated, _ = model.updateMenu(tea.KeyMsg{Type: tea.KeyEnd})
	model = updated.(clusterBrowserModel)
	if model.menuIndex != len(model.menuItems)-1 {
		t.Fatalf("end menu index = %d, want last", model.menuIndex)
	}

	updated, _ = model.updateMenu(tea.KeyMsg{Type: tea.KeyHome})
	model = updated.(clusterBrowserModel)
	if model.menuIndex != 1 || model.menuOff != 0 {
		t.Fatalf("home menu index/off = %d/%d, want 1/0", model.menuIndex, model.menuOff)
	}
}

func TestTUIActionMenuNumberShortcutRunsVisibleItem(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.menuOpen = true
	model.menuItems = []tuiMenuItem{
		{label: "Close menu", action: "close-menu"},
		{label: "Sort clusters by size", action: "sort-size"},
	}
	model.menuOff = 1

	updated, _ := model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'1'}})
	model = updated.(clusterBrowserModel)

	if model.payload.Sort != "size" {
		t.Fatalf("number shortcut sort = %q, want size", model.payload.Sort)
	}
	if model.menuOpen {
		t.Fatalf("number shortcut should close menu after running action")
	}
}

func TestTUIActionMenuCanOpenHelp(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.openActionMenu()

	updated, _ := model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}})
	model = updated.(clusterBrowserModel)

	if model.menuOpen || !model.showHelp || model.status != "Help" {
		t.Fatalf("menu help state menu=%v help=%v status=%q", model.menuOpen, model.showHelp, model.status)
	}
}

func TestTUIActionMenuQuickKeysStartInputs(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.openActionMenu()

	updated, _ := model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = updated.(clusterBrowserModel)
	if model.menuOpen || !model.searching || model.searchInput.Prompt != "/ " {
		t.Fatalf("menu filter key state menu=%v searching=%v prompt=%q", model.menuOpen, model.searching, model.searchInput.Prompt)
	}

	model.openActionMenu()
	updated, _ = model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'#'}})
	model = updated.(clusterBrowserModel)
	if model.menuOpen || !model.jumping || model.searchInput.Prompt != "# " {
		t.Fatalf("menu jump key state menu=%v jumping=%v prompt=%q", model.menuOpen, model.jumping, model.searchInput.Prompt)
	}
}

func TestTUIActionMenuQuickKeysRunViewActions(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 160
	model.height = 40

	model.openActionMenu()
	updated, _ := model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	model = updated.(clusterBrowserModel)
	if model.menuOpen || model.wideLayout != wideLayoutRightStack {
		t.Fatalf("menu layout key state menu=%v layout=%q", model.menuOpen, model.wideLayout)
	}

	model.openActionMenu()
	updated, _ = model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	model = updated.(clusterBrowserModel)
	if model.menuOpen || !model.compactDetail {
		t.Fatalf("menu detail key state menu=%v compact=%v", model.menuOpen, model.compactDetail)
	}

	model.openActionMenu()
	updated, _ = model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model = updated.(clusterBrowserModel)
	if model.menuOpen || model.payload.Sort != "size" {
		t.Fatalf("menu sort key state menu=%v sort=%q", model.menuOpen, model.payload.Sort)
	}

	model.openActionMenu()
	updated, _ = model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'m'}})
	model = updated.(clusterBrowserModel)
	if model.menuOpen || model.memberSort == memberSortKind {
		t.Fatalf("menu member-sort key state menu=%v sort=%q", model.menuOpen, model.memberSort)
	}
}

func TestTUIActionMenuRepositoryShortcutOpensPicker(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-04-27T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.openActionMenu()

	updated, _ := model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	model = updated.(clusterBrowserModel)

	if !model.menuOpen || model.menuTitle != "Repositories" {
		t.Fatalf("repository shortcut menu=%v title=%q", model.menuOpen, model.menuTitle)
	}
}

func TestTUIActionMenuSectionsAreNotSelectable(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.menuOpen = true
	model.menuItems = []tuiMenuItem{
		tuiMenuSection("View"),
		{label: "Sort clusters by size", action: "sort-size"},
		{label: "Close menu", action: "close-menu"},
	}
	model.detailView.Height = 8
	model.menuIndex = 0
	model.keepMenuVisible()
	if model.menuIndex != 1 {
		t.Fatalf("menu selected section index %d, want first action", model.menuIndex)
	}
	if index, ok := visibleMenuShortcutIndex("1", model.menuItems, 0, 3); !ok || index != 1 {
		t.Fatalf("shortcut index = %d/%v, want first selectable action", index, ok)
	}

	lines := strings.Join(model.menuLines(80), "\n")
	if !strings.Contains(lines, "View") || strings.Contains(lines, "1. View") {
		t.Fatalf("section rendered as selectable:\n%s", lines)
	}
}

func TestTUIJumpToLoadedThreadNumber(t *testing.T) {
	clusters := sampleTUIClusters()
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   clusters,
	})
	model.detailCache[1] = store.ClusterDetail{
		Cluster: clusters[0],
		Members: []store.ClusterMemberDetail{{
			Thread: store.Thread{
				ID:      99,
				Number:  99,
				Kind:    "issue",
				State:   "open",
				Title:   "Jump target",
				HTMLURL: "https://github.com/openclaw/openclaw/issues/99",
			},
		}},
	}

	model.jumpToThreadNumber(99)

	cluster, ok := model.selectedCluster()
	if !ok || cluster.ID != 1 {
		t.Fatalf("selected cluster = %#v, want cluster 1", cluster)
	}
	thread, ok := model.selectedThread()
	if !ok || thread.Number != 99 {
		t.Fatalf("selected thread = %#v, want #99", thread)
	}
	if model.focus != focusMembers {
		t.Fatalf("focus = %q, want members", model.focus)
	}
	if model.status != "Jumped to #99" {
		t.Fatalf("status = %q, want jump confirmation", model.status)
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

func TestTUIRightClickClosesOpenMenu(t *testing.T) {
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
		Button: tea.MouseButtonRight,
	})

	if model.menuOpen {
		t.Fatal("expected right click to close open menu")
	}
	if model.status != "Menu closed" {
		t.Fatalf("right click close status = %q, want Menu closed", model.status)
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
	model.hasDetail = true
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
			Summaries:   map[string]string{"key_summary": "Useful summary."},
		},
	}}

	model.openActionMenu()

	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joined := strings.Join(labels, "\n")
	for _, want := range []string{"Copy title", "Copy cluster summary", "Copy selected detail", "Copy body preview", "Copy summaries", "Load neighbors", "Open first body link", "Copy first body link", "Open body link...", "Copy body link...", "Copy all body links"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("menu labels missing %q in:\n%s", want, joined)
		}
	}
}

func TestTUIThreadDetailClipboardText(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.memberIndex = 0
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{
			Thread: store.Thread{
				ID:              42,
				Number:          42,
				Kind:            "issue",
				State:           "open",
				Title:           "Thread with context",
				AuthorLogin:     "maintainer",
				UpdatedAtGitHub: "2026-04-27T10:00:00Z",
				HTMLURL:         "https://github.com/openclaw/openclaw/issues/42",
			},
			BodySnippet: "Body with https://example.com/repro.",
			Summaries:   map[string]string{"key_summary": "Summary text."},
		},
	}}
	model.neighborCache[42] = []tuiNeighbor{{
		Thread: store.Thread{Number: 43, Kind: "issue", Title: "Neighbor issue"},
		Score:  0.91,
	}}

	text := model.threadDetailClipboardText()
	for _, want := range []string{"Issue #42: Thread with context", "Summary text.", "Body with https://example.com/repro.", "https://example.com/repro", "#43 Issue 91.0% Neighbor issue"} {
		if !strings.Contains(text, want) {
			t.Fatalf("thread detail clipboard missing %q in:\n%s", want, text)
		}
	}
}

func TestTUIActionMenuIncludesLoadedNeighborCopy(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.memberIndex = 0
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{Thread: store.Thread{
			ID:      42,
			Number:  42,
			Kind:    "issue",
			State:   "open",
			Title:   "Thread with neighbors",
			HTMLURL: "https://github.com/openclaw/openclaw/issues/42",
		}},
	}}
	model.neighborCache[42] = []tuiNeighbor{{Thread: store.Thread{Number: 43, Kind: "issue", Title: "Neighbor issue"}, Score: 0.91}}

	model.openActionMenu()

	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	if !strings.Contains(strings.Join(labels, "\n"), "Copy neighbors") {
		t.Fatalf("menu missing Copy neighbors: %+v", model.menuItems)
	}
	if got := model.neighborsClipboardText(); !strings.Contains(got, "#43 Issue 91.0% Neighbor issue") {
		t.Fatalf("neighbor clipboard text mismatch: %q", got)
	}
}

func TestTUILoadNeighborsFromStore(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-04-27T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	targetID, err := seedTUIThreadVector(ctx, st, repoID, 1, "Target issue", []float64{1, 0})
	if err != nil {
		t.Fatalf("target: %v", err)
	}
	neighborID, err := seedTUIThreadVector(ctx, st, repoID, 2, "Related issue", []float64{0.9, 0.1})
	if err != nil {
		t.Fatalf("neighbor: %v", err)
	}
	if _, err := seedTUIThreadVector(ctx, st, repoID, 3, "Unrelated issue", []float64{0, 1}); err != nil {
		t.Fatalf("unrelated: %v", err)
	}
	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository:     "openclaw/openclaw",
		Sort:           "recent",
		EmbedModel:     "test",
		EmbeddingBasis: "title_original",
		Clusters:       sampleTUIClusters(),
	})
	model.memberIndex = 0
	model.hasDetail = true
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{Thread: store.Thread{
			ID:      targetID,
			Number:  1,
			Kind:    "issue",
			State:   "open",
			Title:   "Target issue",
			HTMLURL: "https://github.com/openclaw/openclaw/issues/1",
		}},
	}}

	model.loadSelectedThreadNeighbors(10, 0.2)

	neighbors := model.neighborCache[targetID]
	if len(neighbors) != 1 || neighbors[0].Thread.ID != neighborID {
		t.Fatalf("neighbors = %+v, want related thread %d", neighbors, neighborID)
	}
	if model.focus != focusDetail {
		t.Fatalf("focus = %s, want detail", model.focus)
	}
	if !strings.Contains(strings.Join(model.detailLines(80), "\n"), "Related issue") {
		t.Fatalf("detail does not render loaded neighbors")
	}

	delete(model.neighborCache, targetID)
	model.focus = focusMembers
	updated, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	model = updated.(clusterBrowserModel)
	if len(model.neighborCache[targetID]) != 1 {
		t.Fatalf("keyboard shortcut did not reload neighbors: %+v", model.neighborCache[targetID])
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
	for _, want := range []string{"Sort clusters by size", "Member sort recent", "Filter clusters", "Refresh from store", "Switch repository", "Jump to issue/PR", "Toggle layout", "Show compact detail", "Min size 1+", "Hide closed", "Help", "Quit"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("menu missing view control %q in:\n%s", want, joined)
		}
	}
	model.search = "alpha"
	model.openActionMenu()
	filterIndex, clearIndex, quitIndex := menuLabelIndex(model.menuItems, "Filter clusters..."), menuLabelIndex(model.menuItems, "Clear filter"), menuLabelIndex(model.menuItems, "Quit")
	if !(filterIndex >= 0 && clearIndex == filterIndex+1 && clearIndex < quitIndex) {
		t.Fatalf("clear filter placement filter/clear/quit = %d/%d/%d", filterIndex, clearIndex, quitIndex)
	}
	model.search = ""

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
	model.runAction("refresh")
	if model.status != "Refresh unavailable for this view" {
		t.Fatalf("refresh menu action status = %q", model.status)
	}
	model.runAction("filter")
	if !model.searching || model.searchInput.Prompt != "/ " {
		t.Fatalf("filter menu action did not start filter input")
	}
	model.searching = false
	model.search = "alpha"
	model.applyClusterFilters()
	model.runAction("clear-filter")
	if model.search != "" || model.status != "Filter cleared" {
		t.Fatalf("clear filter action search/status = %q/%q", model.search, model.status)
	}
	model.runAction("jump")
	if !model.jumping || model.searchInput.Prompt != "# " {
		t.Fatalf("jump action did not start jump input")
	}
	model.jumping = false
	model.width = 160
	model.height = 40
	model.runAction("toggle-layout")
	if model.wideLayout != wideLayoutRightStack {
		t.Fatalf("layout menu action set %q, want right-stack", model.wideLayout)
	}
	model.runAction("toggle-detail")
	if !model.compactDetail || model.status != "Detail mode: compact" {
		t.Fatalf("detail menu action compact=%v status=%q", model.compactDetail, model.status)
	}
	model.runAction("show-help")
	if !model.showHelp {
		t.Fatal("help menu action did not show help")
	}
	model.runAction("quit")
	if !model.quitRequested {
		t.Fatal("quit menu action did not request quit")
	}
}

func TestTUIRepositoryPickerSwitchesRepository(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoOneID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "one", FullName: "openclaw/one", RawJSON: "{}", UpdatedAt: "2026-04-27T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo one: %v", err)
	}
	repoTwoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "two", FullName: "openclaw/two", RawJSON: "{}", UpdatedAt: "2026-04-27T01:00:00Z"})
	if err != nil {
		t.Fatalf("repo two: %v", err)
	}
	if err := seedTUICluster(ctx, st, repoTwoID, 20, 200, "repo two cluster"); err != nil {
		t.Fatalf("seed repo two cluster: %v", err)
	}

	model := newClusterBrowserModel(ctx, st, repoOneID, clusterBrowserPayload{
		Repository: "openclaw/one",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.openRepositoryMenu()

	labels := make([]string, 0, len(model.menuItems))
	for _, item := range model.menuItems {
		labels = append(labels, item.label)
	}
	joined := strings.Join(labels, "\n")
	if !strings.Contains(joined, "openclaw/two") {
		t.Fatalf("repository menu missing repo two:\n%s", joined)
	}

	model.runMenuItem(tuiMenuItem{action: "select-repo", value: "openclaw/two"})

	if model.repoID != repoTwoID || model.payload.Repository != "openclaw/two" {
		t.Fatalf("selected repo id/name = %d/%q, want %d/openclaw/two", model.repoID, model.payload.Repository, repoTwoID)
	}
	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 20 {
		t.Fatalf("switched clusters = %#v, want cluster 20", model.payload.Clusters)
	}
	if model.status != "Repository: openclaw/two" {
		t.Fatalf("switch status = %q", model.status)
	}
}

func TestTUIRepositorySwitchRelaxesEmptyFilters(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoOneID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "one", FullName: "openclaw/one", RawJSON: "{}", UpdatedAt: "2026-04-27T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo one: %v", err)
	}
	repoTwoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "two", FullName: "openclaw/two", RawJSON: "{}", UpdatedAt: "2026-04-27T01:00:00Z"})
	if err != nil {
		t.Fatalf("repo two: %v", err)
	}
	if err := seedTUICluster(ctx, st, repoTwoID, 21, 201, "singleton cluster"); err != nil {
		t.Fatalf("seed repo two cluster: %v", err)
	}

	model := newClusterBrowserModel(ctx, st, repoOneID, clusterBrowserPayload{
		Repository: "openclaw/one",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.minSize = 10

	model.switchRepository("openclaw/two")

	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 21 {
		t.Fatalf("relaxed switch clusters = %#v, want singleton cluster", model.payload.Clusters)
	}
	if model.minSize != 1 {
		t.Fatalf("relaxed min size = %d, want 1", model.minSize)
	}
	if !strings.Contains(model.status, "filters relaxed") {
		t.Fatalf("relaxed switch status = %q", model.status)
	}
}

func TestTUIQuitMenuReturnsQuitCommand(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.openActionMenu()
	model.menuItems = []tuiMenuItem{{label: "Quit", action: "quit"}}
	model.menuIndex = 0

	_, cmd := model.updateMenu(tea.KeyMsg{Type: tea.KeyEnter})

	if cmd == nil {
		t.Fatal("expected quit command from menu action")
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
	if !strings.Contains(joined, "Open representative #11") || !strings.Contains(joined, "Copy representative URL") {
		t.Fatalf("menu should include representative actions:\n%s", joined)
	}

	url, ok := model.selectedClusterURL()
	if !ok || url != "https://github.com/openclaw/openclaw/issues/11" {
		t.Fatalf("selected cluster URL = %q/%v, want representative issue URL", url, ok)
	}
}

func TestTUISelectedActionURLFallsBackToRepresentative(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	url, ok := model.selectedActionURL()
	if !ok || url != "https://github.com/openclaw/openclaw/issues/11" {
		t.Fatalf("cluster action URL = %q/%v, want representative issue URL", url, ok)
	}

	model.memberIndex = 0
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{Thread: store.Thread{
			Number:  42,
			Kind:    "issue",
			Title:   "Selected issue",
			HTMLURL: "https://github.com/openclaw/openclaw/issues/42",
		}},
	}}
	url, ok = model.selectedActionURL()
	if !ok || url != "https://github.com/openclaw/openclaw/issues/42" {
		t.Fatalf("thread action URL = %q/%v, want selected issue URL", url, ok)
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

func TestTUILoadSelectedClusterResetsDetailScroll(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.detailView.Width = 40
	model.detailView.Height = 2
	model.detailView.SetContent(strings.Repeat("line\n", 20))
	model.detailView.SetYOffset(8)

	model.loadSelectedCluster()

	if model.detailView.YOffset != 0 {
		t.Fatalf("detail scroll offset = %d, want 0", model.detailView.YOffset)
	}
}

func TestTUIMemberChangeResetsDetailScroll(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.focus = focusMembers
	model.memberRows = []memberRow{
		{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{ID: 1, Number: 10, Kind: "issue", State: "open", Title: "First"}}},
		{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{ID: 2, Number: 11, Kind: "issue", State: "open", Title: "Second"}}},
	}
	model.memberIndex = 0
	model.detailView.Width = 40
	model.detailView.Height = 2
	model.detailView.SetContent(strings.Repeat("line\n", 20))
	model.detailView.SetYOffset(8)

	model.move(1)

	if model.memberIndex != 1 {
		t.Fatalf("member index = %d, want 1", model.memberIndex)
	}
	if model.detailView.YOffset != 0 {
		t.Fatalf("detail scroll offset = %d, want 0", model.detailView.YOffset)
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

func TestTUIRefreshRelaxesEmptyFilters(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-04-27T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if err := seedTUICluster(ctx, st, repoID, 30, 300, "singleton refresh cluster"); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}

	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   nil,
	})
	model.minSize = 10

	model.refreshFromStore()

	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 30 {
		t.Fatalf("refresh clusters = %#v, want singleton cluster", model.payload.Clusters)
	}
	if model.minSize != 1 || !strings.Contains(model.status, "filters relaxed") {
		t.Fatalf("refresh min/status = %d/%q", model.minSize, model.status)
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

func menuLabelIndex(items []tuiMenuItem, label string) int {
	for index, item := range items {
		if item.label == label {
			return index
		}
	}
	return -1
}

func seedTUIThreadVector(ctx context.Context, st *store.Store, repoID int64, number int, title string, vector []float64) (int64, error) {
	threadID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      fmt.Sprintf("%d", number),
		Number:        number,
		Kind:          "issue",
		State:         "open",
		Title:         title,
		HTMLURL:       fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", number),
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   fmt.Sprintf("hash-%d", number),
		UpdatedAt:     "2026-04-27T00:00:00Z",
	})
	if err != nil {
		return 0, err
	}
	err = st.UpsertThreadVector(ctx, store.ThreadVector{
		ThreadID:    threadID,
		Basis:       "title_original",
		Model:       "test",
		Dimensions:  len(vector),
		ContentHash: fmt.Sprintf("hash-%d", number),
		Vector:      vector,
		CreatedAt:   "2026-04-27T00:00:00Z",
		UpdatedAt:   "2026-04-27T00:00:00Z",
	})
	return threadID, err
}

func seedTUICluster(ctx context.Context, st *store.Store, repoID, clusterID int64, threadNumber int, title string) error {
	threadID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      fmt.Sprintf("%d", threadNumber),
		Number:        threadNumber,
		Kind:          "issue",
		State:         "open",
		Title:         title,
		HTMLURL:       fmt.Sprintf("https://github.com/openclaw/two/issues/%d", threadNumber),
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   fmt.Sprintf("cluster-hash-%d", threadNumber),
		UpdatedAt:     "2026-04-27T00:00:00Z",
	})
	if err != nil {
		return err
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(?, ?, ?, ?, 'active', ?, ?, '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, clusterID, repoID, fmt.Sprintf("cluster-%d", clusterID), fmt.Sprintf("repo-%d", clusterID), threadID, title); err != nil {
		return err
	}
	_, err = st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(?, ?, 'member', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, clusterID, threadID)
	return err
}
