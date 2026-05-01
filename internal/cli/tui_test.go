package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
	if model.clusterViewportHeight() < 1 {
		t.Fatalf("cluster table viewport height = %d, want at least 1", model.clusterViewportHeight())
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

func TestTUIHeaderDoesNotWrapAtTerminalWidth(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: strings.Repeat("openclaw/", 20),
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	header := model.renderHeader(80)
	lines := strings.Split(header, "\n")
	if len(lines) != 1 {
		t.Fatalf("header rendered %d lines, want 1:\n%s", len(lines), header)
	}
	if width := lipgloss.Width(lines[0]); width > 80 {
		t.Fatalf("header width = %d, want <= 80: %q", width, lines[0])
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

func TestTUIFooterShowsLocalDatabaseLocation(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		DBSource:   "local",
		DBLocation: "gitcrawl.db",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	footer := model.renderFooter(120)
	if !strings.Contains(footer, "local gitcrawl.db") {
		t.Fatalf("footer missing local database location:\n%s", footer)
	}
	bg, _ := footerPalette(model.payload.DBSource)
	if bg != lipgloss.Color("#5bc0eb") {
		t.Fatalf("local footer background = %q, want blue", bg)
	}
}

func TestTUIFooterShowsRemoteDatabaseLocation(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		DBSource:   "remote",
		DBLocation: "openclaw/gitcrawl-store:openclaw__openclaw.sync.db",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	footer := model.renderFooter(140)
	if !strings.Contains(footer, "remote openclaw/gitcrawl-store:openclaw__openclaw.sync.db") {
		t.Fatalf("footer missing remote database location:\n%s", footer)
	}
	bg, _ := footerPalette(model.payload.DBSource)
	if bg == lipgloss.Color("#5bc0eb") {
		t.Fatalf("remote footer background should not use local blue")
	}
}

func TestTUIFooterShowsRemoteRefreshLoadingState(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository:      "openclaw/openclaw",
		DBSource:        "remote",
		DBLocation:      "openclaw/gitcrawl-store:openclaw__openclaw.sync.db",
		DBRefreshSource: "/tmp/source.db",
		DBRuntimePath:   "/tmp/runtime.db",
		Sort:            "recent",
		Clusters:        sampleTUIClusters(),
	})

	if !model.remoteRefreshing {
		t.Fatal("remote model should start in refresh loading state")
	}
	footer := model.renderFooter(140)
	if !strings.Contains(footer, "Refreshing remote data") {
		t.Fatalf("footer missing remote refresh loading state:\n%s", footer)
	}
}

func TestTUIFooterDoesNotWrapLongRemoteLocation(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		DBSource:   "remote",
		DBLocation: "openclaw/gitcrawl-store:" + strings.Repeat("openclaw__openclaw.sync.db", 6),
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.status = "Cluster 14316"

	footer := model.renderFooter(80)
	lines := strings.Split(footer, "\n")
	if len(lines) != 2 {
		t.Fatalf("footer rendered %d lines, want 2:\n%s", len(lines), footer)
	}
	if !strings.Contains(lines[1], "? help") || !strings.Contains(lines[1], "q quit") {
		t.Fatalf("footer controls were displaced:\n%s", footer)
	}
	for index, line := range lines {
		if width := lipgloss.Width(line); width > 80 {
			t.Fatalf("footer line %d width = %d, want <= 80: %q", index, width, line)
		}
	}
}

func TestTUIViewFitsTerminalFrame(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "size",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 190
	model.height = 32
	model.focus = focusMembers
	model.showClosed = true
	model.memberRows = []memberRow{
		{label: "ISSUES (37)"},
		{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{Number: 44718, State: "closed", Title: strings.Repeat("ReferenceError ", 20), UpdatedAtGitHub: "2026-04-27T00:00:00Z"}}},
		{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{Number: 45057, State: "closed", Title: strings.Repeat("ReferenceError ", 20), UpdatedAtGitHub: "2026-04-27T00:00:00Z"}}},
	}
	model.memberIndex = 1

	view := model.View()
	if got := lipgloss.Height(view); got != model.height {
		t.Fatalf("view height = %d, want %d\n%s", got, model.height, view)
	}
	for lineIndex, line := range strings.Split(view, "\n") {
		if got := lipgloss.Width(line); got > model.width {
			t.Fatalf("line %d width = %d, want <= %d: %q", lineIndex, got, model.width, line)
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
	for _, want := range []string{"left click menu row", "wheel in menu", "a: open action menu", "b in submenu", "#: jump to issue/PR number", "p: switch repository", "n: load neighbors", "open selected thread or representative", "open link picker", "repos, filter, jump, sort"} {
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
	if model.menuFloating {
		t.Fatal("keyboard action menu should use the detail pane, not floating placement")
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

func TestTUIMouseDoubleClickOpensClusterRepresentative(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	layout := model.layout()
	restoreOpenURL, opened := captureOpenURL(t)

	msg := tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	}
	model.handleMouse(msg)
	if len(*opened) != 0 {
		t.Fatalf("single click opened URL: %#v", *opened)
	}
	model.handleMouse(msg)
	restoreOpenURL()

	if got := *opened; len(got) != 1 || got[0] != "https://github.com/openclaw/openclaw/issues/11" {
		t.Fatalf("opened URLs = %#v", got)
	}
}

func TestTUIMouseDoubleClickOpensMemberThread(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.memberRows = []memberRow{
		{label: "ISSUES (1)"},
		{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{
			ID:              42,
			Number:          42,
			Kind:            "issue",
			State:           "open",
			Title:           "Selected issue",
			HTMLURL:         "https://github.com/openclaw/openclaw/issues/42",
			UpdatedAtGitHub: "2026-04-27T10:00:00Z",
		}}},
	}
	model.memberIndex = 0
	layout := model.layout()
	restoreOpenURL, opened := captureOpenURL(t)

	msg := tea.MouseMsg{
		X:      layout.members.x + 2,
		Y:      layout.members.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	}
	model.handleMouse(msg)
	if len(*opened) != 0 {
		t.Fatalf("single click opened URL: %#v", *opened)
	}
	model.handleMouse(msg)
	restoreOpenURL()

	if got := *opened; len(got) != 1 || got[0] != "https://github.com/openclaw/openclaw/issues/42" {
		t.Fatalf("opened URLs = %#v", got)
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
	model.keepVisible()
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

func TestTUIFastWheelScrollKeepsFrameStable(t *testing.T) {
	clusters := make([]store.ClusterSummary, 0, 120)
	for i := 0; i < 120; i++ {
		clusters = append(clusters, store.ClusterSummary{
			ID:                   int64(i + 1),
			StableSlug:           fmt.Sprintf("cluster-%03d", i+1),
			Status:               "active",
			RepresentativeKind:   "issue",
			RepresentativeTitle:  fmt.Sprintf("Issue %03d", i+1),
			RepresentativeNumber: 1000 + i,
			MemberCount:          5,
			UpdatedAt:            fmt.Sprintf("2026-04-27T%02d:00:00Z", i%24),
		})
	}
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		DBSource:   "remote",
		DBLocation: "openclaw/gitcrawl-store:openclaw__openclaw.sync.db",
		Sort:       "recent",
		Clusters:   clusters,
	})
	model.width = 190
	model.height = 34
	layout := model.layout()

	for i := 0; i < 80; i++ {
		cmd := model.handleMouse(tea.MouseMsg{
			X:      layout.clusters.x + 2,
			Y:      layout.clusters.y + 4,
			Action: tea.MouseActionPress,
			Button: tea.MouseButtonWheelDown,
		})
		if cmd == nil && model.selected < len(model.payload.Clusters)-1 {
			t.Fatal("cluster wheel movement should defer detail reload until scrolling settles")
		}
		model.keepVisible()
		model.syncComponents()
	}

	view := model.View()
	lines := strings.Split(view, "\n")
	if len(lines) != model.height {
		t.Fatalf("view height = %d, want %d", len(lines), model.height)
	}
	if !strings.Contains(lines[0], "openclaw/openclaw") {
		t.Fatalf("header moved or disappeared: %q", lines[0])
	}
	if !strings.Contains(lines[len(lines)-1], "q quit") {
		t.Fatalf("footer moved or disappeared: %q", lines[len(lines)-1])
	}
	if count := strings.Count(view, "openclaw/openclaw  "); count != 1 {
		t.Fatalf("header count = %d, want 1", count)
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
	model.keepVisible()
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

func TestTUIClusterAgeHeaderTogglesDirection(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	columns := clusterColumns(maxInt(24, model.layout().clusters.w-4), model.payload.Sort)
	ageX := columnLeftEdge(columns, len(columns)-1)

	model.sortClustersFromHeader(ageX)
	if model.payload.Sort != "oldest" {
		t.Fatalf("age header sort = %q, want oldest", model.payload.Sort)
	}
	if model.payload.Clusters[0].ID != 1 {
		t.Fatalf("oldest sort first cluster id = %d, want 1", model.payload.Clusters[0].ID)
	}

	columns = clusterColumns(maxInt(24, model.layout().clusters.w-4), model.payload.Sort)
	model.sortClustersFromHeader(columnLeftEdge(columns, len(columns)-1))
	if model.payload.Sort != "recent" {
		t.Fatalf("age header second sort = %q, want recent", model.payload.Sort)
	}
	if model.payload.Clusters[0].ID != 2 {
		t.Fatalf("recent sort first cluster id = %d, want 2", model.payload.Clusters[0].ID)
	}
}

func TestTUIMemberAgeHeaderTogglesDirection(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.detail = store.ClusterDetail{Cluster: sampleTUIClusters()[0], Members: []store.ClusterMemberDetail{
		{Thread: store.Thread{ID: 1, Number: 10, Kind: "issue", State: "open", Title: "Older", UpdatedAtGitHub: "2026-04-27T10:00:00Z"}},
		{Thread: store.Thread{ID: 2, Number: 11, Kind: "issue", State: "open", Title: "Newer", UpdatedAtGitHub: "2026-04-27T11:00:00Z"}},
	}}
	model.hasDetail = true
	model.sortMembers()
	columns := memberColumns(maxInt(24, model.layout().members.w-4), model.memberSort)
	ageX := columnLeftEdge(columns, 2)

	model.sortMembersFromHeader(ageX)
	if model.memberSort != memberSortRecent {
		t.Fatalf("member age header sort = %q, want recent", model.memberSort)
	}
	if model.memberRows[0].member.Thread.ID != 2 {
		t.Fatalf("recent member first id = %d, want 2", model.memberRows[0].member.Thread.ID)
	}

	columns = memberColumns(maxInt(24, model.layout().members.w-4), model.memberSort)
	model.sortMembersFromHeader(columnLeftEdge(columns, 2))
	if model.memberSort != memberSortOldest {
		t.Fatalf("member age header second sort = %q, want oldest", model.memberSort)
	}
	if model.memberRows[0].member.Thread.ID != 1 {
		t.Fatalf("oldest member first id = %d, want 1", model.memberRows[0].member.Thread.ID)
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

func TestTUIClusterRowsShowReadableState(t *testing.T) {
	clusters := sampleTUIClusters()
	clusters[1].Status = "closed"
	clusters[1].ClosedAt = "2026-04-27T12:00:00Z"
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "size",
		Clusters:   clusters,
	})

	rows := model.clusterRows()
	if len(rows) < 2 {
		t.Fatalf("expected two cluster rows, got %+v", rows)
	}
	if !strings.Contains(rows[0][2], "CLOSED") {
		t.Fatalf("first row state = %q, want CLOSED", rows[0][2])
	}
	if !strings.Contains(rows[1][2], "OPEN") {
		t.Fatalf("second row state = %q, want OPEN", rows[1][2])
	}
	for rowIndex, row := range rows {
		for cellIndex, cell := range row {
			if strings.Contains(cell, "\x1b[") {
				t.Fatalf("cluster row %d cell %d contains ANSI escapes: %q", rowIndex, cellIndex, cell)
			}
		}
	}
}

func TestTUIRowsStripEmojiFromRenderedTitles(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters: []store.ClusterSummary{{
			ID:                   1,
			Status:               "active",
			StableSlug:           "emoji-title",
			RepresentativeKind:   "issue",
			RepresentativeTitle:  "🔥 Gateway crash 🧨 after upgrade",
			RepresentativeNumber: 123,
			MemberCount:          3,
			UpdatedAt:            "2026-04-27T00:00:00Z",
		}},
	})
	clusterRows := model.clusterRows()
	if strings.ContainsAny(clusterRows[0][4], "🔥🧨") {
		t.Fatalf("cluster title still contains emoji: %q", clusterRows[0][4])
	}
	if clusterRows[0][4] != "Gateway crash after upgrade" {
		t.Fatalf("cluster title = %q, want sanitized title", clusterRows[0][4])
	}

	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{Thread: store.Thread{
			Number:          123,
			State:           "open",
			Title:           "🚨 Browser snapshot fails ✅",
			UpdatedAtGitHub: "2026-04-27T00:00:00Z",
		}},
	}}
	memberRows := model.memberTableRows()
	if strings.ContainsAny(memberRows[0][3], "🚨✅") {
		t.Fatalf("member title still contains emoji: %q", memberRows[0][3])
	}
	if memberRows[0][3] != "Browser snapshot fails" {
		t.Fatalf("member title = %q, want sanitized title", memberRows[0][3])
	}
}

func TestTUIRenderedRowsStyleOpenAndClosedStates(t *testing.T) {
	openCluster := clusterRowStyle(store.ClusterSummary{Status: "active"}, false, false)
	closedCluster := clusterRowStyle(store.ClusterSummary{Status: "closed"}, false, false)
	if openCluster.GetForeground() == nil || openCluster.GetBackground() == nil {
		t.Fatalf("open cluster style missing foreground/background")
	}
	if closedCluster.GetForeground() == nil || closedCluster.GetBackground() == nil {
		t.Fatalf("closed cluster style missing foreground/background")
	}
	if fmt.Sprint(openCluster.GetBackground()) == fmt.Sprint(closedCluster.GetBackground()) {
		t.Fatalf("open and closed cluster backgrounds should differ")
	}
	if fmt.Sprint(openCluster.GetForeground()) != tuiOpenRowFG {
		t.Fatalf("open cluster foreground = %v, want %s", openCluster.GetForeground(), tuiOpenRowFG)
	}
	if fmt.Sprint(openCluster.GetBackground()) != tuiOpenRowBG {
		t.Fatalf("open cluster background = %v, want %s", openCluster.GetBackground(), tuiOpenRowBG)
	}
	if fmt.Sprint(closedCluster.GetForeground()) != tuiClosedRowFG {
		t.Fatalf("closed cluster foreground = %v, want %s", closedCluster.GetForeground(), tuiClosedRowFG)
	}
	selectedCluster := clusterRowStyle(store.ClusterSummary{Status: "active"}, true, true)
	if fmt.Sprint(selectedCluster.GetBackground()) != tuiOpenSelectedBG {
		t.Fatalf("selected cluster background = %v, want %s", selectedCluster.GetBackground(), tuiOpenSelectedBG)
	}
	clusterView := renderStyledTable([]table.Column{{Title: "id", Width: 8}, {Title: "state", Width: 8}}, []table.Row{{"C1", "OPEN"}, {"C2", "CLOSED"}}, 0, 2, 20, "#5bc0eb", func(index int) lipgloss.Style {
		if index == 0 {
			return openCluster
		}
		return closedCluster
	})
	if !strings.Contains(clusterView, "C1") || !strings.Contains(clusterView, "OPEN") || !strings.Contains(clusterView, "C2") || !strings.Contains(clusterView, "CLOSED") {
		t.Fatalf("styled cluster rows lost text: %q", clusterView)
	}
	for lineIndex, line := range strings.Split(clusterView, "\n") {
		if lipgloss.Width(line) > 20 {
			t.Fatalf("cluster line %d width = %d, want <= 20: %q", lineIndex, lipgloss.Width(line), line)
		}
	}

	openMember := memberRowStyle(memberRow{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{State: "open"}}}, false, false)
	closedMember := memberRowStyle(memberRow{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{State: "closed"}}}, false, false)
	if openMember.GetForeground() == nil || openMember.GetBackground() == nil {
		t.Fatalf("open member style missing foreground/background")
	}
	if closedMember.GetForeground() == nil || closedMember.GetBackground() == nil {
		t.Fatalf("closed member style missing foreground/background")
	}
	if fmt.Sprint(openMember.GetBackground()) == fmt.Sprint(closedMember.GetBackground()) {
		t.Fatalf("open and closed member backgrounds should differ")
	}
	if fmt.Sprint(openMember.GetForeground()) != tuiOpenRowFG {
		t.Fatalf("open member foreground = %v, want %s", openMember.GetForeground(), tuiOpenRowFG)
	}
	if fmt.Sprint(closedMember.GetForeground()) != tuiClosedRowFG {
		t.Fatalf("closed member foreground = %v, want %s", closedMember.GetForeground(), tuiClosedRowFG)
	}
	memberView := renderStyledTable([]table.Column{{Title: "number", Width: 8}, {Title: "st", Width: 8}}, []table.Row{{"#1", "opn"}, {"#2", "cls"}}, 0, 2, 20, "#9bc53d", func(index int) lipgloss.Style {
		if index == 0 {
			return openMember
		}
		return closedMember
	})
	if !strings.Contains(memberView, "#1") || !strings.Contains(memberView, "opn") || !strings.Contains(memberView, "#2") || !strings.Contains(memberView, "cls") {
		t.Fatalf("styled member rows lost text: %q", memberView)
	}
	for lineIndex, line := range strings.Split(memberView, "\n") {
		if lipgloss.Width(line) > 20 {
			t.Fatalf("member line %d width = %d, want <= 20: %q", lineIndex, lipgloss.Width(line), line)
		}
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

func TestTUIMemberMovementDoesNotWrapPastEdges(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.memberRows = []memberRow{
		{label: "ISSUES (2)"},
		{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{Number: 1, State: "open"}}},
		{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{Number: 2, State: "open"}}},
	}

	if got := model.nextSelectableMemberIndex(2, 1); got != 2 {
		t.Fatalf("member down from last = %d, want last row", got)
	}
	if got := model.nextSelectableMemberIndex(1, -1); got != 1 {
		t.Fatalf("member up from first = %d, want first row", got)
	}
	if got := model.nextSelectableMemberIndex(1, 10); got != 2 {
		t.Fatalf("member page down = %d, want last row", got)
	}
}

func TestTUIRightClickOpensFloatingMenu(t *testing.T) {
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
	if !model.menuOpen || !model.menuFloating {
		t.Fatalf("right click menu state open=%v floating=%v", model.menuOpen, model.menuFloating)
	}
	if !model.menuRect.contains(layout.clusters.x+3, layout.clusters.y+4) {
		t.Fatalf("floating menu rect %+v should be placed at the right-click row", model.menuRect)
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

func TestTUIRefreshPreservesUnlimitedWorkingSet(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters: []store.ClusterSummary{
			{ID: 1, Status: "active", MemberCount: 5, UpdatedAt: "2026-04-27T00:00:00Z"},
			{ID: 2, Status: "active", MemberCount: 5, UpdatedAt: "2026-04-27T00:00:00Z"},
			{ID: 3, Status: "active", MemberCount: 5, UpdatedAt: "2026-04-27T00:00:00Z"},
		},
	})

	model.applyClusterRefresh([]store.ClusterSummary{
		{ID: 2, Status: "active", MemberCount: 7, UpdatedAt: "2026-04-28T00:00:00Z"},
	}, 2)

	if len(model.payload.Clusters) != 3 {
		t.Fatalf("refresh collapsed working set to %d clusters: %#v", len(model.payload.Clusters), model.payload.Clusters)
	}
	if model.payload.Clusters[0].ID != 2 || model.payload.Clusters[0].MemberCount != 7 {
		t.Fatalf("fresh cluster update was not preserved first: %#v", model.payload.Clusters)
	}
}

func TestTUIRefreshHonorsExplicitClusterLimit(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Limit:      1,
		Clusters: []store.ClusterSummary{
			{ID: 1, Status: "active", MemberCount: 5, UpdatedAt: "2026-04-27T00:00:00Z"},
			{ID: 2, Status: "active", MemberCount: 5, UpdatedAt: "2026-04-27T00:00:00Z"},
		},
	})

	model.applyClusterRefresh([]store.ClusterSummary{
		{ID: 2, Status: "active", MemberCount: 7, UpdatedAt: "2026-04-28T00:00:00Z"},
	}, 2)

	if len(model.payload.Clusters) != 1 || model.payload.Clusters[0].ID != 2 {
		t.Fatalf("explicit limit was not honored: %#v", model.payload.Clusters)
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
	if !model.menuFloating {
		t.Fatal("expected right click action menu to float")
	}
	if model.menuTitle != "Cluster Actions" || model.menuContext != focusClusters {
		t.Fatalf("cluster context menu title/context = %q/%q", model.menuTitle, model.menuContext)
	}
	if model.selected != 1 {
		t.Fatalf("right click selected %d, want 1", model.selected)
	}
	joinedLabels := strings.Join(menuLabels(model.menuItems), "\n")
	for _, want := range []string{"Copy cluster ID", "Copy cluster name", "Copy cluster title", "Copy cluster summary"} {
		if !strings.Contains(joinedLabels, want) {
			t.Fatalf("expected cluster action %q, got %+v", want, model.menuItems)
		}
	}
	if !strings.Contains(joinedLabels, "Copy visible clusters") {
		t.Fatalf("expected visible cluster action menu item, got %+v", model.menuItems)
	}
	if strings.Contains(joinedLabels, "Copy selected URL") {
		t.Fatalf("cluster menu should not include selected member actions:\n%s", joinedLabels)
	}
}

func TestTUIRightClickMemberRowOpensMemberActions(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.memberRows = []memberRow{
		{label: "ISSUES (1)"},
		{
			selectable: true,
			member: store.ClusterMemberDetail{Thread: store.Thread{
				Number:  42,
				Kind:    "issue",
				State:   "open",
				Title:   "Selected issue",
				HTMLURL: "https://github.com/openclaw/openclaw/issues/42",
			}},
		},
	}
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.members.x + 2,
		Y:      layout.members.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonRight,
	})

	if !model.menuOpen || !model.menuFloating {
		t.Fatalf("expected floating member action menu, open=%v floating=%v", model.menuOpen, model.menuFloating)
	}
	if model.menuTitle != "Member Actions" || model.menuContext != focusMembers {
		t.Fatalf("member context menu title/context = %q/%q", model.menuTitle, model.menuContext)
	}
	joinedLabels := strings.Join(menuLabels(model.menuItems), "\n")
	if !strings.Contains(joinedLabels, "Open #42 in browser") {
		t.Fatalf("member menu should include selected thread action:\n%s", joinedLabels)
	}
	if !strings.Contains(joinedLabels, "Copy cluster summary") {
		t.Fatalf("member menu should keep cluster context actions:\n%s", joinedLabels)
	}
	if strings.Contains(joinedLabels, "Copy visible clusters") {
		t.Fatalf("member menu should not include cluster-table bulk actions:\n%s", joinedLabels)
	}
}

func TestTUIRightClickMemberHeaderOpensClusterActions(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.memberIndex = 1
	model.memberRows = []memberRow{
		{label: "ISSUES (1)"},
		{
			selectable: true,
			member: store.ClusterMemberDetail{Thread: store.Thread{
				Number:  42,
				Kind:    "issue",
				State:   "open",
				Title:   "Selected issue",
				HTMLURL: "https://github.com/openclaw/openclaw/issues/42",
			}},
		},
	}
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.members.x + 2,
		Y:      layout.members.y + 3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonRight,
	})

	if !model.menuOpen {
		t.Fatal("expected right click to open action menu")
	}
	if model.menuTitle != "Cluster Actions" || model.menuContext != focusClusters {
		t.Fatalf("member header context menu title/context = %q/%q", model.menuTitle, model.menuContext)
	}
	joinedLabels := strings.Join(menuLabels(model.menuItems), "\n")
	if strings.Contains(joinedLabels, "Copy selected URL") {
		t.Fatalf("member header menu should not use stale selected thread:\n%s", joinedLabels)
	}
	if !strings.Contains(joinedLabels, "Copy cluster summary") {
		t.Fatalf("member header menu should keep cluster actions:\n%s", joinedLabels)
	}
}

func TestTUILeftClickMemberHeaderClearsThreadSelection(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.memberIndex = 1
	model.memberRows = []memberRow{
		{label: "ISSUES (1)"},
		{
			selectable: true,
			member: store.ClusterMemberDetail{Thread: store.Thread{
				Number:  42,
				Kind:    "issue",
				State:   "open",
				Title:   "Selected issue",
				HTMLURL: "https://github.com/openclaw/openclaw/issues/42",
			}},
		},
	}
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.members.x + 2,
		Y:      layout.members.y + 3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})

	if model.memberIndex != 0 {
		t.Fatalf("member index = %d, want header row", model.memberIndex)
	}
	if _, ok := model.selectedThread(); ok {
		t.Fatal("member header should clear selected thread")
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

func TestTUIUpdateCoversKeyboardStateMachine(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
		Clusters:   sampleTUIClusters(),
	})
	model.detail = store.ClusterDetail{
		Cluster: sampleTUIClusters()[0],
		Members: []store.ClusterMemberDetail{
			{Thread: store.Thread{ID: 10, Number: 10, Kind: "issue", State: "open", Title: "A", HTMLURL: ""}},
			{Thread: store.Thread{ID: 11, Number: 11, Kind: "pull_request", State: "closed", Title: "B", HTMLURL: ""}},
		},
	}
	model.hasDetail = true
	model.sortMembers()

	messages := []tea.Msg{
		tea.WindowSizeMsg{Width: 150, Height: 30},
		tea.KeyMsg{Type: tea.KeyTab},
		tea.KeyMsg{Type: tea.KeyRight},
		tea.KeyMsg{Type: tea.KeyShiftTab},
		tea.KeyMsg{Type: tea.KeyDown},
		tea.KeyMsg{Type: tea.KeyUp},
		tea.KeyMsg{Type: tea.KeyPgDown},
		tea.KeyMsg{Type: tea.KeyPgUp},
		tea.KeyMsg{Type: tea.KeyHome},
		tea.KeyMsg{Type: tea.KeyEnd},
		tea.KeyMsg{Type: tea.KeyEnter},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'m'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}},
		tea.KeyMsg{Type: tea.KeyEsc},
	}
	var updated tea.Model = model
	for _, msg := range messages {
		next, _ := updated.Update(msg)
		updated = next
	}
	model = updated.(clusterBrowserModel)
	if model.width != 150 || model.height != 30 {
		t.Fatalf("window size not applied: %dx%d", model.width, model.height)
	}
	if model.status == "" {
		t.Fatalf("expected status after key flow")
	}

	next, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	model = next.(clusterBrowserModel)
	if cmd == nil {
		t.Fatal("q should return a quit command")
	}
	if model.quitRequested {
		t.Fatal("keyboard quit should not set mouse quit flag")
	}
}

func TestTUIUpdateCoversMainKeysAfterMenuBoundary(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    2,
		Clusters:   sampleTUIClusters(),
	})
	model.width = 150
	model.height = 30
	model.detail = store.ClusterDetail{
		Cluster: sampleTUIClusters()[0],
		Members: []store.ClusterMemberDetail{
			{Thread: store.Thread{ID: 10, Number: 10, Kind: "issue", State: "open", Title: "A", HTMLURL: ""}},
		},
	}
	model.hasDetail = true
	model.sortMembers()

	for _, msg := range []tea.Msg{
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}},
		tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}},
		tea.KeyMsg{Type: tea.KeyEsc},
	} {
		next, _ := model.Update(msg)
		model = next.(clusterBrowserModel)
	}
	if model.showHelp {
		t.Fatal("esc should close help")
	}
	next, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = next.(clusterBrowserModel)
	if !model.searching || cmd == nil {
		t.Fatalf("slash should start search, searching=%v cmd=%v", model.searching, cmd)
	}
	next, _ = model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	model = next.(clusterBrowserModel)
	next, cmd = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'#'}})
	model = next.(clusterBrowserModel)
	if !model.jumping || cmd == nil {
		t.Fatalf("hash should start jump, jumping=%v cmd=%v", model.jumping, cmd)
	}

	model.jumping = false
	model.focus = focusMembers
	next, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = next.(clusterBrowserModel)
	if model.focus != focusDetail {
		t.Fatalf("enter on members should focus detail, got %v", model.focus)
	}
	layout := model.layout()
	next, _ = model.Update(tea.MouseMsg{X: layout.clusters.x + 2, Y: layout.clusters.y + 3, Action: tea.MouseActionPress, Button: tea.MouseButtonLeft})
	model = next.(clusterBrowserModel)
	if model.focus != focusClusters {
		t.Fatalf("mouse update should focus clusters, got %v", model.focus)
	}
}

func TestTUIRichDetailRenderingBranches(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
		Clusters:   sampleTUIClusters(),
	})
	model.width = 150
	model.height = 32
	thread := store.Thread{
		ID:              44,
		Number:          44,
		Kind:            "pull_request",
		State:           "closed",
		Title:           "Render **markdown** labels",
		HTMLURL:         "https://github.com/openclaw/openclaw/pull/44",
		LabelsJSON:      `[{"name":"bug"},{"name":"ui"}]`,
		AuthorLogin:     "alice",
		UpdatedAtGitHub: time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
	}
	model.detail = store.ClusterDetail{
		Cluster: sampleTUIClusters()[0],
		Members: []store.ClusterMemberDetail{{
			Thread: thread,
			Summaries: map[string]string{
				"key_summary":               "# Heading\n\n> quoted\n\n  - nested list item with [link](https://example.test)\n\n```go\nfmt.Println(\"hi\")\n```",
				"maintainer_signal_summary": "maintainer signal",
			},
			BodySnippet: "Preview line with `code` and ~~strike~~",
		}},
	}
	model.hasDetail = true
	model.memberRows = []memberRow{{selectable: true, member: model.detail.Members[0]}}
	model.memberIndex = 0
	model.neighborCache = map[int64][]tuiNeighbor{
		thread.ID: []tuiNeighbor{{Thread: store.Thread{Number: 45, Kind: "issue", Title: "Neighbor title"}, Score: 0.91}},
	}
	view := model.View()
	lines := strings.Join(model.detailLines(80), "\n")
	for _, want := range []string{"labels: bug, ui", "Neighbors", "Key summary", "Maintainer signal", "code"} {
		if !strings.Contains(view, want) && !strings.Contains(lines, want) {
			t.Fatalf("rich detail missing %q:\nview:\n%s\nlines:\n%s", want, view, lines)
		}
	}
	if got := layoutLabel(tuiLayout{}); got != string(wideLayoutColumns) {
		t.Fatalf("default layout label = %q", got)
	}
	if got := formatSummaryLabel("custom_summary"); got != "custom summary" {
		t.Fatalf("summary label = %q", got)
	}
	if !isEmojiRune(rune(0x1F600)) || isEmojiRune('A') {
		t.Fatal("emoji detection branch mismatch")
	}
}

func TestTUIRunMenuItemCoversNonExternalActions(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    5,
		Clusters:   sampleTUIClusters(),
	})
	model.detail = store.ClusterDetail{Cluster: sampleTUIClusters()[0], Members: []store.ClusterMemberDetail{{
		Thread: store.Thread{ID: 42, Number: 42, Kind: "issue", State: "open", Title: "Selected", HTMLURL: "https://github.com/openclaw/openclaw/issues/42"},
	}}}
	model.hasDetail = true
	model.sortMembers()

	actions := []tuiMenuItem{
		{label: "section", action: tuiMenuSeparatorAction},
		{action: "sort-size"},
		{action: "sort-recent"},
		{action: "member-sort-kind"},
		{action: "member-sort-recent"},
		{action: "refresh"},
		{action: "filter"},
		{action: "clear-filter"},
		{action: "repository-picker"},
		{action: "jump"},
		{action: "toggle-layout"},
		{action: "toggle-detail"},
		{action: "min-size-1"},
		{action: "min-size-5"},
		{action: "min-size-10"},
		{action: "toggle-closed"},
		{action: "show-help"},
		{action: "open-cluster-representative"},
		{action: "copy-cluster-url"},
		{action: "close-cluster-confirm"},
		{action: "close-cluster-local"},
		{action: "reopen-cluster-confirm"},
		{action: "reopen-cluster-local"},
		{action: "exclude-member-confirm"},
		{action: "exclude-member-local"},
		{action: "include-member-confirm"},
		{action: "include-member-local"},
		{action: "canonical-member-confirm"},
		{action: "canonical-member-local"},
		{action: "load-neighbors"},
		{action: "close-thread-confirm"},
		{action: "close-thread-local"},
		{action: "reopen-thread-confirm"},
		{action: "reopen-thread-local"},
		{action: "copy-body-preview"},
		{action: "copy-summaries"},
		{action: "copy-neighbors"},
		{action: "open-link-picker"},
		{action: "copy-link-picker"},
		{action: "copy-reference-links"},
		{action: "close-menu"},
		{action: "quit"},
	}
	for _, item := range actions {
		model.searching = false
		model.jumping = false
		_ = model.runMenuItem(item)
	}
	if !model.quitRequested {
		t.Fatal("quit action should mark quitRequested")
	}
	if model.status == "" {
		t.Fatal("menu actions should update status")
	}

	empty := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{Repository: "openclaw/openclaw"})
	for _, action := range []string{"copy-cluster-id", "copy-cluster-name", "copy-cluster-title", "open", "copy-url", "copy-markdown", "copy-title", "open-first-link", "copy-first-link"} {
		if !empty.runMenuItem(tuiMenuItem{action: action}) {
			t.Fatalf("empty action %s should be handled", action)
		}
		if empty.status == "" {
			t.Fatalf("empty action %s should set status", action)
		}
	}
}

func TestTUILocalActionUnavailableBranches(t *testing.T) {
	durable := sampleTUIClusters()[0]
	durable.Source = store.ClusterSourceDurable
	durable.Status = "active"
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
		Clusters:   []store.ClusterSummary{durable},
	})
	model.detail = store.ClusterDetail{Cluster: durable, Members: []store.ClusterMemberDetail{{
		Thread: store.Thread{ID: 42, Number: 42, Kind: "issue", State: "open", Title: "Selected", HTMLURL: "https://github.com/openclaw/openclaw/issues/42"},
		State:  "active",
	}}}
	model.hasDetail = true
	model.memberRows = []memberRow{{selectable: true, member: model.detail.Members[0]}}
	model.memberIndex = 0
	for _, action := range []string{
		"close-thread-local",
		"reopen-thread-local",
		"close-cluster-local",
		"reopen-cluster-local",
		"exclude-member-local",
		"include-member-local",
		"canonical-member-local",
	} {
		if !model.runMenuItem(tuiMenuItem{action: action}) {
			t.Fatalf("action %s should be handled", action)
		}
		if !strings.Contains(model.status, "unavailable") {
			t.Fatalf("action %s status = %q", action, model.status)
		}
	}

	empty := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{Repository: "openclaw/openclaw"})
	for _, fn := range []func(){
		empty.openCloseThreadMenu,
		empty.openReopenThreadMenu,
		empty.openCloseClusterMenu,
		empty.openReopenClusterMenu,
		empty.openExcludeMemberMenu,
		empty.openIncludeMemberMenu,
		empty.openCanonicalMemberMenu,
		empty.closeSelectedThreadLocally,
		empty.reopenSelectedThreadLocally,
		empty.closeSelectedClusterLocally,
		empty.reopenSelectedClusterLocally,
		empty.excludeSelectedClusterMemberLocally,
		empty.includeSelectedClusterMemberLocally,
		empty.setSelectedClusterCanonicalLocally,
	} {
		fn()
		if empty.status == "" {
			t.Fatal("empty local action should set status")
		}
	}
}

func TestTUIRunMenuItemCoversDesktopActionsWithFakeCommands(t *testing.T) {
	installFakeDesktopCommands(t)
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
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
				Title:           "Thread with links",
				AuthorLogin:     "alice",
				UpdatedAtGitHub: "2026-04-30T00:00:00Z",
				HTMLURL:         "https://github.com/openclaw/openclaw/issues/42",
				LabelsJSON:      `[{"name":"bug"}]`,
			},
			BodySnippet: "See [docs](https://example.com/docs) and https://example.com/log.",
			Summaries:   map[string]string{"key_summary": "Useful summary."},
		},
	}}
	model.detail = store.ClusterDetail{Cluster: sampleTUIClusters()[0], Members: []store.ClusterMemberDetail{model.memberRows[0].member}}
	model.hasDetail = true
	model.neighborCache[42] = []tuiNeighbor{{Thread: store.Thread{Number: 43, Kind: "pull_request", Title: "Neighbor"}, Score: 0.88}}

	for _, item := range []tuiMenuItem{
		{action: "open-cluster-representative", value: "https://github.com/openclaw/openclaw/issues/11"},
		{action: "copy-cluster-url", value: "https://github.com/openclaw/openclaw/issues/11"},
		{action: "copy-thread-detail"},
		{action: "copy-body-preview"},
		{action: "copy-summaries"},
		{action: "copy-neighbors"},
		{action: "copy-cluster-id"},
		{action: "copy-cluster-name"},
		{action: "copy-cluster-title"},
		{action: "copy-member-list"},
		{action: "open-picked-link", value: "https://example.com/docs"},
		{action: "copy-picked-link", value: "https://example.com/docs"},
		{action: "copy-cluster"},
		{action: "copy-visible-clusters"},
		{action: "copy-reference-links"},
		{action: "open"},
		{action: "copy-url"},
		{action: "copy-markdown"},
		{action: "copy-title"},
		{action: "open-first-link"},
		{action: "copy-first-link"},
	} {
		if !model.runMenuItem(item) {
			t.Fatalf("action %s was not handled", item.action)
		}
		if strings.Contains(model.status, "copy text:") || strings.Contains(model.status, "open URL:") {
			t.Fatalf("desktop action %s failed: %s", item.action, model.status)
		}
	}
}

func installFakeDesktopCommands(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("desktop command fakes are shell scripts")
	}
	dir := t.TempDir()
	script := "#!/bin/sh\ncat >/dev/null\nexit 0\n"
	for _, name := range []string{"open", "pbcopy", "xdg-open", "xclip"} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
			t.Fatalf("write fake %s: %v", name, err)
		}
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestTUIRemoteRefreshMessages(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository:      "openclaw/openclaw",
		DBRefreshSource: "/tmp/missing-source.db",
		DBRuntimePath:   "/tmp/missing-runtime.db",
		Clusters:        sampleTUIClusters(),
	})
	model.remoteRefreshing = true

	updated, cmd := model.Update(tuiRemoteRefreshTickMsg{})
	model = updated.(clusterBrowserModel)
	if model.remoteFrame != 1 || cmd == nil {
		t.Fatalf("remote tick frame/cmd = %d/%v", model.remoteFrame, cmd)
	}
	updated, _ = model.Update(tuiRemoteRefreshMsg{err: fmt.Errorf("network down")})
	model = updated.(clusterBrowserModel)
	if model.remoteRefreshing || !strings.Contains(model.status, "network down") {
		t.Fatalf("remote refresh error state = refreshing:%v status:%q", model.remoteRefreshing, model.status)
	}
	updated, _ = model.Update(tuiRemoteRefreshMsg{changed: false})
	model = updated.(clusterBrowserModel)
	if model.status != "Remote data already current" {
		t.Fatalf("remote no-change status = %q", model.status)
	}
}

func TestTUIUpdateRefreshMessageBranches(t *testing.T) {
	st, repoID, _ := seedTUIDurableStore(t)
	defer st.Close()

	model := newClusterBrowserModel(context.Background(), st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})

	model.menuOpen = true
	updated, cmd := model.Update(tuiAutoRefreshMsg{})
	model = updated.(clusterBrowserModel)
	if cmd == nil {
		t.Fatal("auto refresh while menu is open should reschedule")
	}

	model.menuOpen = false
	updated, cmd = model.Update(tuiAutoRefreshMsg{})
	model = updated.(clusterBrowserModel)
	if cmd == nil {
		t.Fatal("auto refresh should reschedule after refreshing from store")
	}

	updated, cmd = model.Update(tuiRemoteRefreshTickMsg{})
	model = updated.(clusterBrowserModel)
	if cmd != nil {
		t.Fatalf("remote tick should be ignored when not refreshing: %v", cmd)
	}

	model.remoteRefreshing = true
	updated, cmd = model.Update(tuiRemoteRefreshTickMsg{})
	model = updated.(clusterBrowserModel)
	if model.remoteFrame == 0 || cmd == nil {
		t.Fatalf("remote tick frame/cmd = %d/%v", model.remoteFrame, cmd)
	}

	updated, cmd = model.Update(tuiRemoteRefreshMsg{err: fmt.Errorf("boom")})
	model = updated.(clusterBrowserModel)
	if cmd != nil || !strings.Contains(model.status, "Remote refresh failed") {
		t.Fatalf("remote error status/cmd = %q/%v", model.status, cmd)
	}
}

func TestTUIInitAndNonInteractiveProgramFallback(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository:      "openclaw/openclaw",
		DBSource:        "remote",
		DBRefreshSource: "/tmp/source.db",
		DBRuntimePath:   "/tmp/runtime.db",
		Clusters:        sampleTUIClusters(),
	})
	if cmd := model.Init(); cmd == nil {
		t.Fatal("remote model init should return batched refresh commands")
	}
	local := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Clusters:   sampleTUIClusters(),
	})
	if cmd := local.Init(); cmd != nil {
		t.Fatalf("local model without store should not create init command: %v", cmd)
	}

	app := New()
	var stdout strings.Builder
	app.Stdout = &stdout
	err := app.runInteractiveTUI(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Clusters:   sampleTUIClusters(),
	})
	if err != nil {
		t.Fatalf("non-interactive fallback: %v", err)
	}
	if !strings.Contains(stdout.String(), "openclaw/openclaw") {
		t.Fatalf("fallback output = %q", stdout.String())
	}
}

func TestTUIRemoteRefreshCommandAndReopenRuntimeStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	runtimePath := filepath.Join(dir, "runtime.db")
	st, err := store.Open(ctx, sourcePath)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	if _, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-04-30T00:00:00Z"}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	model := newClusterBrowserModel(ctx, nil, 0, clusterBrowserPayload{
		Repository:      "openclaw/openclaw",
		DBSource:        "remote",
		DBRefreshSource: sourcePath,
		DBRuntimePath:   runtimePath,
		Clusters:        sampleTUIClusters(),
	})
	msg := model.remoteRefreshCmd()()
	refresh, ok := msg.(tuiRemoteRefreshMsg)
	if !ok || refresh.err != nil || !refresh.changed {
		t.Fatalf("remote refresh msg = %#v", msg)
	}
	if err := model.reopenRuntimeStore(); err != nil {
		t.Fatalf("reopen runtime store: %v", err)
	}
	if model.store == nil {
		t.Fatal("runtime store was not reopened")
	}
	_ = model.store.Close()
}

func TestTUILocalActionsMutateStore(t *testing.T) {
	ctx := context.Background()
	st, repoID, clusterID := seedTUIDurableStore(t)
	defer st.Close()
	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "size",
		MinSize:    1,
		Clusters: []store.ClusterSummary{{
			ID:                   clusterID,
			Source:               store.ClusterSourceDurable,
			Status:               "active",
			StableSlug:           "local-actions",
			RepresentativeKind:   "issue",
			RepresentativeTitle:  "First member",
			RepresentativeNumber: 201,
			MemberCount:          2,
			UpdatedAt:            "2026-04-30T00:00:00Z",
		}},
	})
	model.loadSelectedCluster()
	if !model.hasDetail || len(model.memberRows) == 0 {
		t.Fatalf("expected loaded detail, got %+v", model.detail)
	}
	model.closeSelectedThreadLocally()
	if !strings.Contains(model.status, "Closed #") {
		t.Fatalf("close thread status = %q", model.status)
	}
	model.showClosed = true
	model.refreshFromStore()
	model.memberIndex = memberRowIndex(model.memberRows, 201)
	model.reopenSelectedThreadLocally()
	if !strings.Contains(model.status, "Reopened #") {
		t.Fatalf("reopen thread status = %q", model.status)
	}
	model.closeSelectedClusterLocally()
	if !strings.Contains(model.status, "Closed cluster") {
		t.Fatalf("close cluster status = %q", model.status)
	}
	model.showClosed = true
	model.refreshFromStore()
	model.reopenSelectedClusterLocally()
	if !strings.Contains(model.status, "Reopened cluster") {
		t.Fatalf("reopen cluster status = %q", model.status)
	}
	model.memberIndex = memberRowIndex(model.memberRows, 202)
	model.excludeSelectedClusterMemberLocally()
	if !strings.Contains(model.status, "Excluded #202") {
		t.Fatalf("exclude member status = %q", model.status)
	}
	model.showClosed = true
	model.refreshFromStore()
	model.memberIndex = memberRowIndex(model.memberRows, 202)
	model.includeSelectedClusterMemberLocally()
	if !strings.Contains(model.status, "Included #202") {
		t.Fatalf("include member status = %q", model.status)
	}
	model.memberIndex = memberRowIndex(model.memberRows, 202)
	model.setSelectedClusterCanonicalLocally()
	if !strings.Contains(model.status, "Set #202 as canonical") {
		t.Fatalf("canonical status = %q", model.status)
	}
}

func TestTUIJumpAndSortHelpersCoverStoreBackedBranches(t *testing.T) {
	ctx := context.Background()
	st, repoID, clusterID := seedTUIDurableStore(t)
	defer st.Close()
	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    5,
		Limit:      1,
		Clusters:   nil,
	})
	model.jumpToThreadNumber(202)
	if model.currentClusterID() != clusterID || model.memberIndex < 0 {
		t.Fatalf("jump did not load cluster/member: cluster=%d memberIndex=%d status=%q", model.currentClusterID(), model.memberIndex, model.status)
	}
	if _, ok := model.clusterFromWorkingSet(clusterID); !ok {
		t.Fatalf("cluster %d not added to working set", clusterID)
	}
	model.sortMembersFromHeader(1)
	if model.memberSort != memberSortNumber {
		t.Fatalf("member sort = %q, want number", model.memberSort)
	}
	model.sortMembersFromHeader(columnRightEdge(memberColumns(80, model.memberSort), 1) - 1)
	if model.memberSort != memberSortState {
		t.Fatalf("member sort = %q, want state", model.memberSort)
	}
	model.width = 120
	model.payload.Sort = "recent"
	model.sortClustersFromHeader(0)
	if model.payload.Sort != "size" {
		t.Fatalf("cluster sort = %q, want size from first header", model.payload.Sort)
	}
	model.sortClustersFromHeader(10_000)
	if model.payload.Sort != "recent" {
		t.Fatalf("cluster sort = %q, want recent from last header", model.payload.Sort)
	}
	model.payload.Sort = "recent"
	model.sortClustersFromHeader(columnRightEdge(clusterColumns(80, model.payload.Sort), 1) + 1)
	if model.payload.Sort != "size" {
		t.Fatalf("cluster sort = %q, want size from middle toggle", model.payload.Sort)
	}
	model.sortClustersFromHeader(columnRightEdge(clusterColumns(80, model.payload.Sort), 1) + 1)
	if model.payload.Sort != "recent" {
		t.Fatalf("cluster sort = %q, want recent from middle toggle", model.payload.Sort)
	}
	before := len(model.allClusters)
	model.ensureClusterInWorkingSet(store.ClusterSummary{})
	model.ensureClusterInWorkingSet(store.ClusterSummary{ID: clusterID})
	model.ensureClusterInWorkingSet(store.ClusterSummary{ID: clusterID + 100, RepresentativeTitle: "new"})
	if len(model.allClusters) != before+1 {
		t.Fatalf("working set size = %d, want %d", len(model.allClusters), before+1)
	}
	model.searchInput.SetValue("not-a-number")
	model.jumping = true
	updated, _ := model.handleJumpKey(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated
	if !strings.Contains(model.status, "Enter a positive") {
		t.Fatalf("bad jump status = %q", model.status)
	}
	model.startJumpInput()
	model.searchInput.SetValue("201")
	updated, _ = model.handleJumpKey(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated
	if !strings.Contains(model.status, "Jumped") {
		t.Fatalf("good jump status = %q", model.status)
	}
}

func TestTUIDisplayHelperBranches(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
		Clusters:   sampleTUIClusters(),
	})
	model.width = 120
	model.height = 32
	model.focus = focusMembers
	if model.pageStep() <= 0 {
		t.Fatal("member page step should be positive")
	}
	model.focus = focusDetail
	model.detailView.Height = 7
	if model.pageStep() != 7 {
		t.Fatalf("detail page step = %d, want 7", model.pageStep())
	}
	for _, source := range []string{"remote", "local", "other", ""} {
		model.payload.DBSource = source
		model.payload.DBLocation = "store.db"
		if got := model.footerLocation(); got == "" {
			t.Fatalf("footer location empty for %q", source)
		}
	}
	for _, width := range []int{80, 110, 160} {
		model.width = width
		layout := model.layout()
		if layoutLabel(layout) == "" {
			t.Fatalf("empty layout label for %+v", layout)
		}
	}
	for _, value := range []int{0, 1, 5, 10} {
		if nextMinSize(value) <= 0 {
			t.Fatalf("next min size for %d should be positive", value)
		}
	}
	for _, value := range []bool{true, false} {
		if boolLabel(value) == "" {
			t.Fatalf("bool label empty")
		}
	}
	for _, kind := range []string{"issue", "pull_request", "discussion", ""} {
		if kindLabel(kind) == "" || kindGlyph(kind) == "" || kindTitle(kind) == "" {
			t.Fatalf("kind helpers empty for %q", kind)
		}
	}
	for _, state := range []string{"open", "closed", "merged", "draft", ""} {
		if stateGlyph(state) == "" {
			t.Fatalf("state glyph empty for %q", state)
		}
	}
	for _, cluster := range []store.ClusterSummary{
		{Status: "active"},
		{Status: "closed", ClosedAt: "2026-04-30T00:00:00Z"},
		{Status: "merged"},
		{Status: "split"},
		{Status: "ignored"},
	} {
		if clusterStateLabel(cluster) == "" {
			t.Fatalf("cluster state empty for %+v", cluster)
		}
		_ = clusterRowStyle(cluster, false, true)
		_ = clusterRowStyle(cluster, true, true)
	}
	for _, member := range []store.ClusterMemberDetail{
		{Thread: store.Thread{State: "open"}},
		{Thread: store.Thread{State: "closed", ClosedAtGitHub: "2026-04-30T00:00:00Z"}},
		{Thread: store.Thread{State: "merged", MergedAtGitHub: "2026-04-30T00:00:00Z"}},
		{Thread: store.Thread{State: "open", IsDraft: true}},
	} {
		if memberDisplayState(member) == "" || closedLabel(member.Thread) == "" {
			t.Fatalf("member display empty for %+v", member)
		}
	}
	for _, ref := range []store.ClusterSummary{
		{RepresentativeNumber: 1, RepresentativeKind: "issue"},
		{RepresentativeTitle: "fallback title"},
	} {
		if threadRef(ref) == "" {
			t.Fatalf("thread ref empty for %+v", ref)
		}
	}
	for _, ts := range []string{"", "bad", time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano), time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339Nano)} {
		if formatRelativeTime(ts) == "" {
			t.Fatalf("relative time empty for %q", ts)
		}
	}
	if link, ok := firstMarkdownLink("see [docs](https://example.com/docs)."); !ok || link != "https://example.com/docs" {
		t.Fatal("first markdown link not found")
	}
	if got := formatSummaryLabel("llm_key_3line"); got == "" {
		t.Fatal("summary label empty")
	}
	for _, key := range []string{"key_summary", "problem_summary", "solution_summary", "maintainer_signal_summary", "dedupe_summary"} {
		if got := formatSummaryLabel(key); got == "" || strings.Contains(got, "_") {
			t.Fatalf("summary label for %q = %q", key, got)
		}
	}
	if got := labelsFromJSON(`[{"name":"bug"},{"name":"needs review"}]`); !strings.Contains(got, "bug") || !strings.Contains(got, "needs review") {
		t.Fatalf("labels = %q", got)
	}
	if got := labelsFromJSON(`["bug","needs review"]`); !strings.Contains(got, "bug") || !strings.Contains(got, "needs review") {
		t.Fatalf("string labels = %q", got)
	}
	if labelsFromJSON(`not-json`) != "" {
		t.Fatal("invalid labels should render empty")
	}
	if selectedColor(true) == "" || selectedColor(false) == "" || selectedFG(true) == "" || selectedFG(false) == "" {
		t.Fatal("selected colors empty")
	}
	if columnRightEdge(nil, 0) != 0 || columnRightEdge([]table.Column{{Width: 3}}, 1) != 0 {
		t.Fatal("invalid column right edge should be zero")
	}
	for _, r := range []rune{'\u200d', '\ufe0f', '\U0001f600', '\u2600', '\u303d'} {
		if !isEmojiRune(r) {
			t.Fatalf("expected %U to be treated as emoji", r)
		}
	}
	if isEmojiRune('A') {
		t.Fatal("ASCII letter should not be emoji")
	}
	if !isMouseWheel(tea.MouseButtonWheelDown) || !isMouseWheel(tea.MouseButtonWheelUp) || isMouseWheel(tea.MouseButtonLeft) {
		t.Fatal("mouse wheel detection mismatch")
	}
	if got := wrapPlain("alpha beta gamma", 5); len(got) == 0 {
		t.Fatalf("wrap = %+v", got)
	}
	if got := markdownLines("**bold** `code` [link](https://example.com)", 12); len(got) == 0 {
		t.Fatal("markdown lines empty")
	}
}

func TestTUISearchMoveAndWheelBranches(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
		Clusters:   sampleTUIClusters(),
	})
	model.width = 150
	model.height = 32
	model.detail = store.ClusterDetail{Cluster: sampleTUIClusters()[0], Members: []store.ClusterMemberDetail{
		{Thread: store.Thread{ID: 1, Number: 1, Kind: "issue", State: "open", Title: "One"}},
		{Thread: store.Thread{ID: 2, Number: 2, Kind: "pull_request", State: "open", Title: "Two"}},
	}}
	model.hasDetail = true
	model.sortMembers()

	model.focus = focusClusters
	model.move(1)
	if model.selected != 1 {
		t.Fatalf("cluster move selected = %d", model.selected)
	}
	model.applyClusterDetail(store.ClusterDetail{Cluster: sampleTUIClusters()[0], Members: []store.ClusterMemberDetail{
		{Thread: store.Thread{ID: 1, Number: 1, Kind: "issue", State: "open", Title: "One"}},
		{Thread: store.Thread{ID: 2, Number: 2, Kind: "pull_request", State: "open", Title: "Two"}},
	}})
	model.focus = focusMembers
	model.move(1)
	if model.memberIndex < 0 {
		t.Fatalf("member move index/status = %d/%q", model.memberIndex, model.status)
	}
	model.focus = focusDetail
	model.detailView.SetContent(strings.Repeat("line\n", 20))
	model.move(3)
	model.move(-2)
	model.jumpEdge(false)
	model.jumpEdge(true)
	model.focus = focusMembers
	model.jumpEdge(false)
	first := model.memberIndex
	model.jumpEdge(true)
	if model.memberIndex == first {
		t.Fatalf("member jump edge did not move from %d", first)
	}
	model.focus = focusClusters
	model.jumpEdge(false)
	model.jumpEdge(true)
	if model.selected != len(model.payload.Clusters)-1 {
		t.Fatalf("cluster jump edge selected = %d", model.selected)
	}

	model.search = "first"
	model.startFilterInput()
	model.searchInput.SetValue("second")
	updated, _ := model.handleSearchKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	model = updated
	if !model.searching {
		t.Fatal("typing should keep search mode active")
	}
	updated, _ = model.handleSearchKey(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated
	if model.searching || !strings.Contains(model.status, "Filter:") {
		t.Fatalf("enter search state = %v status=%q", model.searching, model.status)
	}

	model.width = 150
	model.height = 32
	layout := model.layout()
	model.mouseWheel(layout, tea.MouseMsg{X: layout.clusters.x + 1, Y: layout.clusters.y + 3}, 1)
	if model.focus != focusClusters {
		t.Fatalf("wheel over clusters focus = %q", model.focus)
	}
	model.mouseWheel(layout, tea.MouseMsg{X: layout.members.x + 1, Y: layout.members.y + 3}, 1)
	if model.focus != focusMembers {
		t.Fatalf("wheel over members focus = %q", model.focus)
	}
	model.mouseWheel(layout, tea.MouseMsg{X: layout.detail.x + 1, Y: layout.detail.y + 3}, 1)
	if model.focus != focusDetail {
		t.Fatalf("wheel over detail focus = %q", model.focus)
	}
	model.mouseWheel(layout, tea.MouseMsg{X: 999, Y: 999}, -1)
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

func TestTUIMouseClickUsesFloatingMenuOffset(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.menuOpen = true
	model.menuFloating = true
	model.menuRect = tuiRect{x: 5, y: 3, w: 40, h: 12}
	model.menuOff = 5
	model.menuItems = make([]tuiMenuItem, 8)
	for index := range model.menuItems {
		model.menuItems[index] = tuiMenuItem{label: fmt.Sprintf("Item %d", index), action: "close-menu"}
	}

	model.handleMouse(tea.MouseMsg{
		X:      model.menuRect.x + 2,
		Y:      model.menuRect.y + 3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})

	if model.menuIndex != 5 {
		t.Fatalf("floating menu click selected %d, want offset row 5", model.menuIndex)
	}
	if model.menuOpen || model.menuFloating {
		t.Fatalf("floating menu should close cleanly, open=%v floating=%v", model.menuOpen, model.menuFloating)
	}
}

func TestTUIMouseMotionHoversFloatingMenuItems(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	model.menuOpen = true
	model.menuFloating = true
	model.menuRect = tuiRect{x: 5, y: 3, w: 40, h: 12}
	model.menuOff = 1
	model.menuItems = make([]tuiMenuItem, 6)
	for index := range model.menuItems {
		model.menuItems[index] = tuiMenuItem{label: fmt.Sprintf("Item %d", index), action: "close-menu"}
	}

	model.handleMouse(tea.MouseMsg{
		X:      model.menuRect.x + 2,
		Y:      model.menuRect.y + 5,
		Action: tea.MouseActionMotion,
		Button: tea.MouseButtonNone,
	})

	if model.menuIndex != 3 {
		t.Fatalf("hover selected %d, want item 3", model.menuIndex)
	}

	model.handleMouse(tea.MouseMsg{
		X:      model.menuRect.x + 2,
		Y:      model.menuRect.y + 6,
		Action: tea.MouseActionMotion,
		Button: tea.MouseButtonRight,
	})

	if model.menuIndex != 4 {
		t.Fatalf("right-button hover selected %d, want item 4", model.menuIndex)
	}
	if !model.menuOpen {
		t.Fatal("right-button motion should not close the menu")
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
	model.menuFloating = true
	model.menuRect = tuiRect{x: 5, y: 5, w: 40, h: 12}
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
	if model.menuFloating {
		t.Fatal("expected right click close to clear floating menu placement")
	}
	if model.status != "Menu closed" {
		t.Fatalf("right click close status = %q, want Menu closed", model.status)
	}
}

func TestOverlayBlockPreservesCoveredRowSuffix(t *testing.T) {
	got := overlayBlock("abcdefghij\nklmnopqrst", "XX", 2, 0, 10)
	want := "abXXefghij\nklmnopqrst"
	if got != want {
		t.Fatalf("overlay result = %q, want %q", got, want)
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

	delete(model.neighborCache, targetID)
	model.focus = focusMembers
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated.(clusterBrowserModel)
	if len(model.neighborCache[targetID]) != 1 {
		t.Fatalf("enter did not load neighbors: %+v", model.neighborCache[targetID])
	}
	if model.focus != focusDetail {
		t.Fatalf("enter focus = %s, want detail", model.focus)
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
	lines := strings.Join(model.menuLines(80), "\n")
	if !strings.Contains(lines, "b back") {
		t.Fatalf("link picker footer missing back hint:\n%s", lines)
	}
}

func TestTUISubmenuBackKeyReturnsToActions(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.memberIndex = 0
	model.memberRows = []memberRow{{
		selectable: true,
		member: store.ClusterMemberDetail{
			BodySnippet: "See https://example.com/run.",
		},
	}}
	model.openReferenceLinkMenu("copy")

	updated, _ := model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}})
	model = updated.(clusterBrowserModel)

	if !model.menuOpen || model.menuTitle != "Actions" {
		t.Fatalf("back key menu=%v title=%q", model.menuOpen, model.menuTitle)
	}

	model.openReferenceLinkMenu("copy")
	updated, _ = model.updateMenu(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'a'}})
	model = updated.(clusterBrowserModel)
	if !model.menuOpen || model.menuTitle != "Actions" {
		t.Fatalf("action key from submenu menu=%v title=%q", model.menuOpen, model.menuTitle)
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
	if model.menuItems[model.menuIndex].value != "openclaw/one" {
		t.Fatalf("repository menu selected %q, want current repo", model.menuItems[model.menuIndex].value)
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

func TestTUICloseThreadLocallyHidesCluster(t *testing.T) {
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
	if err := seedTUICluster(ctx, st, repoID, 50, 500, "close me"); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20, Sort: "recent"})
	if err != nil {
		t.Fatalf("clusters: %v", err)
	}

	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		HideClosed: true,
		MinSize:    1,
		Clusters:   clusters,
	})
	model.openActionMenu()
	if menuLabelIndex(model.menuItems, "Close locally...") < 0 {
		t.Fatalf("action menu missing local close: %+v", model.menuItems)
	}
	model.runAction("close-thread-confirm")
	if model.menuTitle != "Close Locally" || !strings.Contains(model.menuItems[0].label, "Close #500 locally") {
		t.Fatalf("close confirmation menu = %q %+v", model.menuTitle, model.menuItems)
	}

	model.runAction("close-thread-local")

	if model.status != "Closed #500 locally" {
		t.Fatalf("close status = %q", model.status)
	}
	if len(model.payload.Clusters) != 0 {
		t.Fatalf("locally closed singleton cluster should be hidden, got %#v", model.payload.Clusters)
	}
	rows, err := st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("locally closed thread should be hidden, got %#v", rows)
	}
}

func TestTUIReopenThreadLocallyRestoresThread(t *testing.T) {
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
	if err := seedTUICluster(ctx, st, repoID, 51, 501, "reopen me"); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	if err := st.CloseThreadLocally(ctx, repoID, 501, "test close"); err != nil {
		t.Fatalf("close thread: %v", err)
	}
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 20, Sort: "recent"})
	if err != nil {
		t.Fatalf("clusters: %v", err)
	}

	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
		Clusters:   clusters,
	})
	model.openActionMenu()
	if menuLabelIndex(model.menuItems, "Reopen locally...") < 0 {
		t.Fatalf("action menu missing local reopen: %+v", model.menuItems)
	}
	if menuLabelIndex(model.menuItems, "Close locally...") >= 0 {
		t.Fatalf("locally closed thread should not offer close again: %+v", model.menuItems)
	}
	model.runAction("reopen-thread-confirm")
	if model.menuTitle != "Reopen Locally" || !strings.Contains(model.menuItems[0].label, "Reopen #501 locally") {
		t.Fatalf("reopen confirmation menu = %q %+v", model.menuTitle, model.menuItems)
	}

	model.runAction("reopen-thread-local")

	if model.status != "Reopened #501 locally" {
		t.Fatalf("reopen status = %q", model.status)
	}
	rows, err := st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(rows) != 1 || rows[0].ClosedAtLocal != "" {
		t.Fatalf("reopened thread should be visible, got %#v", rows)
	}
}

func TestTUICloseClusterLocallyHidesCluster(t *testing.T) {
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
	if err := seedTUICluster(ctx, st, repoID, 52, 502, "close cluster"); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20, Sort: "recent"})
	if err != nil {
		t.Fatalf("clusters: %v", err)
	}
	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		HideClosed: true,
		MinSize:    1,
		Clusters:   clusters,
	})
	model.openActionMenu()
	if menuLabelIndex(model.menuItems, "Close cluster locally...") < 0 {
		t.Fatalf("action menu missing cluster close: %+v", model.menuItems)
	}
	model.runAction("close-cluster-confirm")
	if model.menuTitle != "Close Cluster" || !strings.Contains(model.menuItems[0].label, "Close cluster C52 locally") {
		t.Fatalf("close cluster confirmation menu = %q %+v", model.menuTitle, model.menuItems)
	}

	model.runAction("close-cluster-local")

	if model.status != "Closed cluster C52 locally" {
		t.Fatalf("close cluster status = %q", model.status)
	}
	if len(model.payload.Clusters) != 0 {
		t.Fatalf("locally closed cluster should be hidden, got %#v", model.payload.Clusters)
	}
}

func TestTUIReopenClusterLocallyRestoresCluster(t *testing.T) {
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
	if err := seedTUICluster(ctx, st, repoID, 53, 503, "reopen cluster"); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	if err := st.CloseClusterLocally(ctx, repoID, 53, "test close"); err != nil {
		t.Fatalf("close cluster: %v", err)
	}
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 20, Sort: "recent"})
	if err != nil {
		t.Fatalf("clusters: %v", err)
	}
	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		MinSize:    1,
		Clusters:   clusters,
	})
	model.openActionMenu()
	if menuLabelIndex(model.menuItems, "Reopen cluster locally...") < 0 {
		t.Fatalf("action menu missing cluster reopen: %+v", model.menuItems)
	}
	if menuLabelIndex(model.menuItems, "Close cluster locally...") >= 0 {
		t.Fatalf("closed cluster should not offer close again: %+v", model.menuItems)
	}
	model.runAction("reopen-cluster-confirm")
	if model.menuTitle != "Reopen Cluster" || !strings.Contains(model.menuItems[0].label, "Reopen cluster C53 locally") {
		t.Fatalf("reopen cluster confirmation menu = %q %+v", model.menuTitle, model.menuItems)
	}

	model.runAction("reopen-cluster-local")

	if model.status != "Reopened cluster C53 locally" {
		t.Fatalf("reopen cluster status = %q", model.status)
	}
	clusters, err = st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20, Sort: "recent"})
	if err != nil {
		t.Fatalf("list reopened clusters: %v", err)
	}
	if len(clusters) != 1 || clusters[0].ClosedAt != "" {
		t.Fatalf("reopened cluster should be visible, got %#v", clusters)
	}
}

func TestTUIClusterMemberOverrideActions(t *testing.T) {
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
	firstID, secondID, err := seedTUIClusterPair(ctx, st, repoID, 54, 540, 541)
	if err != nil {
		t.Fatalf("seed cluster pair: %v", err)
	}
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20, Sort: "recent"})
	if err != nil {
		t.Fatalf("clusters: %v", err)
	}
	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		HideClosed: true,
		MinSize:    1,
		Clusters:   clusters,
	})
	model.openActionMenu()
	if menuLabelIndex(model.menuItems, "Exclude #540 from C54...") < 0 {
		t.Fatalf("action menu missing member exclude: %+v", model.menuItems)
	}
	if menuLabelIndex(model.menuItems, "Set #540 as canonical...") < 0 {
		t.Fatalf("action menu missing canonical action: %+v", model.menuItems)
	}
	model.runAction("exclude-member-confirm")
	if model.menuTitle != "Exclude Member" || !strings.Contains(model.menuItems[0].label, "Exclude #540 from C54") {
		t.Fatalf("exclude member confirmation menu = %q %+v", model.menuTitle, model.menuItems)
	}

	model.runAction("exclude-member-local")

	if model.status != "Excluded #540 from C54 locally" {
		t.Fatalf("exclude status = %q", model.status)
	}
	if len(model.memberRows) < 2 || model.memberRows[1].thread().Number != 541 {
		t.Fatalf("excluded member should be hidden while closed rows are hidden: %#v", model.memberRows)
	}
	detail, err := st.ClusterDetail(ctx, store.ClusterDetailOptions{RepoID: repoID, ClusterID: 54, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("detail after exclude: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != secondID {
		t.Fatalf("representative should refresh after excluding first member: %#v", detail.Cluster)
	}

	model.showClosed = true
	model.refreshFromStore()
	model.memberIndex = memberRowIndex(model.memberRows, 540)
	model.openActionMenu()
	if menuLabelIndex(model.menuItems, "Include #540 in C54...") < 0 {
		t.Fatalf("action menu missing member include: %+v", model.menuItems)
	}
	model.runAction("include-member-confirm")
	if model.menuTitle != "Include Member" || !strings.Contains(model.menuItems[0].label, "Include #540 in C54") {
		t.Fatalf("include member confirmation menu = %q %+v", model.menuTitle, model.menuItems)
	}
	model.runAction("include-member-local")
	if model.status != "Included #540 in C54 locally" {
		t.Fatalf("include status = %q", model.status)
	}
	model.memberIndex = memberRowIndex(model.memberRows, 541)
	model.runAction("canonical-member-confirm")
	if model.menuTitle != "Canonical Member" || !strings.Contains(model.menuItems[0].label, "Set #541 as canonical for C54") {
		t.Fatalf("canonical confirmation menu = %q %+v", model.menuTitle, model.menuItems)
	}
	model.runAction("canonical-member-local")
	if model.status != "Set #541 as canonical for C54" {
		t.Fatalf("canonical status = %q", model.status)
	}
	detail, err = st.ClusterDetail(ctx, store.ClusterDetailOptions{RepoID: repoID, ClusterID: 54, IncludeClosed: false, MemberLimit: 10})
	if err != nil {
		t.Fatalf("detail after canonical: %v", err)
	}
	if detail.Cluster.RepresentativeThreadID != secondID || detail.Members[0].Thread.ID != secondID || detail.Members[0].Role != "canonical" || detail.Members[1].Thread.ID != firstID {
		t.Fatalf("canonical member should sort first and become representative: %#v", detail)
	}
}

func TestTUIRepositoryPickerKeepsCurrentRepoVisible(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var currentRepoID int64
	for index := 0; index < 6; index++ {
		fullName := fmt.Sprintf("openclaw/repo-%d", index)
		repoID, err := st.UpsertRepository(ctx, store.Repository{
			Owner:     "openclaw",
			Name:      fmt.Sprintf("repo-%d", index),
			FullName:  fullName,
			RawJSON:   "{}",
			UpdatedAt: fmt.Sprintf("2026-04-27T0%d:00:00Z", index),
		})
		if err != nil {
			t.Fatalf("repo %d: %v", index, err)
		}
		if index == 0 {
			currentRepoID = repoID
		}
	}

	model := newClusterBrowserModel(ctx, st, currentRepoID, clusterBrowserPayload{
		Repository: "openclaw/repo-0",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.detailView.Height = 6
	model.openRepositoryMenu()

	visible := model.menuVisibleCount()
	if model.menuIndex < model.menuOff || model.menuIndex >= model.menuOff+visible {
		t.Fatalf("current repo index %d outside visible window [%d,%d)", model.menuIndex, model.menuOff, model.menuOff+visible)
	}
	if model.menuItems[model.menuIndex].value != "openclaw/repo-0" {
		t.Fatalf("repository menu selected %q, want current repo", model.menuItems[model.menuIndex].value)
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

func TestTUILocallyClosedMembersUseLocalState(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	member := store.ClusterMemberDetail{Thread: store.Thread{
		Number:           43,
		Kind:             "issue",
		State:            "open",
		Title:            "Locally closed bug",
		HTMLURL:          "https://github.com/openclaw/openclaw/issues/43",
		ClosedAtLocal:    "2026-04-27T00:00:00Z",
		CloseReasonLocal: "TUI manual close",
	}}
	model.memberRows = []memberRow{{selectable: true, member: member}}
	if got := model.memberTableRows()[0][1]; got != "loc" {
		t.Fatalf("member table state = %q, want loc", got)
	}
	if got := model.memberListClipboardText(); !strings.Contains(got, "#43 [local]") {
		t.Fatalf("member clipboard should show local state: %q", got)
	}

	model.showClosed = false
	model.detail = store.ClusterDetail{Members: []store.ClusterMemberDetail{member}}
	model.sortMembers()
	if len(model.memberRows) != 0 {
		t.Fatalf("locally closed member should be hidden when closed rows are hidden: %#v", model.memberRows)
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

func TestTUIAutoRefreshIsQuietUntilClustersChange(t *testing.T) {
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
	if err := seedTUICluster(ctx, st, repoID, 40, 400, "first cluster"); err != nil {
		t.Fatalf("seed first cluster: %v", err)
	}
	clusters, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 20, Sort: "recent"})
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}

	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   clusters,
	})
	model.status = "Reading detail"
	model.detailCache[40] = store.ClusterDetail{Cluster: clusters[0]}
	model.autoRefreshFromStore()
	if model.status != "Reading detail" {
		t.Fatalf("unchanged auto refresh status = %q", model.status)
	}
	if _, ok := model.detailCache[40]; !ok {
		t.Fatal("unchanged auto refresh should not clear detail cache")
	}

	if err := seedTUICluster(ctx, st, repoID, 41, 401, "second cluster"); err != nil {
		t.Fatalf("seed second cluster: %v", err)
	}
	model.autoRefreshFromStore()
	if model.status != "Auto refreshed 2 cluster(s)" {
		t.Fatalf("changed auto refresh status = %q", model.status)
	}
}

func TestTUIAutoRefreshPreservesUnboundedViewport(t *testing.T) {
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
	for i := 0; i < 35; i++ {
		clusterID := int64(100 + i)
		if err := seedTUICluster(ctx, st, repoID, clusterID, 1000+i, fmt.Sprintf("cluster %02d", i)); err != nil {
			t.Fatalf("seed cluster %d: %v", clusterID, err)
		}
	}
	clusters, err := st.ListDisplayClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: false, MinSize: 1, Limit: 0, Sort: "recent"})
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}
	if len(clusters) <= 20 {
		t.Fatalf("seeded viewport has %d clusters, want more than refresh floor", len(clusters))
	}

	model := newClusterBrowserModel(ctx, st, repoID, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   clusters,
	})
	if err := seedTUICluster(ctx, st, repoID, 200, 2000, "new refresh cluster"); err != nil {
		t.Fatalf("seed new cluster: %v", err)
	}

	model.autoRefreshFromStore()

	if len(model.payload.Clusters) <= 20 {
		t.Fatalf("auto refresh collapsed viewport to %d clusters", len(model.payload.Clusters))
	}
	if len(model.payload.Clusters) != 36 {
		t.Fatalf("auto refresh clusters = %d, want 36", len(model.payload.Clusters))
	}
	if model.status != "Auto refreshed 36 cluster(s)" {
		t.Fatalf("auto refresh status = %q", model.status)
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
	if len(rows) != 1 || !strings.Contains(rows[0][4], "No clusters visible") {
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

func captureOpenURL(t *testing.T) (func(), *[]string) {
	t.Helper()
	previous := openURL
	opened := []string{}
	openURL = func(url string) error {
		opened = append(opened, url)
		return nil
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		openURL = previous
		restored = true
	}
	t.Cleanup(restore)
	return restore, &opened
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

func seedTUIDurableStore(t *testing.T) (*store.Store, int64, int64) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-30T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	firstID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "201",
		Number:          201,
		Kind:            "issue",
		State:           "open",
		Title:           "First member",
		Body:            "First body",
		HTMLURL:         "https://github.com/openclaw/openclaw/issues/201",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "hash-201",
		UpdatedAtGitHub: "2026-04-30T01:00:00Z",
		UpdatedAt:       "2026-04-30T01:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed first thread: %v", err)
	}
	secondID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "202",
		Number:          202,
		Kind:            "pull_request",
		State:           "open",
		Title:           "Second member",
		Body:            "Second body",
		HTMLURL:         "https://github.com/openclaw/openclaw/pull/202",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "hash-202",
		UpdatedAtGitHub: "2026-04-30T02:00:00Z",
		UpdatedAt:       "2026-04-30T02:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed second thread: %v", err)
	}
	result, err := st.SaveDurableClusters(ctx, repoID, []store.DurableClusterInput{{
		StableKey:              "local-actions-key",
		StableSlug:             "local-actions",
		ClusterType:            "duplicate_candidate",
		RepresentativeThreadID: firstID,
		Title:                  "Local actions",
		Members: []store.DurableClusterMemberInput{
			{ThreadID: firstID, Role: "canonical"},
			{ThreadID: secondID, Role: "related"},
		},
	}})
	if err != nil {
		t.Fatalf("save durable clusters: %v", err)
	}
	summaries, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repoID, IncludeClosed: true, MinSize: 1, Limit: 10})
	if err != nil {
		t.Fatalf("list durable clusters: %v", err)
	}
	if len(summaries) != result.ClusterCount || len(summaries) == 0 {
		t.Fatalf("durable summaries = %+v result=%+v", summaries, result)
	}
	return st, repoID, summaries[0].ID
}

func menuLabelIndex(items []tuiMenuItem, label string) int {
	for index, item := range items {
		if item.label == label {
			return index
		}
	}
	return -1
}

func memberRowIndex(rows []memberRow, number int) int {
	for index, row := range rows {
		if row.selectable && row.thread().Number == number {
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

func menuLabels(items []tuiMenuItem) []string {
	labels := make([]string, 0, len(items))
	for _, item := range items {
		labels = append(labels, item.label)
	}
	return labels
}

func seedTUIClusterPair(ctx context.Context, st *store.Store, repoID, clusterID int64, firstNumber, secondNumber int) (int64, int64, error) {
	firstID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      fmt.Sprintf("%d", firstNumber),
		Number:        firstNumber,
		Kind:          "issue",
		State:         "open",
		Title:         fmt.Sprintf("member %d", firstNumber),
		HTMLURL:       fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", firstNumber),
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   fmt.Sprintf("cluster-pair-hash-%d", firstNumber),
		UpdatedAt:     "2026-04-27T00:00:00Z",
	})
	if err != nil {
		return 0, 0, err
	}
	secondID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:        repoID,
		GitHubID:      fmt.Sprintf("%d", secondNumber),
		Number:        secondNumber,
		Kind:          "issue",
		State:         "open",
		Title:         fmt.Sprintf("member %d", secondNumber),
		HTMLURL:       fmt.Sprintf("https://github.com/openclaw/openclaw/issues/%d", secondNumber),
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   fmt.Sprintf("cluster-pair-hash-%d", secondNumber),
		UpdatedAt:     "2026-04-27T00:00:00Z",
	})
	if err != nil {
		return 0, 0, err
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, representative_thread_id, title, created_at, updated_at)
		values(?, ?, ?, ?, 'active', ?, ?, '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, clusterID, repoID, fmt.Sprintf("cluster-%d", clusterID), fmt.Sprintf("repo-%d", clusterID), firstID, fmt.Sprintf("cluster %d", clusterID)); err != nil {
		return 0, 0, err
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(?, ?, 'representative', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, clusterID, firstID); err != nil {
		return 0, 0, err
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at)
		values(?, ?, 'member', 'active', 'system', '{}', '2026-04-27T00:00:00Z', '2026-04-27T00:00:00Z')
	`, clusterID, secondID); err != nil {
		return 0, 0, err
	}
	return firstID, secondID, nil
}
