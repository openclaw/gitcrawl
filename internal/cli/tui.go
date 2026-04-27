package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
	"github.com/openclaw/gitcrawl/internal/store"
)

type clusterBrowserPayload struct {
	Repository         string                 `json:"repository"`
	InferredRepository bool                   `json:"inferred_repository"`
	Mode               string                 `json:"mode"`
	Sort               string                 `json:"sort"`
	Clusters           []store.ClusterSummary `json:"clusters"`
}

type tuiFocus string

const (
	focusClusters tuiFocus = "clusters"
	focusMembers  tuiFocus = "members"
	focusDetail   tuiFocus = "detail"
)

type tuiMemberSort string

const (
	memberSortKind   tuiMemberSort = "kind"
	memberSortRecent tuiMemberSort = "recent"
	memberSortNumber tuiMemberSort = "number"
	memberSortState  tuiMemberSort = "state"
	memberSortTitle  tuiMemberSort = "title"
)

type tuiRect struct {
	x int
	y int
	w int
	h int
}

type clusterBrowserModel struct {
	payload      clusterBrowserPayload
	allClusters  []store.ClusterSummary
	ctx          context.Context
	store        *store.Store
	repoID       int64
	focus        tuiFocus
	width        int
	height       int
	status       string
	search       string
	searching    bool
	showHelp     bool
	menuOpen     bool
	menuIndex    int
	menuItems    []tuiMenuItem
	showClosed   bool
	minSize      int
	memberSort   tuiMemberSort
	selected     int
	clusterOff   int
	memberRows   []memberRow
	memberOff    int
	memberIndex  int
	detailScroll int
	clusterTable table.Model
	memberTable  table.Model
	detailView   viewport.Model
	searchInput  textinput.Model
	detailCache  map[int64]store.ClusterDetail
	detail       store.ClusterDetail
	hasDetail    bool
}

type memberRow struct {
	member store.ClusterMemberDetail
}

type tuiMenuItem struct {
	label  string
	action string
}

func (a *App) canRunInteractiveTUI() bool {
	out, ok := a.Stdout.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(out.Fd()) && isatty.IsTerminal(os.Stdin.Fd())
}

func (a *App) runInteractiveTUI(ctx context.Context, st *store.Store, repoID int64, payload clusterBrowserPayload) error {
	out, ok := a.Stdout.(*os.File)
	if !ok {
		return a.writeOutput("tui", payload, true)
	}
	model := newClusterBrowserModel(ctx, st, repoID, payload)
	program := tea.NewProgram(model, tea.WithInput(os.Stdin), tea.WithOutput(out), tea.WithAltScreen(), tea.WithMouseCellMotion())
	_, err := program.Run()
	return err
}

func newClusterBrowserModel(ctx context.Context, st *store.Store, repoID int64, payload clusterBrowserPayload) clusterBrowserModel {
	clusters := append([]store.ClusterSummary(nil), payload.Clusters...)
	payload.Clusters = clusters
	search := textinput.New()
	search.Prompt = "/ "
	search.Placeholder = "filter clusters"
	search.CharLimit = 80
	search.Width = 40
	model := clusterBrowserModel{
		payload:      payload,
		allClusters:  clusters,
		ctx:          ctx,
		store:        st,
		repoID:       repoID,
		focus:        focusClusters,
		status:       "Ready",
		showClosed:   true,
		minSize:      1,
		memberSort:   memberSortKind,
		memberIndex:  -1,
		clusterTable: newTUITable(),
		memberTable:  newTUITable(),
		detailView:   viewport.New(1, 1),
		searchInput:  search,
		detailCache:  map[int64]store.ClusterDetail{},
	}
	model.applyClusterFilters()
	model.loadSelectedCluster()
	return model
}

func (m clusterBrowserModel) Init() tea.Cmd {
	return nil
}

func (m clusterBrowserModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.syncComponents()
		m.keepVisible()
	case tea.KeyMsg:
		if m.menuOpen {
			return m.updateMenu(msg)
		}
		if m.searching {
			var cmd tea.Cmd
			m, cmd = m.handleSearchKey(msg)
			m.keepVisible()
			return m, cmd
		}
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab", "right":
			m.focus = nextFocus(m.focus, 1)
		case "shift+tab", "left":
			m.focus = nextFocus(m.focus, -1)
		case "up", "k":
			m.move(-1)
		case "down", "j":
			m.move(1)
		case "pgup", "ctrl+b":
			m.move(-m.pageStep())
		case "pgdown", "ctrl+f":
			m.move(m.pageStep())
		case "home", "g":
			m.jumpEdge(false)
		case "end", "G":
			m.jumpEdge(true)
		case "enter":
			if m.focus == focusClusters {
				m.focus = focusMembers
			} else if m.focus == focusMembers {
				m.focus = focusDetail
			}
		case "o":
			m.runAction("open")
		case "c":
			m.runAction("copy-url")
		case "s":
			if m.payload.Sort == "recent" {
				m.payload.Sort = "size"
			} else {
				m.payload.Sort = "recent"
			}
			m.sortClusters()
			m.loadSelectedCluster()
			m.status = "Sort: " + m.payload.Sort
		case "m":
			m.memberSort = nextMemberSort(m.memberSort)
			m.sortMembers()
			m.status = "Member sort: " + string(m.memberSort)
		case "f":
			m.minSize = nextMinSize(m.minSize)
			m.applyClusterFilters()
			m.status = fmt.Sprintf("Min size: %s", minSizeLabel(m.minSize))
		case "x":
			m.showClosed = !m.showClosed
			m.applyClusterFilters()
			if m.showClosed {
				m.status = "Showing closed clusters"
			} else {
				m.status = "Hiding closed clusters"
			}
		case "/":
			m.searching = true
			m.showHelp = false
			m.menuOpen = false
			m.searchInput.SetValue(m.search)
			cmd := m.searchInput.Focus()
			m.status = "Filter: " + m.search
			return m, cmd
		case "esc":
			if m.showHelp {
				m.showHelp = false
			}
		case "h", "?":
			m.showHelp = !m.showHelp
			if m.showHelp {
				m.status = "Help"
			} else {
				m.status = "Ready"
			}
		}
		m.keepVisible()
		m.syncComponents()
	case tea.MouseMsg:
		m.handleMouse(msg)
		m.keepVisible()
		m.syncComponents()
	}
	return m, nil
}

func (m clusterBrowserModel) View() string {
	if m.width <= 0 || m.height <= 0 {
		return "loading gitcrawl tui..."
	}
	layout := m.layout()
	m.syncComponents()
	header := m.renderHeader(layout.header.w)
	clusters := m.renderClusters(layout.clusters)
	members := m.renderMembers(layout.members)
	detail := m.renderDetail(layout.detail)
	footer := m.renderFooter(layout.footer.w)
	body := lipgloss.JoinHorizontal(lipgloss.Top, clusters, members, detail)
	if layout.stacked {
		if layout.members.x == 0 {
			body = lipgloss.JoinVertical(lipgloss.Left, clusters, members, detail)
		} else {
			top := lipgloss.JoinHorizontal(lipgloss.Top, clusters, members)
			body = lipgloss.JoinVertical(lipgloss.Left, top, detail)
		}
	}
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

type tuiLayout struct {
	header   tuiRect
	clusters tuiRect
	members  tuiRect
	detail   tuiRect
	footer   tuiRect
	stacked  bool
}

func (m clusterBrowserModel) layout() tuiLayout {
	width := maxInt(m.width, 80)
	height := maxInt(m.height, 24)
	headerH := 2
	footerH := 2
	bodyH := maxInt(8, height-headerH-footerH)
	layout := tuiLayout{
		header: tuiRect{x: 0, y: 0, w: width, h: headerH},
		footer: tuiRect{x: 0, y: headerH + bodyH, w: width, h: footerH},
	}
	if width >= 118 {
		clusterW := minInt(50, maxInt(36, width*36/100))
		memberW := minInt(42, maxInt(30, width*28/100))
		detailW := maxInt(32, width-clusterW-memberW)
		layout.clusters = tuiRect{x: 0, y: headerH, w: clusterW, h: bodyH}
		layout.members = tuiRect{x: clusterW, y: headerH, w: memberW, h: bodyH}
		layout.detail = tuiRect{x: clusterW + memberW, y: headerH, w: detailW, h: bodyH}
		return layout
	}
	if width < 100 {
		layout.stacked = true
		clusterH := maxInt(7, bodyH*36/100)
		memberH := maxInt(6, bodyH*28/100)
		detailH := maxInt(6, bodyH-clusterH-memberH)
		layout.clusters = tuiRect{x: 0, y: headerH, w: width, h: clusterH}
		layout.members = tuiRect{x: 0, y: headerH + clusterH, w: width, h: memberH}
		layout.detail = tuiRect{x: 0, y: headerH + clusterH + memberH, w: width, h: detailH}
		return layout
	}
	layout.stacked = true
	topH := maxInt(8, bodyH/2)
	bottomH := bodyH - topH
	clusterW := width / 2
	layout.clusters = tuiRect{x: 0, y: headerH, w: clusterW, h: topH}
	layout.members = tuiRect{x: clusterW, y: headerH, w: width - clusterW, h: topH}
	layout.detail = tuiRect{x: 0, y: headerH + topH, w: width, h: bottomH}
	return layout
}

func (m clusterBrowserModel) renderHeader(width int) string {
	openCounts := m.openCounts()
	repoLine := fmt.Sprintf("%s  %d PR  %d issues  clusters:%d  sort:%s  members:%s  min:%s  closed:%s  filter:%s",
		m.payload.Repository,
		openCounts.pulls,
		openCounts.issues,
		len(m.payload.Clusters),
		m.payload.Sort,
		m.memberSort,
		minSizeLabel(m.minSize),
		boolLabel(m.showClosed),
		firstNonEmpty(m.search, "none"),
	)
	if m.payload.InferredRepository {
		repoLine += "  inferred"
	}
	style := lipgloss.NewStyle().Width(width).Height(2).Background(lipgloss.Color("#0d1321")).Foreground(lipgloss.Color("#f7f7ff")).Padding(0, 1)
	return style.Render(lipgloss.JoinVertical(lipgloss.Left, bold(repoLine), dim("local SQLite cluster browser")))
}

func (m clusterBrowserModel) renderFooter(width int) string {
	controls := "Tab focus  click select  wheel scroll  / filter  s sort  m members  f min  x closed  ? help  q quit"
	line := firstNonEmpty(m.status, "Ready")
	if m.searching {
		line = "Filter: " + m.searchInput.View()
	}
	return lipgloss.NewStyle().Width(width).Height(2).Background(lipgloss.Color("#5bc0eb")).Foreground(lipgloss.Color("#05070d")).Padding(0, 1).Render(truncateCells(line, width-2) + "\n" + truncateCells(controls, maxInt(1, width-2)))
}

func (m clusterBrowserModel) renderClusters(rect tuiRect) string {
	return paneStyle(focusClusters, m.focus, rect.w, rect.h).Render(lipgloss.JoinVertical(lipgloss.Left, paneTitle(focusClusters, m.focus), m.clusterTable.View()))
}

func (m clusterBrowserModel) renderMembers(rect tuiRect) string {
	return paneStyle(focusMembers, m.focus, rect.w, rect.h).Render(lipgloss.JoinVertical(lipgloss.Left, paneTitle(focusMembers, m.focus), m.memberTable.View()))
}

func (m clusterBrowserModel) renderDetail(rect tuiRect) string {
	lines := append([]string{paneTitle(focusDetail, m.focus)}, m.detailLines(rect.w-4)...)
	if m.showHelp {
		lines = append([]string{paneTitle(focusDetail, m.focus)}, m.helpLines(rect.w-4)...)
	}
	if m.menuOpen {
		lines = append([]string{paneTitle(focusDetail, m.focus)}, m.menuLines(rect.w-4)...)
	}
	m.detailView.SetContent(strings.Join(lines, "\n"))
	return paneStyle(focusDetail, m.focus, rect.w, rect.h).Render(m.detailView.View())
}

func (m clusterBrowserModel) detailLines(width int) []string {
	if len(m.payload.Clusters) == 0 {
		return []string{"No clusters visible in this view.", "", "Run sync/embed/cluster, then reopen the TUI."}
	}
	cluster := m.payload.Clusters[m.selected]
	lines := []string{
		bold(fmt.Sprintf("Cluster %d", cluster.ID)),
		color("#5bc0eb", cluster.StableSlug),
	}
	lines = append(lines, wrapPlain(firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster"), width)...)
	lines = append(lines,
		"",
		fmt.Sprintf("members: %d   status: %s   updated: %s", cluster.MemberCount, firstNonEmpty(cluster.Status, "unknown"), formatRelativeTime(cluster.UpdatedAt)),
		fmt.Sprintf("representative: %s", threadRef(cluster)),
		"",
	)
	if !m.hasDetail {
		lines = append(lines, "Cluster details unavailable.", m.status)
		return lines
	}
	if len(m.memberRows) == 0 {
		lines = append(lines, "Select a cluster to inspect members.")
		return lines
	}
	member := m.memberRows[clampInt(m.memberIndex, 0, len(m.memberRows)-1)]
	lines = append(lines,
		bold(fmt.Sprintf("%s #%d", kindTitle(member.thread().Kind), member.thread().Number)),
	)
	lines = append(lines, wrapPlain(member.thread().Title, width)...)
	lines = append(lines,
		"",
		fmt.Sprintf("state: %s   updated: %s   author: %s", member.thread().State, formatRelativeTime(member.thread().UpdatedAtGitHub), firstNonEmpty(member.thread().AuthorLogin, "unknown")),
		fmt.Sprintf("url: %s", member.thread().HTMLURL),
		"",
	)
	if labels := labelsFromJSON(member.thread().LabelsJSON); labels != "" {
		lines = append(lines, "labels: "+labels, "")
	}
	if len(member.member.Summaries) > 0 {
		lines = append(lines, bold("LLM Summary"))
		for _, key := range sortedSummaryKeys(member.member.Summaries) {
			lines = append(lines, dim(key+":"))
			lines = append(lines, markdownLines(member.member.Summaries[key], width)...)
			lines = append(lines, "")
		}
	}
	if strings.TrimSpace(member.member.BodySnippet) != "" {
		lines = append(lines, bold("Main Preview"))
		lines = append(lines, markdownLines(member.member.BodySnippet, width)...)
	}
	return lines
}

func (m clusterBrowserModel) helpLines(width int) []string {
	lines := []string{
		bold("Gitcrawl TUI"),
		"",
		"Mouse",
		"  left click: focus/select a pane row",
		"  wheel: scroll the pane under the pointer",
		"  right click: open a stable action menu",
		"",
		"Keyboard",
		"  Tab / Shift-Tab: cycle focus",
		"  arrows or j/k: move selection or scroll detail",
		"  PageUp/PageDown: page the active pane",
		"  Enter: drill into the next pane",
		"  /: filter clusters",
		"  s: toggle cluster sort",
		"  m: cycle member sort",
		"  f: cycle minimum cluster size",
		"  x: show/hide closed clusters",
		"  o: open selected thread in browser",
		"  c: copy selected thread URL",
		"  ?: toggle this help",
		"  q: quit",
		"",
		"This Go TUI intentionally avoids ghcrawl's old fragile right-click popover.",
	}
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "  ") {
			out = append(out, line)
			continue
		}
		out = append(out, wrapPlain(line, width)...)
	}
	return out
}

func (m clusterBrowserModel) menuLines(width int) []string {
	lines := []string{bold("Actions"), ""}
	for index, item := range m.menuItems {
		prefix := "  "
		if index == m.menuIndex {
			prefix = "> "
		}
		lines = append(lines, truncateCells(prefix+item.label, width))
	}
	lines = append(lines, "", dim("Enter run  Esc close"))
	return lines
}

func (m clusterBrowserModel) updateMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.menuOpen = false
		m.status = "Menu closed"
	case "up", "k":
		m.menuIndex = clampInt(m.menuIndex-1, 0, maxInt(0, len(m.menuItems)-1))
	case "down", "j":
		m.menuIndex = clampInt(m.menuIndex+1, 0, maxInt(0, len(m.menuItems)-1))
	case "enter":
		if m.menuIndex >= 0 && m.menuIndex < len(m.menuItems) {
			m.runAction(m.menuItems[m.menuIndex].action)
		}
		m.menuOpen = false
	}
	return m, nil
}

func (m *clusterBrowserModel) move(delta int) {
	if m.focus == focusDetail {
		if delta > 0 {
			m.detailView.LineDown(delta)
		} else {
			m.detailView.LineUp(-delta)
		}
		return
	}
	if m.focus == focusMembers {
		if len(m.memberRows) == 0 {
			return
		}
		m.memberIndex = clampInt(m.memberIndex+delta, 0, len(m.memberRows)-1)
		m.status = fmt.Sprintf("Selected #%d", m.memberRows[m.memberIndex].thread().Number)
		return
	}
	if len(m.payload.Clusters) == 0 {
		return
	}
	m.selected = clampInt(m.selected+delta, 0, len(m.payload.Clusters)-1)
	m.loadSelectedCluster()
	m.status = fmt.Sprintf("Cluster %d", m.payload.Clusters[m.selected].ID)
}

func (m clusterBrowserModel) handleSearchKey(msg tea.KeyMsg) (clusterBrowserModel, tea.Cmd) {
	switch msg.String() {
	case "enter":
		m.searching = false
		m.search = m.searchInput.Value()
		m.searchInput.Blur()
		m.applyClusterFilters()
		if m.search == "" {
			m.status = "Filter cleared"
		} else {
			m.status = "Filter: " + m.search
		}
	case "esc":
		m.searching = false
		m.searchInput.Blur()
		m.status = "Filter cancelled"
	default:
		var cmd tea.Cmd
		m.searchInput, cmd = m.searchInput.Update(msg)
		m.search = m.searchInput.Value()
		m.applyClusterFilters()
		return m, cmd
	}
	return m, nil
}

func (m *clusterBrowserModel) handleMouse(msg tea.MouseMsg) {
	layout := m.layout()
	if msg.Button != tea.MouseButtonLeft && msg.Button != tea.MouseButtonRight && !isMouseWheel(msg.Button) {
		return
	}
	switch msg.Button {
	case tea.MouseButtonWheelUp:
		m.mouseWheel(layout, msg, -3)
	case tea.MouseButtonWheelDown:
		m.mouseWheel(layout, msg, 3)
	case tea.MouseButtonLeft:
		if msg.Action != tea.MouseActionPress {
			return
		}
		switch {
		case layout.clusters.contains(msg.X, msg.Y):
			m.focus = focusClusters
			row := msg.Y - layout.clusters.y - 3
			if row < 0 {
				return
			}
			index := m.clusterOff + row
			if index >= 0 && index < len(m.payload.Clusters) {
				m.selected = index
				m.loadSelectedCluster()
				m.status = fmt.Sprintf("Cluster %d", m.payload.Clusters[m.selected].ID)
			}
		case layout.members.contains(msg.X, msg.Y):
			m.focus = focusMembers
			row := msg.Y - layout.members.y - 3
			if row < 0 {
				return
			}
			index := m.memberOff + row
			if index >= 0 && index < len(m.memberRows) {
				m.memberIndex = index
				m.status = fmt.Sprintf("Selected #%d", m.memberRows[m.memberIndex].thread().Number)
			}
		case layout.detail.contains(msg.X, msg.Y):
			m.focus = focusDetail
		}
	case tea.MouseButtonRight:
		if msg.Action != tea.MouseActionPress {
			return
		}
		m.selectByMousePosition(layout, msg.X, msg.Y)
		m.openActionMenu()
	}
}

func (m *clusterBrowserModel) selectByMousePosition(layout tuiLayout, x, y int) {
	switch {
	case layout.clusters.contains(x, y):
		m.focus = focusClusters
		row := y - layout.clusters.y - 3
		if row >= 0 {
			index := m.clusterOff + row
			if index >= 0 && index < len(m.payload.Clusters) {
				m.selected = index
				m.loadSelectedCluster()
			}
		}
	case layout.members.contains(x, y):
		m.focus = focusMembers
		row := y - layout.members.y - 3
		if row >= 0 {
			index := m.memberOff + row
			if index >= 0 && index < len(m.memberRows) {
				m.memberIndex = index
			}
		}
	case layout.detail.contains(x, y):
		m.focus = focusDetail
	}
}

func (m *clusterBrowserModel) openActionMenu() {
	m.menuItems = []tuiMenuItem{
		{label: "Open selected thread", action: "open"},
		{label: "Copy selected URL", action: "copy-url"},
		{label: "Copy markdown link", action: "copy-markdown"},
		{label: "Close menu", action: "close-menu"},
	}
	m.menuIndex = 0
	m.menuOpen = true
	m.showHelp = false
	m.status = "Action menu"
}

func (m *clusterBrowserModel) runAction(action string) {
	if action == "close-menu" {
		m.status = "Menu closed"
		return
	}
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return
	}
	switch action {
	case "open":
		if err := openURL(thread.HTMLURL); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Opened " + thread.HTMLURL
		}
	case "copy-url":
		if err := copyText(thread.HTMLURL); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied URL"
		}
	case "copy-markdown":
		link := fmt.Sprintf("[#%d %s](%s)", thread.Number, thread.Title, thread.HTMLURL)
		if err := copyText(link); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied markdown link"
		}
	case "close-menu":
		m.status = "Menu closed"
	}
}

func isMouseWheel(button tea.MouseButton) bool {
	return button == tea.MouseButtonWheelUp || button == tea.MouseButtonWheelDown || button == tea.MouseButtonWheelLeft || button == tea.MouseButtonWheelRight
}

func (m *clusterBrowserModel) mouseWheel(layout tuiLayout, msg tea.MouseMsg, delta int) {
	switch {
	case layout.clusters.contains(msg.X, msg.Y):
		m.focus = focusClusters
		m.move(delta)
	case layout.members.contains(msg.X, msg.Y):
		m.focus = focusMembers
		m.move(delta)
	case layout.detail.contains(msg.X, msg.Y):
		m.focus = focusDetail
		m.move(delta)
	default:
		m.move(delta)
	}
}

func (m *clusterBrowserModel) jumpEdge(end bool) {
	if m.focus == focusDetail {
		if end {
			m.detailView.GotoBottom()
		} else {
			m.detailView.GotoTop()
		}
		return
	}
	if m.focus == focusMembers && len(m.memberRows) > 0 {
		if end {
			m.memberIndex = len(m.memberRows) - 1
		} else {
			m.memberIndex = 0
		}
		return
	}
	if len(m.payload.Clusters) > 0 {
		if end {
			m.selected = len(m.payload.Clusters) - 1
		} else {
			m.selected = 0
		}
		m.loadSelectedCluster()
	}
}

func (r tuiRect) contains(x, y int) bool {
	return x >= r.x && x < r.x+r.w && y >= r.y && y < r.y+r.h
}

func (m *clusterBrowserModel) keepVisible() {
	clusterRows := maxInt(1, m.layout().clusters.h-5)
	if m.selected < m.clusterOff {
		m.clusterOff = m.selected
	}
	if m.selected >= m.clusterOff+clusterRows {
		m.clusterOff = m.selected - clusterRows + 1
	}
	m.clusterOff = maxInt(0, m.clusterOff)
	memberRows := maxInt(1, m.layout().members.h-5)
	if m.memberIndex < m.memberOff {
		m.memberOff = m.memberIndex
	}
	if m.memberIndex >= m.memberOff+memberRows {
		m.memberOff = m.memberIndex - memberRows + 1
	}
	m.memberOff = maxInt(0, m.memberOff)
}

func (m *clusterBrowserModel) syncComponents() {
	layout := m.layout()
	clusterW := maxInt(24, layout.clusters.w-4)
	memberW := maxInt(24, layout.members.w-4)
	detailW := maxInt(24, layout.detail.w-4)
	detailH := maxInt(2, layout.detail.h-4)

	m.clusterTable.SetWidth(clusterW)
	m.clusterTable.SetHeight(maxInt(1, layout.clusters.h-6))
	m.clusterTable.SetColumns(clusterColumns(clusterW))
	m.clusterTable.SetRows(m.clusterRows())
	m.clusterTable.SetCursor(clampInt(m.selected, 0, maxInt(0, len(m.payload.Clusters)-1)))
	if m.focus == focusClusters {
		m.clusterTable.Focus()
	} else {
		m.clusterTable.Blur()
	}

	m.memberTable.SetWidth(memberW)
	m.memberTable.SetHeight(maxInt(1, layout.members.h-6))
	m.memberTable.SetColumns(memberColumns(memberW))
	m.memberTable.SetRows(m.memberTableRows())
	m.memberTable.SetCursor(clampInt(m.memberIndex, 0, maxInt(0, len(m.memberRows)-1)))
	if m.focus == focusMembers {
		m.memberTable.Focus()
	} else {
		m.memberTable.Blur()
	}

	m.detailView.Width = detailW
	m.detailView.Height = detailH
	m.detailView.MouseWheelEnabled = true
	m.detailView.MouseWheelDelta = 3
	m.searchInput.Width = maxInt(20, m.width-16)
}

func newTUITable() table.Model {
	styles := table.DefaultStyles()
	styles.Header = styles.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("#4a5568")).
		BorderBottom(true).
		Bold(true).
		Foreground(lipgloss.Color("#f7f7ff"))
	styles.Cell = styles.Cell.Foreground(lipgloss.Color("#dfe7ef")).Padding(0, 1)
	styles.Selected = styles.Selected.
		Foreground(lipgloss.Color("#05070d")).
		Background(lipgloss.Color("#f7f7ff")).
		Bold(true)
	return table.New(table.WithStyles(styles), table.WithFocused(false))
}

func clusterColumns(width int) []table.Column {
	titleW := maxInt(14, width-36)
	return []table.Column{
		{Title: "cnt", Width: 4},
		{Title: "cluster", Width: 20},
		{Title: "title", Width: titleW},
		{Title: "kind", Width: 6},
		{Title: "age", Width: 8},
	}
}

func memberColumns(width int) []table.Column {
	titleW := maxInt(12, width-28)
	return []table.Column{
		{Title: "number", Width: 8},
		{Title: "state", Width: 7},
		{Title: "age", Width: 8},
		{Title: "title", Width: titleW},
	}
}

func (m clusterBrowserModel) clusterRows() []table.Row {
	rows := make([]table.Row, 0, len(m.payload.Clusters))
	for _, cluster := range m.payload.Clusters {
		rows = append(rows, table.Row{
			fmt.Sprintf("%d", cluster.MemberCount),
			cluster.StableSlug,
			splitClusterTitle(cluster),
			kindLabel(cluster.RepresentativeKind),
			formatRelativeTime(cluster.UpdatedAt),
		})
	}
	return rows
}

func (m clusterBrowserModel) memberTableRows() []table.Row {
	rows := make([]table.Row, 0, len(m.memberRows))
	for _, member := range m.memberRows {
		thread := member.thread()
		rows = append(rows, table.Row{
			fmt.Sprintf("#%d", thread.Number),
			thread.State,
			formatRelativeTime(thread.UpdatedAtGitHub),
			thread.Title,
		})
	}
	return rows
}

func (m clusterBrowserModel) pageStep() int {
	switch m.focus {
	case focusMembers:
		return maxInt(1, m.layout().members.h-5)
	case focusDetail:
		return maxInt(1, m.layout().detail.h-5)
	default:
		return maxInt(1, m.layout().clusters.h-5)
	}
}

func (m *clusterBrowserModel) sortClusters() {
	sort.SliceStable(m.payload.Clusters, func(i, j int) bool {
		left := m.payload.Clusters[i]
		right := m.payload.Clusters[j]
		if m.payload.Sort == "size" {
			if left.MemberCount != right.MemberCount {
				return left.MemberCount > right.MemberCount
			}
		}
		return parseTime(left.UpdatedAt).After(parseTime(right.UpdatedAt))
	})
	m.selected = clampInt(m.selected, 0, maxInt(0, len(m.payload.Clusters)-1))
}

func (m *clusterBrowserModel) applyClusterFilters() {
	currentID := int64(0)
	if len(m.payload.Clusters) > 0 && m.selected >= 0 && m.selected < len(m.payload.Clusters) {
		currentID = m.payload.Clusters[m.selected].ID
	}
	query := strings.ToLower(strings.TrimSpace(m.search))
	next := make([]store.ClusterSummary, 0, len(m.allClusters))
	for _, cluster := range m.allClusters {
		if !m.showClosed && (cluster.Status != "active" || cluster.ClosedAt != "") {
			continue
		}
		if cluster.MemberCount < m.minSize {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(cluster.StableSlug+" "+cluster.Title+" "+cluster.RepresentativeTitle+" "+cluster.RepresentativeKind), query) {
			continue
		}
		next = append(next, cluster)
	}
	m.payload.Clusters = next
	m.sortClusters()
	m.selected = 0
	if currentID != 0 {
		for index, cluster := range m.payload.Clusters {
			if cluster.ID == currentID {
				m.selected = index
				break
			}
		}
	}
	m.clusterOff = 0
	m.loadSelectedCluster()
}

func (m *clusterBrowserModel) loadSelectedCluster() {
	m.detailScroll = 0
	m.memberOff = 0
	m.memberIndex = -1
	m.memberRows = nil
	m.hasDetail = false
	if len(m.payload.Clusters) == 0 {
		return
	}
	cluster := m.payload.Clusters[m.selected]
	if cached, ok := m.detailCache[cluster.ID]; ok {
		m.applyClusterDetail(cached)
		return
	}
	if m.store == nil {
		return
	}
	detail, err := m.store.ClusterDetail(m.ctx, store.ClusterDetailOptions{
		RepoID:        m.repoID,
		ClusterID:     cluster.ID,
		IncludeClosed: true,
		MemberLimit:   200,
		BodyChars:     1600,
	})
	if err != nil {
		m.status = err.Error()
		return
	}
	m.detailCache[cluster.ID] = detail
	m.applyClusterDetail(detail)
}

func (m *clusterBrowserModel) applyClusterDetail(detail store.ClusterDetail) {
	m.detail = detail
	m.hasDetail = true
	for _, member := range detail.Members {
		m.memberRows = append(m.memberRows, memberRow{member: member})
	}
	m.sortMembers()
	if len(m.memberRows) > 0 {
		m.memberIndex = 0
	}
}

func (m *clusterBrowserModel) sortMembers() {
	sort.SliceStable(m.memberRows, func(i, j int) bool {
		left := m.memberRows[i].thread()
		right := m.memberRows[j].thread()
		switch m.memberSort {
		case memberSortRecent:
			return parseTime(left.UpdatedAtGitHub).After(parseTime(right.UpdatedAtGitHub))
		case memberSortNumber:
			return left.Number < right.Number
		case memberSortState:
			if left.State != right.State {
				return left.State > right.State
			}
			return left.Number < right.Number
		case memberSortTitle:
			return strings.ToLower(left.Title) < strings.ToLower(right.Title)
		default:
			if left.Kind != right.Kind {
				return left.Kind < right.Kind
			}
			return left.Number < right.Number
		}
	})
	m.memberIndex = clampInt(m.memberIndex, 0, maxInt(0, len(m.memberRows)-1))
}

func (m clusterBrowserModel) openCounts() struct{ pulls, issues int } {
	var out struct{ pulls, issues int }
	for _, cluster := range m.payload.Clusters {
		switch cluster.RepresentativeKind {
		case "pull_request":
			out.pulls++
		case "issue":
			out.issues++
		}
	}
	return out
}

func (m clusterBrowserModel) selectedThread() (store.Thread, bool) {
	if len(m.memberRows) == 0 || m.memberIndex < 0 || m.memberIndex >= len(m.memberRows) {
		return store.Thread{}, false
	}
	thread := m.memberRows[m.memberIndex].thread()
	if strings.TrimSpace(thread.HTMLURL) == "" {
		return store.Thread{}, false
	}
	return thread, true
}

func (r memberRow) format(width int) string {
	thread := r.thread()
	return truncateCells(fmt.Sprintf("#%-7d %-7s %-8s %s", thread.Number, thread.State, formatRelativeTime(thread.UpdatedAtGitHub), thread.Title), width)
}

func (r memberRow) thread() store.Thread {
	return r.member.Thread
}

func openURL(url string) error {
	if strings.TrimSpace(url) == "" {
		return fmt.Errorf("no URL selected")
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("open URL: %w", err)
	}
	return nil
}

func copyText(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("nothing to copy")
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("pbcopy")
	case "windows":
		cmd = exec.Command("clip")
	default:
		cmd = exec.Command("xclip", "-selection", "clipboard")
	}
	cmd.Stdin = strings.NewReader(value)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("copy text: %w", err)
	}
	return nil
}

func paneStyle(pane, focus tuiFocus, width, height int) lipgloss.Style {
	borderColor := "#4a5568"
	switch pane {
	case focusClusters:
		borderColor = "#5bc0eb"
	case focusMembers:
		borderColor = "#9bc53d"
	case focusDetail:
		borderColor = "#fde74c"
	}
	if pane == focus {
		borderColor = "#f7f7ff"
	}
	return lipgloss.NewStyle().
		Width(width-2).
		Height(height-2).
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color(borderColor)).
		Foreground(lipgloss.Color("#dfe7ef")).
		Padding(0, 1)
}

func paneTitle(pane, focus tuiFocus) string {
	label := map[tuiFocus]string{
		focusClusters: "Clusters",
		focusMembers:  "Members",
		focusDetail:   "Detail",
	}[pane]
	prefix := "[ ] "
	if pane == focus {
		prefix = "[*] "
	}
	return bold(prefix + label)
}

func nextFocus(current tuiFocus, delta int) tuiFocus {
	order := []tuiFocus{focusClusters, focusMembers, focusDetail}
	index := 0
	for i, item := range order {
		if item == current {
			index = i
			break
		}
	}
	index = (index + delta + len(order)) % len(order)
	return order[index]
}

func nextMemberSort(current tuiMemberSort) tuiMemberSort {
	order := []tuiMemberSort{memberSortKind, memberSortRecent, memberSortNumber, memberSortState, memberSortTitle}
	for index, item := range order {
		if item == current {
			return order[(index+1)%len(order)]
		}
	}
	return memberSortKind
}

func nextMinSize(current int) int {
	order := []int{1, 2, 5, 10, 20, 50}
	for index, item := range order {
		if item == current {
			return order[(index+1)%len(order)]
		}
	}
	return 1
}

func minSizeLabel(value int) string {
	if value <= 1 {
		return "all"
	}
	return fmt.Sprintf("%d+", value)
}

func boolLabel(value bool) string {
	if value {
		return "shown"
	}
	return "hidden"
}

func splitClusterTitle(cluster store.ClusterSummary) string {
	return firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster")
}

func sortedSummaryKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if strings.TrimSpace(value) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func labelsFromJSON(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var labels []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(raw), &labels); err == nil && len(labels) > 0 {
		names := make([]string, 0, len(labels))
		for _, label := range labels {
			if strings.TrimSpace(label.Name) != "" {
				names = append(names, label.Name)
			}
		}
		if len(names) > 0 {
			return strings.Join(names, ", ")
		}
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err == nil && len(names) > 0 {
		return strings.Join(names, ", ")
	}
	return ""
}

func kindLabel(kind string) string {
	if kind == "pull_request" {
		return "PR"
	}
	if kind == "issue" {
		return "issue"
	}
	return firstNonEmpty(kind, "thread")
}

func kindTitle(kind string) string {
	if kind == "pull_request" {
		return "PR"
	}
	return "Issue"
}

func threadRef(cluster store.ClusterSummary) string {
	if cluster.RepresentativeNumber == 0 {
		return "none"
	}
	return fmt.Sprintf("%s #%d", kindLabel(cluster.RepresentativeKind), cluster.RepresentativeNumber)
}

func formatRelativeTime(value string) string {
	if strings.TrimSpace(value) == "" {
		return "never"
	}
	parsed := parseTime(value)
	if parsed.IsZero() {
		return value
	}
	diff := time.Since(parsed)
	if diff < time.Minute {
		return "now"
	}
	if diff < time.Hour {
		return fmt.Sprintf("%dm ago", int(diff/time.Minute))
	}
	if diff < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(diff/time.Hour))
	}
	if diff < 60*24*time.Hour {
		return fmt.Sprintf("%dd ago", int(diff/(24*time.Hour)))
	}
	return fmt.Sprintf("%dmo ago", maxInt(1, int(diff/(30*24*time.Hour))))
}

func parseTime(value string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func wrapPlain(value string, width int) []string {
	width = maxInt(20, width)
	words := strings.Fields(value)
	if len(words) == 0 {
		return []string{""}
	}
	var lines []string
	var line string
	for _, word := range words {
		if lipgloss.Width(line)+1+lipgloss.Width(word) > width && line != "" {
			lines = append(lines, line)
			line = word
			continue
		}
		if line == "" {
			line = word
		} else {
			line += " " + word
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

func markdownLines(value string, width int) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	renderer, err := glamour.NewTermRenderer(
		glamour.WithStandardStyle("dark"),
		glamour.WithWordWrap(maxInt(24, width)),
	)
	if err != nil {
		return wrapPlain(value, width)
	}
	rendered, err := renderer.Render(value)
	if err != nil {
		return wrapPlain(value, width)
	}
	rendered = strings.TrimRight(rendered, "\n")
	if rendered == "" {
		return nil
	}
	return strings.Split(rendered, "\n")
}

func truncateCells(value string, max int) string {
	if max <= 0 {
		return ""
	}
	if lipgloss.Width(value) <= max {
		return value
	}
	if max <= 3 {
		return strings.Repeat(".", max)
	}
	runes := []rune(value)
	for len(runes) > 0 && lipgloss.Width(string(runes))+3 > max {
		runes = runes[:len(runes)-1]
	}
	return string(runes) + "..."
}

func bold(value string) string {
	return lipgloss.NewStyle().Bold(true).Render(value)
}

func dim(value string) string {
	return lipgloss.NewStyle().Foreground(lipgloss.Color("#8b95a7")).Render(value)
}

func color(hex, value string) string {
	return lipgloss.NewStyle().Foreground(lipgloss.Color(hex)).Render(value)
}

func selectedColor(focused bool) string {
	if focused {
		return "#f7f7ff"
	}
	return "#23445c"
}

func selectedFG(focused bool) string {
	if focused {
		return "#05070d"
	}
	return "#f7f7ff"
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func clampInt(value, minValue, maxValue int) int {
	if maxValue < minValue {
		return minValue
	}
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
