package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/vector"
)

var (
	markdownLinkRE    = regexp.MustCompile(`\[([^\]]+)\]\((https?://[^)\s]+)\)`)
	bareLinkRE        = regexp.MustCompile(`(^|[\s(<])(https?://[^\s<>)]+)`)
	markdownHeadingRE = regexp.MustCompile(`^(#{1,6})\s+(.+)$`)
	markdownListRE    = regexp.MustCompile(`^(\s*)([-*+]|\d+[.)])\s+(.+)$`)
	terminalControlRE = regexp.MustCompile(`[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]`)
	summaryKeyOrder   = []string{"key_summary", "problem_summary", "solution_summary", "maintainer_signal_summary", "dedupe_summary"}
)

type clusterBrowserPayload struct {
	Repository         string                 `json:"repository"`
	InferredRepository bool                   `json:"inferred_repository"`
	Mode               string                 `json:"mode"`
	Sort               string                 `json:"sort"`
	MinSize            int                    `json:"min_size"`
	Limit              int                    `json:"limit,omitempty"`
	HideClosed         bool                   `json:"hide_closed,omitempty"`
	EmbedModel         string                 `json:"embed_model,omitempty"`
	EmbeddingBasis     string                 `json:"embedding_basis,omitempty"`
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

type tuiWideLayout string

const (
	wideLayoutColumns    tuiWideLayout = "columns"
	wideLayoutRightStack tuiWideLayout = "right-stack"
)

type tuiRect struct {
	x int
	y int
	w int
	h int
}

type clusterBrowserModel struct {
	payload       clusterBrowserPayload
	allClusters   []store.ClusterSummary
	ctx           context.Context
	store         *store.Store
	repoID        int64
	focus         tuiFocus
	width         int
	height        int
	status        string
	search        string
	searching     bool
	jumping       bool
	showHelp      bool
	menuOpen      bool
	menuTitle     string
	menuIndex     int
	menuOff       int
	menuItems     []tuiMenuItem
	quitRequested bool
	showClosed    bool
	compactDetail bool
	minSize       int
	memberSort    tuiMemberSort
	wideLayout    tuiWideLayout
	selected      int
	clusterOff    int
	memberRows    []memberRow
	memberOff     int
	memberIndex   int
	clusterTable  table.Model
	memberTable   table.Model
	detailView    viewport.Model
	searchInput   textinput.Model
	detailCache   map[int64]store.ClusterDetail
	neighborCache map[int64][]tuiNeighbor
	detail        store.ClusterDetail
	hasDetail     bool
}

type memberRow struct {
	member     store.ClusterMemberDetail
	label      string
	selectable bool
}

type tuiMenuItem struct {
	label  string
	action string
	value  string
}

type tuiNeighbor struct {
	Thread store.Thread
	Score  float64
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
		payload:       payload,
		allClusters:   clusters,
		ctx:           ctx,
		store:         st,
		repoID:        repoID,
		focus:         focusClusters,
		status:        "Ready",
		showClosed:    !payload.HideClosed,
		minSize:       maxInt(1, payload.MinSize),
		memberSort:    memberSortKind,
		wideLayout:    wideLayoutColumns,
		memberIndex:   -1,
		clusterTable:  newTUITable(),
		memberTable:   newTUITable(),
		detailView:    viewport.New(1, 1),
		searchInput:   search,
		detailCache:   map[int64]store.ClusterDetail{},
		neighborCache: map[int64][]tuiNeighbor{},
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
		if m.jumping {
			var cmd tea.Cmd
			m, cmd = m.handleJumpKey(msg)
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
		case "d":
			m.toggleDetailMode()
		case "l":
			m.toggleWideLayout()
		case "p":
			m.openRepositoryMenu()
		case "r":
			m.refreshFromStore()
		case "f":
			m.minSize = nextMinSize(m.minSize)
			m.applyClusterFilters()
			m.status = fmt.Sprintf("Min size: %s", minSizeLabel(m.minSize))
		case "x":
			m.showClosed = !m.showClosed
			m.applyClusterFilters()
			if m.showClosed {
				m.status = "Showing closed clusters and members"
			} else {
				m.status = "Hiding closed clusters and members"
			}
		case "/":
			cmd := m.startFilterInput()
			return m, cmd
		case "#":
			m.jumping = true
			m.searching = false
			m.showHelp = false
			m.menuOpen = false
			m.searchInput.Prompt = "# "
			m.searchInput.Placeholder = "issue or PR number"
			m.searchInput.SetValue("")
			cmd := m.searchInput.Focus()
			m.status = "Jump to issue/PR"
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
		if m.quitRequested {
			return m, tea.Quit
		}
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
	if !layout.stacked && layout.detail.y > layout.members.y {
		body = lipgloss.JoinHorizontal(lipgloss.Top, clusters, lipgloss.JoinVertical(lipgloss.Left, members, detail))
	}
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
	mode     string
}

func (m clusterBrowserModel) layout() tuiLayout {
	width := maxInt(m.width, 80)
	height := maxInt(m.height, 24)
	headerH := 1
	footerH := 2
	bodyH := maxInt(8, height-headerH-footerH)
	layout := tuiLayout{
		header: tuiRect{x: 0, y: 0, w: width, h: headerH},
		footer: tuiRect{x: 0, y: headerH + bodyH, w: width, h: footerH},
	}
	if width >= 140 {
		if m.wideLayout == wideLayoutRightStack {
			clusterW := maxInt(56, width*44/100)
			rightW := width - clusterW
			memberH := maxInt(8, bodyH*42/100)
			layout.mode = string(wideLayoutRightStack)
			layout.clusters = tuiRect{x: 0, y: headerH, w: clusterW, h: bodyH}
			layout.members = tuiRect{x: clusterW, y: headerH, w: rightW, h: memberH}
			layout.detail = tuiRect{x: clusterW, y: headerH + memberH, w: rightW, h: bodyH - memberH}
			return layout
		}
		clusterW := maxInt(48, width*34/100)
		memberW := maxInt(40, width*30/100)
		detailW := maxInt(42, width-clusterW-memberW)
		layout.mode = string(wideLayoutColumns)
		layout.clusters = tuiRect{x: 0, y: headerH, w: clusterW, h: bodyH}
		layout.members = tuiRect{x: clusterW, y: headerH, w: memberW, h: bodyH}
		layout.detail = tuiRect{x: clusterW + memberW, y: headerH, w: detailW, h: bodyH}
		return layout
	}
	if width < 100 {
		layout.stacked = true
		layout.mode = "stacked"
		clusterH := maxInt(7, bodyH*36/100)
		memberH := maxInt(6, bodyH*28/100)
		detailH := maxInt(6, bodyH-clusterH-memberH)
		layout.clusters = tuiRect{x: 0, y: headerH, w: width, h: clusterH}
		layout.members = tuiRect{x: 0, y: headerH + clusterH, w: width, h: memberH}
		layout.detail = tuiRect{x: 0, y: headerH + clusterH + memberH, w: width, h: detailH}
		return layout
	}
	layout.stacked = true
	layout.mode = "split"
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
	line := fmt.Sprintf("%s  %d PR  %d issues  clusters:%d  sort:%s  members:%s  min:%s  layout:%s  detail:%s  closed:%s  filter:%s",
		m.payload.Repository,
		openCounts.pulls,
		openCounts.issues,
		len(m.payload.Clusters),
		m.payload.Sort,
		m.memberSort,
		minSizeLabel(m.minSize),
		layoutLabel(m.layout()),
		detailModeLabel(m.compactDetail),
		boolLabel(m.showClosed),
		firstNonEmpty(m.search, "none"),
	)
	if m.payload.InferredRepository {
		line += "  inferred"
	}
	style := lipgloss.NewStyle().Width(width).Height(1).Background(lipgloss.Color("#0d1321")).Foreground(lipgloss.Color("#f7f7ff")).Padding(0, 1)
	return style.Render(truncateCells(bold(line), maxInt(1, width-2)))
}

func (m clusterBrowserModel) renderFooter(width int) string {
	controls := "Tab focus  click select  header sort  wheel scroll  / filter  # jump  p repos  s sort  m members  d detail  r refresh  f min  l layout  x closed  ? help  q quit"
	if width < 100 {
		controls = "Tab focus click right-click menu / filter # jump p repos ? help q quit"
	}
	line := firstNonEmpty(m.status, "Ready")
	if m.searching {
		line = "Filter: " + m.searchInput.View()
	}
	if m.jumping {
		line = "Jump: " + m.searchInput.View()
	}
	return lipgloss.NewStyle().Width(width).Height(2).Background(lipgloss.Color("#5bc0eb")).Foreground(lipgloss.Color("#05070d")).Padding(0, 1).Render(truncateCells(line, width-2) + "\n" + truncateCells(controls, maxInt(1, width-2)))
}

func (m clusterBrowserModel) renderClusters(rect tuiRect) string {
	return paneStyle(focusClusters, m.focus, rect.w, rect.h).Render(lipgloss.JoinVertical(lipgloss.Left, paneTitle(focusClusters, m.focus, m.clusterPositionLabel()), m.clusterTable.View()))
}

func (m clusterBrowserModel) renderMembers(rect tuiRect) string {
	return paneStyle(focusMembers, m.focus, rect.w, rect.h).Render(lipgloss.JoinVertical(lipgloss.Left, paneTitle(focusMembers, m.focus, m.memberPositionLabel()), m.memberTable.View()))
}

func (m clusterBrowserModel) renderDetail(rect tuiRect) string {
	mode := "full"
	if m.compactDetail {
		mode = "compact"
	}
	lines := append([]string{paneTitle(focusDetail, m.focus, mode)}, m.detailLines(rect.w-4)...)
	if m.showHelp {
		lines = append([]string{paneTitle(focusDetail, m.focus, mode)}, m.helpLines(rect.w-4)...)
	}
	if m.menuOpen {
		lines = append([]string{paneTitle(focusDetail, m.focus, mode)}, m.menuLines(rect.w-4)...)
	}
	m.detailView.SetContent(strings.Join(lines, "\n"))
	return paneStyle(focusDetail, m.focus, rect.w, rect.h).Render(m.detailView.View())
}

func (m clusterBrowserModel) detailLines(width int) []string {
	if len(m.payload.Clusters) == 0 {
		return []string{
			bold("No clusters visible"),
			"",
			"No clusters match the current view.",
			"",
			"Try f to lower the minimum size, / to clear the filter, x to show closed clusters, or r to refresh from the local store.",
			"",
			"If the store is empty, run sync, refresh summaries/embeddings, and cluster first.",
		}
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
	member, ok := m.selectedMember()
	if !ok {
		lines = append(lines, "Select a cluster to inspect members.")
		return lines
	}
	thread := member.Thread
	lines = append(lines,
		dim(tuiRule(width)),
		bold(fmt.Sprintf("%s #%d", kindTitle(thread.Kind), thread.Number)),
	)
	lines = append(lines, wrapPlain(thread.Title, width)...)
	lines = append(lines,
		"",
	)
	lines = append(lines, wrapPlain(fmt.Sprintf("closed: %s", closedLabel(thread)), width)...)
	lines = append(lines, wrapPlain(fmt.Sprintf("updated: %s   author: %s", formatRelativeTime(thread.UpdatedAtGitHub), firstNonEmpty(thread.AuthorLogin, "unknown")), width)...)
	if labels := labelsFromJSON(thread.LabelsJSON); labels != "" {
		lines = append(lines, wrapPlain("labels: "+labels, width)...)
		lines = append(lines, "")
	}
	lines = append(lines, wrapPlain(fmt.Sprintf("url: %s", thread.HTMLURL), width)...)
	lines = append(lines, "")
	if neighbors, ok := m.neighborCache[thread.ID]; ok {
		lines = append(lines, dim(tuiRule(width)))
		lines = append(lines, bold("Neighbors"))
		if len(neighbors) == 0 {
			lines = append(lines, "No neighbors above threshold.", "")
		} else {
			for _, neighbor := range neighbors {
				lines = append(lines, truncateCells(fmt.Sprintf("#%d %s %.1f%%  %s",
					neighbor.Thread.Number,
					kindTitle(neighbor.Thread.Kind),
					neighbor.Score*100,
					neighbor.Thread.Title,
				), width))
			}
			lines = append(lines, "")
		}
	}
	if len(member.Summaries) > 0 {
		lines = append(lines, dim(tuiRule(width)))
		lines = append(lines, bold("LLM Summary"))
		for _, key := range sortedSummaryKeys(member.Summaries) {
			lines = append(lines, dim(formatSummaryLabel(key)+":"))
			lines = append(lines, markdownLines(member.Summaries[key], width)...)
			lines = append(lines, "")
		}
	}
	if strings.TrimSpace(member.BodySnippet) != "" {
		lines = append(lines, dim(tuiRule(width)))
		lines = append(lines, bold("Main Preview"))
		lines = appendLimitedLines(lines, markdownLines(member.BodySnippet, width), m.detailBodyLimit())
	}
	return lines
}

func (m clusterBrowserModel) helpLines(width int) []string {
	lines := []string{
		bold("Gitcrawl TUI"),
		"",
		"Mouse",
		"  left click: focus/select a pane row",
		"  left click menu row: run that action",
		"  wheel: scroll the pane under the pointer",
		"  wheel in menu: move the highlighted action",
		"  right click: open a stable action menu",
		"  menu actions: copy, links, neighbors, sort, refresh, layout, quit",
		"",
		"Keyboard",
		"  Tab / Shift-Tab: cycle focus",
		"  arrows or j/k: move selection or scroll detail",
		"  PageUp/PageDown: page the active pane",
		"  Enter: drill into the next pane",
		"  /: filter clusters",
		"  #: jump to issue/PR number",
		"  s: toggle cluster sort",
		"  m: cycle member sort",
		"  d: toggle compact/full detail",
		"  r: refresh from local store",
		"  p: switch repository",
		"  l: toggle wide layout",
		"  f: cycle minimum cluster size",
		"  x: show/hide closed clusters",
		"  o: open selected thread in browser",
		"  c: copy selected thread URL",
		"  Enter in menu: run action or open link picker",
		"  ?: toggle this help",
		"  q: quit",
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
	lines := []string{bold(firstNonEmpty(m.menuTitle, "Actions")), ""}
	visible := m.menuVisibleCount()
	start := clampInt(m.menuOff, 0, maxInt(0, len(m.menuItems)-visible))
	end := minInt(len(m.menuItems), start+visible)
	for index := start; index < end; index++ {
		item := m.menuItems[index]
		prefix := "  "
		if index == m.menuIndex {
			prefix = "> "
		}
		key := fmt.Sprintf("%d. ", index-start+1)
		lines = append(lines, truncateCells(prefix+key+item.label, width))
	}
	footer := "Enter/1-9 run  Esc close"
	if len(m.menuItems) > visible {
		footer = fmt.Sprintf("Enter/1-9 run  Esc close  Pg page  %d-%d/%d", start+1, end, len(m.menuItems))
	}
	lines = append(lines, "", dim(footer))
	return lines
}

func (m clusterBrowserModel) updateMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	page := maxInt(1, m.menuVisibleCount())
	if index, ok := visibleMenuShortcutIndex(msg.String(), m.menuOff, len(m.menuItems)); ok {
		m.menuIndex = index
		if m.runMenuItem(m.menuItems[m.menuIndex]) {
			m.menuOpen = false
		}
		if m.quitRequested {
			return m, tea.Quit
		}
		return m, nil
	}
	switch msg.String() {
	case "esc", "q":
		m.menuOpen = false
		m.status = "Menu closed"
	case "up", "k":
		m.menuIndex = clampInt(m.menuIndex-1, 0, maxInt(0, len(m.menuItems)-1))
		m.keepMenuVisible()
	case "down", "j":
		m.menuIndex = clampInt(m.menuIndex+1, 0, maxInt(0, len(m.menuItems)-1))
		m.keepMenuVisible()
	case "pgup", "ctrl+b":
		m.menuIndex = clampInt(m.menuIndex-page, 0, maxInt(0, len(m.menuItems)-1))
		m.keepMenuVisible()
	case "pgdown", "ctrl+f":
		m.menuIndex = clampInt(m.menuIndex+page, 0, maxInt(0, len(m.menuItems)-1))
		m.keepMenuVisible()
	case "home", "g":
		m.menuIndex = 0
		m.keepMenuVisible()
	case "end", "G":
		m.menuIndex = maxInt(0, len(m.menuItems)-1)
		m.keepMenuVisible()
	case "enter":
		if m.menuIndex >= 0 && m.menuIndex < len(m.menuItems) {
			if m.runMenuItem(m.menuItems[m.menuIndex]) {
				m.menuOpen = false
			}
			if m.quitRequested {
				return m, tea.Quit
			}
		}
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
		previous := m.memberIndex
		m.memberIndex = m.nextSelectableMemberIndex(m.memberIndex, delta)
		if m.memberIndex != previous {
			m.detailView.GotoTop()
		}
		if thread, ok := m.selectedThread(); ok {
			m.status = fmt.Sprintf("Selected #%d", thread.Number)
		}
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

func (m *clusterBrowserModel) startFilterInput() tea.Cmd {
	m.searching = true
	m.jumping = false
	m.showHelp = false
	m.menuOpen = false
	m.searchInput.Prompt = "/ "
	m.searchInput.Placeholder = "filter clusters"
	m.searchInput.SetValue(m.search)
	m.status = "Filter: " + m.search
	return m.searchInput.Focus()
}

func (m clusterBrowserModel) handleJumpKey(msg tea.KeyMsg) (clusterBrowserModel, tea.Cmd) {
	switch msg.String() {
	case "enter":
		m.jumping = false
		value := strings.TrimPrefix(strings.TrimSpace(m.searchInput.Value()), "#")
		m.searchInput.Blur()
		number, err := strconv.Atoi(value)
		if err != nil || number <= 0 {
			m.status = "Enter a positive issue or PR number"
			return m, nil
		}
		m.jumpToThreadNumber(number)
	case "esc":
		m.jumping = false
		m.searchInput.Blur()
		m.status = "Jump cancelled"
	default:
		var cmd tea.Cmd
		m.searchInput, cmd = m.searchInput.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m *clusterBrowserModel) handleMouse(msg tea.MouseMsg) {
	layout := m.layout()
	if msg.Button != tea.MouseButtonLeft && msg.Button != tea.MouseButtonRight && !isMouseWheel(msg.Button) {
		return
	}
	if m.menuOpen {
		m.handleMenuMouse(layout, msg)
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
			if row == -1 {
				m.sortClustersFromHeader(msg.X - layout.clusters.x - 2)
				return
			}
			if row < 0 {
				return
			}
			index := m.clusterVisibleStart() + row
			if index >= 0 && index < len(m.payload.Clusters) {
				m.selected = index
				m.loadSelectedCluster()
				m.status = fmt.Sprintf("Cluster %d", m.payload.Clusters[m.selected].ID)
			}
		case layout.members.contains(msg.X, msg.Y):
			m.focus = focusMembers
			row := msg.Y - layout.members.y - 3
			if row == -1 {
				m.sortMembersFromHeader(msg.X - layout.members.x - 2)
				return
			}
			if row < 0 {
				return
			}
			index := m.memberVisibleStart() + row
			if index >= 0 && index < len(m.memberRows) {
				if !m.memberRows[index].selectable {
					m.status = m.memberRows[index].label
					return
				}
				previous := m.memberIndex
				m.memberIndex = index
				if m.memberIndex != previous {
					m.detailView.GotoTop()
				}
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

func (m *clusterBrowserModel) handleMenuMouse(layout tuiLayout, msg tea.MouseMsg) {
	switch msg.Button {
	case tea.MouseButtonWheelUp:
		m.menuIndex = clampInt(m.menuIndex-1, 0, maxInt(0, len(m.menuItems)-1))
		m.keepMenuVisible()
		return
	case tea.MouseButtonWheelDown:
		m.menuIndex = clampInt(m.menuIndex+1, 0, maxInt(0, len(m.menuItems)-1))
		m.keepMenuVisible()
		return
	case tea.MouseButtonRight:
		if msg.Action == tea.MouseActionPress {
			m.menuOpen = false
			m.status = "Menu closed"
		}
		return
	}
	if msg.Button != tea.MouseButtonLeft || msg.Action != tea.MouseActionPress {
		return
	}
	if !layout.detail.contains(msg.X, msg.Y) {
		m.menuOpen = false
		m.status = "Menu closed"
		return
	}
	index := m.menuOff + msg.Y - layout.detail.y - 4
	if index < 0 || index >= len(m.menuItems) {
		return
	}
	m.menuIndex = index
	m.keepMenuVisible()
	if m.runMenuItem(m.menuItems[m.menuIndex]) {
		m.menuOpen = false
	}
}

func (m *clusterBrowserModel) selectByMousePosition(layout tuiLayout, x, y int) {
	switch {
	case layout.clusters.contains(x, y):
		m.focus = focusClusters
		row := y - layout.clusters.y - 3
		if row >= 0 {
			index := m.clusterVisibleStart() + row
			if index >= 0 && index < len(m.payload.Clusters) {
				m.selected = index
				m.loadSelectedCluster()
			}
		}
	case layout.members.contains(x, y):
		m.focus = focusMembers
		row := y - layout.members.y - 3
		if row >= 0 {
			index := m.memberVisibleStart() + row
			if index >= 0 && index < len(m.memberRows) {
				if !m.memberRows[index].selectable {
					return
				}
				previous := m.memberIndex
				m.memberIndex = index
				if m.memberIndex != previous {
					m.detailView.GotoTop()
				}
			}
		}
	case layout.detail.contains(x, y):
		m.focus = focusDetail
	}
}

func (m *clusterBrowserModel) openActionMenu() {
	m.menuItems = nil
	if thread, ok := m.selectedThread(); ok {
		m.menuItems = append(m.menuItems,
			tuiMenuItem{label: fmt.Sprintf("Open #%d in browser", thread.Number), action: "open"},
			tuiMenuItem{label: "Copy selected URL", action: "copy-url"},
			tuiMenuItem{label: "Copy title", action: "copy-title"},
			tuiMenuItem{label: "Copy markdown link", action: "copy-markdown"},
			tuiMenuItem{label: "Copy selected detail", action: "copy-thread-detail"},
			tuiMenuItem{label: "Load neighbors", action: "load-neighbors"},
		)
	}
	if member, ok := m.selectedMember(); ok {
		if strings.TrimSpace(member.BodySnippet) != "" {
			m.menuItems = append(m.menuItems, tuiMenuItem{label: "Copy body preview", action: "copy-body-preview"})
		}
		if len(member.Summaries) > 0 {
			m.menuItems = append(m.menuItems, tuiMenuItem{label: "Copy summaries", action: "copy-summaries"})
		}
		if _, ok := m.neighborCache[member.Thread.ID]; ok {
			m.menuItems = append(m.menuItems, tuiMenuItem{label: "Copy neighbors", action: "copy-neighbors"})
		}
	}
	if m.hasSelectedCluster() {
		if url, ok := m.selectedClusterURL(); ok {
			cluster, _ := m.selectedCluster()
			m.menuItems = append(m.menuItems,
				tuiMenuItem{label: fmt.Sprintf("Open representative #%d", cluster.RepresentativeNumber), action: "open-cluster-representative", value: url},
				tuiMenuItem{label: "Copy representative URL", action: "copy-cluster-url", value: url},
			)
		}
		m.menuItems = append(m.menuItems,
			tuiMenuItem{label: "Copy cluster ID", action: "copy-cluster-id"},
			tuiMenuItem{label: "Copy cluster name", action: "copy-cluster-name"},
			tuiMenuItem{label: "Copy cluster title", action: "copy-cluster-title"},
			tuiMenuItem{label: "Copy cluster summary", action: "copy-cluster"},
		)
		if m.hasDetail {
			m.menuItems = append(m.menuItems, tuiMenuItem{label: "Copy member list", action: "copy-member-list"})
		}
	}
	if len(m.payload.Clusters) > 0 {
		m.menuItems = append(m.menuItems, tuiMenuItem{label: "Copy visible clusters", action: "copy-visible-clusters"})
	}
	referenceLinks := m.referenceLinks()
	if len(referenceLinks) > 0 {
		m.menuItems = append(m.menuItems,
			tuiMenuItem{label: "Open first body link", action: "open-first-link"},
			tuiMenuItem{label: "Copy first body link", action: "copy-first-link"},
		)
	}
	if len(referenceLinks) > 1 {
		m.menuItems = append(m.menuItems,
			tuiMenuItem{label: "Open body link...", action: "open-link-picker"},
			tuiMenuItem{label: "Copy body link...", action: "copy-link-picker"},
			tuiMenuItem{label: "Copy all body links", action: "copy-reference-links"},
		)
	}
	m.menuItems = append(m.menuItems,
		tuiMenuItem{label: "Sort clusters by size", action: "sort-size"},
		tuiMenuItem{label: "Sort clusters by recent", action: "sort-recent"},
		tuiMenuItem{label: "Member sort grouped", action: "member-sort-kind"},
		tuiMenuItem{label: "Member sort recent", action: "member-sort-recent"},
		tuiMenuItem{label: "Filter clusters...", action: "filter"},
		tuiMenuItem{label: "Refresh from store", action: "refresh"},
		tuiMenuItem{label: "Switch repository...", action: "repository-picker"},
		tuiMenuItem{label: "Jump to issue/PR...", action: "jump"},
		tuiMenuItem{label: "Toggle layout", action: "toggle-layout"},
		tuiMenuItem{label: detailModeToggleLabel(m.compactDetail), action: "toggle-detail"},
		tuiMenuItem{label: "Min size 1+", action: "min-size-1"},
		tuiMenuItem{label: "Min size 5+", action: "min-size-5"},
		tuiMenuItem{label: "Min size 10+", action: "min-size-10"},
		tuiMenuItem{label: closedToggleLabel(m.showClosed), action: "toggle-closed"},
		tuiMenuItem{label: "Help", action: "show-help"},
		tuiMenuItem{label: "Quit", action: "quit"},
	)
	if strings.TrimSpace(m.search) != "" {
		m.menuItems = append(m.menuItems, tuiMenuItem{label: "Clear filter", action: "clear-filter"})
	}
	if len(m.menuItems) == 0 {
		m.menuItems = append(m.menuItems, tuiMenuItem{label: "No actions available", action: "close-menu"})
	}
	m.menuItems = append(m.menuItems, tuiMenuItem{label: "Close menu", action: "close-menu"})
	m.menuTitle = "Actions"
	m.menuIndex = 0
	m.menuOff = 0
	m.menuOpen = true
	m.showHelp = false
	m.status = "Action menu"
}

func (m *clusterBrowserModel) openRepositoryMenu() {
	if m.store == nil {
		m.status = "Repository picker unavailable for this view"
		return
	}
	repos, err := m.store.ListRepositories(m.ctx)
	if err != nil {
		m.status = "Repository picker failed: " + err.Error()
		return
	}
	if len(repos) == 0 {
		m.status = "No local repositories found"
		return
	}
	items := make([]tuiMenuItem, 0, len(repos)+1)
	for _, repo := range repos {
		label := repo.FullName
		if repo.FullName == m.payload.Repository {
			label = "* " + label
		}
		items = append(items, tuiMenuItem{label: label, action: "select-repo", value: repo.FullName})
	}
	items = append(items, tuiMenuItem{label: "Back to actions", action: "back-to-actions"})
	m.menuItems = items
	m.menuTitle = "Repositories"
	m.menuIndex = 0
	m.menuOff = 0
	m.menuOpen = true
	m.showHelp = false
	m.searching = false
	m.jumping = false
	m.status = "Repository picker"
}

func (m *clusterBrowserModel) runAction(action string) bool {
	return m.runMenuItem(tuiMenuItem{action: action})
}

func (m *clusterBrowserModel) runMenuItem(item tuiMenuItem) bool {
	action := item.action
	if action == "close-menu" {
		m.status = "Menu closed"
		return true
	}
	switch action {
	case "quit":
		m.quitRequested = true
		return true
	case "sort-size":
		m.payload.Sort = "size"
		m.sortClusters()
		m.loadSelectedCluster()
		m.status = "Sort: size"
		return true
	case "sort-recent":
		m.payload.Sort = "recent"
		m.sortClusters()
		m.loadSelectedCluster()
		m.status = "Sort: recent"
		return true
	case "member-sort-kind":
		m.memberSort = memberSortKind
		m.sortMembers()
		m.status = "Member sort: kind"
		return true
	case "member-sort-recent":
		m.memberSort = memberSortRecent
		m.sortMembers()
		m.status = "Member sort: recent"
		return true
	case "refresh":
		m.refreshFromStore()
		return true
	case "filter":
		m.startFilterInput()
		return true
	case "clear-filter":
		m.search = ""
		m.searchInput.SetValue("")
		m.applyClusterFilters()
		m.status = "Filter cleared"
		return true
	case "repository-picker":
		m.openRepositoryMenu()
		return false
	case "jump":
		m.jumping = true
		m.searching = false
		m.showHelp = false
		m.searchInput.Prompt = "# "
		m.searchInput.Placeholder = "issue or PR number"
		m.searchInput.SetValue("")
		_ = m.searchInput.Focus()
		m.status = "Jump to issue/PR"
		return true
	case "toggle-layout":
		m.toggleWideLayout()
		return true
	case "toggle-detail":
		m.toggleDetailMode()
		return true
	case "min-size-1":
		m.setMinSizeFromMenu(1)
		return true
	case "min-size-5":
		m.setMinSizeFromMenu(5)
		return true
	case "min-size-10":
		m.setMinSizeFromMenu(10)
		return true
	case "toggle-closed":
		m.showClosed = !m.showClosed
		m.applyClusterFilters()
		if m.showClosed {
			m.status = "Showing closed clusters and members"
		} else {
			m.status = "Hiding closed clusters and members"
		}
		return true
	case "show-help":
		m.showHelp = true
		m.status = "Help"
		return true
	case "open-cluster-representative":
		if strings.TrimSpace(item.value) == "" {
			m.status = "No representative URL"
			return true
		}
		openURL(item.value)
		m.status = "Opened " + item.value
		return true
	case "copy-cluster-url":
		if strings.TrimSpace(item.value) == "" {
			m.status = "No representative URL"
			return true
		}
		if err := copyText(item.value); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied representative URL"
		}
		return true
	case "load-neighbors":
		m.loadSelectedThreadNeighbors(10, 0.2)
		return true
	case "copy-thread-detail":
		if err := copyText(m.threadDetailClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied selected detail"
		}
		return true
	case "copy-body-preview":
		member, ok := m.selectedMember()
		if !ok || strings.TrimSpace(member.BodySnippet) == "" {
			m.status = "No body preview"
			return true
		}
		if err := copyText(member.BodySnippet); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied body preview"
		}
		return true
	case "copy-summaries":
		if err := copyText(m.summariesClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied summaries"
		}
		return true
	case "copy-neighbors":
		if err := copyText(m.neighborsClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied neighbors"
		}
		return true
	case "copy-cluster-id":
		cluster, ok := m.selectedCluster()
		if !ok {
			m.status = "No selected cluster"
			return true
		}
		if err := copyText(fmt.Sprintf("%d", cluster.ID)); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster ID"
		}
		return true
	case "copy-cluster-name":
		cluster, ok := m.selectedCluster()
		if !ok {
			m.status = "No selected cluster"
			return true
		}
		if err := copyText(cluster.StableSlug); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster name"
		}
		return true
	case "copy-cluster-title":
		cluster, ok := m.selectedCluster()
		if !ok {
			m.status = "No selected cluster"
			return true
		}
		if err := copyText(firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster")); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster title"
		}
		return true
	case "copy-member-list":
		if err := copyText(m.memberListClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied member list"
		}
		return true
	case "back-to-actions":
		m.openActionMenu()
		return false
	case "select-repo":
		m.switchRepository(item.value)
		return true
	case "open-link-picker":
		m.openReferenceLinkMenu("open")
		return false
	case "copy-link-picker":
		m.openReferenceLinkMenu("copy")
		return false
	case "open-picked-link":
		if err := openURL(item.value); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Opened " + item.value
		}
		return true
	case "copy-picked-link":
		if err := copyText(item.value); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied body link"
		}
		return true
	case "copy-cluster":
		if err := copyText(m.clusterClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster summary"
		}
		return true
	case "copy-visible-clusters":
		if err := copyText(m.visibleClustersClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied visible clusters"
		}
		return true
	case "copy-reference-links":
		links := m.referenceLinks()
		if len(links) == 0 {
			m.status = "No body links found"
			return true
		}
		if err := copyText(strings.Join(links, "\n")); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied body links"
		}
		return true
	}
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return true
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
	case "copy-title":
		title := fmt.Sprintf("#%d %s", thread.Number, thread.Title)
		if err := copyText(title); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied title"
		}
	case "open-first-link":
		link, ok := m.firstReferenceLink()
		if !ok {
			m.status = "No body link found"
			return true
		}
		if err := openURL(link); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Opened " + link
		}
	case "copy-first-link":
		link, ok := m.firstReferenceLink()
		if !ok {
			m.status = "No body link found"
			return true
		}
		if err := copyText(link); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied first body link"
		}
	case "close-menu":
		m.status = "Menu closed"
	}
	return true
}

func (m *clusterBrowserModel) setMinSizeFromMenu(value int) {
	m.minSize = maxInt(1, value)
	m.applyClusterFilters()
	m.status = fmt.Sprintf("Min size: %s", minSizeLabel(m.minSize))
}

func (m *clusterBrowserModel) loadSelectedThreadNeighbors(limit int, threshold float64) {
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Neighbors unavailable for this view"
		return
	}
	if limit <= 0 {
		limit = 10
	}
	if threshold <= 0 {
		threshold = 0.2
	}
	targetThread, targetVector, err := m.store.ThreadVectorByNumber(m.ctx, store.ThreadVectorQuery{
		RepoID: m.repoID,
		Model:  m.payload.EmbedModel,
		Basis:  m.payload.EmbeddingBasis,
	}, thread.Number)
	if err != nil {
		var fallbackErr error
		targetThread, targetVector, fallbackErr = m.store.ThreadVectorByNumber(m.ctx, store.ThreadVectorQuery{RepoID: m.repoID}, thread.Number)
		if fallbackErr != nil {
			m.status = err.Error()
			return
		}
	}
	vectors, err := m.store.ListThreadVectorsFiltered(m.ctx, store.ThreadVectorQuery{
		RepoID:     m.repoID,
		Model:      targetVector.Model,
		Basis:      targetVector.Basis,
		Dimensions: targetVector.Dimensions,
	})
	if err != nil {
		m.status = err.Error()
		return
	}
	items := make([]vector.Item, 0, len(vectors))
	for _, stored := range vectors {
		items = append(items, vector.Item{ThreadID: stored.ThreadID, Vector: stored.Vector})
	}
	candidates := vector.Query(items, targetVector.Vector, limit*2, targetThread.ID)
	filtered := make([]vector.Neighbor, 0, limit)
	for _, candidate := range candidates {
		if candidate.Score < threshold {
			continue
		}
		filtered = append(filtered, candidate)
		if len(filtered) >= limit {
			break
		}
	}
	ids := make([]int64, 0, len(filtered))
	for _, candidate := range filtered {
		ids = append(ids, candidate.ThreadID)
	}
	threads, err := m.store.ThreadsByIDs(m.ctx, m.repoID, ids)
	if err != nil {
		m.status = err.Error()
		return
	}
	neighbors := make([]tuiNeighbor, 0, len(filtered))
	for _, candidate := range filtered {
		neighborThread, ok := threads[candidate.ThreadID]
		if !ok {
			continue
		}
		neighbors = append(neighbors, tuiNeighbor{Thread: neighborThread, Score: candidate.Score})
	}
	m.neighborCache[targetThread.ID] = neighbors
	m.focus = focusDetail
	m.detailView.GotoTop()
	m.status = fmt.Sprintf("Loaded %d neighbors for #%d", len(neighbors), targetThread.Number)
}

func (m *clusterBrowserModel) openReferenceLinkMenu(mode string) {
	links := m.referenceLinks()
	if len(links) == 0 {
		m.status = "No body links found"
		return
	}
	action := "copy-picked-link"
	m.menuTitle = "Copy Link"
	if mode == "open" {
		action = "open-picked-link"
		m.menuTitle = "Open Link"
	}
	items := make([]tuiMenuItem, 0, len(links)+1)
	for index, link := range links {
		items = append(items, tuiMenuItem{
			label:  formatLinkChoiceLabel(link, index),
			action: action,
			value:  link,
		})
	}
	items = append(items, tuiMenuItem{label: "Back to actions", action: "back-to-actions"})
	m.menuItems = items
	m.menuIndex = 0
	m.menuOff = 0
	m.status = m.menuTitle
}

func (m clusterBrowserModel) menuVisibleCount() int {
	height := m.detailView.Height
	if height <= 0 {
		height = maxInt(1, m.layout().detail.h-2)
	}
	return maxInt(1, height-4)
}

func visibleMenuShortcutIndex(key string, menuOff, menuLen int) (int, bool) {
	if len(key) != 1 || key[0] < '1' || key[0] > '9' {
		return 0, false
	}
	index := menuOff + int(key[0]-'1')
	if index < 0 || index >= menuLen {
		return 0, false
	}
	return index, true
}

func (m *clusterBrowserModel) keepMenuVisible() {
	if len(m.menuItems) == 0 {
		m.menuOff = 0
		return
	}
	visible := m.menuVisibleCount()
	m.menuIndex = clampInt(m.menuIndex, 0, len(m.menuItems)-1)
	if m.menuIndex < m.menuOff {
		m.menuOff = m.menuIndex
	}
	if m.menuIndex >= m.menuOff+visible {
		m.menuOff = m.menuIndex - visible + 1
	}
	m.menuOff = clampInt(m.menuOff, 0, maxInt(0, len(m.menuItems)-visible))
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
		previous := m.memberIndex
		if end {
			m.memberIndex = m.lastSelectableMemberIndex()
		} else {
			m.memberIndex = m.firstSelectableMemberIndex()
		}
		if m.memberIndex != previous {
			m.detailView.GotoTop()
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
	m.clusterOff = m.clusterVisibleStart()
	m.memberOff = m.memberVisibleStart()
}

func (m clusterBrowserModel) clusterVisibleStart() int {
	return tableVisibleStart(m.selected, len(m.payload.Clusters), m.clusterViewportHeight())
}

func (m clusterBrowserModel) memberVisibleStart() int {
	return tableVisibleStart(m.memberIndex, len(m.memberRows), m.memberViewportHeight())
}

func (m clusterBrowserModel) clusterViewportHeight() int {
	if height := m.clusterTable.Height(); height > 0 {
		return height
	}
	return fallbackTableViewportHeight(m.layout().clusters)
}

func (m clusterBrowserModel) memberViewportHeight() int {
	if height := m.memberTable.Height(); height > 0 {
		return height
	}
	return fallbackTableViewportHeight(m.layout().members)
}

func fallbackTableViewportHeight(rect tuiRect) int {
	return maxInt(1, maxInt(2, rect.h-3)-1)
}

func tableVisibleStart(cursor, rowCount, viewportHeight int) int {
	if rowCount <= 0 || cursor < 0 {
		return 0
	}
	cursor = clampInt(cursor, 0, rowCount-1)
	return clampInt(cursor-maxInt(1, viewportHeight), 0, cursor)
}

func (m *clusterBrowserModel) syncComponents() {
	layout := m.layout()
	clusterW := maxInt(24, layout.clusters.w-4)
	memberW := maxInt(24, layout.members.w-4)
	detailW := maxInt(24, layout.detail.w-4)
	detailH := maxInt(2, layout.detail.h-2)

	m.clusterTable.SetWidth(clusterW)
	m.clusterTable.SetHeight(maxInt(2, layout.clusters.h-3))
	m.clusterTable.SetStyles(tuiTableStyles(m.focus == focusClusters, "#5bc0eb", "#23445c"))
	m.clusterTable.SetColumns(clusterColumns(clusterW, m.payload.Sort))
	m.clusterTable.SetRows(m.clusterRows())
	m.clusterTable.SetCursor(clampInt(m.selected, 0, maxInt(0, len(m.payload.Clusters)-1)))
	if m.focus == focusClusters {
		m.clusterTable.Focus()
	} else {
		m.clusterTable.Blur()
	}

	m.memberTable.SetWidth(memberW)
	m.memberTable.SetHeight(maxInt(2, layout.members.h-3))
	m.memberTable.SetStyles(tuiTableStyles(m.focus == focusMembers, "#9bc53d", "#33521e"))
	m.memberTable.SetColumns(memberColumns(memberW, m.memberSort))
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
	return table.New(table.WithStyles(tuiTableStyles(false, "#5bc0eb", "#23445c")), table.WithFocused(false))
}

func tuiTableStyles(focused bool, accent, inactive string) table.Styles {
	styles := table.DefaultStyles()
	styles.Header = styles.Header.
		Bold(true).
		Padding(0, 1, 0, 0).
		Foreground(lipgloss.Color(accent))
	styles.Cell = styles.Cell.Foreground(lipgloss.Color("#dfe7ef")).Padding(0, 1, 0, 0)
	selectedBG := inactive
	selectedFG := "#f7f7ff"
	if focused {
		selectedBG = "#f7f7ff"
		selectedFG = "#05070d"
	}
	styles.Selected = styles.Selected.
		Foreground(lipgloss.Color(selectedFG)).
		Background(lipgloss.Color(selectedBG)).
		Bold(true)
	return styles
}

func clusterColumns(width int, sortMode string) []table.Column {
	width = maxInt(28, width)
	available := maxInt(23, width-5)
	idW := 7
	cntW := 4
	kindW := 3
	ageW := 7
	clusterW := clampInt(available/3, 10, 16)
	titleW := maxInt(8, available-idW-cntW-clusterW-kindW-ageW)
	cntTitle := "cnt"
	ageTitle := "age"
	if sortMode == "size" {
		cntTitle = "cnt*"
	}
	if sortMode == "recent" {
		ageTitle = "age*"
	}
	return []table.Column{
		{Title: "id", Width: idW},
		{Title: cntTitle, Width: cntW},
		{Title: "cluster", Width: clusterW},
		{Title: "title", Width: titleW},
		{Title: "k", Width: kindW},
		{Title: ageTitle, Width: ageW},
	}
}

func memberColumns(width int, sortMode tuiMemberSort) []table.Column {
	width = maxInt(28, width)
	available := maxInt(24, width-4)
	numberW := 8
	stateW := 4
	ageW := 7
	titleW := maxInt(8, available-numberW-stateW-ageW)
	numberTitle := "number"
	stateTitle := "st"
	ageTitle := "age"
	titleTitle := "title"
	if sortMode == memberSortNumber {
		numberTitle = "number*"
	}
	if sortMode == memberSortState {
		stateTitle = "st*"
	}
	if sortMode == memberSortRecent {
		ageTitle = "age*"
	}
	if sortMode == memberSortTitle {
		titleTitle = "title*"
	}
	return []table.Column{
		{Title: numberTitle, Width: numberW},
		{Title: stateTitle, Width: stateW},
		{Title: ageTitle, Width: ageW},
		{Title: titleTitle, Width: titleW},
	}
}

func (m clusterBrowserModel) clusterRows() []table.Row {
	if len(m.payload.Clusters) == 0 {
		return []table.Row{{"", "", "", "No clusters visible. Press f, /, x, or r.", "", ""}}
	}
	rows := make([]table.Row, 0, len(m.payload.Clusters))
	for _, cluster := range m.payload.Clusters {
		rows = append(rows, table.Row{
			fmt.Sprintf("C%d", cluster.ID),
			fmt.Sprintf("%d", cluster.MemberCount),
			cluster.StableSlug,
			splitClusterTitle(cluster),
			kindGlyph(cluster.RepresentativeKind),
			formatRelativeTime(cluster.UpdatedAt),
		})
	}
	return rows
}

func (m clusterBrowserModel) memberTableRows() []table.Row {
	if len(m.memberRows) == 0 {
		return []table.Row{{"", "", "", "Select a cluster to inspect members."}}
	}
	rows := make([]table.Row, 0, len(m.memberRows))
	for _, member := range m.memberRows {
		if !member.selectable {
			rows = append(rows, table.Row{"", "", "", member.label})
			continue
		}
		thread := member.thread()
		rows = append(rows, table.Row{
			fmt.Sprintf("#%d", thread.Number),
			stateGlyph(thread.State),
			formatRelativeTime(thread.UpdatedAtGitHub),
			thread.Title,
		})
	}
	return rows
}

func (m clusterBrowserModel) pageStep() int {
	switch m.focus {
	case focusMembers:
		return m.memberViewportHeight()
	case focusDetail:
		return maxInt(1, m.detailView.Height)
	default:
		return m.clusterViewportHeight()
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

func (m *clusterBrowserModel) sortClustersFromHeader(relativeX int) {
	columns := clusterColumns(maxInt(24, m.layout().clusters.w-4), m.payload.Sort)
	if relativeX < columnRightEdge(columns, 1) {
		m.payload.Sort = "size"
	} else if relativeX >= columnLeftEdge(columns, len(columns)-1) {
		m.payload.Sort = "recent"
	} else if m.payload.Sort == "recent" {
		m.payload.Sort = "size"
	} else {
		m.payload.Sort = "recent"
	}
	m.sortClusters()
	m.loadSelectedCluster()
	m.status = "Sort: " + m.payload.Sort
}

func (m *clusterBrowserModel) jumpToThreadNumber(number int) {
	if number <= 0 {
		m.status = "Enter a positive issue or PR number"
		return
	}
	clusterID := m.findLoadedClusterIDForThreadNumber(number)
	if clusterID == 0 && m.store != nil && m.repoID != 0 {
		foundID, err := m.store.ClusterIDForThreadNumber(m.ctx, m.repoID, number, true)
		if err != nil {
			m.status = err.Error()
			return
		}
		clusterID = foundID
		if _, ok := m.detailCache[clusterID]; !ok {
			detail, err := m.store.ClusterDetail(m.ctx, store.ClusterDetailOptions{
				RepoID:        m.repoID,
				ClusterID:     clusterID,
				IncludeClosed: true,
				MemberLimit:   200,
				BodyChars:     1600,
			})
			if err != nil {
				m.status = "Jump failed: " + err.Error()
				return
			}
			m.detailCache[clusterID] = detail
			m.ensureClusterInWorkingSet(detail.Cluster)
		}
	}
	if clusterID == 0 {
		m.status = fmt.Sprintf("Thread #%d was not found in loaded clusters", number)
		return
	}
	if !m.selectClusterIDForJump(clusterID) {
		m.status = fmt.Sprintf("Cluster %d is not available in this view", clusterID)
		return
	}
	if m.selectMemberByNumber(number) {
		m.focus = focusMembers
		m.status = fmt.Sprintf("Jumped to #%d", number)
		return
	}
	m.focus = focusMembers
	m.status = fmt.Sprintf("Jumped to cluster %d; #%d is outside loaded members", clusterID, number)
}

func (m clusterBrowserModel) findLoadedClusterIDForThreadNumber(number int) int64 {
	if m.hasDetail {
		for _, member := range m.detail.Members {
			if member.Thread.Number == number {
				return m.detail.Cluster.ID
			}
		}
	}
	for _, detail := range m.detailCache {
		for _, member := range detail.Members {
			if member.Thread.Number == number {
				return detail.Cluster.ID
			}
		}
	}
	for _, cluster := range m.allClusters {
		if cluster.RepresentativeNumber == number {
			return cluster.ID
		}
	}
	return 0
}

func (m *clusterBrowserModel) ensureClusterInWorkingSet(cluster store.ClusterSummary) {
	if cluster.ID == 0 {
		return
	}
	for _, existing := range m.allClusters {
		if existing.ID == cluster.ID {
			return
		}
	}
	m.allClusters = append(m.allClusters, cluster)
}

func (m *clusterBrowserModel) selectClusterIDForJump(clusterID int64) bool {
	if m.selectVisibleClusterID(clusterID) {
		return true
	}
	cluster, ok := m.clusterFromWorkingSet(clusterID)
	if !ok {
		return false
	}
	m.search = ""
	if m.minSize > cluster.MemberCount {
		m.minSize = 1
	}
	if cluster.Status != "active" || cluster.ClosedAt != "" {
		m.showClosed = true
	}
	if m.payload.Limit > 0 && len(m.allClusters) > m.payload.Limit {
		m.payload.Limit = len(m.allClusters)
	}
	m.applyClusterFilters()
	return m.selectVisibleClusterID(clusterID)
}

func (m *clusterBrowserModel) selectVisibleClusterID(clusterID int64) bool {
	for index, cluster := range m.payload.Clusters {
		if cluster.ID == clusterID {
			m.selected = index
			m.loadSelectedCluster()
			return true
		}
	}
	return false
}

func (m clusterBrowserModel) clusterFromWorkingSet(clusterID int64) (store.ClusterSummary, bool) {
	for _, cluster := range m.allClusters {
		if cluster.ID == clusterID {
			return cluster, true
		}
	}
	return store.ClusterSummary{}, false
}

func (m *clusterBrowserModel) selectMemberByNumber(number int) bool {
	for index, row := range m.memberRows {
		if row.selectable && row.member.Thread.Number == number {
			m.memberIndex = index
			m.detailView.GotoTop()
			return true
		}
	}
	return false
}

func (m *clusterBrowserModel) refreshFromStore() {
	if m.store == nil || m.repoID == 0 {
		m.status = "Refresh unavailable for this view"
		return
	}
	currentID := int64(0)
	if len(m.payload.Clusters) > 0 && m.selected >= 0 && m.selected < len(m.payload.Clusters) {
		currentID = m.payload.Clusters[m.selected].ID
	}
	viewLimit := maxInt(20, m.payload.Limit)
	clusters, err := m.store.ListClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        m.repoID,
		IncludeClosed: m.showClosed,
		MinSize:       m.minSize,
		Limit:         viewLimit,
		Sort:          m.payload.Sort,
	})
	if err != nil {
		m.status = "Refresh failed: " + err.Error()
		return
	}
	workingSet, err := m.store.ListClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        m.repoID,
		IncludeClosed: true,
		MinSize:       1,
		Limit:         maxInt(defaultTUIWorkingSetLimit, maxInt(m.payload.Limit, len(m.allClusters))),
		Sort:          m.payload.Sort,
	})
	if err != nil {
		m.status = "Refresh failed: " + err.Error()
		return
	}
	clusters = mergeClusterSummaries(clusters, workingSet)
	if clusters == nil {
		clusters = []store.ClusterSummary{}
	}
	m.detailCache = map[int64]store.ClusterDetail{}
	m.allClusters = append([]store.ClusterSummary(nil), clusters...)
	m.payload.Clusters = append([]store.ClusterSummary(nil), clusters...)
	m.applyClusterFilters()
	if currentID != 0 {
		for index, cluster := range m.payload.Clusters {
			if cluster.ID == currentID {
				m.selected = index
				m.loadSelectedCluster()
				break
			}
		}
	}
	m.status = fmt.Sprintf("Refreshed %d cluster(s)", len(m.payload.Clusters))
}

func (m *clusterBrowserModel) switchRepository(fullName string) {
	if m.store == nil {
		m.status = "Repository picker unavailable for this view"
		return
	}
	fullName = strings.TrimSpace(fullName)
	if fullName == "" {
		m.status = "No repository selected"
		return
	}
	repo, err := m.store.RepositoryByFullName(m.ctx, fullName)
	if err != nil {
		m.status = "Repository switch failed: " + err.Error()
		return
	}
	clusters, err := m.store.ListClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: m.showClosed,
		MinSize:       m.minSize,
		Limit:         maxInt(20, m.payload.Limit),
		Sort:          m.payload.Sort,
	})
	if err != nil {
		m.status = "Repository switch failed: " + err.Error()
		return
	}
	workingSet, err := m.store.ListClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: true,
		MinSize:       1,
		Limit:         maxInt(defaultTUIWorkingSetLimit, m.payload.Limit),
		Sort:          m.payload.Sort,
	})
	if err != nil {
		m.status = "Repository switch failed: " + err.Error()
		return
	}
	clusters = mergeClusterSummaries(clusters, workingSet)
	if clusters == nil {
		clusters = []store.ClusterSummary{}
	}
	m.repoID = repo.ID
	m.payload.Repository = repo.FullName
	m.payload.InferredRepository = false
	m.detailCache = map[int64]store.ClusterDetail{}
	m.neighborCache = map[int64][]tuiNeighbor{}
	m.allClusters = append([]store.ClusterSummary(nil), clusters...)
	m.payload.Clusters = append([]store.ClusterSummary(nil), clusters...)
	m.search = ""
	m.searchInput.SetValue("")
	m.selected = 0
	m.clusterOff = 0
	m.memberOff = 0
	m.memberIndex = -1
	m.hasDetail = false
	m.detail = store.ClusterDetail{}
	m.applyClusterFilters()
	m.focus = focusClusters
	m.status = "Repository: " + repo.FullName
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
	if m.payload.Limit > 0 && len(m.payload.Clusters) > m.payload.Limit {
		m.payload.Clusters = m.payload.Clusters[:m.payload.Limit]
	}
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

func (m *clusterBrowserModel) sortMembersFromHeader(relativeX int) {
	columns := memberColumns(maxInt(24, m.layout().members.w-4), m.memberSort)
	switch {
	case relativeX < columnRightEdge(columns, 0):
		m.memberSort = memberSortNumber
	case relativeX < columnRightEdge(columns, 1):
		m.memberSort = memberSortState
	case relativeX < columnRightEdge(columns, 2):
		m.memberSort = memberSortRecent
	default:
		if m.memberSort == memberSortTitle {
			m.memberSort = memberSortKind
		} else {
			m.memberSort = memberSortTitle
		}
	}
	m.sortMembers()
	m.status = "Member sort: " + string(m.memberSort)
}

func (m *clusterBrowserModel) loadSelectedCluster() {
	m.detailView.GotoTop()
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
	m.sortMembers()
}

func (m *clusterBrowserModel) sortMembers() {
	selectedID := int64(0)
	if member, ok := m.selectedMember(); ok {
		selectedID = member.Thread.ID
	}
	members := make([]store.ClusterMemberDetail, 0, len(m.detail.Members))
	for _, member := range m.detail.Members {
		if !m.showClosed && member.Thread.State != "open" {
			continue
		}
		members = append(members, member)
	}
	sort.SliceStable(members, func(i, j int) bool {
		left := members[i].Thread
		right := members[j].Thread
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
	m.memberRows = m.buildMemberRows(members)
	m.memberIndex = m.firstSelectableMemberIndex()
	if selectedID != 0 {
		for index, row := range m.memberRows {
			if row.selectable && row.member.Thread.ID == selectedID {
				m.memberIndex = index
				break
			}
		}
	}
}

func (m clusterBrowserModel) buildMemberRows(members []store.ClusterMemberDetail) []memberRow {
	if m.memberSort != memberSortKind {
		rows := make([]memberRow, 0, len(members))
		for _, member := range members {
			rows = append(rows, memberRow{member: member, selectable: true})
		}
		return rows
	}
	issues := make([]store.ClusterMemberDetail, 0, len(members))
	pulls := make([]store.ClusterMemberDetail, 0, len(members))
	other := make([]store.ClusterMemberDetail, 0)
	for _, member := range members {
		switch member.Thread.Kind {
		case "issue":
			issues = append(issues, member)
		case "pull_request":
			pulls = append(pulls, member)
		default:
			other = append(other, member)
		}
	}
	rows := make([]memberRow, 0, len(members)+3)
	appendGroup := func(label string, group []store.ClusterMemberDetail) {
		if len(group) == 0 {
			return
		}
		rows = append(rows, memberRow{label: fmt.Sprintf("%s (%d)", label, len(group))})
		for _, member := range group {
			rows = append(rows, memberRow{member: member, selectable: true})
		}
	}
	appendGroup("ISSUES", issues)
	appendGroup("PULL REQUESTS", pulls)
	appendGroup("OTHER", other)
	return rows
}

func (m clusterBrowserModel) firstSelectableMemberIndex() int {
	for index, row := range m.memberRows {
		if row.selectable {
			return index
		}
	}
	return -1
}

func (m clusterBrowserModel) lastSelectableMemberIndex() int {
	for index := len(m.memberRows) - 1; index >= 0; index-- {
		if m.memberRows[index].selectable {
			return index
		}
	}
	return -1
}

func (m clusterBrowserModel) nextSelectableMemberIndex(current, delta int) int {
	if len(m.memberRows) == 0 {
		return -1
	}
	step := 1
	if delta < 0 {
		step = -1
	}
	steps := maxInt(1, absInt(delta))
	if current < 0 || current >= len(m.memberRows) || !m.memberRows[current].selectable {
		if step < 0 {
			return m.lastSelectableMemberIndex()
		}
		return m.firstSelectableMemberIndex()
	}
	index := current
	for moved := 0; moved < steps; moved++ {
		for attempts := 0; attempts < len(m.memberRows); attempts++ {
			index += step
			if index < 0 {
				index = len(m.memberRows) - 1
			}
			if index >= len(m.memberRows) {
				index = 0
			}
			if m.memberRows[index].selectable {
				break
			}
		}
	}
	return index
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

func (m clusterBrowserModel) selectableMemberCount() int {
	count := 0
	for _, row := range m.memberRows {
		if row.selectable {
			count++
		}
	}
	return count
}

func (m clusterBrowserModel) clusterPositionLabel() string {
	total := len(m.payload.Clusters)
	if total == 0 {
		return "0"
	}
	return fmt.Sprintf("%d/%d", clampInt(m.selected+1, 1, total), total)
}

func (m clusterBrowserModel) memberPositionLabel() string {
	total := m.selectableMemberCount()
	if total == 0 {
		return "0"
	}
	position := 0
	for _, row := range m.memberRows[:clampInt(m.memberIndex+1, 0, len(m.memberRows))] {
		if row.selectable {
			position++
		}
	}
	if position == 0 {
		position = 1
	}
	return fmt.Sprintf("%d/%d", position, total)
}

func (m clusterBrowserModel) selectedThread() (store.Thread, bool) {
	if len(m.memberRows) == 0 || m.memberIndex < 0 || m.memberIndex >= len(m.memberRows) {
		return store.Thread{}, false
	}
	if !m.memberRows[m.memberIndex].selectable {
		return store.Thread{}, false
	}
	thread := m.memberRows[m.memberIndex].thread()
	if strings.TrimSpace(thread.HTMLURL) == "" {
		return store.Thread{}, false
	}
	return thread, true
}

func (m clusterBrowserModel) hasSelectedCluster() bool {
	return len(m.payload.Clusters) > 0 && m.selected >= 0 && m.selected < len(m.payload.Clusters)
}

func (m clusterBrowserModel) selectedCluster() (store.ClusterSummary, bool) {
	if !m.hasSelectedCluster() {
		return store.ClusterSummary{}, false
	}
	return m.payload.Clusters[m.selected], true
}

func (m clusterBrowserModel) selectedClusterURL() (string, bool) {
	cluster, ok := m.selectedCluster()
	if !ok || cluster.RepresentativeNumber <= 0 || strings.TrimSpace(m.payload.Repository) == "" {
		return "", false
	}
	path := "issues"
	if cluster.RepresentativeKind == "pull_request" {
		path = "pull"
	}
	return fmt.Sprintf("https://github.com/%s/%s/%d", m.payload.Repository, path, cluster.RepresentativeNumber), true
}

func (m clusterBrowserModel) selectedMember() (store.ClusterMemberDetail, bool) {
	if len(m.memberRows) == 0 || m.memberIndex < 0 || m.memberIndex >= len(m.memberRows) {
		return store.ClusterMemberDetail{}, false
	}
	if !m.memberRows[m.memberIndex].selectable {
		return store.ClusterMemberDetail{}, false
	}
	return m.memberRows[m.memberIndex].member, true
}

func (m clusterBrowserModel) firstReferenceLink() (string, bool) {
	links := m.referenceLinks()
	if len(links) > 0 {
		return links[0], true
	}
	return "", false
}

func (m clusterBrowserModel) referenceLinks() []string {
	member, ok := m.selectedMember()
	if !ok {
		return nil
	}
	links := make([]string, 0, 4)
	seen := map[string]bool{}
	for _, value := range append([]string{member.BodySnippet}, sortedSummaryValues(member.Summaries)...) {
		for _, link := range markdownLinks(value) {
			if !seen[link] {
				links = append(links, link)
				seen[link] = true
			}
		}
	}
	return links
}

func (m clusterBrowserModel) threadDetailClipboardText() string {
	member, ok := m.selectedMember()
	if !ok {
		return ""
	}
	thread := member.Thread
	lines := []string{
		fmt.Sprintf("%s #%d: %s", kindTitle(thread.Kind), thread.Number, thread.Title),
		"State: " + firstNonEmpty(thread.State, "unknown"),
		"Author: " + firstNonEmpty(thread.AuthorLogin, "unknown"),
		"Updated: " + firstNonEmpty(thread.UpdatedAtGitHub, thread.UpdatedAt, "unknown"),
		"URL: " + thread.HTMLURL,
	}
	if summaries := summariesClipboardText(member.Summaries); summaries != "" {
		lines = append(lines, "", "Summaries", summaries)
	}
	if strings.TrimSpace(member.BodySnippet) != "" {
		lines = append(lines, "", "Body preview", member.BodySnippet)
	}
	if links := m.referenceLinks(); len(links) > 0 {
		lines = append(lines, "", "Links", strings.Join(links, "\n"))
	}
	if neighbors := m.neighborsClipboardText(); neighbors != "" {
		lines = append(lines, "", "Neighbors")
		lines = append(lines, neighbors)
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) summariesClipboardText() string {
	member, ok := m.selectedMember()
	if !ok {
		return ""
	}
	return summariesClipboardText(member.Summaries)
}

func summariesClipboardText(summaries map[string]string) string {
	if len(summaries) == 0 {
		return ""
	}
	lines := make([]string, 0, len(summaries)*2)
	for _, key := range sortedSummaryKeys(summaries) {
		lines = append(lines, formatSummaryLabel(key)+":", summaries[key], "")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func (m clusterBrowserModel) neighborsClipboardText() string {
	member, ok := m.selectedMember()
	if !ok {
		return ""
	}
	neighbors, ok := m.neighborCache[member.Thread.ID]
	if !ok {
		return ""
	}
	if len(neighbors) == 0 {
		return "No neighbors above threshold."
	}
	lines := make([]string, 0, len(neighbors))
	for _, neighbor := range neighbors {
		lines = append(lines, fmt.Sprintf("#%d %s %.1f%% %s",
			neighbor.Thread.Number,
			kindTitle(neighbor.Thread.Kind),
			neighbor.Score*100,
			neighbor.Thread.Title,
		))
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) clusterClipboardText() string {
	if len(m.payload.Clusters) == 0 || m.selected < 0 || m.selected >= len(m.payload.Clusters) {
		return ""
	}
	cluster := m.payload.Clusters[m.selected]
	lines := []string{
		fmt.Sprintf("Cluster %d", cluster.ID),
		"Name: " + cluster.StableSlug,
		"Title: " + firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster"),
		fmt.Sprintf("State: %s", firstNonEmpty(cluster.Status, "unknown")),
		fmt.Sprintf("Members: %d", cluster.MemberCount),
		"Updated: " + firstNonEmpty(cluster.UpdatedAt, "unknown"),
		"Representative: " + threadRef(cluster),
	}
	if member, ok := m.selectedMember(); ok {
		thread := member.Thread
		lines = append(lines, "", fmt.Sprintf("%s #%d: %s", kindTitle(thread.Kind), thread.Number, thread.Title), thread.HTMLURL)
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) visibleClustersClipboardText() string {
	if len(m.payload.Clusters) == 0 {
		return ""
	}
	lines := make([]string, 0, len(m.payload.Clusters))
	for _, cluster := range m.payload.Clusters {
		lines = append(lines, fmt.Sprintf(
			"C%d [%s] %d items %s - %s (%s)",
			cluster.ID,
			firstNonEmpty(cluster.Status, "unknown"),
			cluster.MemberCount,
			cluster.StableSlug,
			firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster"),
			threadRef(cluster),
		))
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) memberListClipboardText() string {
	if len(m.memberRows) == 0 {
		return ""
	}
	lines := make([]string, 0, len(m.memberRows))
	for _, row := range m.memberRows {
		if !row.selectable {
			continue
		}
		thread := row.thread()
		lines = append(lines, fmt.Sprintf("#%d [%s] %s %s %s",
			thread.Number,
			firstNonEmpty(thread.State, "unknown"),
			kindTitle(thread.Kind),
			thread.Title,
			thread.HTMLURL,
		))
	}
	return strings.Join(lines, "\n")
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

func paneTitle(pane, focus tuiFocus, suffix string) string {
	label := map[tuiFocus]string{
		focusClusters: "Clusters",
		focusMembers:  "Members",
		focusDetail:   "Detail",
	}[pane]
	if strings.TrimSpace(suffix) != "" {
		label += " " + suffix
	}
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

func (m *clusterBrowserModel) toggleWideLayout() {
	if m.wideLayout == wideLayoutColumns {
		m.wideLayout = wideLayoutRightStack
	} else {
		m.wideLayout = wideLayoutColumns
	}
	m.status = "Layout: " + string(m.wideLayout)
}

func (m *clusterBrowserModel) toggleDetailMode() {
	m.compactDetail = !m.compactDetail
	if m.compactDetail {
		m.status = "Detail mode: compact"
		return
	}
	m.status = "Detail mode: full"
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

func closedToggleLabel(showClosed bool) string {
	if showClosed {
		return "Hide closed"
	}
	return "Show closed"
}

func detailModeToggleLabel(compact bool) string {
	if compact {
		return "Show full detail"
	}
	return "Show compact detail"
}

func detailModeLabel(compact bool) string {
	if compact {
		return "compact"
	}
	return "full"
}

func layoutLabel(layout tuiLayout) string {
	if layout.mode != "" {
		return layout.mode
	}
	if layout.stacked {
		return "stacked"
	}
	return string(wideLayoutColumns)
}

func splitClusterTitle(cluster store.ClusterSummary) string {
	return firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster")
}

func sortedSummaryKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, key := range summaryKeyOrder {
		if strings.TrimSpace(values[key]) != "" {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	var extra []string
	for key, value := range values {
		if !seen[key] && strings.TrimSpace(value) != "" {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	keys = append(keys, extra...)
	return keys
}

func sortedSummaryValues(values map[string]string) []string {
	keys := sortedSummaryKeys(values)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, values[key])
	}
	return out
}

func formatSummaryLabel(key string) string {
	switch key {
	case "key_summary":
		return "Key summary"
	case "problem_summary":
		return "Purpose"
	case "solution_summary":
		return "Solution"
	case "maintainer_signal_summary":
		return "Maintainer signal"
	case "dedupe_summary":
		return "Cluster signal"
	default:
		return strings.ReplaceAll(key, "_", " ")
	}
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

func kindGlyph(kind string) string {
	if kind == "pull_request" {
		return "PR"
	}
	if kind == "issue" {
		return "I"
	}
	return truncateCells(firstNonEmpty(kind, "?"), 2)
}

func kindTitle(kind string) string {
	if kind == "pull_request" {
		return "PR"
	}
	return "Issue"
}

func stateGlyph(state string) string {
	switch state {
	case "open":
		return "opn"
	case "closed":
		return "cls"
	case "merged":
		return "mrg"
	default:
		return truncateCells(firstNonEmpty(state, "?"), 3)
	}
}

func closedLabel(thread store.Thread) string {
	if thread.State == "open" {
		return "no"
	}
	closedAt := firstNonEmpty(thread.ClosedAtLocal, thread.ClosedAtGitHub, thread.State)
	if thread.CloseReasonLocal != "" {
		return closedAt + " (" + thread.CloseReasonLocal + ")"
	}
	return closedAt
}

func tuiRule(width int) string {
	return strings.Repeat("-", minInt(72, maxInt(12, width)))
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
		if lipgloss.Width(word) > width {
			if line != "" {
				lines = append(lines, line)
				line = ""
			}
			lines = append(lines, truncateCells(word, width))
			continue
		}
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
	width = maxInt(20, width)
	var lines []string
	inFence := false
	blankRun := 0
	for _, rawLine := range strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n") {
		line := strings.TrimRight(stripTerminalControls(rawLine), " \t")
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inFence = !inFence
			lines = append(lines, dim("--- code ---"))
			blankRun = 0
			continue
		}
		if inFence {
			lines = append(lines, dim(truncateCells(line, width)))
			blankRun = 0
			continue
		}
		if trimmed == "" {
			blankRun++
			if blankRun <= 1 {
				lines = append(lines, "")
			}
			continue
		}
		blankRun = 0
		if match := markdownHeadingRE.FindStringSubmatch(trimmed); match != nil {
			lines = appendWrappedStyled(lines, "", renderInlineMarkdown(match[2]), width, bold)
			continue
		}
		if strings.HasPrefix(trimmed, ">") {
			quote := strings.TrimSpace(strings.TrimPrefix(trimmed, ">"))
			lines = appendWrappedStyled(lines, "> ", renderInlineMarkdown(quote), width, dim)
			continue
		}
		if match := markdownListRE.FindStringSubmatch(line); match != nil {
			indent := match[1]
			if lipgloss.Width(indent) > 4 {
				indent = strings.Repeat(" ", 4)
			}
			lines = appendWrappedStyled(lines, indent+"- ", renderInlineMarkdown(match[3]), width, nil)
			continue
		}
		lines = appendWrappedStyled(lines, "", renderInlineMarkdown(line), width, nil)
	}
	return trimTrailingBlankLines(lines)
}

func appendWrappedStyled(lines []string, prefix, value string, width int, styler func(string) string) []string {
	contentWidth := maxInt(8, width-lipgloss.Width(prefix))
	wrapped := wrapPlain(value, contentWidth)
	if len(wrapped) == 0 {
		return lines
	}
	continuation := strings.Repeat(" ", lipgloss.Width(prefix))
	for index, line := range wrapped {
		prefixForLine := prefix
		if index > 0 {
			prefixForLine = continuation
		}
		if styler != nil {
			line = styler(line)
		}
		lines = append(lines, prefixForLine+line)
	}
	return lines
}

func renderInlineMarkdown(value string) string {
	value = markdownLinkRE.ReplaceAllString(value, "$1 <$2>")
	replacer := strings.NewReplacer(
		"`", "",
		"**", "",
		"__", "",
		"~~", "",
	)
	return strings.TrimSpace(replacer.Replace(value))
}

func firstMarkdownLink(value string) (string, bool) {
	links := markdownLinks(value)
	if len(links) == 0 {
		return "", false
	}
	return links[0], true
}

func markdownLinks(value string) []string {
	links := make([]string, 0, 2)
	seen := map[string]bool{}
	for _, match := range markdownLinkRE.FindAllStringSubmatch(value, -1) {
		if len(match) > 2 {
			link := stripTrailingURLPunctuation(match[2])
			if !seen[link] {
				links = append(links, link)
				seen[link] = true
			}
		}
	}
	for _, match := range bareLinkRE.FindAllStringSubmatch(value, -1) {
		if len(match) > 2 {
			link := stripTrailingURLPunctuation(match[2])
			if !seen[link] {
				links = append(links, link)
				seen[link] = true
			}
		}
	}
	return links
}

func formatLinkChoiceLabel(url string, index int) string {
	return fmt.Sprintf("%2d  %s", index+1, url)
}

func stripTrailingURLPunctuation(value string) string {
	return strings.TrimRight(value, ".,;:!?")
}

func stripTerminalControls(value string) string {
	return terminalControlRE.ReplaceAllString(value, "")
}

func trimTrailingBlankLines(lines []string) []string {
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func (m clusterBrowserModel) detailBodyLimit() int {
	if m.compactDetail {
		return 18
	}
	return 240
}

func appendLimitedLines(out, lines []string, limit int) []string {
	if limit <= 0 || len(lines) <= limit {
		return append(out, lines...)
	}
	omitted := len(lines) - limit
	out = append(out, lines[:limit]...)
	return append(out, dim(fmt.Sprintf("... %d more line(s). Press d for full detail.", omitted)))
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

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
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

func columnLeftEdge(columns []table.Column, index int) int {
	left := 0
	for i := 0; i < index && i < len(columns); i++ {
		left += columns[i].Width + 1
	}
	return left
}

func columnRightEdge(columns []table.Column, index int) int {
	if index < 0 || index >= len(columns) {
		return 0
	}
	return columnLeftEdge(columns, index) + columns[index].Width
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
