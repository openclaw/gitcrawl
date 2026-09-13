package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

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
	if !layout.stacked && layout.mode == string(wideLayoutFocus) {
		top := lipgloss.JoinHorizontal(lipgloss.Top, clusters, detail)
		body = lipgloss.JoinVertical(lipgloss.Left, top, members)
	}
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
	body = fitBlock(body, layout.header.w, max(1, layout.footer.y-layout.header.h))
	view := lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
	if m.menuOpen && m.menuFloating {
		view = m.renderFloatingMenu(view)
	}
	return fitBlock(view, layout.header.w, m.height)
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
	width := max(m.width, 80)
	height := max(m.height, 24)
	headerH := 1
	footerH := 2
	bodyH := max(8, height-headerH-footerH)
	layout := tuiLayout{
		header: tuiRect{x: 0, y: 0, w: width, h: headerH},
		footer: tuiRect{x: 0, y: headerH + bodyH, w: width, h: footerH},
	}
	if width >= 140 {
		if m.wideLayout == wideLayoutFocus {
			topH := max(10, bodyH*68/100)
			memberH := max(6, bodyH-topH)
			topH = bodyH - memberH
			clusterW := max(48, width*32/100)
			detailW := width - clusterW
			layout.mode = string(wideLayoutFocus)
			layout.clusters = tuiRect{x: 0, y: headerH, w: clusterW, h: topH}
			layout.detail = tuiRect{x: clusterW, y: headerH, w: detailW, h: topH}
			layout.members = tuiRect{x: 0, y: headerH + topH, w: width, h: memberH}
			return layout
		}
		if m.wideLayout == wideLayoutRightStack {
			clusterW := max(56, width*44/100)
			rightW := width - clusterW
			memberH := max(8, bodyH*42/100)
			layout.mode = string(wideLayoutRightStack)
			layout.clusters = tuiRect{x: 0, y: headerH, w: clusterW, h: bodyH}
			layout.members = tuiRect{x: clusterW, y: headerH, w: rightW, h: memberH}
			layout.detail = tuiRect{x: clusterW, y: headerH + memberH, w: rightW, h: bodyH - memberH}
			return layout
		}
		clusterW := max(48, width*34/100)
		memberW := max(40, width*30/100)
		detailW := max(42, width-clusterW-memberW)
		layout.mode = string(wideLayoutColumns)
		layout.clusters = tuiRect{x: 0, y: headerH, w: clusterW, h: bodyH}
		layout.members = tuiRect{x: clusterW, y: headerH, w: memberW, h: bodyH}
		layout.detail = tuiRect{x: clusterW + memberW, y: headerH, w: detailW, h: bodyH}
		return layout
	}
	if width < 100 {
		layout.stacked = true
		layout.mode = "stacked"
		clusterH := max(7, bodyH*36/100)
		memberH := max(6, bodyH*28/100)
		detailH := max(6, bodyH-clusterH-memberH)
		layout.clusters = tuiRect{x: 0, y: headerH, w: width, h: clusterH}
		layout.members = tuiRect{x: 0, y: headerH + clusterH, w: width, h: memberH}
		layout.detail = tuiRect{x: 0, y: headerH + clusterH + memberH, w: width, h: detailH}
		return layout
	}
	layout.stacked = true
	layout.mode = "split"
	topH := max(8, bodyH/2)
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
	content := padCells(" "+truncateCells(line, max(1, width-2)), width)
	style := lipgloss.NewStyle().Width(width).Height(1).Background(lipgloss.Color("#0d1321")).Foreground(lipgloss.Color("#f7f7ff")).Bold(true)
	return style.Render(content)
}

func (m clusterBrowserModel) renderFooter(width int) string {
	controls := footerControls(width)
	line := firstNonEmpty(m.status, "Ready")
	if m.searching {
		line = "Filter: " + m.searchInput.View()
	}
	if m.jumping {
		line = "Jump: " + m.searchInput.View()
	}
	if m.remoteRefreshing {
		line = fmt.Sprintf("Refreshing remote data %s  %s", loadingFrame(m.remoteFrame), line)
	}
	if location := m.footerLocation(); location != "" {
		line = strings.TrimSpace(line + "  " + location)
	}
	bg, fg := footerPalette(m.payload.DBSource)
	statusLine := padCells(" "+truncateCells(line, max(1, width-2)), width)
	controlsLine := padCells(" "+truncateCells(controls, max(1, width-2)), width)
	return lipgloss.NewStyle().Width(width).Height(2).Background(bg).Foreground(fg).Render(statusLine + "\n" + controlsLine)
}

func footerControls(width int) string {
	full := "Tab focus  click select  right-click menu  a actions  header sort  wheel scroll  / filter  # jump  p repos  n neighbors  s sort  m members  d detail  r refresh  f min  l layout  x closed  ? help  q quit"
	if lipgloss.Width(full) <= max(1, width-2) {
		return full
	}
	compact := "Tab focus  click select  right-click menu  a actions  wheel scroll  / filter  # jump  r refresh  ? help  q quit"
	if lipgloss.Width(compact) <= max(1, width-2) {
		return compact
	}
	return "Tab focus click right-click menu a actions / filter # jump ? help q quit"
}

func loadingFrame(index int) string {
	frames := []string{"-", "\\", "|", "/"}
	return frames[index%len(frames)]
}

func (m clusterBrowserModel) footerLocation() string {
	location := strings.TrimSpace(m.payload.DBLocation)
	if location == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(m.payload.DBSource)) {
	case "remote":
		return "remote " + location
	case "local":
		return "local " + location
	default:
		return location
	}
}

func footerPalette(source string) (lipgloss.Color, lipgloss.Color) {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "remote":
		return lipgloss.Color("#f2c14e"), lipgloss.Color("#05070d")
	default:
		return lipgloss.Color("#5bc0eb"), lipgloss.Color("#05070d")
	}
}

func (m clusterBrowserModel) renderClusters(rect tuiRect) string {
	tableWidth := tableViewportWidth(rect)
	tableView := renderStyledTable(clusterColumns(tableWidth, m.payload.Sort), m.clusterRows(), m.clusterOff, tableViewportHeight(rect), tableWidth, "#5bc0eb", func(index int) lipgloss.Style {
		if index < 0 || index >= len(m.payload.Clusters) {
			return lipgloss.NewStyle().Foreground(lipgloss.Color("#dfe7ef"))
		}
		return clusterRowStyle(m.payload.Clusters[index], index == m.selected, m.focus == focusClusters)
	})
	return paneStyle(focusClusters, m.focus, rect.w, rect.h).Render(lipgloss.JoinVertical(lipgloss.Left, paneTitle(focusClusters, m.focus, m.clusterPositionLabel()), tableView))
}

func (m clusterBrowserModel) renderMembers(rect tuiRect) string {
	tableWidth := tableViewportWidth(rect)
	tableView := renderStyledTable(memberColumns(tableWidth, m.memberSort), m.memberTableRows(), m.memberOff, tableViewportHeight(rect), tableWidth, "#9bc53d", func(index int) lipgloss.Style {
		if index < 0 || index >= len(m.memberRows) {
			return lipgloss.NewStyle().Foreground(lipgloss.Color("#dfe7ef"))
		}
		return memberRowStyle(m.memberRows[index], index == m.memberIndex, m.focus == focusMembers)
	})
	return paneStyle(focusMembers, m.focus, rect.w, rect.h).Render(lipgloss.JoinVertical(lipgloss.Left, paneTitle(focusMembers, m.focus, m.memberPositionLabel()), tableView))
}

// detailPaneText builds the full text shown in the detail pane for the given
// content width. It is the single source of truth for both the per-frame
// render (renderDetail) and the persistent viewport content set in
// syncComponents, so scrolling and display never diverge.
func (m clusterBrowserModel) detailPaneText(width int) string {
	mode := "full"
	if m.compactDetail {
		mode = "compact"
	}
	lines := append([]string{paneTitle(focusDetail, m.focus, mode)}, m.detailLines(width)...)
	if m.showHelp {
		lines = append([]string{paneTitle(focusDetail, m.focus, mode)}, m.helpLines(width)...)
	}
	if m.menuOpen && !m.menuFloating {
		lines = append([]string{paneTitle(focusDetail, m.focus, mode)}, m.menuLines(width)...)
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) renderDetail(rect tuiRect) string {
	// renderDetail runs from View() on a value copy, so the SetContent here
	// only ever affects this frame's copy. The persistent viewport content is
	// set in syncComponents (Update path); without that, the live model's
	// viewport stays empty and keyboard/wheel scrolling has nothing to move.
	m.detailView.SetContent(m.detailPaneText(rect.w - 4))
	return paneStyle(focusDetail, m.focus, rect.w, rect.h).Render(m.detailView.View())
}

func (r tuiRect) contains(x, y int) bool {
	return x >= r.x && x < r.x+r.w && y >= r.y && y < r.y+r.h
}

func (m *clusterBrowserModel) keepVisible() {
	m.clusterOff = keepRowVisible(m.clusterOff, m.selected, len(m.payload.Clusters), m.clusterViewportHeight())
	m.memberOff = keepRowVisible(m.memberOff, m.memberIndex, len(m.memberRows), m.memberViewportHeight())
}

func (m clusterBrowserModel) clusterVisibleStart() int {
	return keepRowVisible(m.clusterOff, m.selected, len(m.payload.Clusters), m.clusterViewportHeight())
}

func (m clusterBrowserModel) memberVisibleStart() int {
	return keepRowVisible(m.memberOff, m.memberIndex, len(m.memberRows), m.memberViewportHeight())
}

func (m clusterBrowserModel) clusterViewportHeight() int {
	return tableViewportHeight(m.layout().clusters)
}

func (m clusterBrowserModel) memberViewportHeight() int {
	return tableViewportHeight(m.layout().members)
}

func tableViewportWidth(rect tuiRect) int {
	return max(24, rect.w-4)
}

func tableViewportHeight(rect tuiRect) int {
	return max(1, max(2, rect.h-3)-1)
}

func keepRowVisible(offset, selected, rowCount, viewportHeight int) int {
	if rowCount <= 0 || selected < 0 {
		return 0
	}
	viewportHeight = max(1, viewportHeight)
	selected = clampInt(selected, 0, rowCount-1)
	maxOffset := max(0, rowCount-viewportHeight)
	offset = clampInt(offset, 0, maxOffset)
	if selected < offset {
		return selected
	}
	if selected >= offset+viewportHeight {
		return clampInt(selected-viewportHeight+1, 0, maxOffset)
	}
	return offset
}

func (m *clusterBrowserModel) syncComponents() {
	layout := m.layout()
	detailW := max(24, layout.detail.w-4)
	detailH := max(2, layout.detail.h-2)

	m.detailView.Width = detailW
	m.detailView.Height = detailH
	m.detailView.MouseWheelEnabled = true
	m.detailView.MouseWheelDelta = 3
	// Set content on the persistent viewport (this runs from Update, which
	// keeps the model) so the line count is real and keyboard/wheel scrolling
	// has something to move. renderDetail repeats this on the View() copy for
	// per-frame display freshness.
	contentKey := m.detailPaneContentKey()
	if contentKey != m.detailContentKey {
		m.detailView.GotoTop()
		m.detailContentKey = contentKey
	}
	m.syncDetailViewContent()
	m.searchInput.Width = max(20, m.width-16)
}

func (m *clusterBrowserModel) syncDetailViewContent() {
	width := m.detailView.Width
	if width <= 0 {
		width = max(1, m.layout().detail.w-4)
	}
	m.detailView.SetContent(m.detailPaneText(width))
	if m.detailView.PastBottom() {
		m.detailView.GotoBottom()
	}
}

func (m clusterBrowserModel) detailPaneContentKey() string {
	if m.showHelp {
		return "help"
	}
	if m.menuOpen && !m.menuFloating {
		return "menu:" + m.menuTitle
	}
	mode := "full"
	if m.compactDetail {
		mode = "compact"
	}
	if member, ok := m.selectedMember(); ok {
		return fmt.Sprintf("detail:%s:%d", mode, member.Thread.ID)
	}
	if len(m.payload.Clusters) > 0 && m.selected >= 0 && m.selected < len(m.payload.Clusters) {
		cluster := m.payload.Clusters[m.selected]
		return fmt.Sprintf("detail:%s:cluster:%d:%s", mode, cluster.ID, cluster.Source)
	}
	return "detail:" + mode
}

func renderStyledTable(columns []table.Column, rows []table.Row, offset, height, width int, headerColor string, styleForRow func(index int) lipgloss.Style) string {
	height = max(1, height)
	width = max(1, width)
	lines := make([]string, 0, height+1)
	lines = append(lines, renderTableHeader(columns, width, headerColor))
	for line := 0; line < height; line++ {
		index := offset + line
		if index < 0 || index >= len(rows) {
			lines = append(lines, lipgloss.NewStyle().Width(width).Render(""))
			continue
		}
		lines = append(lines, renderTableRow(columns, rows[index], width, styleForRow(index)))
	}
	return strings.Join(lines, "\n")
}

func renderTableHeader(columns []table.Column, width int, headerColor string) string {
	values := make(table.Row, 0, len(columns))
	for _, column := range columns {
		values = append(values, column.Title)
	}
	line := truncateCells(renderTableCells(columns, values), width)
	return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(headerColor)).Width(width).Render(line)
}

func renderTableRow(columns []table.Column, row table.Row, width int, rowStyle lipgloss.Style) string {
	line := truncateCells(renderTableCells(columns, row), width)
	return rowStyle.Width(width).Render(line)
}

func renderTableCells(columns []table.Column, row table.Row) string {
	cells := make([]string, 0, min(len(columns), len(row)))
	cellStyle := lipgloss.NewStyle().Padding(0, 1, 0, 0)
	for index, value := range row {
		if index >= len(columns) || columns[index].Width <= 0 {
			continue
		}
		column := columns[index]
		cell := lipgloss.NewStyle().Width(column.Width).MaxWidth(column.Width).Inline(true).Render(truncateCells(value, column.Width))
		cells = append(cells, cellStyle.Render(cell))
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, cells...)
}

func clusterColumns(width int, sortMode string) []table.Column {
	width = max(28, width)
	available := max(30, width-5)
	idW := 7
	cntW := 4
	stateW := 7
	kindW := 3
	ageW := 7
	clusterW := clampInt(available/4, 10, 16)
	titleW := max(8, available-idW-cntW-stateW-clusterW-kindW-ageW)
	cntTitle := "cnt"
	ageTitle := "age"
	if sortMode == "size" {
		cntTitle = "cnt*"
	}
	if sortMode == "recent" {
		ageTitle = "age-"
	}
	if sortMode == "oldest" {
		ageTitle = "age+"
	}
	return []table.Column{
		{Title: "id", Width: idW},
		{Title: cntTitle, Width: cntW},
		{Title: "state", Width: stateW},
		{Title: "cluster", Width: clusterW},
		{Title: "title", Width: titleW},
		{Title: "k", Width: kindW},
		{Title: ageTitle, Width: ageW},
	}
}

func memberColumns(width int, sortMode tuiMemberSort) []table.Column {
	width = max(28, width)
	available := max(24, width-4)
	numberW := 8
	stateW := 4
	ageW := 7
	titleW := max(8, available-numberW-stateW-ageW)
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
		ageTitle = "age-"
	}
	if sortMode == memberSortOldest {
		ageTitle = "age+"
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

func overlayBlock(base, block string, x, y, width int) string {
	baseLines := strings.Split(base, "\n")
	blockLines := strings.Split(block, "\n")
	for offset, line := range blockLines {
		row := y + offset
		if row < 0 || row >= len(baseLines) {
			continue
		}
		baseLine := baseLines[row]
		prefix := strings.Repeat(" ", max(0, x))
		if x > 0 && baseLine != "" {
			prefix = padCells(ansi.Cut(baseLine, 0, x), x)
		}
		lineWidth := ansi.StringWidth(line)
		suffixStart := max(0, x+lineWidth)
		suffix := ""
		if suffixStart < ansi.StringWidth(baseLine) {
			suffix = ansi.Cut(baseLine, suffixStart, width)
		}
		rendered := prefix + line + suffix
		if width > 0 {
			rendered = truncateCells(rendered, width)
		}
		baseLines[row] = rendered
	}
	return strings.Join(baseLines, "\n")
}

func padCells(value string, width int) string {
	if width <= 0 {
		return ""
	}
	cellWidth := ansi.StringWidth(value)
	if cellWidth >= width {
		return ansi.Cut(value, 0, width)
	}
	return value + strings.Repeat(" ", width-cellWidth)
}

func fitBlock(value string, width, height int) string {
	width = max(1, width)
	height = max(1, height)
	lines := strings.Split(value, "\n")
	if len(lines) > height {
		lines = lines[:height]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	for index, line := range lines {
		lines[index] = padCells(line, width)
	}
	return strings.Join(lines, "\n")
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
