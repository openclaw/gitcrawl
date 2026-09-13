package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/openclaw/gitcrawl/internal/store"
)

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
	order := []tuiMemberSort{memberSortKind, memberSortRecent, memberSortOldest, memberSortNumber, memberSortState, memberSortTitle}
	for index, item := range order {
		if item == current {
			return order[(index+1)%len(order)]
		}
	}
	return memberSortKind
}

func (m *clusterBrowserModel) toggleWideLayout() {
	switch m.wideLayout {
	case wideLayoutColumns:
		m.wideLayout = wideLayoutRightStack
	case wideLayoutRightStack:
		m.wideLayout = wideLayoutFocus
	default:
		m.wideLayout = wideLayoutColumns
	}
	m.status = "Layout: " + string(m.wideLayout)
}

func normalizeTUILayout(value string) tuiWideLayout {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(wideLayoutColumns), "":
		return wideLayoutColumns
	case string(wideLayoutRightStack), "right_stack", "rightstack":
		return wideLayoutRightStack
	case string(wideLayoutFocus):
		return wideLayoutFocus
	default:
		return wideLayoutColumns
	}
}

func isSupportedTUILayout(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case string(wideLayoutColumns), string(wideLayoutRightStack), "right_stack", "rightstack", string(wideLayoutFocus), "":
		return true
	default:
		return false
	}
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

func clusterRowStyle(cluster store.ClusterSummary, selected bool, focused bool) lipgloss.Style {
	status := strings.ToLower(firstNonEmpty(cluster.Status, "active"))
	if cluster.ClosedAt != "" && status == "active" {
		status = "closed"
	}
	switch status {
	case "closed":
		if selected {
			return selectedRowStyle(focused, tuiClosedSelectedBG, tuiClosedSelectedFG, tuiClosedSelectedBlurBG, tuiClosedSelectedBlurFG)
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color(tuiClosedRowFG)).Background(lipgloss.Color(tuiClosedRowBG))
	case "merged", "split":
		if selected {
			return selectedRowStyle(focused, "#394052", "#d8c4ff", "#242936", "#b8a3d8")
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color("#b8a3d8")).Background(lipgloss.Color("#151620"))
	default:
		if selected {
			return selectedRowStyle(focused, tuiOpenSelectedBG, tuiOpenSelectedFG, tuiOpenSelectedBlurBG, tuiOpenSelectedBlurFG)
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color(tuiOpenRowFG)).Background(lipgloss.Color(tuiOpenRowBG))
	}
}

func memberRowStyle(row memberRow, selected bool, focused bool) lipgloss.Style {
	if !row.selectable {
		return lipgloss.NewStyle().Foreground(lipgloss.Color(tuiMutedAccent)).Bold(true)
	}
	state := strings.ToLower(memberDisplayState(row.member))
	switch state {
	case "closed", "local", "merged":
		if selected {
			return selectedRowStyle(focused, tuiClosedSelectedBG, tuiClosedSelectedFG, tuiClosedSelectedBlurBG, tuiClosedSelectedBlurFG)
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color(tuiClosedRowFG)).Background(lipgloss.Color(tuiClosedRowBG))
	default:
		if selected {
			return selectedRowStyle(focused, tuiOpenSelectedBG, tuiOpenSelectedFG, tuiOpenSelectedBlurBG, tuiOpenSelectedBlurFG)
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color(tuiOpenRowFG)).Background(lipgloss.Color(tuiOpenRowBG))
	}
}

func selectedRowStyle(focused bool, focusedBG, focusedFG, blurredBG, blurredFG string) lipgloss.Style {
	style := lipgloss.NewStyle()
	if focused {
		return style.Foreground(lipgloss.Color(focusedFG)).Background(lipgloss.Color(focusedBG))
	}
	return style.Foreground(lipgloss.Color(blurredFG)).Background(lipgloss.Color(blurredBG))
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

func floatingMenuStyle(width, height int, palette actionMenuPalette) lipgloss.Style {
	return lipgloss.NewStyle().
		Width(max(1, width-2)).
		Height(max(1, height-2)).
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color(palette.accent)).
		Background(lipgloss.Color(palette.background)).
		Foreground(lipgloss.Color(palette.foreground))
}

func selectedMenuLineStyle(width int, palette actionMenuPalette) lipgloss.Style {
	return lipgloss.NewStyle().
		Width(max(1, width)).
		Background(lipgloss.Color(palette.selectedBG)).
		Foreground(lipgloss.Color(palette.selectedFG)).
		Bold(true)
}
