package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type tuiMenuItem struct {
	label  string
	action string
	value  string
}

const tuiMenuSeparatorAction = "separator"

func (item tuiMenuItem) selectable() bool {
	return item.action != "" && item.action != tuiMenuSeparatorAction
}

func tuiMenuSection(label string) tuiMenuItem {
	return tuiMenuItem{label: label, action: tuiMenuSeparatorAction}
}

func menuHasSection(items []tuiMenuItem, label string) bool {
	for _, item := range items {
		if item.action == tuiMenuSeparatorAction && item.label == label {
			return true
		}
	}
	return false
}

func actionMenuTitle(context tuiFocus) string {
	switch context {
	case focusClusters:
		return "Cluster Actions"
	case focusMembers:
		return "Member Actions"
	case focusDetail:
		return "Detail Actions"
	default:
		return "Actions"
	}
}

func actionMenuSubtitle(context tuiFocus) string {
	switch context {
	case focusClusters:
		return "cluster scope"
	case focusMembers:
		return "selected member scope"
	case focusDetail:
		return "detail scope"
	default:
		return "current selection"
	}
}

type actionMenuPalette struct {
	accent     string
	background string
	foreground string
	selectedBG string
	selectedFG string
}

func actionMenuColors(context tuiFocus) actionMenuPalette {
	switch context {
	case focusClusters:
		return actionMenuPalette{
			accent:     "#8fb8d8",
			background: "#111827",
			foreground: "#d7dee8",
			selectedBG: "#2f3f56",
			selectedFG: "#f8fafc",
		}
	case focusMembers:
		return actionMenuPalette{
			accent:     "#a8b8a0",
			background: "#111a16",
			foreground: "#d7dee8",
			selectedBG: "#344337",
			selectedFG: "#f8fafc",
		}
	default:
		return actionMenuPalette{
			accent:     "#b8aa8f",
			background: "#151922",
			foreground: "#d7dee8",
			selectedBG: "#3f3a31",
			selectedFG: "#f8fafc",
		}
	}
}

func (m *clusterBrowserModel) openActionMenu() {
	m.openActionMenuFor("")
}

func (m *clusterBrowserModel) openActionMenuFor(context tuiFocus) {
	if context == focusMembers {
		if _, ok := m.selectedMember(); !ok {
			context = focusClusters
		}
	}
	if context == focusDetail {
		if _, ok := m.selectedThread(); !ok {
			context = focusClusters
		}
	}

	items := make([]tuiMenuItem, 0, 32)
	if context == "" {
		m.appendThreadMenuItems(&items)
		m.appendMemberClusterMenuItems(&items)
		m.appendClusterMenuItems(&items, true)
		m.appendReferenceLinkMenuItems(&items)
		m.appendViewMenuItems(&items)
	} else if context == focusMembers || context == focusDetail {
		m.appendThreadMenuItems(&items)
		m.appendMemberClusterMenuItems(&items)
		m.appendReferenceLinkMenuItems(&items)
		m.appendClusterContextMenuItems(&items)
		m.appendViewMenuItems(&items)
	} else if context == focusClusters {
		m.appendClusterMenuItems(&items, true)
		m.appendViewMenuItems(&items)
	}
	if len(items) == 0 {
		items = append(items, tuiMenuItem{label: "No actions available", action: "close-menu"})
	}
	items = append(items, tuiMenuItem{label: "Close menu", action: "close-menu"})

	m.menuItems = items
	m.menuContext = context
	m.menuTitle = actionMenuTitle(context)
	m.menuIndex = m.firstSelectableMenuIndex()
	m.menuOff = 0
	m.menuOpen = true
	m.showHelp = false
	m.status = m.menuTitle
}

func (m clusterBrowserModel) appendThreadMenuItems(items *[]tuiMenuItem) {
	if thread, ok := m.selectedThread(); ok {
		*items = append(*items,
			tuiMenuSection("Thread"),
			tuiMenuItem{label: fmt.Sprintf("Open #%d in browser", thread.Number), action: "open"},
			tuiMenuItem{label: "Copy selected URL", action: "copy-url"},
			tuiMenuItem{label: "Copy title", action: "copy-title"},
			tuiMenuItem{label: "Copy markdown link", action: "copy-markdown"},
			tuiMenuItem{label: "Copy selected detail", action: "copy-thread-detail"},
			tuiMenuItem{label: "Load neighbors", action: "load-neighbors"},
		)
		if thread.ClosedAtLocal != "" {
			*items = append(*items, tuiMenuItem{label: "Reopen locally...", action: "reopen-thread-confirm"})
		} else {
			*items = append(*items, tuiMenuItem{label: "Close locally...", action: "close-thread-confirm"})
		}
	}
}

func (m clusterBrowserModel) appendMemberClusterMenuItems(items *[]tuiMenuItem) {
	if member, ok := m.selectedMember(); ok {
		sectionAdded := false
		if cluster, clusterOK := m.selectedCluster(); clusterOK {
			if clusterSupportsDurableLocalActions(cluster) && member.State == "excluded" {
				if !sectionAdded {
					*items = append(*items, tuiMenuSection("Member in cluster"))
					sectionAdded = true
				}
				*items = append(*items, tuiMenuItem{label: fmt.Sprintf("Include #%d in C%d...", member.Thread.Number, cluster.ID), action: "include-member-confirm"})
			} else if clusterSupportsDurableLocalActions(cluster) {
				if !sectionAdded {
					*items = append(*items, tuiMenuSection("Member in cluster"))
					sectionAdded = true
				}
				*items = append(*items,
					tuiMenuItem{label: fmt.Sprintf("Exclude #%d from C%d...", member.Thread.Number, cluster.ID), action: "exclude-member-confirm"},
					tuiMenuItem{label: fmt.Sprintf("Set #%d as canonical...", member.Thread.Number), action: "canonical-member-confirm"},
				)
			}
		}
		if strings.TrimSpace(member.BodySnippet) != "" {
			if !menuHasSection(*items, "Thread") {
				*items = append(*items, tuiMenuSection("Thread"))
				sectionAdded = true
			}
			*items = append(*items, tuiMenuItem{label: "Copy body preview", action: "copy-body-preview"})
		}
		if len(member.Summaries) > 0 {
			if !sectionAdded && !menuHasSection(*items, "Thread") {
				*items = append(*items, tuiMenuSection("Thread"))
				sectionAdded = true
			}
			*items = append(*items, tuiMenuItem{label: "Copy summaries", action: "copy-summaries"})
		}
		if _, ok := m.neighborCache[member.Thread.ID]; ok {
			if !sectionAdded && !menuHasSection(*items, "Thread") {
				*items = append(*items, tuiMenuSection("Thread"))
			}
			*items = append(*items, tuiMenuItem{label: "Copy neighbors", action: "copy-neighbors"})
		}
	}
}

func (m clusterBrowserModel) appendClusterMenuItems(items *[]tuiMenuItem, includeVisible bool) {
	if m.hasSelectedCluster() {
		*items = append(*items, tuiMenuSection("Cluster"))
		if url, ok := m.selectedClusterURL(); ok {
			cluster, _ := m.selectedCluster()
			*items = append(*items,
				tuiMenuItem{label: fmt.Sprintf("Open representative #%d", cluster.RepresentativeNumber), action: "open-cluster-representative", value: url},
				tuiMenuItem{label: "Copy representative URL", action: "copy-cluster-url", value: url},
			)
		}
		*items = append(*items,
			tuiMenuItem{label: "Copy cluster ID", action: "copy-cluster-id"},
			tuiMenuItem{label: "Copy cluster name", action: "copy-cluster-name"},
			tuiMenuItem{label: "Copy cluster title", action: "copy-cluster-title"},
			tuiMenuItem{label: "Copy cluster summary", action: "copy-cluster"},
		)
		cluster, _ := m.selectedCluster()
		if clusterSupportsDurableLocalActions(cluster) {
			if cluster.Status == "closed" || cluster.ClosedAt != "" {
				*items = append(*items, tuiMenuItem{label: "Reopen cluster locally...", action: "reopen-cluster-confirm"})
			} else {
				*items = append(*items, tuiMenuItem{label: "Close cluster locally...", action: "close-cluster-confirm"})
			}
		}
		if m.hasDetail {
			*items = append(*items, tuiMenuItem{label: "Copy member list", action: "copy-member-list"})
		}
	}
	if includeVisible && len(m.payload.Clusters) > 0 {
		if !menuHasSection(*items, "Cluster") {
			*items = append(*items, tuiMenuSection("Cluster"))
		}
		*items = append(*items, tuiMenuItem{label: "Copy visible clusters", action: "copy-visible-clusters"})
	}
}

func (m clusterBrowserModel) appendClusterContextMenuItems(items *[]tuiMenuItem) {
	if !m.hasSelectedCluster() {
		return
	}
	*items = append(*items,
		tuiMenuSection("Cluster context"),
		tuiMenuItem{label: "Copy cluster summary", action: "copy-cluster"},
	)
	if m.hasDetail {
		*items = append(*items, tuiMenuItem{label: "Copy member list", action: "copy-member-list"})
	}
}

func (m clusterBrowserModel) appendReferenceLinkMenuItems(items *[]tuiMenuItem) {
	referenceLinks := m.referenceLinks()
	if len(referenceLinks) > 0 {
		*items = append(*items,
			tuiMenuSection("Links"),
			tuiMenuItem{label: "Open first body link", action: "open-first-link"},
			tuiMenuItem{label: "Copy first body link", action: "copy-first-link"},
		)
	}
	if len(referenceLinks) > 1 {
		*items = append(*items,
			tuiMenuItem{label: "Open body link...", action: "open-link-picker"},
			tuiMenuItem{label: "Copy body link...", action: "copy-link-picker"},
			tuiMenuItem{label: "Copy all body links", action: "copy-reference-links"},
		)
	}
}

func (m clusterBrowserModel) appendViewMenuItems(items *[]tuiMenuItem) {
	viewItems := []tuiMenuItem{
		tuiMenuSection("View"),
		tuiMenuItem{label: "Sort clusters by size", action: "sort-size"},
		tuiMenuItem{label: "Sort clusters by recent", action: "sort-recent"},
		tuiMenuItem{label: "Sort clusters by oldest", action: "sort-oldest"},
		tuiMenuItem{label: "Member sort grouped", action: "member-sort-kind"},
		tuiMenuItem{label: "Member sort recent", action: "member-sort-recent"},
		tuiMenuItem{label: "Member sort oldest", action: "member-sort-oldest"},
		tuiMenuItem{label: "Filter clusters...", action: "filter"},
	}
	if strings.TrimSpace(m.search) != "" {
		viewItems = append(viewItems, tuiMenuItem{label: "Clear filter", action: "clear-filter"})
	}
	viewItems = append(viewItems,
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
	*items = append(*items, viewItems...)
}

func (m *clusterBrowserModel) clearMenuPlacement() {
	m.menuFloating = false
	m.menuRect = tuiRect{}
}

func (m *clusterBrowserModel) closeMenu(status string) {
	m.menuOpen = false
	m.clearMenuPlacement()
	if status != "" {
		m.status = status
	}
}

func (m *clusterBrowserModel) placeFloatingMenu(layout tuiLayout, x, y int) {
	if !m.menuOpen {
		return
	}
	maxWidth := max(24, m.width-2)
	width := clampInt(m.preferredMenuWidth(), 34, min(58, maxWidth))
	availableHeight := max(1, m.height-layout.header.h-layout.footer.h)
	visibleRows := min(max(1, len(m.menuItems)), 12)
	height := min(visibleRows+7, availableHeight)
	if height < min(8, availableHeight) {
		height = min(8, availableHeight)
	}
	maxX := max(0, m.width-width)
	minY := layout.header.h
	maxY := max(minY, m.height-layout.footer.h-height)
	m.menuFloating = true
	m.menuRect = tuiRect{
		x: clampInt(x+1, 0, maxX),
		y: clampInt(y, minY, maxY),
		w: width,
		h: height,
	}
	m.keepMenuVisible()
}

func (m clusterBrowserModel) preferredMenuWidth() int {
	width := lipgloss.Width(firstNonEmpty(m.menuTitle, "Actions")) + 4
	for _, item := range m.menuItems {
		width = max(width, lipgloss.Width(item.label)+8)
	}
	return width
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
	currentIndex := 0
	for _, repo := range repos {
		label := repo.FullName
		if repo.FullName == m.payload.Repository {
			label = "* " + label
			currentIndex = len(items)
		}
		items = append(items, tuiMenuItem{label: label, action: "select-repo", value: repo.FullName})
	}
	items = append(items, tuiMenuItem{label: "Back to actions", action: "back-to-actions"})
	m.menuItems = items
	m.menuTitle = "Repositories"
	m.menuIndex = currentIndex
	m.menuOff = 0
	m.menuOpen = true
	m.showHelp = false
	m.searching = false
	m.jumping = false
	m.status = "Repository picker"
	m.keepMenuVisible()
}
