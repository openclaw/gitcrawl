package cli

import (
	tea "github.com/charmbracelet/bubbletea"
)

func (m clusterBrowserModel) updateMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	page := max(1, m.menuVisibleCount())
	if index, ok := visibleMenuShortcutIndex(msg.String(), m.menuItems, m.menuOff, page); ok {
		m.menuIndex = index
		if m.runMenuItem(m.menuItems[m.menuIndex]) {
			m.closeMenu("")
		}
		if m.quitRequested {
			return m, tea.Quit
		}
		return m, m.takePendingCmd()
	}
	switch msg.String() {
	case "esc", "q":
		m.closeMenu("Menu closed")
	case "h", "?":
		m.closeMenu("")
		m.showHelp = true
		m.status = "Help"
	case "b", "left", "backspace":
		if m.inMenuSubmenu() {
			m.openActionMenuFor(m.menuContext)
		}
	case "a":
		if m.inMenuSubmenu() {
			m.openActionMenuFor(m.menuContext)
		}
	case "/":
		cmd := m.startFilterInput()
		return m, cmd
	case "#":
		cmd := m.startJumpInput()
		return m, cmd
	case "p":
		m.openRepositoryMenu()
	case "n":
		m.closeMenu("")
		return m, m.requestSelectedThreadNeighbors(10, 0.2)
	case "r":
		m.closeMenu("")
		m.refreshFromStore()
	case "l":
		m.closeMenu("")
		m.toggleWideLayout()
	case "d":
		m.closeMenu("")
		m.toggleDetailMode()
	case "s":
		m.closeMenu("")
		if m.payload.Sort == "recent" {
			m.payload.Sort = "size"
		} else {
			m.payload.Sort = "recent"
		}
		m.sortClustersPreservingSelection()
		m.loadSelectedCluster()
		m.status = "Sort: " + m.payload.Sort
	case "m":
		m.closeMenu("")
		m.memberSort = nextMemberSort(m.memberSort)
		m.sortMembers()
		m.status = "Member sort: " + string(m.memberSort)
	case "up", "k":
		m.menuIndex = m.nextSelectableMenuIndex(-1)
		m.keepMenuVisible()
	case "down", "j":
		m.menuIndex = m.nextSelectableMenuIndex(1)
		m.keepMenuVisible()
	case "pgup", "ctrl+b":
		m.menuIndex = m.nearestSelectableMenuIndex(m.menuIndex-page, -1)
		m.keepMenuVisible()
	case "pgdown", "ctrl+f":
		m.menuIndex = m.nearestSelectableMenuIndex(m.menuIndex+page, 1)
		m.keepMenuVisible()
	case "home", "g":
		m.menuIndex = m.firstSelectableMenuIndex()
		m.keepMenuVisible()
	case "end", "G":
		m.menuIndex = m.lastSelectableMenuIndex()
		m.keepMenuVisible()
	case "enter":
		if m.menuIndex >= 0 && m.menuIndex < len(m.menuItems) {
			if m.runMenuItem(m.menuItems[m.menuIndex]) {
				m.closeMenu("")
			}
			if m.quitRequested {
				return m, tea.Quit
			}
			return m, m.takePendingCmd()
		}
	}
	return m, nil
}

func (m clusterBrowserModel) menuVisibleCount() int {
	if m.menuFloating && m.menuRect.h > 0 {
		return max(1, m.menuRect.h-7)
	}
	height := m.detailView.Height
	if height <= 0 {
		height = max(1, m.layout().detail.h-2)
	}
	return max(1, height-4)
}

func visibleMenuShortcutIndex(key string, items []tuiMenuItem, menuOff, visible int) (int, bool) {
	if len(key) != 1 || key[0] < '1' || key[0] > '9' {
		return 0, false
	}
	want := int(key[0] - '0')
	seen := 0
	end := min(len(items), menuOff+max(1, visible))
	for index := menuOff; index < end; index++ {
		if !items[index].selectable() {
			continue
		}
		seen++
		if seen == want {
			return index, true
		}
	}
	return 0, false
}

func (m clusterBrowserModel) firstSelectableMenuIndex() int {
	for index, item := range m.menuItems {
		if item.selectable() {
			return index
		}
	}
	return 0
}

func (m clusterBrowserModel) lastSelectableMenuIndex() int {
	for index := len(m.menuItems) - 1; index >= 0; index-- {
		if m.menuItems[index].selectable() {
			return index
		}
	}
	return max(0, len(m.menuItems)-1)
}

func (m clusterBrowserModel) nextSelectableMenuIndex(delta int) int {
	if delta == 0 || len(m.menuItems) == 0 {
		return m.menuIndex
	}
	for index := m.menuIndex + delta; index >= 0 && index < len(m.menuItems); index += delta {
		if m.menuItems[index].selectable() {
			return index
		}
	}
	return m.menuIndex
}

func (m clusterBrowserModel) nearestSelectableMenuIndex(index, direction int) int {
	if len(m.menuItems) == 0 {
		return 0
	}
	index = clampInt(index, 0, len(m.menuItems)-1)
	if m.menuItems[index].selectable() {
		return index
	}
	if direction == 0 {
		direction = 1
	}
	for next := index + direction; next >= 0 && next < len(m.menuItems); next += direction {
		if m.menuItems[next].selectable() {
			return next
		}
	}
	if direction > 0 {
		return m.lastSelectableMenuIndex()
	}
	return m.firstSelectableMenuIndex()
}

func (m *clusterBrowserModel) keepMenuVisible() {
	if len(m.menuItems) == 0 {
		m.menuOff = 0
		return
	}
	visible := m.menuVisibleCount()
	m.menuIndex = m.nearestSelectableMenuIndex(m.menuIndex, 1)
	if m.menuIndex > 0 && !m.menuItems[m.menuIndex-1].selectable() && m.menuIndex-1 < m.menuOff {
		m.menuOff = m.menuIndex - 1
	} else if m.menuIndex < m.menuOff {
		m.menuOff = m.menuIndex
	}
	if m.menuIndex >= m.menuOff+visible {
		m.menuOff = m.menuIndex - visible + 1
	}
	m.menuOff = clampInt(m.menuOff, 0, max(0, len(m.menuItems)-visible))
}
