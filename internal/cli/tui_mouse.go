package cli

import (
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func (m *clusterBrowserModel) handleMouse(msg tea.MouseMsg) tea.Cmd {
	layout := m.layout()
	if msg.Action == tea.MouseActionMotion && msg.Button == tea.MouseButtonNone {
		if m.menuOpen {
			return m.handleMenuMouse(layout, msg)
		}
		return nil
	}
	if msg.Button != tea.MouseButtonLeft && msg.Button != tea.MouseButtonRight && !isMouseWheel(msg.Button) {
		return nil
	}
	if !isMouseWheel(msg.Button) {
		m.cancelQueuedWheelScroll()
	}
	if m.menuOpen {
		return m.handleMenuMouse(layout, msg)
	}
	switch msg.Button {
	case tea.MouseButtonWheelUp:
		return m.mouseWheel(layout, msg, -3)
	case tea.MouseButtonWheelDown:
		return m.mouseWheel(layout, msg, 3)
	case tea.MouseButtonLeft:
		if msg.Action != tea.MouseActionPress {
			return nil
		}
		now := time.Now()
		switch {
		case layout.clusters.contains(msg.X, msg.Y):
			m.focus = focusClusters
			row := msg.Y - layout.clusters.y - 3
			if row == -1 {
				m.sortClustersFromHeader(msg.X - layout.clusters.x - 2)
				return nil
			}
			if row < 0 {
				return nil
			}
			index := m.clusterOff + row
			if index >= 0 && index < len(m.payload.Clusters) {
				m.selected = index
				m.loadSelectedCluster()
				m.status = fmt.Sprintf("Cluster %d", m.payload.Clusters[m.selected].ID)
				m.finishRowClick(focusClusters, index, msg.X, msg.Y, now)
			}
		case layout.members.contains(msg.X, msg.Y):
			m.focus = focusMembers
			row := msg.Y - layout.members.y - 3
			if row == -1 {
				m.sortMembersFromHeader(msg.X - layout.members.x - 2)
				return nil
			}
			if row < 0 {
				return nil
			}
			index := m.memberOff + row
			if index >= 0 && index < len(m.memberRows) {
				if !m.memberRows[index].selectable {
					m.memberIndex = index
					m.status = m.memberRows[index].label
					m.clearLastClick()
					return nil
				}
				previous := m.memberIndex
				m.memberIndex = index
				if m.memberIndex != previous {
					m.detailView.GotoTop()
				}
				m.status = fmt.Sprintf("Selected #%d", m.memberRows[m.memberIndex].thread().Number)
				m.finishRowClick(focusMembers, index, msg.X, msg.Y, now)
			}
		case layout.detail.contains(msg.X, msg.Y):
			m.focus = focusDetail
		}
	case tea.MouseButtonRight:
		if msg.Action != tea.MouseActionPress {
			return nil
		}
		context := m.actionMenuContextAt(layout, msg.X, msg.Y)
		m.selectByMousePosition(layout, msg.X, msg.Y)
		if context == focusMembers {
			if _, ok := m.selectedMember(); !ok {
				context = focusClusters
			}
		}
		m.openActionMenuFor(context)
		m.placeFloatingMenu(layout, msg.X, msg.Y)
	}
	return nil
}

func (m *clusterBrowserModel) finishRowClick(focus tuiFocus, index, x, y int, now time.Time) {
	if m.isDoubleClick(focus, index, x, y, now) {
		m.clearLastClick()
		m.runAction("open")
		return
	}
	m.lastClickFocus = focus
	m.lastClickIndex = index
	m.lastClickX = x
	m.lastClickY = y
	m.lastClickAt = now
}

func (m *clusterBrowserModel) isDoubleClick(focus tuiFocus, index, x, y int, now time.Time) bool {
	return !m.lastClickAt.IsZero() &&
		m.lastClickFocus == focus &&
		m.lastClickIndex == index &&
		m.lastClickX == x &&
		m.lastClickY == y &&
		now.Sub(m.lastClickAt) <= tuiDoubleClickWindow
}

func (m *clusterBrowserModel) clearLastClick() {
	m.lastClickAt = time.Time{}
}

func (m *clusterBrowserModel) handleMenuMouse(layout tuiLayout, msg tea.MouseMsg) tea.Cmd {
	if msg.Action == tea.MouseActionMotion {
		index, ok := m.menuIndexAtMouse(layout, msg.X, msg.Y)
		if !ok {
			return nil
		}
		if index < 0 || index >= len(m.menuItems) {
			return nil
		}
		if !m.menuItems[index].selectable() {
			index = m.nearestSelectableMenuIndex(index, 1)
		}
		if index >= 0 && index < len(m.menuItems) && m.menuItems[index].selectable() {
			m.menuIndex = index
			m.keepMenuVisible()
		}
		return nil
	}
	switch msg.Button {
	case tea.MouseButtonWheelUp:
		m.menuIndex = m.nextSelectableMenuIndex(-1)
		m.keepMenuVisible()
		return nil
	case tea.MouseButtonWheelDown:
		m.menuIndex = m.nextSelectableMenuIndex(1)
		m.keepMenuVisible()
		return nil
	case tea.MouseButtonRight:
		if msg.Action == tea.MouseActionPress {
			m.closeMenu("Menu closed")
		}
		return nil
	}
	if msg.Button != tea.MouseButtonLeft || msg.Action != tea.MouseActionPress {
		return nil
	}
	index, ok := m.menuIndexAtMouse(layout, msg.X, msg.Y)
	if !ok {
		m.closeMenu("Menu closed")
		return nil
	}
	if index < 0 || index >= len(m.menuItems) {
		return nil
	}
	if !m.menuItems[index].selectable() {
		m.menuIndex = m.nearestSelectableMenuIndex(index, 1)
		m.keepMenuVisible()
		return nil
	}
	m.menuIndex = index
	m.keepMenuVisible()
	if m.runMenuItem(m.menuItems[m.menuIndex]) {
		m.closeMenu("")
	}
	return m.takePendingCmd()
}

func (m clusterBrowserModel) menuIndexAtMouse(layout tuiLayout, x, y int) (int, bool) {
	menuRect := layout.detail
	rowOffset := 4
	if m.menuFloating {
		menuRect = m.menuRect
		rowOffset = 3
	}
	if !menuRect.contains(x, y) {
		return 0, false
	}
	return m.menuOff + y - menuRect.y - rowOffset, true
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
				if !m.memberRows[index].selectable {
					m.memberIndex = index
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

func (m clusterBrowserModel) actionMenuContextAt(layout tuiLayout, x, y int) tuiFocus {
	switch {
	case layout.clusters.contains(x, y):
		return focusClusters
	case layout.members.contains(x, y):
		return focusMembers
	case layout.detail.contains(x, y):
		return focusDetail
	default:
		return ""
	}
}

func isMouseWheel(button tea.MouseButton) bool {
	return button == tea.MouseButtonWheelUp || button == tea.MouseButtonWheelDown || button == tea.MouseButtonWheelLeft || button == tea.MouseButtonWheelRight
}

func (m *clusterBrowserModel) mouseWheel(layout tuiLayout, msg tea.MouseMsg, delta int) tea.Cmd {
	m.clearLastClick()
	switch {
	case layout.clusters.contains(msg.X, msg.Y):
		return m.queueWheelScroll(focusClusters, delta)
	case layout.members.contains(msg.X, msg.Y):
		return m.queueWheelScroll(focusMembers, delta)
	case layout.detail.contains(msg.X, msg.Y):
		return m.queueWheelScroll(focusDetail, delta)
	default:
		return m.queueWheelScroll(m.focus, delta)
	}
}

func (m *clusterBrowserModel) queueWheelScroll(focus tuiFocus, delta int) tea.Cmd {
	if delta == 0 {
		return nil
	}
	if m.wheelPending && m.wheelFocus != focus {
		m.cancelQueuedWheelScroll()
	}
	m.focus = focus
	m.wheelFocus = focus
	m.wheelDelta = clampInt(m.wheelDelta+delta, -tuiWheelMaxBufferedDelta, tuiWheelMaxBufferedDelta)
	if m.wheelPending {
		return nil
	}
	m.wheelPending = true
	m.wheelScrollSeq++
	seq := m.wheelScrollSeq
	return tea.Tick(tuiWheelScrollDelay, func(time.Time) tea.Msg {
		return tuiWheelScrollMsg{seq: seq}
	})
}

func (m *clusterBrowserModel) cancelQueuedWheelScroll() {
	if !m.wheelPending && m.wheelDelta == 0 {
		return
	}
	m.wheelPending = false
	m.wheelDelta = 0
	m.wheelScrollSeq++
}

func (m *clusterBrowserModel) applyQueuedWheelScroll() tea.Cmd {
	delta := m.wheelDelta
	focus := m.wheelFocus
	m.wheelPending = false
	m.wheelDelta = 0
	if delta == 0 {
		return nil
	}
	switch focus {
	case focusClusters:
		m.focus = focusClusters
		return m.moveClusterByWheel(delta)
	case focusMembers:
		m.focus = focusMembers
		m.move(delta)
	case focusDetail:
		m.focus = focusDetail
		m.move(delta)
	default:
		m.move(delta)
	}
	return nil
}

func (m *clusterBrowserModel) moveClusterByWheel(delta int) tea.Cmd {
	if len(m.payload.Clusters) == 0 {
		return nil
	}
	previous := m.selected
	m.selected = clampInt(m.selected+delta, 0, len(m.payload.Clusters)-1)
	if m.selected == previous {
		return nil
	}
	m.status = fmt.Sprintf("Cluster %d", m.payload.Clusters[m.selected].ID)
	m.wheelSeq++
	seq := m.wheelSeq
	return tea.Tick(tuiWheelSettleDelay, func(time.Time) tea.Msg {
		return tuiWheelSettledMsg{seq: seq}
	})
}
