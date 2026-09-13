package cli

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

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
		m.search = m.searchBeforeEdit
		m.searchInput.Blur()
		m.applyClusterFilters()
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
	m.searchBeforeEdit = m.search
	m.jumping = false
	m.showHelp = false
	m.closeMenu("")
	m.searchInput.Prompt = "/ "
	m.searchInput.Placeholder = "filter clusters"
	m.searchInput.SetValue(m.search)
	m.status = "Filter: " + m.search
	return m.searchInput.Focus()
}

func (m *clusterBrowserModel) startJumpInput() tea.Cmd {
	m.jumping = true
	m.searching = false
	m.showHelp = false
	m.closeMenu("")
	m.searchInput.Prompt = "# "
	m.searchInput.Placeholder = "issue, PR, or GitHub URL"
	m.searchInput.SetValue("")
	m.status = "Jump to issue/PR"
	return m.searchInput.Focus()
}

func (m clusterBrowserModel) handleJumpKey(msg tea.KeyMsg) (clusterBrowserModel, tea.Cmd) {
	switch msg.String() {
	case "enter":
		m.jumping = false
		value := strings.TrimSpace(m.searchInput.Value())
		m.searchInput.Blur()
		number, err := parseOptionalThreadNumber(value)
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
