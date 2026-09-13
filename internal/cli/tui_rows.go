package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (m clusterBrowserModel) clusterRows() []table.Row {
	if len(m.payload.Clusters) == 0 {
		return []table.Row{{"", "", "", "", "No clusters visible. Press f, /, x, or r.", "", ""}}
	}
	rows := make([]table.Row, 0, len(m.payload.Clusters))
	for _, cluster := range m.payload.Clusters {
		rows = append(rows, table.Row{
			fmt.Sprintf("C%d", cluster.ID),
			fmt.Sprintf("%d", cluster.MemberCount),
			clusterStateLabel(cluster),
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
			stateGlyph(memberDisplayState(member.member)),
			formatRelativeTime(thread.UpdatedAtGitHub),
			renderTitleText(thread.Title),
		})
	}
	return rows
}

func (m clusterBrowserModel) pageStep() int {
	switch m.focus {
	case focusMembers:
		return m.memberViewportHeight()
	case focusDetail:
		return max(1, m.detailView.Height)
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
		if m.payload.Sort == "oldest" {
			leftUpdated := parseTime(left.UpdatedAt)
			rightUpdated := parseTime(right.UpdatedAt)
			if !leftUpdated.Equal(rightUpdated) {
				return leftUpdated.Before(rightUpdated)
			}
			return left.ID < right.ID
		}
		return parseTime(left.UpdatedAt).After(parseTime(right.UpdatedAt))
	})
	m.selected = clampInt(m.selected, 0, max(0, len(m.payload.Clusters)-1))
}

func (m *clusterBrowserModel) sortClustersPreservingSelection() {
	currentKey := m.currentClusterKey()
	m.sortClusters()
	if currentKey == "" {
		return
	}
	for index, cluster := range m.payload.Clusters {
		if clusterSummaryKey(cluster) == currentKey {
			m.selected = index
			return
		}
	}
}

func (m *clusterBrowserModel) sortClustersFromHeader(relativeX int) {
	columns := clusterColumns(max(24, m.layout().clusters.w-4), m.payload.Sort)
	if relativeX < columnRightEdge(columns, 1) {
		m.payload.Sort = "size"
	} else if relativeX >= columnLeftEdge(columns, len(columns)-1) {
		if m.payload.Sort == "recent" {
			m.payload.Sort = "oldest"
		} else {
			m.payload.Sort = "recent"
		}
	} else if m.payload.Sort == "recent" {
		m.payload.Sort = "size"
	} else {
		m.payload.Sort = "recent"
	}
	m.sortClustersPreservingSelection()
	m.loadSelectedCluster()
	m.status = "Sort: " + m.payload.Sort
}

func (m *clusterBrowserModel) sortMembersFromHeader(relativeX int) {
	columns := memberColumns(max(24, m.layout().members.w-4), m.memberSort)
	switch {
	case relativeX < columnRightEdge(columns, 0):
		m.memberSort = memberSortNumber
	case relativeX < columnRightEdge(columns, 1):
		m.memberSort = memberSortState
	case relativeX < columnRightEdge(columns, 2):
		if m.memberSort == memberSortRecent {
			m.memberSort = memberSortOldest
		} else {
			m.memberSort = memberSortRecent
		}
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
	// Capture the currently selected member before the state below is wiped.
	// sortMembers tries to restore the selection itself, but it reads
	// selectedMember() which depends on memberRows/memberIndex that this
	// function clears first, so on every reload path (auto-refresh, manual
	// refresh, sort/filter toggles, ...) that restore is a no-op and the
	// selection silently snaps back to the first row. Snapshot the stable
	// thread id here and re-apply it after the rebuild.
	prevMemberID := int64(0)
	if member, ok := m.selectedMember(); ok {
		prevMemberID = member.Thread.ID
	}
	prevClusterKey := ""
	if m.hasDetail {
		prevClusterKey = clusterSummaryKey(m.detail.Cluster)
	}
	prevDetailOffset := m.detailView.YOffset
	m.detailView.GotoTop()
	m.memberOff = 0
	m.memberIndex = -1
	m.memberRows = nil
	m.hasDetail = false
	if len(m.payload.Clusters) == 0 {
		return
	}
	cluster := m.payload.Clusters[m.selected]
	restoreSameCluster := prevClusterKey != "" && prevClusterKey == clusterSummaryKey(cluster)
	cacheKey := clusterSummaryKey(cluster)
	if cached, ok := m.detailCache[cacheKey]; ok {
		m.applyClusterDetail(cached)
		if restoreSameCluster && m.restoreMemberSelection(prevMemberID) {
			m.detailView.YOffset = prevDetailOffset
		}
		return
	}
	if m.store == nil {
		return
	}
	detail, err := m.store.ClusterDetail(m.ctx, store.ClusterDetailOptions{
		RepoID:        m.repoID,
		ClusterID:     cluster.ID,
		Source:        cluster.Source,
		IncludeClosed: true,
		MemberLimit:   200,
		BodyChars:     1600,
	})
	if err != nil {
		m.status = err.Error()
		return
	}
	m.detailCache[clusterSummaryKey(detail.Cluster)] = detail
	m.applyClusterDetail(detail)
	if restoreSameCluster && m.restoreMemberSelection(prevMemberID) {
		m.detailView.YOffset = prevDetailOffset
	}
}

// restoreMemberSelection re-selects the member whose thread matches id after a
// reload rebuilt memberRows. Threads are disjoint across clusters (a thread
// belongs to at most one cluster), so a match implies the same cluster is
// still selected and the user's selection should survive the refresh. When id
// is absent — the cluster was switched, or the member was filtered out — the
// first-selectable default chosen by sortMembers stands.
func (m *clusterBrowserModel) restoreMemberSelection(id int64) bool {
	if id == 0 || len(m.memberRows) == 0 {
		return false
	}
	for index, row := range m.memberRows {
		if row.selectable && row.member.Thread.ID == id {
			m.memberIndex = index
			m.keepVisible()
			return true
		}
	}
	return false
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
		if !memberVisible(member, m.showClosed) {
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
		case memberSortOldest:
			return parseTime(left.UpdatedAtGitHub).Before(parseTime(right.UpdatedAtGitHub))
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
	steps := max(1, absInt(delta))
	if current < 0 || current >= len(m.memberRows) || !m.memberRows[current].selectable {
		if step < 0 {
			return m.lastSelectableMemberIndex()
		}
		return m.firstSelectableMemberIndex()
	}
	index := current
	for moved := 0; moved < steps; moved++ {
		next := index + step
		for next >= 0 && next < len(m.memberRows) && !m.memberRows[next].selectable {
			next += step
		}
		if next < 0 || next >= len(m.memberRows) {
			return index
		}
		index = next
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
