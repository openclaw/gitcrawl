package cli

import (
	"fmt"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (m *clusterBrowserModel) jumpToThreadNumber(number int) {
	if number <= 0 {
		m.status = "Enter a positive issue or PR number"
		return
	}
	cluster, clusterOK := m.findLoadedClusterForThreadNumber(number)
	if !clusterOK && m.store != nil && m.repoID != 0 {
		foundID, err := m.store.ClusterIDForThreadNumber(m.ctx, m.repoID, number, true)
		if err != nil {
			m.status = err.Error()
			return
		}
		cluster = store.ClusterSummary{ID: foundID, Source: store.ClusterSourceDurable}
		cacheKey := clusterSummaryKey(cluster)
		if cached, ok := m.detailCache[cacheKey]; ok {
			cluster = cached.Cluster
			m.ensureClusterInWorkingSet(cluster)
		} else {
			detail, err := m.store.ClusterDetail(m.ctx, store.ClusterDetailOptions{
				RepoID:        m.repoID,
				ClusterID:     foundID,
				Source:        store.ClusterSourceDurable,
				IncludeClosed: true,
				MemberLimit:   200,
				BodyChars:     1600,
			})
			if err != nil {
				m.status = "Jump failed: " + err.Error()
				return
			}
			m.detailCache[clusterSummaryKey(detail.Cluster)] = detail
			m.ensureClusterInWorkingSet(detail.Cluster)
			cluster = detail.Cluster
		}
		clusterOK = true
	}
	if !clusterOK || cluster.ID == 0 {
		m.status = fmt.Sprintf("Thread #%d was not found in loaded clusters", number)
		return
	}
	if !m.selectClusterForJump(cluster) {
		m.status = fmt.Sprintf("Cluster %d is not available in this view", cluster.ID)
		return
	}
	if m.selectMemberByNumber(number) {
		m.focus = focusMembers
		m.status = fmt.Sprintf("Jumped to #%d", number)
		return
	}
	m.focus = focusMembers
	m.status = fmt.Sprintf("Jumped to cluster %d; #%d is outside loaded members", cluster.ID, number)
}

func (m clusterBrowserModel) findLoadedClusterIDForThreadNumber(number int) int64 {
	cluster, ok := m.findLoadedClusterForThreadNumber(number)
	if !ok {
		return 0
	}
	return cluster.ID
}

func (m clusterBrowserModel) findLoadedClusterForThreadNumber(number int) (store.ClusterSummary, bool) {
	if m.hasDetail {
		for _, member := range m.detail.Members {
			if member.Thread.Number == number {
				return m.detail.Cluster, true
			}
		}
	}
	for _, detail := range m.detailCache {
		for _, member := range detail.Members {
			if member.Thread.Number == number {
				return detail.Cluster, true
			}
		}
	}
	for _, cluster := range m.allClusters {
		if cluster.RepresentativeNumber == number {
			return cluster, true
		}
	}
	return store.ClusterSummary{}, false
}

func (m *clusterBrowserModel) ensureClusterInWorkingSet(cluster store.ClusterSummary) {
	if cluster.ID == 0 {
		return
	}
	for _, existing := range m.allClusters {
		if sameClusterSummary(existing, cluster) {
			return
		}
	}
	m.allClusters = append(m.allClusters, cluster)
}

func (m *clusterBrowserModel) selectClusterIDForJump(clusterID int64) bool {
	return m.selectClusterForJump(store.ClusterSummary{ID: clusterID})
}

func (m *clusterBrowserModel) selectClusterForJump(cluster store.ClusterSummary) bool {
	if m.selectVisibleCluster(cluster) {
		return true
	}
	cluster, ok := m.clusterSummaryFromWorkingSet(cluster)
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
	return m.selectVisibleCluster(cluster)
}

func (m *clusterBrowserModel) selectVisibleClusterID(clusterID int64) bool {
	return m.selectVisibleCluster(store.ClusterSummary{ID: clusterID})
}

func (m *clusterBrowserModel) selectVisibleCluster(target store.ClusterSummary) bool {
	for index, cluster := range m.payload.Clusters {
		if sameClusterSummary(cluster, target) {
			m.selected = index
			m.loadSelectedCluster()
			return true
		}
	}
	return false
}

func (m clusterBrowserModel) clusterFromWorkingSet(clusterID int64) (store.ClusterSummary, bool) {
	return m.clusterSummaryFromWorkingSet(store.ClusterSummary{ID: clusterID})
}

func (m clusterBrowserModel) clusterSummaryFromWorkingSet(target store.ClusterSummary) (store.ClusterSummary, bool) {
	for _, cluster := range m.allClusters {
		if sameClusterSummary(cluster, target) {
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

func clusterSupportsDurableLocalActions(cluster store.ClusterSummary) bool {
	return cluster.Source == "" || cluster.Source == store.ClusterSourceDurable
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

func (m clusterBrowserModel) selectedActionURL() (string, bool) {
	if thread, ok := m.selectedThread(); ok {
		return thread.HTMLURL, true
	}
	return m.selectedClusterURL()
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
