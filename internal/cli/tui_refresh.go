package cli

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (m *clusterBrowserModel) refreshFromStore() {
	if m.store == nil || m.repoID == 0 {
		m.status = "Refresh unavailable for this view"
		return
	}
	clusters, err := m.loadClusterSummariesFromStore()
	if err != nil {
		m.status = "Refresh failed: " + err.Error()
		return
	}
	relaxedFilters := m.applyClusterRefresh(clusters, m.currentClusterKey())
	m.status = fmt.Sprintf("Refreshed %d cluster(s)", len(m.payload.Clusters))
	if relaxedFilters {
		m.status += " (filters relaxed)"
	}
}

func (m clusterBrowserModel) autoRefreshCmd() tea.Cmd {
	if m.store == nil || m.repoID == 0 {
		return nil
	}
	return tea.Tick(tuiAutoRefreshInterval, func(time.Time) tea.Msg {
		return tuiAutoRefreshMsg{}
	})
}

func (m *clusterBrowserModel) autoRefreshFromStore() {
	if m.store == nil || m.repoID == 0 {
		m.status = "Refresh unavailable for this view"
		return
	}
	clusters, err := m.loadClusterSummariesFromStore()
	if err != nil {
		m.status = "Refresh failed: " + err.Error()
		return
	}
	if clusterSummariesSignature(clusters) == m.clusterSignature() {
		return
	}
	m.applyClusterRefresh(clusters, m.currentClusterKey())
	m.status = fmt.Sprintf("Auto refreshed %d cluster(s)", len(m.payload.Clusters))
}

func (m clusterBrowserModel) clusterSignature() string {
	return clusterSummariesSignature(m.payload.Clusters)
}

func clusterSummariesSignature(clusters []store.ClusterSummary) string {
	if len(clusters) == 0 {
		return ""
	}
	parts := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		parts = append(parts, fmt.Sprintf("%s:%d:%s", clusterSummaryKey(cluster), cluster.MemberCount, cluster.UpdatedAt))
	}
	return strings.Join(parts, "|")
}

func (m clusterBrowserModel) currentClusterID() int64 {
	if len(m.payload.Clusters) == 0 || m.selected < 0 || m.selected >= len(m.payload.Clusters) {
		return 0
	}
	return m.payload.Clusters[m.selected].ID
}

func (m clusterBrowserModel) currentClusterKey() string {
	if len(m.payload.Clusters) == 0 || m.selected < 0 || m.selected >= len(m.payload.Clusters) {
		return ""
	}
	return clusterSummaryKey(m.payload.Clusters[m.selected])
}

func (m clusterBrowserModel) clusterRefreshLimit() int {
	if m.payload.Limit > 0 {
		return m.payload.Limit
	}
	return max(defaultTUIWorkingSetLimit, max(len(m.payload.Clusters), len(m.allClusters)))
}

func (m *clusterBrowserModel) loadClusterSummariesFromStore() ([]store.ClusterSummary, error) {
	viewLimit := m.clusterRefreshLimit()
	clusters, err := m.store.ListDisplayClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        m.repoID,
		IncludeClosed: m.showClosed,
		MinSize:       m.minSize,
		Limit:         viewLimit,
		Sort:          m.payload.Sort,
	})
	if err != nil {
		return nil, err
	}
	workingSet, err := m.store.ListDisplayClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        m.repoID,
		IncludeClosed: m.showClosed,
		MinSize:       1,
		Limit:         viewLimit,
		Sort:          m.payload.Sort,
	})
	if err != nil {
		return nil, err
	}
	return mergeClusterSummaries(clusters, workingSet), nil
}

func (m *clusterBrowserModel) applyClusterRefresh(clusters []store.ClusterSummary, currentKey string) bool {
	m.invalidateNeighborLoad()
	if clusters == nil {
		clusters = []store.ClusterSummary{}
	}
	prevMemberID := int64(0)
	prevDetailOffset := m.detailView.YOffset
	if currentKey != "" && m.hasDetail && clusterSummaryKey(m.detail.Cluster) == currentKey {
		if member, ok := m.selectedMember(); ok {
			prevMemberID = member.Thread.ID
		}
	}
	if m.payload.Limit <= 0 && len(clusters) > 0 && len(clusters) < len(m.allClusters) {
		clusters = mergeClusterSummaries(clusters, m.allClusters)
	}
	m.detailCache = map[string]store.ClusterDetail{}
	m.allClusters = append([]store.ClusterSummary(nil), clusters...)
	m.payload.Clusters = append([]store.ClusterSummary(nil), clusters...)
	m.applyClusterFilters()
	relaxedFilters := m.relaxFiltersIfEmpty()
	if currentKey != "" {
		for index, cluster := range m.payload.Clusters {
			if clusterSummaryKey(cluster) == currentKey {
				m.selected = index
				m.loadSelectedCluster()
				if m.restoreMemberSelection(prevMemberID) {
					m.detailView.YOffset = prevDetailOffset
				}
				break
			}
		}
	}
	return relaxedFilters
}

func (m *clusterBrowserModel) switchRepository(fullName string) {
	if m.store == nil {
		m.status = "Repository picker unavailable for this view"
		return
	}
	m.invalidateNeighborLoad()
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
	clusters, err := m.store.ListDisplayClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: m.showClosed,
		MinSize:       m.minSize,
		Limit:         max(20, m.payload.Limit),
		Sort:          m.payload.Sort,
	})
	if err != nil {
		m.status = "Repository switch failed: " + err.Error()
		return
	}
	workingSet, err := m.store.ListDisplayClusterSummaries(m.ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: m.showClosed,
		MinSize:       1,
		Limit:         max(defaultTUIWorkingSetLimit, m.payload.Limit),
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
	m.detailCache = map[string]store.ClusterDetail{}
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
	relaxedFilters := m.relaxFiltersIfEmpty()
	m.focus = focusClusters
	m.status = "Repository: " + repo.FullName
	if relaxedFilters {
		m.status += " (filters relaxed)"
	}
}

func (m *clusterBrowserModel) relaxFiltersIfEmpty() bool {
	if len(m.payload.Clusters) > 0 || len(m.allClusters) == 0 {
		return false
	}
	m.showClosed = true
	m.minSize = 1
	m.applyClusterFilters()
	return len(m.payload.Clusters) > 0
}

func (m *clusterBrowserModel) applyClusterFilters() {
	currentKey := m.currentClusterKey()
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
	if currentKey != "" {
		for index, cluster := range m.payload.Clusters {
			if clusterSummaryKey(cluster) == currentKey {
				m.selected = index
				break
			}
		}
	}
	m.clusterOff = 0
	m.loadSelectedCluster()
}
