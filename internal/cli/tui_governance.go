package cli

import (
	"fmt"
)

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

func (m *clusterBrowserModel) openCloseThreadMenu() {
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return
	}
	m.menuTitle = "Close Locally"
	m.menuItems = []tuiMenuItem{
		{label: fmt.Sprintf("Close #%d locally", thread.Number), action: "close-thread-local"},
		{label: "Back to actions", action: "back-to-actions"},
	}
	m.menuIndex = 0
	m.menuOff = 0
	m.status = fmt.Sprintf("Confirm local close for #%d", thread.Number)
}

func (m *clusterBrowserModel) openReopenThreadMenu() {
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return
	}
	m.menuTitle = "Reopen Locally"
	m.menuItems = []tuiMenuItem{
		{label: fmt.Sprintf("Reopen #%d locally", thread.Number), action: "reopen-thread-local"},
		{label: "Back to actions", action: "back-to-actions"},
	}
	m.menuIndex = 0
	m.menuOff = 0
	m.status = fmt.Sprintf("Confirm local reopen for #%d", thread.Number)
}

func (m *clusterBrowserModel) openCloseClusterMenu() {
	cluster, ok := m.selectedCluster()
	if !ok {
		m.status = "No selected cluster"
		return
	}
	m.menuTitle = "Close Cluster"
	m.menuItems = []tuiMenuItem{
		{label: fmt.Sprintf("Close cluster C%d locally", cluster.ID), action: "close-cluster-local"},
		{label: "Back to actions", action: "back-to-actions"},
	}
	m.menuIndex = 0
	m.menuOff = 0
	m.status = fmt.Sprintf("Confirm local close for cluster C%d", cluster.ID)
}

func (m *clusterBrowserModel) openReopenClusterMenu() {
	cluster, ok := m.selectedCluster()
	if !ok {
		m.status = "No selected cluster"
		return
	}
	m.menuTitle = "Reopen Cluster"
	m.menuItems = []tuiMenuItem{
		{label: fmt.Sprintf("Reopen cluster C%d locally", cluster.ID), action: "reopen-cluster-local"},
		{label: "Back to actions", action: "back-to-actions"},
	}
	m.menuIndex = 0
	m.menuOff = 0
	m.status = fmt.Sprintf("Confirm local reopen for cluster C%d", cluster.ID)
}

func (m *clusterBrowserModel) openExcludeMemberMenu() {
	cluster, clusterOK := m.selectedCluster()
	member, memberOK := m.selectedMember()
	if !clusterOK || !memberOK {
		m.status = "No selected cluster member"
		return
	}
	m.menuTitle = "Exclude Member"
	m.menuItems = []tuiMenuItem{
		{label: fmt.Sprintf("Exclude #%d from C%d", member.Thread.Number, cluster.ID), action: "exclude-member-local"},
		{label: "Back to actions", action: "back-to-actions"},
	}
	m.menuIndex = 0
	m.menuOff = 0
	m.status = fmt.Sprintf("Confirm local exclude for #%d", member.Thread.Number)
}

func (m *clusterBrowserModel) openIncludeMemberMenu() {
	cluster, clusterOK := m.selectedCluster()
	member, memberOK := m.selectedMember()
	if !clusterOK || !memberOK {
		m.status = "No selected cluster member"
		return
	}
	m.menuTitle = "Include Member"
	m.menuItems = []tuiMenuItem{
		{label: fmt.Sprintf("Include #%d in C%d", member.Thread.Number, cluster.ID), action: "include-member-local"},
		{label: "Back to actions", action: "back-to-actions"},
	}
	m.menuIndex = 0
	m.menuOff = 0
	m.status = fmt.Sprintf("Confirm local include for #%d", member.Thread.Number)
}

func (m *clusterBrowserModel) openCanonicalMemberMenu() {
	cluster, clusterOK := m.selectedCluster()
	member, memberOK := m.selectedMember()
	if !clusterOK || !memberOK {
		m.status = "No selected cluster member"
		return
	}
	m.menuTitle = "Canonical Member"
	m.menuItems = []tuiMenuItem{
		{label: fmt.Sprintf("Set #%d as canonical for C%d", member.Thread.Number, cluster.ID), action: "canonical-member-local"},
		{label: "Back to actions", action: "back-to-actions"},
	}
	m.menuIndex = 0
	m.menuOff = 0
	m.status = fmt.Sprintf("Confirm canonical member #%d", member.Thread.Number)
}

func (m *clusterBrowserModel) closeSelectedThreadLocally() {
	m.invalidateNeighborLoad()
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Local close unavailable for this view"
		return
	}
	if err := m.store.CloseThreadLocally(m.ctx, m.repoID, thread.Number, "TUI manual close"); err != nil {
		m.status = err.Error()
		return
	}
	delete(m.neighborCache, thread.ID)
	m.refreshFromStore()
	m.status = fmt.Sprintf("Closed #%d locally", thread.Number)
}

func (m *clusterBrowserModel) reopenSelectedThreadLocally() {
	m.invalidateNeighborLoad()
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Local reopen unavailable for this view"
		return
	}
	if err := m.store.ReopenThreadLocally(m.ctx, m.repoID, thread.Number); err != nil {
		m.status = err.Error()
		return
	}
	m.refreshFromStore()
	m.status = fmt.Sprintf("Reopened #%d locally", thread.Number)
}

func (m *clusterBrowserModel) closeSelectedClusterLocally() {
	m.invalidateNeighborLoad()
	cluster, ok := m.selectedCluster()
	if !ok {
		m.status = "No selected cluster"
		return
	}
	if !clusterSupportsDurableLocalActions(cluster) {
		m.status = "Local cluster close is only available for durable clusters"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Local cluster close unavailable for this view"
		return
	}
	if err := m.store.CloseClusterLocally(m.ctx, m.repoID, cluster.ID, "TUI manual close"); err != nil {
		m.status = err.Error()
		return
	}
	m.refreshFromStore()
	m.status = fmt.Sprintf("Closed cluster C%d locally", cluster.ID)
}

func (m *clusterBrowserModel) reopenSelectedClusterLocally() {
	m.invalidateNeighborLoad()
	cluster, ok := m.selectedCluster()
	if !ok {
		m.status = "No selected cluster"
		return
	}
	if !clusterSupportsDurableLocalActions(cluster) {
		m.status = "Local cluster reopen is only available for durable clusters"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Local cluster reopen unavailable for this view"
		return
	}
	if err := m.store.ReopenClusterLocally(m.ctx, m.repoID, cluster.ID); err != nil {
		m.status = err.Error()
		return
	}
	m.refreshFromStore()
	m.status = fmt.Sprintf("Reopened cluster C%d locally", cluster.ID)
}

func (m *clusterBrowserModel) excludeSelectedClusterMemberLocally() {
	m.invalidateNeighborLoad()
	cluster, clusterOK := m.selectedCluster()
	member, memberOK := m.selectedMember()
	if !clusterOK || !memberOK {
		m.status = "No selected cluster member"
		return
	}
	if !clusterSupportsDurableLocalActions(cluster) {
		m.status = "Local member triage is only available for durable clusters"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Local member exclude unavailable for this view"
		return
	}
	if _, err := m.store.ExcludeClusterMemberLocally(m.ctx, m.repoID, cluster.ID, member.Thread.Number, "TUI manual exclude"); err != nil {
		m.status = err.Error()
		return
	}
	delete(m.neighborCache, member.Thread.ID)
	m.refreshFromStore()
	m.status = fmt.Sprintf("Excluded #%d from C%d locally", member.Thread.Number, cluster.ID)
}

func (m *clusterBrowserModel) includeSelectedClusterMemberLocally() {
	m.invalidateNeighborLoad()
	cluster, clusterOK := m.selectedCluster()
	member, memberOK := m.selectedMember()
	if !clusterOK || !memberOK {
		m.status = "No selected cluster member"
		return
	}
	if !clusterSupportsDurableLocalActions(cluster) {
		m.status = "Local member triage is only available for durable clusters"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Local member include unavailable for this view"
		return
	}
	if _, err := m.store.IncludeClusterMemberLocally(m.ctx, m.repoID, cluster.ID, member.Thread.Number, "TUI manual include"); err != nil {
		m.status = err.Error()
		return
	}
	m.refreshFromStore()
	m.status = fmt.Sprintf("Included #%d in C%d locally", member.Thread.Number, cluster.ID)
}

func (m *clusterBrowserModel) setSelectedClusterCanonicalLocally() {
	m.invalidateNeighborLoad()
	cluster, clusterOK := m.selectedCluster()
	member, memberOK := m.selectedMember()
	if !clusterOK || !memberOK {
		m.status = "No selected cluster member"
		return
	}
	if !clusterSupportsDurableLocalActions(cluster) {
		m.status = "Local member triage is only available for durable clusters"
		return
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Local canonical unavailable for this view"
		return
	}
	if _, err := m.store.SetClusterCanonicalLocally(m.ctx, m.repoID, cluster.ID, member.Thread.Number, "TUI manual canonical"); err != nil {
		m.status = err.Error()
		return
	}
	m.refreshFromStore()
	m.status = fmt.Sprintf("Set #%d as canonical for C%d", member.Thread.Number, cluster.ID)
}
