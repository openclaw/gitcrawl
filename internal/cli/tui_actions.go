package cli

import (
	"fmt"
	"strings"
)

func (m *clusterBrowserModel) runAction(action string) bool {
	return m.runMenuItem(tuiMenuItem{action: action})
}

func (m clusterBrowserModel) inMenuSubmenu() bool {
	title := strings.TrimSpace(m.menuTitle)
	return title != "" && title != "Actions"
}

func (m *clusterBrowserModel) runMenuItem(item tuiMenuItem) bool {
	if !item.selectable() {
		return false
	}
	action := item.action
	if action == "close-menu" {
		m.status = "Menu closed"
		return true
	}
	switch action {
	case "quit":
		m.quitRequested = true
		return true
	case "sort-size":
		m.payload.Sort = "size"
		m.sortClustersPreservingSelection()
		m.loadSelectedCluster()
		m.status = "Sort: size"
		return true
	case "sort-recent":
		m.payload.Sort = "recent"
		m.sortClustersPreservingSelection()
		m.loadSelectedCluster()
		m.status = "Sort: recent"
		return true
	case "sort-oldest":
		m.payload.Sort = "oldest"
		m.sortClustersPreservingSelection()
		m.loadSelectedCluster()
		m.status = "Sort: oldest"
		return true
	case "member-sort-kind":
		m.memberSort = memberSortKind
		m.sortMembers()
		m.status = "Member sort: kind"
		return true
	case "member-sort-recent":
		m.memberSort = memberSortRecent
		m.sortMembers()
		m.status = "Member sort: recent"
		return true
	case "member-sort-oldest":
		m.memberSort = memberSortOldest
		m.sortMembers()
		m.status = "Member sort: oldest"
		return true
	case "refresh":
		m.refreshFromStore()
		return true
	case "filter":
		m.startFilterInput()
		return true
	case "clear-filter":
		m.search = ""
		m.searchInput.SetValue("")
		m.applyClusterFilters()
		m.status = "Filter cleared"
		return true
	case "repository-picker":
		m.openRepositoryMenu()
		return false
	case "jump":
		m.startJumpInput()
		return true
	case "toggle-layout":
		m.toggleWideLayout()
		return true
	case "toggle-detail":
		m.toggleDetailMode()
		return true
	case "min-size-1":
		m.setMinSizeFromMenu(1)
		return true
	case "min-size-5":
		m.setMinSizeFromMenu(5)
		return true
	case "min-size-10":
		m.setMinSizeFromMenu(10)
		return true
	case "toggle-closed":
		m.toggleClosedVisibility()
		return true
	case "show-help":
		m.showHelp = true
		m.status = "Help"
		return true
	case "open-cluster-representative":
		if strings.TrimSpace(item.value) == "" {
			m.status = "No representative URL"
			return true
		}
		openURL(item.value)
		m.status = "Opened " + item.value
		return true
	case "copy-cluster-url":
		if strings.TrimSpace(item.value) == "" {
			m.status = "No representative URL"
			return true
		}
		if err := copyText(item.value); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied representative URL"
		}
		return true
	case "close-cluster-confirm":
		m.openCloseClusterMenu()
		return false
	case "close-cluster-local":
		m.closeSelectedClusterLocally()
		return true
	case "reopen-cluster-confirm":
		m.openReopenClusterMenu()
		return false
	case "reopen-cluster-local":
		m.reopenSelectedClusterLocally()
		return true
	case "exclude-member-confirm":
		m.openExcludeMemberMenu()
		return false
	case "exclude-member-local":
		m.excludeSelectedClusterMemberLocally()
		return true
	case "include-member-confirm":
		m.openIncludeMemberMenu()
		return false
	case "include-member-local":
		m.includeSelectedClusterMemberLocally()
		return true
	case "canonical-member-confirm":
		m.openCanonicalMemberMenu()
		return false
	case "canonical-member-local":
		m.setSelectedClusterCanonicalLocally()
		return true
	case "load-neighbors":
		m.pendingCmd = m.requestSelectedThreadNeighbors(10, 0.2)
		return true
	case "close-thread-confirm":
		m.openCloseThreadMenu()
		return false
	case "close-thread-local":
		m.closeSelectedThreadLocally()
		return true
	case "reopen-thread-confirm":
		m.openReopenThreadMenu()
		return false
	case "reopen-thread-local":
		m.reopenSelectedThreadLocally()
		return true
	case "copy-thread-detail":
		if err := copyText(m.threadDetailClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied selected detail"
		}
		return true
	case "copy-body-preview":
		member, ok := m.selectedMember()
		if !ok || strings.TrimSpace(member.BodySnippet) == "" {
			m.status = "No body preview"
			return true
		}
		if err := copyText(member.BodySnippet); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied body preview"
		}
		return true
	case "copy-summaries":
		if err := copyText(m.summariesClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied summaries"
		}
		return true
	case "copy-neighbors":
		if err := copyText(m.neighborsClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied neighbors"
		}
		return true
	case "copy-cluster-id":
		cluster, ok := m.selectedCluster()
		if !ok {
			m.status = "No selected cluster"
			return true
		}
		if err := copyText(fmt.Sprintf("%d", cluster.ID)); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster ID"
		}
		return true
	case "copy-cluster-name":
		cluster, ok := m.selectedCluster()
		if !ok {
			m.status = "No selected cluster"
			return true
		}
		if err := copyText(cluster.StableSlug); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster name"
		}
		return true
	case "copy-cluster-title":
		cluster, ok := m.selectedCluster()
		if !ok {
			m.status = "No selected cluster"
			return true
		}
		if err := copyText(firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster")); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster title"
		}
		return true
	case "copy-member-list":
		if err := copyText(m.memberListClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied member list"
		}
		return true
	case "back-to-actions":
		m.openActionMenuFor(m.menuContext)
		return false
	case "select-repo":
		m.switchRepository(item.value)
		return true
	case "open-link-picker":
		m.openReferenceLinkMenu("open")
		return false
	case "copy-link-picker":
		m.openReferenceLinkMenu("copy")
		return false
	case "open-picked-link":
		if err := openURL(item.value); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Opened " + item.value
		}
		return true
	case "copy-picked-link":
		if err := copyText(item.value); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied body link"
		}
		return true
	case "copy-cluster":
		if err := copyText(m.clusterClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied cluster summary"
		}
		return true
	case "copy-visible-clusters":
		if err := copyText(m.visibleClustersClipboardText()); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied visible clusters"
		}
		return true
	case "copy-reference-links":
		links := m.referenceLinks()
		if len(links) == 0 {
			m.status = "No body links found"
			return true
		}
		if err := copyText(strings.Join(links, "\n")); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied body links"
		}
		return true
	}
	thread, ok := m.selectedThread()
	if !ok {
		if action == "open" || action == "copy-url" {
			url, urlOK := m.selectedActionURL()
			if !urlOK {
				m.status = "No selected thread"
				return true
			}
			if action == "open" {
				if err := openURL(url); err != nil {
					m.status = err.Error()
				} else {
					m.status = "Opened " + url
				}
				return true
			}
			if err := copyText(url); err != nil {
				m.status = err.Error()
			} else {
				m.status = "Copied representative URL"
			}
			return true
		}
		m.status = "No selected thread"
		return true
	}
	switch action {
	case "open":
		if err := openURL(thread.HTMLURL); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Opened " + thread.HTMLURL
		}
	case "copy-url":
		if err := copyText(thread.HTMLURL); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied URL"
		}
	case "copy-markdown":
		link := fmt.Sprintf("[#%d %s](%s)", thread.Number, thread.Title, thread.HTMLURL)
		if err := copyText(link); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied markdown link"
		}
	case "copy-title":
		title := fmt.Sprintf("#%d %s", thread.Number, thread.Title)
		if err := copyText(title); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied title"
		}
	case "open-first-link":
		link, ok := m.firstReferenceLink()
		if !ok {
			m.status = "No body link found"
			return true
		}
		if err := openURL(link); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Opened " + link
		}
	case "copy-first-link":
		link, ok := m.firstReferenceLink()
		if !ok {
			m.status = "No body link found"
			return true
		}
		if err := copyText(link); err != nil {
			m.status = err.Error()
		} else {
			m.status = "Copied first body link"
		}
	case "close-menu":
		m.status = "Menu closed"
	}
	return true
}

func (m *clusterBrowserModel) setMinSizeFromMenu(value int) {
	m.minSize = max(1, value)
	m.applyClusterFilters()
	m.status = fmt.Sprintf("Min size: %s", minSizeLabel(m.minSize))
}

func (m *clusterBrowserModel) toggleClosedVisibility() {
	m.showClosed = !m.showClosed
	if m.store != nil && m.repoID != 0 {
		m.refreshFromStore()
	} else {
		m.applyClusterFilters()
	}
	if m.showClosed {
		m.status = "Showing closed clusters and members"
	} else {
		m.status = "Hiding closed clusters and members"
	}
}
