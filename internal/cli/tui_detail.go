package cli

import (
	"fmt"
	"strings"
)

func (m clusterBrowserModel) detailLines(width int) []string {
	if len(m.payload.Clusters) == 0 {
		return []string{
			bold("No clusters visible"),
			"",
			"No clusters match the current view.",
			"",
			"Try f to lower the minimum size, / to clear the filter, x to show closed clusters, or r to refresh from the local store.",
			"",
			"If the store is empty, run sync, refresh summaries/embeddings, and cluster first.",
		}
	}
	cluster := m.payload.Clusters[m.selected]
	lines := []string{
		bold(fmt.Sprintf("Cluster %d", cluster.ID)),
		color("#5bc0eb", cluster.StableSlug),
	}
	lines = append(lines, wrapPlain(splitClusterTitle(cluster), width)...)
	lines = append(lines,
		"",
		fmt.Sprintf("members: %d   status: %s   updated: %s", cluster.MemberCount, firstNonEmpty(cluster.Status, "unknown"), formatRelativeTime(cluster.UpdatedAt)),
		fmt.Sprintf("representative: %s", threadRef(cluster)),
		"",
	)
	if !m.hasDetail {
		lines = append(lines, "Cluster details unavailable.", m.status)
		return lines
	}
	member, ok := m.selectedMember()
	if !ok {
		lines = append(lines, "Select a cluster to inspect members.")
		return lines
	}
	thread := member.Thread
	lines = append(lines,
		dim(tuiRule(width)),
		bold(fmt.Sprintf("%s #%d", kindTitle(thread.Kind), thread.Number)),
	)
	lines = append(lines, wrapPlain(renderTitleText(thread.Title), width)...)
	lines = append(lines,
		"",
	)
	lines = append(lines, wrapPlain(fmt.Sprintf("closed: %s", closedLabel(thread)), width)...)
	lines = append(lines, wrapPlain(fmt.Sprintf("updated: %s   author: %s", formatRelativeTime(thread.UpdatedAtGitHub), firstNonEmpty(thread.AuthorLogin, "unknown")), width)...)
	if labels := labelsFromJSON(thread.LabelsJSON); labels != "" {
		lines = append(lines, wrapPlain("labels: "+labels, width)...)
		lines = append(lines, "")
	}
	lines = append(lines, wrapPlain(fmt.Sprintf("url: %s", thread.HTMLURL), width)...)
	lines = append(lines, "")
	if neighbors, ok := m.neighborCache[thread.ID]; ok {
		lines = append(lines, dim(tuiRule(width)))
		lines = append(lines, bold("Neighbors"))
		if len(neighbors) == 0 {
			lines = append(lines, "No neighbors above threshold.", "")
		} else {
			for _, neighbor := range neighbors {
				lines = append(lines, truncateCells(fmt.Sprintf("#%d %s %.1f%%  %s",
					neighbor.Thread.Number,
					kindTitle(neighbor.Thread.Kind),
					neighbor.Score*100,
					renderTitleText(neighbor.Thread.Title),
				), width))
			}
			lines = append(lines, "")
		}
	}
	if len(member.Summaries) > 0 {
		lines = append(lines, dim(tuiRule(width)))
		lines = append(lines, bold("LLM Summary"))
		for _, key := range sortedSummaryKeys(member.Summaries) {
			lines = append(lines, dim(formatSummaryLabel(key)+":"))
			lines = append(lines, markdownLines(member.Summaries[key], width)...)
			lines = append(lines, "")
		}
	}
	if strings.TrimSpace(member.BodySnippet) != "" {
		lines = append(lines, dim(tuiRule(width)))
		lines = append(lines, bold("Main Preview"))
		lines = appendLimitedLines(lines, markdownLines(member.BodySnippet, width), m.detailBodyLimit())
	}
	return lines
}

func (m clusterBrowserModel) helpLines(width int) []string {
	lines := []string{
		bold("Gitcrawl TUI"),
		"",
		"Mouse",
		"  left click: focus/select a pane row",
		"  left click menu row: run that action",
		"  wheel: scroll the pane under the pointer",
		"  wheel in menu: move the highlighted action",
		"  right click: open a stable action menu",
		"  menu actions: copy, links, neighbors, member triage, local close/reopen, repos, filter, jump, sort, refresh, layout, quit",
		"",
		"Keyboard",
		"  Tab / Shift-Tab: cycle focus",
		"  arrows or j/k: move selection or scroll detail",
		"  PageUp/PageDown: page the active pane",
		"  Enter: drill into the next pane, loading neighbors from members",
		"  a: open action menu",
		"  /: filter clusters",
		"  #: jump to issue/PR number",
		"  s: toggle cluster sort",
		"  m: cycle member sort",
		"  n: load neighbors for selected thread",
		"  d: toggle compact/full detail",
		"  r: refresh from local store",
		"  p: switch repository",
		"  l: cycle layout",
		"  f: cycle minimum cluster size",
		"  x: show/hide closed clusters",
		"  o: open selected thread or representative",
		"  c: copy selected thread or representative URL",
		"  auto-refresh: local store changes are picked up every 15s",
		"  Enter in menu: run action or open link picker",
		"  b in submenu: back to actions",
		"  ?: toggle this help",
		"  q: quit",
	}
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "  ") {
			out = append(out, line)
			continue
		}
		out = append(out, wrapPlain(line, width)...)
	}
	return out
}

func (m clusterBrowserModel) detailBodyLimit() int {
	if m.compactDetail {
		return 18
	}
	return 240
}
