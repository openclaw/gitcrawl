package cli

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (m clusterBrowserModel) referenceLinks() []string {
	member, ok := m.selectedMember()
	if !ok {
		return nil
	}
	links := make([]string, 0, 4)
	seen := map[string]bool{}
	for _, value := range append([]string{member.BodySnippet}, sortedSummaryValues(member.Summaries)...) {
		for _, link := range markdownLinks(value) {
			if !seen[link] {
				links = append(links, link)
				seen[link] = true
			}
		}
	}
	return links
}

func (m clusterBrowserModel) threadDetailClipboardText() string {
	member, ok := m.selectedMember()
	if !ok {
		return ""
	}
	thread := member.Thread
	lines := []string{
		fmt.Sprintf("%s #%d: %s", kindTitle(thread.Kind), thread.Number, thread.Title),
		"State: " + memberDisplayState(member),
		"Author: " + firstNonEmpty(thread.AuthorLogin, "unknown"),
		"Updated: " + firstNonEmpty(thread.UpdatedAtGitHub, thread.UpdatedAt, "unknown"),
		"URL: " + thread.HTMLURL,
	}
	if summaries := summariesClipboardText(member.Summaries); summaries != "" {
		lines = append(lines, "", "Summaries", summaries)
	}
	if strings.TrimSpace(member.BodySnippet) != "" {
		lines = append(lines, "", "Body preview", member.BodySnippet)
	}
	if links := m.referenceLinks(); len(links) > 0 {
		lines = append(lines, "", "Links", strings.Join(links, "\n"))
	}
	if neighbors := m.neighborsClipboardText(); neighbors != "" {
		lines = append(lines, "", "Neighbors")
		lines = append(lines, neighbors)
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) summariesClipboardText() string {
	member, ok := m.selectedMember()
	if !ok {
		return ""
	}
	return summariesClipboardText(member.Summaries)
}

func summariesClipboardText(summaries map[string]string) string {
	if len(summaries) == 0 {
		return ""
	}
	lines := make([]string, 0, len(summaries)*2)
	for _, key := range sortedSummaryKeys(summaries) {
		lines = append(lines, formatSummaryLabel(key)+":", summaries[key], "")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func (m clusterBrowserModel) neighborsClipboardText() string {
	member, ok := m.selectedMember()
	if !ok {
		return ""
	}
	neighbors, ok := m.neighborCache[member.Thread.ID]
	if !ok {
		return ""
	}
	if len(neighbors) == 0 {
		return "No neighbors above threshold."
	}
	lines := make([]string, 0, len(neighbors))
	for _, neighbor := range neighbors {
		lines = append(lines, fmt.Sprintf("#%d %s %.1f%% %s",
			neighbor.Thread.Number,
			kindTitle(neighbor.Thread.Kind),
			neighbor.Score*100,
			neighbor.Thread.Title,
		))
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) clusterClipboardText() string {
	if len(m.payload.Clusters) == 0 || m.selected < 0 || m.selected >= len(m.payload.Clusters) {
		return ""
	}
	cluster := m.payload.Clusters[m.selected]
	lines := []string{
		fmt.Sprintf("Cluster %d", cluster.ID),
		"Name: " + cluster.StableSlug,
		"Title: " + firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster"),
		fmt.Sprintf("State: %s", firstNonEmpty(cluster.Status, "unknown")),
		fmt.Sprintf("Members: %d", cluster.MemberCount),
		"Updated: " + firstNonEmpty(cluster.UpdatedAt, "unknown"),
		"Representative: " + threadRef(cluster),
	}
	if member, ok := m.selectedMember(); ok {
		thread := member.Thread
		lines = append(lines, "", fmt.Sprintf("%s #%d: %s", kindTitle(thread.Kind), thread.Number, thread.Title), thread.HTMLURL)
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) visibleClustersClipboardText() string {
	if len(m.payload.Clusters) == 0 {
		return ""
	}
	lines := make([]string, 0, len(m.payload.Clusters))
	for _, cluster := range m.payload.Clusters {
		lines = append(lines, fmt.Sprintf(
			"C%d [%s] %d items %s - %s (%s)",
			cluster.ID,
			firstNonEmpty(cluster.Status, "unknown"),
			cluster.MemberCount,
			cluster.StableSlug,
			firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster"),
			threadRef(cluster),
		))
	}
	return strings.Join(lines, "\n")
}

func (m clusterBrowserModel) memberListClipboardText() string {
	if len(m.memberRows) == 0 {
		return ""
	}
	lines := make([]string, 0, len(m.memberRows))
	for _, row := range m.memberRows {
		if !row.selectable {
			continue
		}
		thread := row.thread()
		lines = append(lines, fmt.Sprintf("#%d [%s] %s %s %s",
			thread.Number,
			memberDisplayState(row.member),
			kindTitle(thread.Kind),
			thread.Title,
			thread.HTMLURL,
		))
	}
	return strings.Join(lines, "\n")
}

func (r memberRow) thread() store.Thread {
	return r.member.Thread
}

var openURL = func(url string) error {
	if strings.TrimSpace(url) == "" {
		return fmt.Errorf("no URL selected")
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("open URL: %w", err)
	}
	return nil
}

func copyText(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("nothing to copy")
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("pbcopy")
	case "windows":
		cmd = exec.Command("clip")
	default:
		cmd = exec.Command("xclip", "-selection", "clipboard")
	}
	cmd.Stdin = strings.NewReader(value)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("copy text: %w", err)
	}
	return nil
}
