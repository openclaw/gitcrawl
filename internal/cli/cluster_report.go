package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func renderClustersReportMarkdown(report clustersReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Cluster Report: %s\n\n", report.Repository)
	fmt.Fprintf(&b, "Generated: %s UTC\n", report.GeneratedAt)
	fmt.Fprintf(&b, "View: top %d clusters, min size %d, sorted by %s", report.Limit, report.MinSize, report.Sort)
	if report.HideClosed {
		b.WriteString(", closed hidden")
	}
	b.WriteString("\n\n")
	fmt.Fprintf(&b, "**Totals:** %d clusters, %d total members, %d open, %d closed.\n\n", report.Totals.ClusterCount, report.Totals.MemberCount, report.Totals.OpenCount, report.Totals.ClosedCount)
	if len(report.Clusters) == 0 {
		b.WriteString("No clusters matched the selected filters.\n")
		return b.String()
	}
	b.WriteString("## At a Glance\n\n")
	b.WriteString("| Rank | Cluster | Members | Status | Representative | Updated |\n")
	b.WriteString("| ---: | --- | ---: | --- | --- | --- |\n")
	for index, detail := range report.Clusters {
		cluster := detail.Cluster
		fmt.Fprintf(&b, "| %d | `%s` | %d | %s | %s | %s |\n",
			index+1,
			markdownTableText(firstNonEmpty(cluster.StableSlug, fmt.Sprintf("cluster-%d", cluster.ID))),
			cluster.MemberCount,
			markdownTableText(firstNonEmpty(cluster.Status, "unknown")),
			markdownTableText(clusterRepresentativeMarkdown(cluster)),
			markdownTableText(shortTime(cluster.UpdatedAt)),
		)
	}
	for index, detail := range report.Clusters {
		cluster := detail.Cluster
		title := firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster")
		fmt.Fprintf(&b, "\n## %d. %s\n\n", index+1, markdownHeadingText(title))
		fmt.Fprintf(&b, "- Cluster: `%s` (`%d`)\n", firstNonEmpty(cluster.StableSlug, fmt.Sprintf("cluster-%d", cluster.ID)), cluster.ID)
		fmt.Fprintf(&b, "- Status: %s\n", firstNonEmpty(cluster.Status, "unknown"))
		fmt.Fprintf(&b, "- Members: %d total, %d shown\n", cluster.MemberCount, len(detail.Members))
		fmt.Fprintf(&b, "- Representative: %s\n", clusterRepresentativeMarkdown(cluster))
		if cluster.UpdatedAt != "" {
			fmt.Fprintf(&b, "- Last updated: %s\n", shortTime(cluster.UpdatedAt))
		}
		if cluster.ClosedAt != "" {
			fmt.Fprintf(&b, "- Closed locally: %s\n", shortTime(cluster.ClosedAt))
		}
		b.WriteString("\n")
		renderClusterMemberTable(&b, detail.Members)
		renderClusterSnippetList(&b, detail.Members)
	}
	return b.String()
}

func renderClusterMemberTable(b *strings.Builder, members []store.ClusterMemberDetail) {
	if len(members) == 0 {
		b.WriteString("No visible members.\n")
		return
	}
	b.WriteString("| Type | Number | State | Score | Title | Labels |\n")
	b.WriteString("| --- | ---: | --- | ---: | --- | --- |\n")
	for _, member := range members {
		thread := member.Thread
		score := ""
		if member.ScoreToRepresentative != nil {
			score = fmt.Sprintf("%.3f", *member.ScoreToRepresentative)
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s |\n",
			markdownTableText(kindTitle(thread.Kind)),
			markdownTableText(threadMarkdownLink(thread)),
			markdownTableText(firstNonEmpty(thread.State, member.State, "unknown")),
			markdownTableText(score),
			markdownTableText(thread.Title),
			markdownTableText(strings.Join(cliLabelNames(thread.LabelsJSON), ", ")),
		)
	}
}

func renderClusterSnippetList(b *strings.Builder, members []store.ClusterMemberDetail) {
	wroteHeading := false
	count := 0
	for _, member := range members {
		snippet := strings.TrimSpace(member.BodySnippet)
		if snippet == "" {
			continue
		}
		if !wroteHeading {
			b.WriteString("\nKey snippets:\n\n")
			wroteHeading = true
		}
		fmt.Fprintf(b, "- %s: %s\n", threadMarkdownLink(member.Thread), markdownInlineText(snippet))
		count++
		if count >= 3 {
			break
		}
	}
}

func clusterRepresentativeMarkdown(cluster store.ClusterSummary) string {
	if cluster.RepresentativeNumber <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%s #%d", kindTitle(cluster.RepresentativeKind), cluster.RepresentativeNumber)
}

func threadMarkdownLink(thread store.Thread) string {
	label := fmt.Sprintf("#%d", thread.Number)
	if strings.TrimSpace(thread.HTMLURL) == "" {
		return label
	}
	return fmt.Sprintf("[%s](%s)", label, strings.TrimSpace(thread.HTMLURL))
}

func markdownHeadingText(value string) string {
	value = strings.ReplaceAll(strings.TrimSpace(value), "\n", " ")
	value = strings.ReplaceAll(value, "#", "\\#")
	return value
}

func markdownInlineText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func markdownTableText(value string) string {
	value = markdownInlineText(value)
	value = strings.ReplaceAll(value, "|", "\\|")
	return value
}

func shortTime(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t.Format("2006-01-02")
	}
	if len(value) >= len("2006-01-02") {
		return value[:len("2006-01-02")]
	}
	return value
}

func cliLabelNames(raw string) []string {
	var labels []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(raw), &labels); err == nil {
		out := make([]string, 0, len(labels))
		for _, label := range labels {
			if name := strings.TrimSpace(label.Name); name != "" {
				out = append(out, name)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		return nil
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if name = strings.TrimSpace(name); name != "" {
			out = append(out, name)
		}
	}
	return out
}
