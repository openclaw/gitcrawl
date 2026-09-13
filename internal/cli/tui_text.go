package cli

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/openclaw/gitcrawl/internal/store"
)

func splitClusterTitle(cluster store.ClusterSummary) string {
	return firstNonEmpty(renderTitleText(cluster.RepresentativeTitle), renderTitleText(cluster.Title), "Untitled cluster")
}

func renderTitleText(value string) string {
	value = strings.TrimSpace(stripEmoji(value))
	if value == "" {
		return ""
	}
	return strings.Join(strings.Fields(value), " ")
}

func stripEmoji(value string) string {
	if value == "" {
		return ""
	}
	var out strings.Builder
	out.Grow(len(value))
	for _, r := range value {
		if isEmojiRune(r) {
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

func isEmojiRune(r rune) bool {
	switch {
	case r == '\u200d' || r == '\u20e3':
		return true
	case r >= '\ufe00' && r <= '\ufe0f':
		return true
	case r >= '\U0001f000' && r <= '\U0001faff':
		return true
	case r >= '\u2600' && r <= '\u27bf':
		return true
	case r == '\u3030' || r == '\u303d' || r == '\u3297' || r == '\u3299':
		return true
	default:
		return false
	}
}

func sortedSummaryKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, key := range summaryKeyOrder {
		if strings.TrimSpace(values[key]) != "" {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	var extra []string
	for key, value := range values {
		if !seen[key] && strings.TrimSpace(value) != "" {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	keys = append(keys, extra...)
	return keys
}

func sortedSummaryValues(values map[string]string) []string {
	keys := sortedSummaryKeys(values)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, values[key])
	}
	return out
}

func formatSummaryLabel(key string) string {
	switch key {
	case "key_summary":
		return "Key summary"
	case "problem_summary":
		return "Purpose"
	case "solution_summary":
		return "Solution"
	case "maintainer_signal_summary":
		return "Maintainer signal"
	case "dedupe_summary":
		return "Cluster signal"
	default:
		return strings.ReplaceAll(key, "_", " ")
	}
}

func labelsFromJSON(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var labels []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(raw), &labels); err == nil && len(labels) > 0 {
		names := make([]string, 0, len(labels))
		for _, label := range labels {
			if strings.TrimSpace(label.Name) != "" {
				names = append(names, label.Name)
			}
		}
		if len(names) > 0 {
			return strings.Join(names, ", ")
		}
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err == nil && len(names) > 0 {
		return strings.Join(names, ", ")
	}
	return ""
}

func kindLabel(kind string) string {
	if kind == "pull_request" {
		return "PR"
	}
	if kind == "issue" {
		return "issue"
	}
	return firstNonEmpty(kind, "thread")
}

func kindGlyph(kind string) string {
	if kind == "pull_request" {
		return "PR"
	}
	if kind == "issue" {
		return "I"
	}
	return truncateCells(firstNonEmpty(kind, "?"), 2)
}

func clusterStateLabel(cluster store.ClusterSummary) string {
	switch strings.ToLower(firstNonEmpty(cluster.Status, "active")) {
	case "closed":
		return "CLOSED"
	case "merged":
		return "MERGED"
	case "split":
		return "SPLIT"
	default:
		if cluster.ClosedAt != "" {
			return "CLOSED"
		}
		return "OPEN"
	}
}

func kindTitle(kind string) string {
	if kind == "pull_request" {
		return "PR"
	}
	return "Issue"
}

func stateGlyph(state string) string {
	switch state {
	case "open":
		return "opn"
	case "closed":
		return "cls"
	case "excluded":
		return "exc"
	case "local":
		return "loc"
	case "merged":
		return "mrg"
	default:
		return truncateCells(firstNonEmpty(state, "?"), 3)
	}
}

func threadDisplayState(thread store.Thread) string {
	if thread.ClosedAtLocal != "" {
		return "local"
	}
	return firstNonEmpty(thread.State, "unknown")
}

func threadVisible(thread store.Thread, showClosed bool) bool {
	if showClosed {
		return true
	}
	return thread.State == "open" && thread.ClosedAtLocal == ""
}

func memberDisplayState(member store.ClusterMemberDetail) string {
	if member.State != "" && member.State != "active" {
		return member.State
	}
	return threadDisplayState(member.Thread)
}

func memberVisible(member store.ClusterMemberDetail, showClosed bool) bool {
	if showClosed {
		return true
	}
	return (member.State == "" || member.State == "active") && threadVisible(member.Thread, false)
}

func closedLabel(thread store.Thread) string {
	if thread.ClosedAtLocal == "" && thread.State == "open" {
		return "no"
	}
	closedAt := firstNonEmpty(thread.ClosedAtLocal, thread.ClosedAtGitHub, thread.State)
	if thread.CloseReasonLocal != "" {
		return closedAt + " (" + thread.CloseReasonLocal + ")"
	}
	return closedAt
}

func tuiRule(width int) string {
	return strings.Repeat("-", min(72, max(12, width)))
}

func threadRef(cluster store.ClusterSummary) string {
	if cluster.RepresentativeNumber == 0 {
		return "none"
	}
	return fmt.Sprintf("%s #%d", kindLabel(cluster.RepresentativeKind), cluster.RepresentativeNumber)
}

func formatRelativeTime(value string) string {
	if strings.TrimSpace(value) == "" {
		return "never"
	}
	parsed := parseTime(value)
	if parsed.IsZero() {
		return value
	}
	diff := time.Since(parsed)
	if diff < time.Minute {
		return "now"
	}
	if diff < time.Hour {
		return fmt.Sprintf("%dm ago", int(diff/time.Minute))
	}
	if diff < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(diff/time.Hour))
	}
	if diff < 60*24*time.Hour {
		return fmt.Sprintf("%dd ago", int(diff/(24*time.Hour)))
	}
	return fmt.Sprintf("%dmo ago", max(1, int(diff/(30*24*time.Hour))))
}

func parseTime(value string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func appendLimitedLines(out, lines []string, limit int) []string {
	if limit <= 0 || len(lines) <= limit {
		return append(out, lines...)
	}
	omitted := len(lines) - limit
	out = append(out, lines[:limit]...)
	return append(out, dim(fmt.Sprintf("... %d more line(s). Press d for full detail.", omitted)))
}

func truncateCells(value string, max int) string {
	if max <= 0 {
		return ""
	}
	if lipgloss.Width(value) <= max {
		return value
	}
	if max <= 3 {
		return strings.Repeat(".", max)
	}
	runes := []rune(value)
	for len(runes) > 0 && lipgloss.Width(string(runes))+3 > max {
		runes = runes[:len(runes)-1]
	}
	return string(runes) + "..."
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func clampInt(value, minValue, maxValue int) int {
	if maxValue < minValue {
		return minValue
	}
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
