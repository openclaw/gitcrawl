package documents

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

var whitespacePattern = regexp.MustCompile(`\s+`)

func Build(thread store.Thread) store.Document {
	return BuildWithComments(thread, nil)
}

func BuildWithComments(thread store.Thread, comments []store.Comment) store.Document {
	labels := labelNames(thread.LabelsJSON)
	sections := []string{
		"# " + thread.Title,
	}
	if strings.TrimSpace(thread.Body) != "" {
		sections = append(sections, strings.TrimSpace(thread.Body))
	}
	if len(labels) > 0 {
		sections = append(sections, "Labels: "+strings.Join(labels, ", "))
	}
	for _, comment := range comments {
		if comment.IsBot || strings.TrimSpace(comment.Body) == "" {
			continue
		}
		sections = append(sections, comment.AuthorLogin+": "+strings.TrimSpace(comment.Body))
	}
	rawText := strings.Join(sections, "\n\n")
	dedupeParts := []string{thread.Title, thread.Body, strings.Join(labels, " ")}
	for _, comment := range comments {
		if comment.IsBot {
			continue
		}
		dedupeParts = append(dedupeParts, comment.Body)
	}
	return store.Document{
		ThreadID:   thread.ID,
		Title:      thread.Title,
		Body:       thread.Body,
		RawText:    rawText,
		DedupeText: normalizeDedupe(strings.Join(dedupeParts, " ")),
		UpdatedAt:  thread.UpdatedAt,
	}
}

func normalizeDedupe(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "\x00", " ")
	value = whitespacePattern.ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func labelNames(raw string) []string {
	var labels []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(raw), &labels); err != nil {
		return nil
	}
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		name := strings.TrimSpace(label.Name)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}
