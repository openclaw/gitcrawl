package documents

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

var whitespacePattern = regexp.MustCompile(`\s+`)

const (
	maxChangedFilePaths = 200
	maxCommitSubjects   = 100
)

func Build(thread store.Thread) store.Document {
	return BuildWithComments(thread, nil)
}

func BuildWithComments(thread store.Thread, comments []store.Comment) store.Document {
	return BuildWithContext(thread, comments, nil, nil)
}

func BuildWithContext(thread store.Thread, comments []store.Comment, files []store.PullRequestFile, commits []store.PullRequestCommit) store.Document {
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
	changedPaths := changedFilePaths(files)
	if len(changedPaths) > 0 {
		sections = append(sections, "Changed files:\n- "+strings.Join(changedPaths, "\n- "))
	}
	commitSubjects := pullCommitSubjects(commits)
	if len(commitSubjects) > 0 {
		sections = append(sections, "Commits:\n- "+strings.Join(commitSubjects, "\n- "))
	}
	rawText := strings.Join(sections, "\n\n")
	dedupeParts := []string{thread.Title, thread.Body, strings.Join(labels, " ")}
	for _, comment := range comments {
		if comment.IsBot {
			continue
		}
		dedupeParts = append(dedupeParts, comment.Body)
	}
	dedupeParts = append(dedupeParts, changedPaths...)
	dedupeParts = append(dedupeParts, commitSubjects...)
	return store.Document{
		ThreadID:   thread.ID,
		Title:      thread.Title,
		Body:       thread.Body,
		RawText:    rawText,
		DedupeText: normalizeDedupe(strings.Join(dedupeParts, " ")),
		UpdatedAt:  thread.UpdatedAt,
	}
}

func changedFilePaths(files []store.PullRequestFile) []string {
	paths := make([]string, 0, min(len(files), maxChangedFilePaths))
	seen := make(map[string]bool, len(files))
	for _, file := range files {
		for _, path := range []string{file.Path, file.PreviousPath} {
			path = strings.TrimSpace(path)
			if path == "" || seen[path] {
				continue
			}
			seen[path] = true
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	if len(paths) > maxChangedFilePaths {
		paths = paths[:maxChangedFilePaths]
	}
	return paths
}

func pullCommitSubjects(commits []store.PullRequestCommit) []string {
	subjects := make([]string, 0, min(len(commits), maxCommitSubjects))
	for _, commit := range commits {
		message := strings.TrimSpace(commit.Message)
		if message == "" {
			continue
		}
		if newline := strings.IndexByte(message, '\n'); newline >= 0 {
			message = strings.TrimSpace(message[:newline])
		}
		if message != "" {
			subjects = append(subjects, message)
		}
		if len(subjects) == maxCommitSubjects {
			break
		}
	}
	return subjects
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
	if err := json.Unmarshal([]byte(raw), &labels); err == nil {
		out := make([]string, 0, len(labels))
		for _, label := range labels {
			name := strings.TrimSpace(label.Name)
			if name != "" {
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
		name = strings.TrimSpace(name)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}
