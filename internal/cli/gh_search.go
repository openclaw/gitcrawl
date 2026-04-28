package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func isGHSearchKind(value string) bool {
	switch value {
	case "issues", "issue", "prs", "pr", "pulls", "pull-requests":
		return true
	default:
		return false
	}
}

func ghSearchKind(value string) string {
	switch value {
	case "prs", "pr", "pulls", "pull-requests":
		return "pull_request"
	default:
		return "issue"
	}
}

func (a *App) runGHSearch(ctx context.Context, args []string) error {
	kind := ghSearchKind(args[0])
	fs := flag.NewFlagSet("search "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	stateRaw := fs.String("state", "", "GitHub state: open|closed|all")
	limitRaw := fs.String("limit", "", "maximum rows")
	limitShortRaw := fs.String("L", "", "maximum rows")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	if err := fs.Parse(normalizeCommandArgs(args[1:], map[string]bool{
		"R": true, "repo": true, "state": true, "limit": true, "L": true, "json": true,
	})); err != nil {
		return usageErr(err)
	}

	query, qualifierRepo, qualifierState := parseGHSearchQuery(strings.Join(fs.Args(), " "))
	repoValue := firstNonEmpty(*repoShort, *repoLong, qualifierRepo)
	if strings.TrimSpace(repoValue) == "" {
		return usageErr(fmt.Errorf("search %s requires -R owner/repo or repo:owner/repo", args[0]))
	}
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return usageErr(err)
	}
	state := firstNonEmpty(strings.TrimSpace(*stateRaw), qualifierState)
	if err := validateGHSearchState(state); err != nil {
		return usageErr(err)
	}
	limit, err := parseGHSearchLimit(*limitRaw, *limitShortRaw)
	if err != nil {
		return usageErr(err)
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	threads, err := rt.Store.SearchThreads(ctx, store.ThreadSearchOptions{
		RepoID:               repo.ID,
		Query:                query,
		Kind:                 kind,
		State:                state,
		IncludeLocallyClosed: true,
		Limit:                limit,
	})
	if err != nil {
		return err
	}

	jsonFields := strings.TrimSpace(*jsonFieldsRaw)
	if jsonFields != "" || a.format == FormatJSON {
		if jsonFields == "" {
			jsonFields = "number,title,state,url"
		}
		rows, err := ghSearchJSONRows(threads, jsonFields)
		if err != nil {
			return usageErr(err)
		}
		data, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.Stdout, "%s\n", data)
		return err
	}

	for _, thread := range threads {
		if _, err := fmt.Fprintf(a.Stdout, "%s\t#%d\t%s\t%s\n", thread.Kind, thread.Number, thread.Title, thread.HTMLURL); err != nil {
			return err
		}
	}
	return nil
}

func parseGHSearchQuery(value string) (query string, repo string, state string) {
	var queryParts []string
	for _, part := range strings.Fields(value) {
		lower := strings.ToLower(part)
		switch {
		case strings.HasPrefix(lower, "repo:"):
			repo = strings.TrimSpace(part[len("repo:"):])
		case strings.HasPrefix(lower, "state:"):
			state = strings.ToLower(strings.TrimSpace(part[len("state:"):]))
		case lower == "is:open" || lower == "is:closed":
			state = strings.TrimPrefix(lower, "is:")
		case lower == "is:issue" || lower == "is:pr" || lower == "is:pull-request":
		default:
			queryParts = append(queryParts, part)
		}
	}
	return strings.TrimSpace(strings.Join(queryParts, " ")), repo, state
}

func validateGHSearchState(state string) error {
	if strings.TrimSpace(state) == "" {
		return nil
	}
	switch state {
	case "open", "closed", "all":
		return nil
	default:
		return fmt.Errorf("unsupported state %q", state)
	}
}

func parseGHSearchLimit(longRaw, shortRaw string) (int, error) {
	if strings.TrimSpace(longRaw) != "" && strings.TrimSpace(shortRaw) != "" && strings.TrimSpace(longRaw) != strings.TrimSpace(shortRaw) {
		return 0, fmt.Errorf("--limit and -L disagree")
	}
	return parseOptionalPositiveInt(firstNonEmpty(longRaw, shortRaw))
}

func ghSearchJSONRows(threads []store.Thread, fieldsRaw string) ([]map[string]any, error) {
	fields := parseJSONFields(fieldsRaw)
	if len(fields) == 0 {
		return nil, fmt.Errorf("--json requires at least one field")
	}
	rows := make([]map[string]any, 0, len(threads))
	for _, thread := range threads {
		row := make(map[string]any, len(fields))
		for _, field := range fields {
			value, err := ghSearchJSONValue(thread, field)
			if err != nil {
				return nil, err
			}
			row[field] = value
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseJSONFields(value string) []string {
	parts := strings.Split(value, ",")
	fields := make([]string, 0, len(parts))
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

func ghSearchJSONValue(thread store.Thread, field string) (any, error) {
	switch field {
	case "number":
		return thread.Number, nil
	case "title":
		return thread.Title, nil
	case "state":
		return thread.State, nil
	case "url":
		return thread.HTMLURL, nil
	case "updatedAt":
		return firstNonEmpty(thread.UpdatedAtGitHub, thread.UpdatedAt), nil
	case "createdAt":
		return thread.CreatedAtGitHub, nil
	case "closedAt":
		return thread.ClosedAtGitHub, nil
	case "mergedAt":
		return thread.MergedAtGitHub, nil
	case "labels":
		return ghLabelsFromJSON(thread.LabelsJSON), nil
	case "isDraft":
		return thread.IsDraft, nil
	case "author":
		return map[string]any{"login": thread.AuthorLogin, "type": thread.AuthorType}, nil
	case "body":
		return thread.Body, nil
	default:
		return nil, fmt.Errorf("unsupported --json field %q", field)
	}
}

type ghLabel struct {
	Name        string `json:"name"`
	Color       string `json:"color,omitempty"`
	Description string `json:"description,omitempty"`
}

func ghLabelsFromJSON(raw string) []ghLabel {
	var labels []ghLabel
	if err := json.Unmarshal([]byte(raw), &labels); err == nil {
		return labels
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		return nil
	}
	labels = make([]ghLabel, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			labels = append(labels, ghLabel{Name: name})
		}
	}
	return labels
}
