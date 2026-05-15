package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) ghThreadViewJSONRow(ctx context.Context, repoValue string, thread store.Thread, fieldsRaw string) (map[string]any, error) {
	fields := parseJSONFields(fieldsRaw)
	if len(fields) == 0 {
		return nil, fmt.Errorf("--json requires at least one field")
	}
	row := make(map[string]any, len(fields))
	var cache *store.PullRequestCache
	for _, field := range fields {
		if field == "comments" {
			comments, err := a.localGHThreadComments(ctx, thread.ID)
			if err != nil {
				return nil, err
			}
			row[field] = ghCommentsJSONValue(comments)
			continue
		}
		value, err := ghSearchJSONValue(thread, field)
		if err == nil {
			row[field] = value
			continue
		}
		if thread.Kind != "pull_request" {
			return nil, err
		}
		if cache == nil {
			loaded, loadErr := a.loadGHPullRequestCache(ctx, repoValue, thread.Number, ghPRFieldsNeedFresh(fields))
			if loadErr != nil {
				return nil, loadErr
			}
			cache = &loaded
		}
		value, err = ghPRDetailJSONValue(thread, *cache, field)
		if err != nil {
			return nil, err
		}
		row[field] = value
	}
	return row, nil
}

func (a *App) localGHPullRequestCache(ctx context.Context, repoValue string, number int) (store.PullRequestCache, error) {
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return store.PullRequestCache{}, err
	}
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return store.PullRequestCache{}, localGHUnsupported(err)
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return store.PullRequestCache{}, localGHUnsupported(err)
	}
	cache, err := rt.Store.PullRequestCache(ctx, repo.ID, number)
	if err != nil {
		return store.PullRequestCache{}, localGHUnsupported(err)
	}
	return cache, nil
}

func (a *App) localGHThreadComments(ctx context.Context, threadID int64) ([]store.Comment, error) {
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return nil, localGHUnsupported(err)
	}
	defer rt.Store.Close()
	comments, err := rt.Store.ListComments(ctx, threadID)
	if err != nil {
		return nil, localGHUnsupported(err)
	}
	return comments, nil
}

func ghCommentsJSONValue(comments []store.Comment) []map[string]any {
	out := make([]map[string]any, 0, len(comments))
	for _, comment := range comments {
		if comment.CommentType == "pull_review" {
			continue
		}
		out = append(out, map[string]any{
			"id":        comment.GitHubID,
			"author":    map[string]any{"login": comment.AuthorLogin, "type": comment.AuthorType},
			"body":      comment.Body,
			"createdAt": comment.CreatedAtGitHub,
			"updatedAt": comment.UpdatedAtGitHub,
		})
	}
	return out
}

func ghPRDetailJSONValue(thread store.Thread, cache store.PullRequestCache, field string) (any, error) {
	switch field {
	case "files":
		files := make([]map[string]any, 0, len(cache.Files))
		for _, file := range cache.Files {
			files = append(files, map[string]any{
				"path":      file.Path,
				"additions": file.Additions,
				"deletions": file.Deletions,
				"status":    file.Status,
			})
		}
		return files, nil
	case "commits":
		commits := make([]map[string]any, 0, len(cache.Commits))
		for _, commit := range cache.Commits {
			headline := commit.Message
			if index := strings.IndexByte(headline, '\n'); index >= 0 {
				headline = headline[:index]
			}
			commits = append(commits, map[string]any{
				"oid":             commit.SHA,
				"messageHeadline": headline,
				"messageBody":     commit.Message,
				"authoredDate":    commit.CommittedAt,
				"url":             commit.HTMLURL,
				"authors": []map[string]any{{
					"login": commit.AuthorLogin,
					"name":  commit.AuthorName,
				}},
			})
		}
		return commits, nil
	case "statusCheckRollup":
		return ghStatusCheckRollup(cache.Checks), nil
	case "headRefName":
		return cache.Detail.HeadRef, nil
	case "headRefOid":
		return cache.Detail.HeadSHA, nil
	case "baseRefOid":
		return cache.Detail.BaseSHA, nil
	case "headRepositoryOwner":
		owner := strings.Split(cache.Detail.HeadRepoFullName, "/")[0]
		return map[string]any{"login": owner}, nil
	case "headRepository":
		return map[string]any{"nameWithOwner": cache.Detail.HeadRepoFullName}, nil
	case "mergeStateStatus":
		return strings.ToUpper(cache.Detail.MergeableState), nil
	case "mergeable":
		return ghMergeableValue(cache.Detail.MergeableState), nil
	case "additions":
		return cache.Detail.Additions, nil
	case "deletions":
		return cache.Detail.Deletions, nil
	case "changedFiles":
		return cache.Detail.ChangedFiles, nil
	case "isDraft":
		return thread.IsDraft, nil
	default:
		return nil, fmt.Errorf("unsupported --json field %q", field)
	}
}

func ghMergeableValue(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "blocked", "behind", "clean", "unstable", "has_hooks":
		return "MERGEABLE"
	case "dirty":
		return "CONFLICTING"
	default:
		return "UNKNOWN"
	}
}

func ghStatusCheckRollup(checks []store.PullRequestCheck) []map[string]any {
	out := make([]map[string]any, 0, len(checks))
	for _, check := range checks {
		state := strings.ToUpper(firstNonEmpty(check.Conclusion, check.Status))
		out = append(out, map[string]any{
			"__typename":   "CheckRun",
			"name":         check.Name,
			"status":       strings.ToUpper(check.Status),
			"conclusion":   strings.ToUpper(check.Conclusion),
			"state":        state,
			"detailsUrl":   check.DetailsURL,
			"workflowName": check.WorkflowName,
			"startedAt":    check.StartedAt,
			"completedAt":  check.CompletedAt,
		})
	}
	return out
}

func (a *App) runGHPRChecks(ctx context.Context, args []string) error {
	if hasAnyGHFlag(args, "--watch", "--web") {
		return localGHUnsupported(fmt.Errorf("interactive PR checks flags require live gh"))
	}
	fs := flag.NewFlagSet("pr checks", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	jqRaw := fs.String("jq", "", "jq filter")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"R": true, "repo": true, "json": true, "jq": true})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("gh pr checks requires a number or GitHub URL"))
	}
	ref, _ := parseThreadReference(fs.Arg(0))
	number, err := parseThreadNumber(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	repoArg := firstNonEmpty(*repoShort, *repoLong)
	if repoArg == "" {
		repoArg = ref.FullName()
	}
	repoValue, err := a.resolveGHRepo(ctx, repoArg)
	if err != nil {
		return localGHUnsupported(err)
	}
	cache, err := a.ensureFreshGHPullRequestCache(ctx, repoValue, number)
	if err != nil {
		return err
	}
	if len(cache.Checks) == 0 {
		return localGHUnsupported(fmt.Errorf("cached PR checks are empty"))
	}
	if strings.TrimSpace(*jsonFieldsRaw) != "" || strings.TrimSpace(*jqRaw) != "" || a.format == FormatJSON {
		fields := firstNonEmpty(strings.TrimSpace(*jsonFieldsRaw), "name,state,conclusion,detailsUrl,workflow")
		rows, err := ghPRChecksJSONRows(cache.Checks, fields)
		if err != nil {
			return localGHUnsupported(err)
		}
		return a.writeJSONValue(rows, strings.TrimSpace(*jqRaw))
	}
	for _, check := range cache.Checks {
		if _, err := fmt.Fprintf(a.Stdout, "%s\t%s\t%s\t%s\n", check.Name, check.Status, check.Conclusion, check.DetailsURL); err != nil {
			return err
		}
	}
	return nil
}

func ghPRChecksJSONRows(checks []store.PullRequestCheck, fieldsRaw string) ([]map[string]any, error) {
	fields := parseJSONFields(fieldsRaw)
	rows := make([]map[string]any, 0, len(checks))
	for _, check := range checks {
		row := make(map[string]any, len(fields))
		for _, field := range fields {
			switch field {
			case "name":
				row[field] = check.Name
			case "state":
				row[field] = strings.ToUpper(firstNonEmpty(check.Conclusion, check.Status))
			case "status":
				row[field] = check.Status
			case "conclusion":
				row[field] = check.Conclusion
			case "detailsUrl", "link":
				row[field] = check.DetailsURL
			case "workflow":
				row[field] = check.WorkflowName
			case "startedAt":
				row[field] = check.StartedAt
			case "completedAt":
				row[field] = check.CompletedAt
			default:
				return nil, fmt.Errorf("unsupported gh pr checks --json field %q", field)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}
