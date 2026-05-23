package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) ghThreadViewJSONRow(ctx context.Context, repoValue string, thread store.Thread, fieldsRaw string, controls ghShimControls) (map[string]any, error) {
	fields := parseJSONFields(fieldsRaw)
	if len(fields) == 0 {
		return nil, fmt.Errorf("--json requires at least one field")
	}
	row := make(map[string]any, len(fields))
	var cache *store.PullRequestCache
	reviewHydrated := false
	for _, field := range fields {
		if field == "comments" {
			comments, err := a.localGHThreadComments(ctx, thread.ID)
			if err != nil {
				return nil, err
			}
			row[field] = ghCommentsJSONValue(comments)
			continue
		}
		if field == "reviews" || field == "latestReviews" || field == "reviewDecision" {
			if thread.Kind != "pull_request" {
				return nil, fmt.Errorf("unsupported --json field %q", field)
			}
			if !reviewHydrated && !controls.Cached {
				if err := a.hydrateGHPRViewReviews(ctx, repoValue, thread.Number); err != nil {
					return nil, err
				}
				loaded, loadErr := a.localGHPullRequestCache(ctx, repoValue, thread.Number)
				if loadErr != nil {
					return nil, loadErr
				}
				cache = &loaded
				reviewHydrated = true
			}
			if cache == nil {
				var loaded store.PullRequestCache
				var loadErr error
				if controls.Cached {
					loaded, loadErr = a.localGHPullRequestCache(ctx, repoValue, thread.Number)
				} else {
					loaded, loadErr = a.loadGHPullRequestCache(ctx, repoValue, thread.Number, ghPRFieldsNeedFresh(fields))
				}
				if loadErr != nil {
					return nil, loadErr
				}
				cache = &loaded
			}
			comments, err := a.localGHThreadComments(ctx, thread.ID)
			if err != nil {
				return nil, err
			}
			if field == "reviewDecision" {
				row[field] = ghReviewDecisionFromSummary(summarizePRReviews(comments, cacheHeadSHA(cache)))
			} else {
				row[field] = ghReviewsJSONValue(comments, cacheHeadSHA(cache), field == "latestReviews")
			}
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
			var loaded store.PullRequestCache
			var loadErr error
			if controls.Cached {
				loaded, loadErr = a.localGHPullRequestCache(ctx, repoValue, thread.Number)
			} else {
				loaded, loadErr = a.loadGHPullRequestCache(ctx, repoValue, thread.Number, ghPRFieldsNeedFresh(fields))
			}
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

func (a *App) hydrateGHPRViewReviews(ctx context.Context, repoValue string, number int) error {
	if !a.shouldAutoHydrateGHThread(nil) {
		return localGHUnsupported(fmt.Errorf("fresh PR reviews require live hydration"))
	}
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return localGHUnsupported(err)
	}
	if _, err := a.syncRepository(ctx, owner, repoName, syncOptions{
		Numbers:          []int{number},
		IncludeComments:  true,
		IncludePRDetails: true,
		Quiet:            true,
	}); err != nil {
		return localGHUnsupported(err)
	}
	return nil
}

func cacheHeadSHA(cache *store.PullRequestCache) string {
	if cache == nil {
		return ""
	}
	return cache.Detail.HeadSHA
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

func ghReviewsJSONValue(comments []store.Comment, headSHA string, latestOnly bool) []map[string]any {
	reviews := ghPullReviewEvents(comments, headSHA)
	out := make([]map[string]any, 0, len(reviews))
	seen := map[string]struct{}{}
	for _, review := range reviews {
		if latestOnly {
			key := strings.ToLower(strings.TrimSpace(review.Author))
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		out = append(out, map[string]any{
			"id":          review.ID,
			"author":      map[string]any{"login": review.Author, "type": review.AuthorType},
			"body":        "",
			"state":       review.State,
			"isStale":     review.IsStale,
			"commit":      map[string]any{"oid": review.CommitID},
			"submittedAt": review.SubmittedAt,
		})
	}
	return out
}

func ghPullReviewEvents(comments []store.Comment, headSHA string) []ghPRStatusReview {
	reviews := make([]ghPRStatusReview, 0, len(comments))
	for _, comment := range comments {
		if comment.CommentType != "pull_review" {
			continue
		}
		raw := decodeRawJSON(comment.RawJSON)
		review := ghPRStatusReview{
			ID:          comment.GitHubID,
			Author:      comment.AuthorLogin,
			AuthorType:  comment.AuthorType,
			State:       strings.ToUpper(firstNonEmpty(rawString(raw, "state"), rawString(raw, "event"))),
			CommitID:    rawString(raw, "commit_id"),
			SubmittedAt: firstNonEmpty(rawString(raw, "submitted_at"), comment.CreatedAtGitHub),
		}
		review.IsStale = headSHA != "" && review.CommitID != "" && review.CommitID != headSHA
		reviews = append(reviews, review)
	}
	sort.SliceStable(reviews, func(i, j int) bool {
		return reviews[i].SubmittedAt > reviews[j].SubmittedAt
	})
	return reviews
}

func ghReviewDecision(reviews []map[string]any) any {
	approved := false
	seen := map[string]struct{}{}
	for _, review := range reviews {
		if stale, _ := review["isStale"].(bool); stale {
			continue
		}
		key := ""
		if author, ok := review["author"].(map[string]any); ok {
			key, _ = author["login"].(string)
		}
		if key == "" {
			key, _ = review["id"].(string)
		}
		if key != "" {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		state, _ := review["state"].(string)
		switch strings.ToUpper(state) {
		case "CHANGES_REQUESTED":
			return "CHANGES_REQUESTED"
		case "APPROVED":
			approved = true
		}
	}
	if approved {
		return "APPROVED"
	}
	return nil
}

func ghReviewDecisionFromSummary(summary ghPRStatusReviews) any {
	if summary.ChangesRequested > 0 {
		return "CHANGES_REQUESTED"
	}
	if summary.Approvals > 0 {
		return "APPROVED"
	}
	return nil
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
	case "baseRefName":
		return rawString(rawMap(decodeRawJSON(cache.Detail.RawJSON), "base"), "ref"), nil
	case "headRefOid":
		return cache.Detail.HeadSHA, nil
	case "baseRefOid":
		return cache.Detail.BaseSHA, nil
	case "headRepositoryOwner":
		owner := strings.Split(cache.Detail.HeadRepoFullName, "/")[0]
		return map[string]any{"login": owner}, nil
	case "headRepository":
		return map[string]any{"nameWithOwner": cache.Detail.HeadRepoFullName}, nil
	case "mergeCommit":
		if strings.TrimSpace(thread.MergedAtGitHub) == "" {
			return nil, nil
		}
		sha := rawString(decodeRawJSON(cache.Detail.RawJSON), "merge_commit_sha")
		if sha == "" {
			return nil, nil
		}
		return map[string]any{"oid": sha}, nil
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
	case "maintainerCanModify":
		return rawBool(decodeRawJSON(cache.Detail.RawJSON), "maintainer_can_modify"), nil
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
	if ghBoolFlagEnabled(args, "--watch") || hasAnyGHFlag(args, "--web") {
		return localGHUnsupported(fmt.Errorf("interactive PR checks flags require live gh"))
	}
	fs := flag.NewFlagSet("pr checks", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	jqRaw := fs.String("jq", "", "jq filter")
	watchRaw := fs.Bool("watch", false, "watch")
	requiredRaw := fs.Bool("required", false, "required")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"R": true, "repo": true, "json": true, "jq": true, "watch": true, "required": false})); err != nil {
		return usageErr(err)
	}
	if *watchRaw {
		return localGHUnsupported(fmt.Errorf("interactive PR checks flags require live gh"))
	}
	if *requiredRaw {
		return localGHUnsupported(fmt.Errorf("required PR checks filtering requires live gh"))
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
