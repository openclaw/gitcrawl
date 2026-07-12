package syncer

import (
	"context"
	"time"

	"github.com/openclaw/gitcrawl/internal/documents"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type pullDetailStats struct {
	files   int
	commits int
	checks  int
	runs    int
}

type pullRequestDetailRows struct {
	fetchedAt  string
	pull       map[string]any
	filesRaw   []map[string]any
	commitsRaw []map[string]any
	checksRaw  []map[string]any
	runsRaw    []map[string]any
}

func (s *Syncer) fetchPullRequestDetails(ctx context.Context, options Options, number int) (pullRequestDetailRows, error) {
	fetchedAt := s.now().Format(time.RFC3339Nano)
	pull, err := s.client.GetPull(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	filesRaw, err := s.client.ListPullFiles(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	commitsRaw, err := s.client.ListPullCommits(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	headSHA := nestedString(pull, "head", "sha")
	var checksRaw []map[string]any
	if headSHA != "" {
		checksRaw, err = s.client.ListCommitCheckRuns(ctx, options.Owner, options.Repo, headSHA, options.Reporter)
		if err != nil {
			return pullRequestDetailRows{}, err
		}
	}
	var runsRaw []map[string]any
	if headSHA != "" {
		runsRaw, err = s.client.ListWorkflowRuns(ctx, options.Owner, options.Repo, gh.ListWorkflowRunsOptions{HeadSHA: headSHA}, options.Reporter)
		if err != nil {
			return pullRequestDetailRows{}, err
		}
	}
	return pullRequestDetailRows{
		fetchedAt:  fetchedAt,
		pull:       pull,
		filesRaw:   filesRaw,
		commitsRaw: commitsRaw,
		checksRaw:  checksRaw,
		runsRaw:    runsRaw,
	}, nil
}

func (s *Syncer) persistPullRequestDetails(ctx context.Context, st *store.Store, thread store.Thread, rows pullRequestDetailRows) (pullDetailStats, error) {
	fetchedAt := rows.fetchedAt
	if fetchedAt == "" {
		fetchedAt = s.now().Format(time.RFC3339Nano)
	}
	detail := mapPullDetail(thread, rows.pull, fetchedAt)
	files := mapPullFiles(thread.ID, rows.filesRaw, fetchedAt)
	commits := mapPullCommits(thread.ID, rows.commitsRaw, fetchedAt)
	checks := mapPullChecks(thread.ID, rows.checksRaw, fetchedAt)
	runs := mapWorkflowRuns(thread.RepoID, rows.runsRaw, fetchedAt)
	comments, err := st.ListComments(ctx, thread.ID)
	if err != nil {
		return pullDetailStats{}, err
	}
	document := documents.BuildWithContext(thread, comments, files, commits)
	if err := st.UpsertPullRequestCacheAndDocument(ctx, detail, files, commits, checks, runs, document); err != nil {
		return pullDetailStats{}, err
	}
	return pullDetailStats{files: len(files), commits: len(commits), checks: len(checks), runs: len(runs)}, nil
}

func mapPullDetail(thread store.Thread, pull map[string]any, fetchedAt string) store.PullRequestDetail {
	return store.PullRequestDetail{
		ThreadID:         thread.ID,
		RepoID:           thread.RepoID,
		Number:           thread.Number,
		BaseSHA:          nestedString(pull, "base", "sha"),
		HeadSHA:          nestedString(pull, "head", "sha"),
		HeadRef:          nestedString(pull, "head", "ref"),
		HeadRepoFullName: nestedString(pull, "head", "repo", "full_name"),
		MergeableState:   stringValue(pull["mergeable_state"]),
		Additions:        intValue(pull["additions"]),
		Deletions:        intValue(pull["deletions"]),
		ChangedFiles:     intValue(pull["changed_files"]),
		RawJSON:          mustJSON(pull),
		FetchedAt:        fetchedAt,
		UpdatedAt:        fetchedAt,
	}
}

func mapPullFiles(threadID int64, rows []map[string]any, fetchedAt string) []store.PullRequestFile {
	out := make([]store.PullRequestFile, 0, len(rows))
	for _, row := range rows {
		filename := stringValue(row["filename"])
		if filename == "" {
			continue
		}
		out = append(out, store.PullRequestFile{
			ThreadID:     threadID,
			Path:         filename,
			Status:       stringValue(row["status"]),
			Additions:    intValue(row["additions"]),
			Deletions:    intValue(row["deletions"]),
			Changes:      intValue(row["changes"]),
			PreviousPath: stringValue(row["previous_filename"]),
			Patch:        stringValue(row["patch"]),
			RawJSON:      mustJSON(row),
			FetchedAt:    fetchedAt,
		})
	}
	return out
}

func mapPullCommits(threadID int64, rows []map[string]any, fetchedAt string) []store.PullRequestCommit {
	out := make([]store.PullRequestCommit, 0, len(rows))
	for _, row := range rows {
		sha := stringValue(row["sha"])
		if sha == "" {
			continue
		}
		out = append(out, store.PullRequestCommit{
			ThreadID:    threadID,
			SHA:         sha,
			Message:     nestedString(row, "commit", "message"),
			AuthorLogin: nestedString(row, "author", "login"),
			AuthorName:  nestedString(row, "commit", "author", "name"),
			CommittedAt: nestedString(row, "commit", "author", "date"),
			HTMLURL:     stringValue(row["html_url"]),
			RawJSON:     mustJSON(row),
			FetchedAt:   fetchedAt,
		})
	}
	return out
}
