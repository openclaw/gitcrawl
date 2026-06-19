package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type PullRequestDetail struct {
	ThreadID         int64  `json:"thread_id"`
	RepoID           int64  `json:"repo_id"`
	Number           int    `json:"number"`
	BaseSHA          string `json:"base_sha,omitempty"`
	HeadSHA          string `json:"head_sha,omitempty"`
	HeadRef          string `json:"head_ref,omitempty"`
	HeadRepoFullName string `json:"head_repo_full_name,omitempty"`
	MergeableState   string `json:"mergeable_state,omitempty"`
	Additions        int    `json:"additions"`
	Deletions        int    `json:"deletions"`
	ChangedFiles     int    `json:"changed_files"`
	RawJSON          string `json:"raw_json,omitempty"`
	FetchedAt        string `json:"fetched_at"`
	UpdatedAt        string `json:"updated_at"`
}

type PullRequestFile struct {
	ThreadID     int64  `json:"thread_id"`
	Position     int    `json:"position"`
	Path         string `json:"path"`
	Status       string `json:"status,omitempty"`
	Additions    int    `json:"additions"`
	Deletions    int    `json:"deletions"`
	Changes      int    `json:"changes"`
	PreviousPath string `json:"previous_path,omitempty"`
	Patch        string `json:"patch,omitempty"`
	RawJSON      string `json:"raw_json,omitempty"`
	FetchedAt    string `json:"fetched_at"`
}

type PullRequestCommit struct {
	ThreadID    int64  `json:"thread_id"`
	SHA         string `json:"sha"`
	Message     string `json:"message,omitempty"`
	AuthorLogin string `json:"author_login,omitempty"`
	AuthorName  string `json:"author_name,omitempty"`
	CommittedAt string `json:"committed_at,omitempty"`
	HTMLURL     string `json:"html_url,omitempty"`
	RawJSON     string `json:"raw_json,omitempty"`
	FetchedAt   string `json:"fetched_at"`
}

type PullRequestCheck struct {
	ID           int64  `json:"id"`
	ThreadID     int64  `json:"thread_id"`
	Name         string `json:"name"`
	Status       string `json:"status,omitempty"`
	Conclusion   string `json:"conclusion,omitempty"`
	DetailsURL   string `json:"details_url,omitempty"`
	WorkflowName string `json:"workflow_name,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
	RawJSON      string `json:"raw_json,omitempty"`
	FetchedAt    string `json:"fetched_at"`
}

type WorkflowRun struct {
	RepoID       int64  `json:"repo_id"`
	RunID        string `json:"run_id"`
	RunNumber    int    `json:"run_number"`
	HeadBranch   string `json:"head_branch,omitempty"`
	HeadSHA      string `json:"head_sha,omitempty"`
	Status       string `json:"status,omitempty"`
	Conclusion   string `json:"conclusion,omitempty"`
	WorkflowName string `json:"workflow_name,omitempty"`
	Event        string `json:"event,omitempty"`
	HTMLURL      string `json:"html_url,omitempty"`
	CreatedAtGH  string `json:"created_at_gh,omitempty"`
	UpdatedAtGH  string `json:"updated_at_gh,omitempty"`
	RawJSON      string `json:"raw_json,omitempty"`
	FetchedAt    string `json:"fetched_at"`
}

type PullRequestCache struct {
	Detail  PullRequestDetail   `json:"detail"`
	Files   []PullRequestFile   `json:"files"`
	Commits []PullRequestCommit `json:"commits"`
	Checks  []PullRequestCheck  `json:"checks"`
}

func (s *Store) UpsertPullRequestCache(ctx context.Context, detail PullRequestDetail, files []PullRequestFile, commits []PullRequestCommit, checks []PullRequestCheck, runs []WorkflowRun) error {
	if s.queries != nil {
		return s.upsertPullRequestCache(ctx, detail, files, commits, checks, runs)
	}
	return s.WithTx(ctx, func(tx *Store) error {
		return tx.upsertPullRequestCache(ctx, detail, files, commits, checks, runs)
	})
}

func (s *Store) UpsertPullRequestCacheAndDocument(ctx context.Context, detail PullRequestDetail, files []PullRequestFile, commits []PullRequestCommit, checks []PullRequestCheck, runs []WorkflowRun, document Document) error {
	persist := func(st *Store) error {
		if err := st.upsertPullRequestCache(ctx, detail, files, commits, checks, runs); err != nil {
			return err
		}
		_, err := st.UpsertDocument(ctx, document)
		return err
	}
	if s.queries == nil {
		return s.WithTx(ctx, persist)
	}
	return persist(s)
}

func (s *Store) upsertPullRequestCache(ctx context.Context, detail PullRequestDetail, files []PullRequestFile, commits []PullRequestCommit, checks []PullRequestCheck, runs []WorkflowRun) error {
	if err := s.qsql().UpsertPullRequestDetail(ctx, storedb.UpsertPullRequestDetailParams{
		ThreadID:         detail.ThreadID,
		RepoID:           detail.RepoID,
		Number:           int64(detail.Number),
		BaseSha:          nullString(detail.BaseSHA),
		HeadSha:          nullString(detail.HeadSHA),
		HeadRef:          nullString(detail.HeadRef),
		HeadRepoFullName: nullString(detail.HeadRepoFullName),
		MergeableState:   nullString(detail.MergeableState),
		Additions:        int64(detail.Additions),
		Deletions:        int64(detail.Deletions),
		ChangedFiles:     int64(detail.ChangedFiles),
		RawJson:          detail.RawJSON,
		FetchedAt:        detail.FetchedAt,
		UpdatedAt:        detail.UpdatedAt,
	}); err != nil {
		return fmt.Errorf("upsert pull request detail: %w", err)
	}
	if err := s.qsql().DeletePullRequestFiles(ctx, detail.ThreadID); err != nil {
		return fmt.Errorf("clear pull request files: %w", err)
	}
	// The GitHub PR files endpoint returns a snapshot array, not stable file
	// identities. A path can appear more than once in one response, so replace
	// the PR's file list and key rows by response position within that snapshot.
	// See https://github.com/openclaw/gitcrawl/issues/77 for the duplicate-path
	// bug and why position is snapshot-local rather than a durable file identity.
	for position, file := range files {
		if err := s.qsql().InsertPullRequestFile(ctx, storedb.InsertPullRequestFileParams{
			ThreadID:     detail.ThreadID,
			Position:     int64(position),
			Path:         file.Path,
			Status:       nullString(file.Status),
			Additions:    int64(file.Additions),
			Deletions:    int64(file.Deletions),
			Changes:      int64(file.Changes),
			PreviousPath: nullString(file.PreviousPath),
			Patch:        nullString(file.Patch),
			RawJson:      file.RawJSON,
			FetchedAt:    file.FetchedAt,
		}); err != nil {
			return fmt.Errorf("upsert pull request file: %w", err)
		}
	}
	if err := s.qsql().DeletePullRequestCommits(ctx, detail.ThreadID); err != nil {
		return fmt.Errorf("clear pull request commits: %w", err)
	}
	for _, commit := range commits {
		if err := s.qsql().InsertPullRequestCommit(ctx, storedb.InsertPullRequestCommitParams{
			ThreadID:    detail.ThreadID,
			Sha:         commit.SHA,
			Message:     nullString(commit.Message),
			AuthorLogin: nullString(commit.AuthorLogin),
			AuthorName:  nullString(commit.AuthorName),
			CommittedAt: nullString(commit.CommittedAt),
			HtmlUrl:     nullString(commit.HTMLURL),
			RawJson:     commit.RawJSON,
			FetchedAt:   commit.FetchedAt,
		}); err != nil {
			return fmt.Errorf("upsert pull request commit: %w", err)
		}
	}
	if err := s.qsql().DeletePullRequestChecks(ctx, detail.ThreadID); err != nil {
		return fmt.Errorf("clear pull request checks: %w", err)
	}
	for _, check := range checks {
		if err := s.qsql().InsertPullRequestCheck(ctx, storedb.InsertPullRequestCheckParams{
			ThreadID:     detail.ThreadID,
			Name:         check.Name,
			Status:       nullString(check.Status),
			Conclusion:   nullString(check.Conclusion),
			DetailsUrl:   nullString(check.DetailsURL),
			WorkflowName: nullString(check.WorkflowName),
			StartedAt:    nullString(check.StartedAt),
			CompletedAt:  nullString(check.CompletedAt),
			RawJson:      check.RawJSON,
			FetchedAt:    check.FetchedAt,
		}); err != nil {
			return fmt.Errorf("upsert pull request check: %w", err)
		}
	}
	for _, run := range runs {
		if err := s.qsql().UpsertWorkflowRun(ctx, storedb.UpsertWorkflowRunParams{
			RepoID:       run.RepoID,
			RunID:        run.RunID,
			RunNumber:    int64(run.RunNumber),
			HeadBranch:   nullString(run.HeadBranch),
			HeadSha:      nullString(run.HeadSHA),
			Status:       nullString(run.Status),
			Conclusion:   nullString(run.Conclusion),
			WorkflowName: nullString(run.WorkflowName),
			Event:        nullString(run.Event),
			HtmlUrl:      nullString(run.HTMLURL),
			CreatedAtGh:  nullString(run.CreatedAtGH),
			UpdatedAtGh:  nullString(run.UpdatedAtGH),
			RawJson:      run.RawJSON,
			FetchedAt:    run.FetchedAt,
		}); err != nil {
			return fmt.Errorf("upsert workflow run: %w", err)
		}
	}
	return nil
}

func (s *Store) PullRequestCache(ctx context.Context, repoID int64, number int) (PullRequestCache, error) {
	var cache PullRequestCache
	detail, err := s.qsql().PullRequestDetail(ctx, storedb.PullRequestDetailParams{RepoID: repoID, Number: int64(number)})
	if err != nil {
		return PullRequestCache{}, fmt.Errorf("pull request detail: %w", err)
	}
	cache.Detail = PullRequestDetail{
		ThreadID:         detail.ThreadID,
		RepoID:           detail.RepoID,
		Number:           int(detail.Number),
		BaseSHA:          stringValue(detail.BaseSha),
		HeadSHA:          stringValue(detail.HeadSha),
		HeadRef:          stringValue(detail.HeadRef),
		HeadRepoFullName: stringValue(detail.HeadRepoFullName),
		MergeableState:   stringValue(detail.MergeableState),
		Additions:        int(detail.Additions),
		Deletions:        int(detail.Deletions),
		ChangedFiles:     int(detail.ChangedFiles),
		RawJSON:          detail.RawJson,
		FetchedAt:        detail.FetchedAt,
		UpdatedAt:        detail.UpdatedAt,
	}
	files, err := s.PullRequestFiles(ctx, cache.Detail.ThreadID)
	if err != nil {
		return PullRequestCache{}, err
	}
	cache.Files = files
	commits, err := s.PullRequestCommits(ctx, cache.Detail.ThreadID)
	if err != nil {
		return PullRequestCache{}, err
	}
	cache.Commits = commits
	checks, err := s.PullRequestChecks(ctx, cache.Detail.ThreadID)
	if err != nil {
		return PullRequestCache{}, err
	}
	cache.Checks = checks
	return cache, nil
}

func (s *Store) PullRequestFiles(ctx context.Context, threadID int64) ([]PullRequestFile, error) {
	rows, err := s.pullRequestFiles(ctx, threadID)
	if err != nil {
		return nil, fmt.Errorf("list pull request files: %w", err)
	}
	out := make([]PullRequestFile, 0, len(rows))
	for _, row := range rows {
		out = append(out, PullRequestFile{
			ThreadID:     row.ThreadID,
			Position:     int(row.Position),
			Path:         row.Path,
			Status:       stringValue(row.Status),
			Additions:    int(row.Additions),
			Deletions:    int(row.Deletions),
			Changes:      int(row.Changes),
			PreviousPath: stringValue(row.PreviousPath),
			Patch:        stringValue(row.Patch),
			RawJSON:      row.RawJson,
			FetchedAt:    row.FetchedAt,
		})
	}
	return out, nil
}

func (s *Store) pullRequestFiles(ctx context.Context, threadID int64) ([]storedb.PullRequestFile, error) {
	if s.queries != nil {
		return s.qsql().PullRequestFiles(ctx, threadID)
	}
	if s.hasColumn(ctx, "pull_request_files", "position") {
		return s.qsql().PullRequestFiles(ctx, threadID)
	}
	return s.pullRequestFilesLegacy(ctx, threadID)
}

func (s *Store) pullRequestFilesLegacy(ctx context.Context, threadID int64) ([]storedb.PullRequestFile, error) {
	rows, err := s.q().QueryContext(ctx, `
		select thread_id, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at
		from pull_request_files
		where thread_id = ?
		order by path`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storedb.PullRequestFile
	for rows.Next() {
		var row storedb.PullRequestFile
		if err := rows.Scan(
			&row.ThreadID,
			&row.Path,
			&row.Status,
			&row.Additions,
			&row.Deletions,
			&row.Changes,
			&row.PreviousPath,
			&row.Patch,
			&row.RawJson,
			&row.FetchedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) PullRequestCommits(ctx context.Context, threadID int64) ([]PullRequestCommit, error) {
	rows, err := s.qsql().PullRequestCommits(ctx, threadID)
	if err != nil {
		return nil, fmt.Errorf("list pull request commits: %w", err)
	}
	out := make([]PullRequestCommit, 0, len(rows))
	for _, row := range rows {
		out = append(out, PullRequestCommit{
			ThreadID:    row.ThreadID,
			SHA:         row.Sha,
			Message:     stringValue(row.Message),
			AuthorLogin: stringValue(row.AuthorLogin),
			AuthorName:  stringValue(row.AuthorName),
			CommittedAt: stringValue(row.CommittedAt),
			HTMLURL:     stringValue(row.HtmlUrl),
			RawJSON:     row.RawJson,
			FetchedAt:   row.FetchedAt,
		})
	}
	return out, nil
}

func (s *Store) PullRequestChecks(ctx context.Context, threadID int64) ([]PullRequestCheck, error) {
	rows, err := s.qsql().PullRequestChecks(ctx, threadID)
	if err != nil {
		return nil, fmt.Errorf("list pull request checks: %w", err)
	}
	out := make([]PullRequestCheck, 0, len(rows))
	for _, row := range rows {
		out = append(out, PullRequestCheck{
			ID:           row.ID,
			ThreadID:     row.ThreadID,
			Name:         row.Name,
			Status:       stringValue(row.Status),
			Conclusion:   stringValue(row.Conclusion),
			DetailsURL:   stringValue(row.DetailsUrl),
			WorkflowName: stringValue(row.WorkflowName),
			StartedAt:    stringValue(row.StartedAt),
			CompletedAt:  stringValue(row.CompletedAt),
			RawJSON:      row.RawJson,
			FetchedAt:    row.FetchedAt,
		})
	}
	return out, nil
}

func (s *Store) PullRequestChecksAPIOrder(ctx context.Context, threadID int64) ([]PullRequestCheck, error) {
	rows, err := s.q().QueryContext(ctx, `
		select id, thread_id, name, status, conclusion, details_url, workflow_name, started_at, completed_at, raw_json, fetched_at
		from pull_request_checks
		where thread_id = ?
		order by id`, threadID)
	if err != nil {
		return nil, fmt.Errorf("list pull request checks: %w", err)
	}
	defer rows.Close()
	out := []PullRequestCheck{}
	for rows.Next() {
		var check PullRequestCheck
		var status, conclusion, detailsURL, workflowName, startedAt, completedAt sql.NullString
		if err := rows.Scan(&check.ID, &check.ThreadID, &check.Name, &status, &conclusion, &detailsURL, &workflowName, &startedAt, &completedAt, &check.RawJSON, &check.FetchedAt); err != nil {
			return nil, err
		}
		check.Status = stringValue(status)
		check.Conclusion = stringValue(conclusion)
		check.DetailsURL = stringValue(detailsURL)
		check.WorkflowName = stringValue(workflowName)
		check.StartedAt = stringValue(startedAt)
		check.CompletedAt = stringValue(completedAt)
		out = append(out, check)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type WorkflowRunListOptions struct {
	Branch  string
	HeadSHA string
	Limit   int
}

func (s *Store) ListWorkflowRuns(ctx context.Context, repoID int64, options WorkflowRunListOptions) ([]WorkflowRun, error) {
	limit := options.Limit
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.qsql().ListWorkflowRuns(ctx, storedb.ListWorkflowRunsParams{
		RepoID:     repoID,
		HeadBranch: optionalString(options.Branch),
		HeadSha:    optionalString(options.HeadSHA),
		RowLimit:   int64(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("list workflow runs: %w", err)
	}
	out := make([]WorkflowRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, WorkflowRun{
			RepoID:       row.RepoID,
			RunID:        row.RunID,
			RunNumber:    int(row.RunNumber),
			HeadBranch:   stringValue(row.HeadBranch),
			HeadSHA:      stringValue(row.HeadSha),
			Status:       stringValue(row.Status),
			Conclusion:   stringValue(row.Conclusion),
			WorkflowName: stringValue(row.WorkflowName),
			Event:        stringValue(row.Event),
			HTMLURL:      stringValue(row.HtmlUrl),
			CreatedAtGH:  stringValue(row.CreatedAtGh),
			UpdatedAtGH:  stringValue(row.UpdatedAtGh),
			RawJSON:      row.RawJson,
			FetchedAt:    row.FetchedAt,
		})
	}
	return out, nil
}
