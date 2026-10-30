package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type Repository struct {
	ID           int64  `json:"id"`
	Owner        string `json:"owner"`
	Name         string `json:"name"`
	FullName     string `json:"full_name"`
	GitHubRepoID string `json:"github_repo_id,omitempty"`
	RawJSON      string `json:"-"`
	UpdatedAt    string `json:"updated_at"`
}

// UpsertRepository records one repository check. With record checks enabled
// the content row is written only when repository content changed (provider
// activity clocks and counters excluded) and the check goes to
// repository_checks.
func (s *Store) UpsertRepository(ctx context.Context, repo Repository) (int64, error) {
	return s.upsertRepository(ctx, repo, true)
}

// EnsureRepository returns the repository id, writing only when the row is
// missing or its content changed. Per-item transactions use it so that a sync
// cycle does not rewrite the repository row or its check for every thread; the
// cycle's run record calls UpsertRepository once to record the check.
func (s *Store) EnsureRepository(ctx context.Context, repo Repository) (int64, error) {
	return s.upsertRepository(ctx, repo, false)
}

func (s *Store) upsertRepository(ctx context.Context, repo Repository, recordCheck bool) (int64, error) {
	if s.recordChecks && s.queries == nil {
		var id int64
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			id, err = tx.upsertRepository(ctx, repo, recordCheck)
			return err
		})
		return id, err
	}
	if repo.FullName == "" {
		repo.FullName = repo.Owner + "/" + repo.Name
	}
	if s.recordChecks {
		var id int64
		var owner, name string
		var githubRepoID sql.NullString
		var raw string
		err := s.q().QueryRowContext(ctx, `
			select id, owner, name, github_repo_id, coalesce(raw_json, '')
			from repositories where full_name = ?
		`, repo.FullName).Scan(&id, &owner, &name, &githubRepoID, &raw)
		if err != nil && err != sql.ErrNoRows {
			return 0, fmt.Errorf("read repository: %w", err)
		}
		if err == nil && owner == repo.Owner && name == repo.Name &&
			githubRepoID == nullString(repo.GitHubRepoID) &&
			repositoryContentRaw(raw) == repositoryContentRaw(repo.RawJSON) {
			if recordCheck {
				if err := s.recordRepositoryCheck(ctx, id, repo); err != nil {
					return 0, err
				}
			}
			return id, nil
		}
	}
	id, err := s.qsql().UpsertRepository(ctx, storedb.UpsertRepositoryParams{
		Owner:        repo.Owner,
		Name:         repo.Name,
		FullName:     repo.FullName,
		GithubRepoID: nullString(repo.GitHubRepoID),
		RawJson:      repo.RawJSON,
		UpdatedAt:    repo.UpdatedAt,
	})
	if err != nil {
		return 0, fmt.Errorf("upsert repository: %w", err)
	}
	if s.recordChecks {
		if err := s.recordRepositoryCheck(ctx, id, repo); err != nil {
			return 0, err
		}
	}
	return id, nil
}

func (s *Store) recordRepositoryCheck(ctx context.Context, repoID int64, repo Repository) error {
	if err := s.retainRecordCheck(ctx, "repositories", fmt.Sprint(repoID), repo.UpdatedAt, repo.RawJSON, map[string]any{"checked_at": repo.UpdatedAt}); err != nil {
		return err
	}
	if _, err := s.q().ExecContext(ctx, `
		insert into repository_checks(repo_id, checked_at, provider_updated_at) values(?, ?, ?)
		on conflict(repo_id) do update set
			checked_at = excluded.checked_at,
			provider_updated_at = excluded.provider_updated_at
	`, repoID, repo.UpdatedAt, repositoryProviderUpdatedAt(repo.RawJSON)); err != nil {
		return fmt.Errorf("record repository check: %w", err)
	}
	return nil
}

func (s *Store) RepositoryByFullName(ctx context.Context, fullName string) (Repository, error) {
	if s.hasColumn(ctx, "repositories", "raw_json") {
		repo, err := s.qsql().RepositoryByFullName(ctx, fullName)
		if err != nil {
			return Repository{}, fmt.Errorf("select repository: %w", err)
		}
		return repositoryFromDB(repo), nil
	}
	var repo Repository
	var githubRepoID sql.NullString
	var rawJSON sql.NullString
	err := s.q().QueryRowContext(ctx, `
		select id, owner, name, full_name, github_repo_id, `+s.repositoryRawJSONExpr(ctx)+`, updated_at
		from repositories
		where full_name = ?
	`, fullName).Scan(&repo.ID, &repo.Owner, &repo.Name, &repo.FullName, &githubRepoID, &rawJSON, &repo.UpdatedAt)
	if err != nil {
		return Repository{}, fmt.Errorf("select repository: %w", err)
	}
	repo.GitHubRepoID = githubRepoID.String
	repo.RawJSON = rawJSON.String
	return repo, nil
}

func (s *Store) ListRepositories(ctx context.Context) ([]Repository, error) {
	if s.hasColumn(ctx, "repositories", "raw_json") {
		rows, err := s.qsql().ListRepositories(ctx)
		if err != nil {
			return nil, fmt.Errorf("list repositories: %w", err)
		}
		repos := make([]Repository, 0, len(rows))
		for _, row := range rows {
			repos = append(repos, repositoryFromDB(row))
		}
		return repos, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		select id, owner, name, full_name, github_repo_id, `+s.repositoryRawJSONExpr(ctx)+`, updated_at
		from repositories
		order by coalesce(updated_at, '') desc, id desc
	`)
	if err != nil {
		return nil, fmt.Errorf("list repositories: %w", err)
	}
	defer rows.Close()

	var repos []Repository
	for rows.Next() {
		var repo Repository
		var githubRepoID sql.NullString
		var rawJSON sql.NullString
		if err := rows.Scan(&repo.ID, &repo.Owner, &repo.Name, &repo.FullName, &githubRepoID, &rawJSON, &repo.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan repository: %w", err)
		}
		repo.GitHubRepoID = githubRepoID.String
		repo.RawJSON = rawJSON.String
		repos = append(repos, repo)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repositories: %w", err)
	}
	return repos, nil
}

func (s *Store) repositoryRawJSONExpr(ctx context.Context) string {
	if s.hasColumn(ctx, "repositories", "raw_json") {
		return "raw_json"
	}
	return "''"
}

func nullString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}
