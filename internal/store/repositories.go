package store

import (
	"context"
	"database/sql"
	"errors"
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

func (s *Store) UpsertRepository(ctx context.Context, repo Repository) (int64, error) {
	if repo.FullName == "" {
		repo.FullName = repo.Owner + "/" + repo.Name
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
	return id, nil
}

// repositoryNotFoundError explains an owner/repo absent from this archive and
// still matches sql.ErrNoRows for callers that treat absence as a first sync.
type repositoryNotFoundError struct{ fullName string }

func (e repositoryNotFoundError) Error() string {
	return fmt.Sprintf("repository %s is not in this archive; check the owner/repo spelling, or sync it first with `gitcrawl sync %s`", e.fullName, e.fullName)
}

func (e repositoryNotFoundError) Unwrap() error { return sql.ErrNoRows }

func (s *Store) RepositoryByFullName(ctx context.Context, fullName string) (Repository, error) {
	if s.hasColumn(ctx, "repositories", "raw_json") {
		repo, err := s.qsql().RepositoryByFullName(ctx, fullName)
		if errors.Is(err, sql.ErrNoRows) {
			return Repository{}, repositoryNotFoundError{fullName: fullName}
		}
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
	if errors.Is(err, sql.ErrNoRows) {
		return Repository{}, repositoryNotFoundError{fullName: fullName}
	}
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
