package store

import (
	"context"
	"database/sql"
	"fmt"
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
	var id int64
	err := s.q().QueryRowContext(ctx, `
		insert into repositories(owner, name, full_name, github_repo_id, raw_json, updated_at)
		values(?, ?, ?, ?, ?, ?)
		on conflict(full_name) do update set
			owner=excluded.owner,
			name=excluded.name,
			github_repo_id=excluded.github_repo_id,
			raw_json=excluded.raw_json,
			updated_at=excluded.updated_at
		returning id
	`, repo.Owner, repo.Name, repo.FullName, nullString(repo.GitHubRepoID), repo.RawJSON, repo.UpdatedAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert repository: %w", err)
	}
	return id, nil
}

func (s *Store) RepositoryByFullName(ctx context.Context, fullName string) (Repository, error) {
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
