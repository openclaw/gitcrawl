package store

import (
	"context"
	"database/sql"
	"fmt"
)

type RunRecord struct {
	ID         int64  `json:"id"`
	RepoID     int64  `json:"repo_id"`
	Kind       string `json:"kind"`
	Scope      string `json:"scope"`
	Status     string `json:"status"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
	StatsJSON  string `json:"stats_json,omitempty"`
	ErrorText  string `json:"error_text,omitempty"`
}

func (s *Store) RecordRun(ctx context.Context, run RunRecord) (int64, error) {
	table, err := runTable(run.Kind)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.db.QueryRowContext(ctx, `
		insert into `+table+`(repo_id, scope, status, started_at, finished_at, stats_json, error_text)
		values(?, ?, ?, ?, ?, ?, ?)
		returning id
	`, run.RepoID, run.Scope, run.Status, run.StartedAt, nullString(run.FinishedAt), nullString(run.StatsJSON), nullString(run.ErrorText)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("record %s run: %w", run.Kind, err)
	}
	return id, nil
}

func (s *Store) ListRuns(ctx context.Context, repoID int64, kind string, limit int) ([]RunRecord, error) {
	table, err := runTable(kind)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		select id, repo_id, scope, status, started_at, finished_at, stats_json, error_text
		from `+table+`
		where repo_id = ?
		order by id desc
		limit ?
	`, repoID, limit)
	if err != nil {
		return nil, fmt.Errorf("list %s runs: %w", kind, err)
	}
	defer rows.Close()

	var out []RunRecord
	for rows.Next() {
		var run RunRecord
		var finishedAt, statsJSON, errorText sql.NullString
		if err := rows.Scan(&run.ID, &run.RepoID, &run.Scope, &run.Status, &run.StartedAt, &finishedAt, &statsJSON, &errorText); err != nil {
			return nil, fmt.Errorf("scan %s run: %w", kind, err)
		}
		run.Kind = kind
		run.FinishedAt = finishedAt.String
		run.StatsJSON = statsJSON.String
		run.ErrorText = errorText.String
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s runs: %w", kind, err)
	}
	return out, nil
}

func runTable(kind string) (string, error) {
	switch kind {
	case "sync":
		return "sync_runs", nil
	case "summary":
		return "summary_runs", nil
	case "embedding":
		return "embedding_runs", nil
	case "cluster":
		return "cluster_runs", nil
	default:
		return "", fmt.Errorf("unsupported run kind %q", kind)
	}
}
