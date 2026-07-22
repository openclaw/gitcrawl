package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
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
	params := storedb.RecordSyncRunParams{
		RepoID:     run.RepoID,
		Scope:      run.Scope,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		FinishedAt: nullString(run.FinishedAt),
		StatsJson:  nullString(run.StatsJSON),
		ErrorText:  nullString(run.ErrorText),
	}
	id, err := s.recordRun(ctx, run.Kind, params)
	if err != nil {
		return 0, fmt.Errorf("record %s run: %w", run.Kind, err)
	}
	return id, nil
}

func (s *Store) recordRun(ctx context.Context, kind string, params storedb.RecordSyncRunParams) (int64, error) {
	switch kind {
	case "sync":
		return s.qsql().RecordSyncRun(ctx, params)
	case "summary":
		return s.qsql().RecordSummaryRun(ctx, storedb.RecordSummaryRunParams(params))
	case "embedding":
		return s.qsql().RecordEmbeddingRun(ctx, storedb.RecordEmbeddingRunParams(params))
	case "cluster":
		return s.qsql().RecordClusterRun(ctx, storedb.RecordClusterRunParams(params))
	default:
		return 0, fmt.Errorf("unsupported run kind %q", kind)
	}
}

func (s *Store) ListRuns(ctx context.Context, repoID int64, kind string, limit int) ([]RunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	out, err := s.listRuns(ctx, repoID, kind, limit)
	if err != nil {
		return nil, fmt.Errorf("list %s runs: %w", kind, err)
	}
	return out, nil
}

func (s *Store) listRuns(ctx context.Context, repoID int64, kind string, limit int) ([]RunRecord, error) {
	params := storedb.ListSyncRunsParams{RepoID: repoID, RowLimit: int64(limit)}
	switch kind {
	case "sync":
		rows, err := s.qsql().ListSyncRuns(ctx, params)
		if err != nil {
			return nil, err
		}
		out := make([]RunRecord, 0, len(rows))
		for _, row := range rows {
			out = append(out, runRecordFromDB(kind, row))
		}
		return out, nil
	case "summary":
		rows, err := s.qsql().ListSummaryRuns(ctx, storedb.ListSummaryRunsParams(params))
		if err != nil {
			return nil, err
		}
		out := make([]RunRecord, 0, len(rows))
		for _, row := range rows {
			out = append(out, summaryRunRecordFromDB(kind, row))
		}
		return out, nil
	case "embedding":
		rows, err := s.qsql().ListEmbeddingRuns(ctx, storedb.ListEmbeddingRunsParams(params))
		if err != nil {
			return nil, err
		}
		out := make([]RunRecord, 0, len(rows))
		for _, row := range rows {
			out = append(out, embeddingRunRecordFromDB(kind, row))
		}
		return out, nil
	case "cluster":
		rows, err := s.qsql().ListClusterRuns(ctx, storedb.ListClusterRunsParams(params))
		if err != nil {
			return nil, err
		}
		out := make([]RunRecord, 0, len(rows))
		for _, row := range rows {
			out = append(out, clusterRunRecordFromDB(kind, row))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported run kind %q", kind)
	}
}

func (s *Store) LastSuccessfulSyncAt(ctx context.Context, repoID int64) (time.Time, error) {
	lastSync, err := s.qsql().LastSuccessfulSyncAt(ctx, repoID)
	if err != nil {
		return time.Time{}, fmt.Errorf("read last successful sync: %w", err)
	}
	if lastSync == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, lastSync)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse last successful sync %q: %w", lastSync, err)
	}
	return parsed, nil
}

func (s *Store) LastSuccessfulListSyncAt(ctx context.Context, repoID int64, state string) (time.Time, error) {
	state = normalizedListSyncState(state)
	if state == "" || !s.hasColumns(ctx, "sync_runs", "repo_id", "scope", "status", "finished_at") {
		return time.Time{}, nil
	}
	lastSync, err := s.qsql().LastSuccessfulListSyncAt(ctx, storedb.LastSuccessfulListSyncAtParams{
		RepoID: repoID,
		State:  state,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("read last successful list sync: %w", err)
	}
	if lastSync == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, lastSync)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse last successful list sync %q: %w", lastSync, err)
	}
	return parsed, nil
}

func normalizedListSyncState(state string) string {
	switch strings.TrimSpace(strings.ToLower(state)) {
	case "", "open":
		return "open"
	case "closed":
		return "closed"
	case "all":
		return "all"
	default:
		return ""
	}
}
