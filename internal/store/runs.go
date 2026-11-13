package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

const octopoolSyncProfile = "gitcrawl-octopool-comments/v1"

var ErrOctopoolSyncCheckpoint = errors.New("incremental sync requires a qualified Octopool all-state comments checkpoint; bootstrap with an explicit --since cutoff")

var ErrOctopoolSyncIdentity = errors.New("incremental repository identity changed; use a separately qualified native source, because --since cannot reset the existing checkpoint")

// OctopoolSyncCheckpoint covers REST discovery and selected comment hydration
// since CoverageSince. It is not a full-history or PR-detail coverage claim.
type OctopoolSyncCheckpoint struct {
	Profile         string `json:"profile"`
	RepositoryID    int64  `json:"repository_id"`
	GitHubRepoID    string `json:"github_repo_id"`
	Transport       string `json:"transport"`
	State           string `json:"state"`
	IncludeComments bool   `json:"include_comments"`
	RequestSince    string `json:"request_since"`
	CoverageSince   string `json:"coverage_since"`
	Through         string `json:"through"`
}

// ReadOctopoolSyncCheckpoint ignores legacy/manual run statistics without an
// explicit qualification. A malformed qualification fails rather than falling
// back to an older checkpoint or a sliding time window.
func (s *Store) ReadOctopoolSyncCheckpoint(ctx context.Context, repoID int64) (*OctopoolSyncCheckpoint, error) {
	var raw, started, finished string
	err := s.q().QueryRowContext(ctx, `
select stats_json,started_at,finished_at from sync_runs
where repo_id=? and scope='all' and status='success'
  and json_type(case when json_valid(stats_json) then stats_json else '{}' end,
                '$.octopool_checkpoint') is not null
order by id desc limit 1`, repoID).Scan(&raw, &started, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var stats struct {
		Checkpoint     *OctopoolSyncCheckpoint `json:"octopool_checkpoint"`
		RequestedSince string                  `json:"requested_since"`
		Limit          int                     `json:"limit"`
		Numbers        []int                   `json:"numbers"`
		MetadataOnly   bool                    `json:"metadata_only"`
	}
	if json.Unmarshal([]byte(raw), &stats) != nil || stats.Checkpoint == nil {
		return nil, ErrOctopoolSyncCheckpoint
	}
	c := stats.Checkpoint
	providerID, providerErr := strconv.ParseInt(c.GitHubRepoID, 10, 64)
	lower, e1 := time.Parse(time.RFC3339Nano, c.CoverageSince)
	request, e2 := time.Parse(time.RFC3339Nano, c.RequestSince)
	through, e3 := time.Parse(time.RFC3339Nano, c.Through)
	start, e4 := time.Parse(time.RFC3339Nano, started)
	finish, e5 := time.Parse(time.RFC3339Nano, finished)
	if c.Profile != octopoolSyncProfile || c.RepositoryID != repoID || providerErr != nil || providerID <= 0 || strconv.FormatInt(providerID, 10) != c.GitHubRepoID || c.Transport != "octopool" || c.State != "all" || !c.IncludeComments || stats.MetadataOnly || stats.Limit != 0 || len(stats.Numbers) != 0 || stats.RequestedSince != c.RequestSince || e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || lower.After(through) || request.After(through) || !through.Equal(start) || finish.Before(through) {
		return nil, ErrOctopoolSyncCheckpoint
	}
	return c, nil
}

// QualifyOctopoolSyncCheckpoint is called in the final successful run
// transaction, after full pagination and all required hydration have succeeded.
// A disjoint manual interval remains a successful run but cannot skip a gap.
func (s *Store) QualifyOctopoolSyncCheckpoint(ctx context.Context, repoID int64, githubRepoID, requestSince string, startedAt time.Time) (*OctopoolSyncCheckpoint, error) {
	request, err := time.Parse(time.RFC3339Nano, requestSince)
	providerID, providerErr := strconv.ParseInt(githubRepoID, 10, 64)
	if err != nil || request.After(startedAt) || providerErr != nil || providerID <= 0 || strconv.FormatInt(providerID, 10) != githubRepoID {
		return nil, nil
	}
	previous, err := s.ReadOctopoolSyncCheckpoint(ctx, repoID)
	if err != nil {
		return nil, err
	}
	coverage := requestSince
	if previous != nil {
		through, _ := time.Parse(time.RFC3339Nano, previous.Through)
		if previous.GitHubRepoID != githubRepoID || request.After(through) || startedAt.Before(through) {
			return nil, nil
		}
		coverage = previous.CoverageSince
	}
	return &OctopoolSyncCheckpoint{Profile: octopoolSyncProfile, RepositoryID: repoID, GitHubRepoID: githubRepoID, Transport: "octopool", State: "all", IncludeComments: true, RequestSince: requestSince, CoverageSince: coverage, Through: startedAt.UTC().Format(time.RFC3339Nano)}, nil
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
			out = append(out, runRecordFromDB(kind, storedb.SyncRun(row)))
		}
		return out, nil
	case "embedding":
		rows, err := s.qsql().ListEmbeddingRuns(ctx, storedb.ListEmbeddingRunsParams(params))
		if err != nil {
			return nil, err
		}
		out := make([]RunRecord, 0, len(rows))
		for _, row := range rows {
			out = append(out, runRecordFromDB(kind, storedb.SyncRun(row)))
		}
		return out, nil
	case "cluster":
		rows, err := s.qsql().ListClusterRuns(ctx, storedb.ListClusterRunsParams(params))
		if err != nil {
			return nil, err
		}
		out := make([]RunRecord, 0, len(rows))
		for _, row := range rows {
			out = append(out, runRecordFromDB(kind, storedb.SyncRun(row)))
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

const recordedClosedSweepWatermarkSQL = `
	select json_extract(stats_json, '$.closed_sweep_through')
	from sync_runs
	where repo_id = ? and status in ('success', 'completed', 'checkpoint')
	  and scope in ('open', 'closed', 'all')
	  and json_valid(stats_json)
	  and json_type(case when json_valid(stats_json) then stats_json else '{}' end,
	                '$.closed_sweep_through') = 'text'
	order by julianday(json_extract(stats_json, '$.closed_sweep_through')) desc, id desc
	limit 1
`

// PreserveClosedSweepWatermark freezes the legacy lower bound before partial
// commits can close or refresh its oldest open thread. A checkpoint is not a
// successful sync and cannot certify list freshness or archive completeness.
// The caller owns the transaction that precedes thread mutations.
func (s *Store) PreserveClosedSweepWatermark(ctx context.Context, repoID int64, startedAt time.Time) error {
	var recorded string
	err := s.q().QueryRowContext(ctx, recordedClosedSweepWatermarkSQL, repoID).Scan(&recorded)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	watermark, err := s.ClosedSweepWatermark(ctx, repoID)
	if err != nil {
		return err
	}
	if watermark.IsZero() {
		watermark = startedAt.Add(-24 * time.Hour)
	}
	stats, err := json.Marshal(map[string]string{"closed_sweep_through": watermark.Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	_, err = s.RecordRun(ctx, RunRecord{
		RepoID: repoID, Kind: "sync", Scope: "open", Status: "checkpoint",
		StartedAt:  startedAt.Format(time.RFC3339Nano),
		FinishedAt: startedAt.Format(time.RFC3339Nano), StatsJSON: string(stats),
	})
	return err
}

// ClosedSweepWatermark ignores incomplete runs. A legacy checkpoint retains
// only the old retry lower bound; complete default syncs advance it.
func (s *Store) ClosedSweepWatermark(ctx context.Context, repoID int64) (time.Time, error) {
	var raw sql.NullString
	err := s.q().QueryRowContext(ctx, `
		select coalesce(
			(`+recordedClosedSweepWatermarkSQL+`),
			(select min(coalesce(nullif(last_pulled_at, ''), nullif(first_pulled_at, ''),
			                     nullif(updated_at_gh, ''), updated_at))
			 from threads where repo_id = ? and state = 'open')
		)
	`, repoID, repoID).Scan(&raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("read closed sweep watermark: %w", err)
	}
	if !raw.Valid || raw.String == "" {
		return time.Time{}, nil
	}
	value, err := time.Parse(time.RFC3339Nano, raw.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse closed sweep watermark: %w", err)
	}
	return value, nil
}

// StateAsOf returns the start of the newest successful complete open or all
// list sync. This is a sweep watermark, not proof of a historical backfill:
// legacy closed rows may predate sweep coverage. Targeted, limited, failed,
// or since-bounded syncs do not move it. It is zero without a qualifying run.
func (s *Store) StateAsOf(ctx context.Context, repoID int64) (time.Time, error) {
	if !s.hasColumns(ctx, "sync_runs", "repo_id", "scope", "status", "stats_json") {
		return time.Time{}, nil
	}
	var raw string
	err := s.q().QueryRowContext(ctx, `
		select json_extract(stats_json, '$.closed_sweep_through')
		from sync_runs
		where repo_id = ? and status in ('success', 'completed')
		  and scope in ('open', 'all')
		  and json_valid(stats_json)
		  and json_type(case when json_valid(stats_json) then stats_json else '{}' end,
		                '$.closed_sweep_through') = 'text'
		order by julianday(json_extract(stats_json, '$.closed_sweep_through')) desc, id desc
		limit 1
	`, repoID).Scan(&raw)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("read state as of: %w", err)
	}
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse state as of %q: %w", raw, err)
	}
	return value, nil
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

// SuccessfulListSyncRuns returns complete non-targeted list runs from newest
// to oldest for the requested state coverage.
func (s *Store) SuccessfulListSyncRuns(
	ctx context.Context,
	repoID int64,
	state string,
) ([]RunRecord, error) {
	state = normalizedListSyncState(state)
	if state == "" {
		return nil, fmt.Errorf("unsupported list sync state")
	}
	rows, err := s.q().QueryContext(ctx, `
		select id, repo_id, scope, status, started_at, finished_at,
			stats_json, error_text
		from sync_runs
		where repo_id = ?
			and status in ('success', 'completed')
			and (
				(? = 'open' and scope in ('open', 'all')) or
				(? = 'closed' and scope in ('closed', 'all')) or
				(? = 'all' and scope = 'all')
			)
		order by finished_at desc, id desc
	`, repoID, state, state, state)
	if err != nil {
		return nil, fmt.Errorf("list successful list sync runs: %w", err)
	}
	defer rows.Close()
	var runs []RunRecord
	for rows.Next() {
		var run RunRecord
		var finishedAt, statsJSON, errorText sql.NullString
		if err := rows.Scan(
			&run.ID,
			&run.RepoID,
			&run.Scope,
			&run.Status,
			&run.StartedAt,
			&finishedAt,
			&statsJSON,
			&errorText,
		); err != nil {
			return nil, fmt.Errorf("scan successful list sync run: %w", err)
		}
		run.Kind = "sync"
		run.FinishedAt = finishedAt.String
		run.StatsJSON = statsJSON.String
		run.ErrorText = errorText.String
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate successful list sync runs: %w", err)
	}
	return runs, nil
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
