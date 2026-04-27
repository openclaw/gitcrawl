package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

type ClusterSummary struct {
	ID                     int64  `json:"id"`
	StableSlug             string `json:"stable_slug"`
	Status                 string `json:"status"`
	Title                  string `json:"title,omitempty"`
	RepresentativeThreadID int64  `json:"representative_thread_id,omitempty"`
	RepresentativeNumber   int    `json:"representative_number,omitempty"`
	RepresentativeKind     string `json:"representative_kind,omitempty"`
	RepresentativeTitle    string `json:"representative_title,omitempty"`
	MemberCount            int    `json:"member_count"`
	UpdatedAt              string `json:"updated_at"`
	ClosedAt               string `json:"closed_at,omitempty"`
}

type ClusterSummaryOptions struct {
	RepoID        int64
	IncludeClosed bool
	MinSize       int
	Limit         int
	Sort          string
}

type ClusterDetailOptions struct {
	RepoID        int64
	ClusterID     int64
	IncludeClosed bool
	MemberLimit   int
	BodyChars     int
}

type ClusterMemberDetail struct {
	Thread                Thread            `json:"thread"`
	Role                  string            `json:"role"`
	State                 string            `json:"state"`
	ScoreToRepresentative *float64          `json:"score_to_representative,omitempty"`
	BodySnippet           string            `json:"body_snippet,omitempty"`
	Summaries             map[string]string `json:"summaries,omitempty"`
}

type ClusterDetail struct {
	Cluster ClusterSummary        `json:"cluster"`
	Members []ClusterMemberDetail `json:"members"`
}

type ClusterMemberOverride struct {
	ClusterID int64  `json:"cluster_id"`
	ThreadID  int64  `json:"thread_id"`
	Number    int    `json:"number"`
	Action    string `json:"action"`
	Reason    string `json:"reason,omitempty"`
}

type DurableClusterInput struct {
	StableKey              string
	StableSlug             string
	RepresentativeThreadID int64
	Title                  string
	Members                []DurableClusterMemberInput
}

type DurableClusterMemberInput struct {
	ThreadID              int64
	Role                  string
	ScoreToRepresentative *float64
}

type SaveDurableClustersResult struct {
	RunID        int64 `json:"run_id"`
	ClusterCount int   `json:"cluster_count"`
	MemberCount  int   `json:"member_count"`
}

func (s *Store) ListClusterSummaries(ctx context.Context, options ClusterSummaryOptions) ([]ClusterSummary, error) {
	where := `cg.repo_id = ?`
	args := []any{options.RepoID}
	if !options.IncludeClosed {
		where += ` and cg.status = 'active' and cg.closed_at is null`
	}
	orderBy := `coalesce(cg.updated_at, '') desc, cg.id desc`
	if options.Sort == "size" {
		orderBy = `member_count desc, cg.id asc`
	}
	limit := options.Limit
	if limit <= 0 {
		limit = 20
	}
	minSize := options.MinSize
	if minSize <= 0 {
		minSize = 1
	}
	args = append(args, minSize, limit)
	memberThreadJoin := `left join threads mt on mt.id = cm.thread_id`
	if !options.IncludeClosed {
		memberThreadJoin += ` and mt.closed_at_local is null`
	}

	rows, err := s.db.QueryContext(ctx, `
		select cg.id, cg.stable_slug, cg.status, cg.title, cg.representative_thread_id,
			rt.number, rt.kind, rt.title,
			count(mt.id) as member_count,
			cg.updated_at, cg.closed_at
		from cluster_groups cg
		left join cluster_memberships cm on cm.cluster_id = cg.id and cm.state = 'active'
		`+memberThreadJoin+`
		left join threads rt on rt.id = cg.representative_thread_id
		where `+where+`
		group by cg.id
		having member_count >= ?
		order by `+orderBy+`
		limit ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list cluster summaries: %w", err)
	}
	defer rows.Close()

	var out []ClusterSummary
	for rows.Next() {
		var summary ClusterSummary
		var title, closedAt, repKind, repTitle sql.NullString
		var repThreadID sql.NullInt64
		var repNumber sql.NullInt64
		if err := rows.Scan(&summary.ID, &summary.StableSlug, &summary.Status, &title, &repThreadID, &repNumber, &repKind, &repTitle, &summary.MemberCount, &summary.UpdatedAt, &closedAt); err != nil {
			return nil, fmt.Errorf("scan cluster summary: %w", err)
		}
		summary.Title = title.String
		summary.ClosedAt = closedAt.String
		summary.RepresentativeThreadID = repThreadID.Int64
		summary.RepresentativeNumber = int(repNumber.Int64)
		summary.RepresentativeKind = repKind.String
		summary.RepresentativeTitle = repTitle.String
		out = append(out, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cluster summaries: %w", err)
	}
	return out, nil
}

func (s *Store) ClusterDetail(ctx context.Context, options ClusterDetailOptions) (ClusterDetail, error) {
	summary, err := s.clusterSummaryByID(ctx, options.RepoID, options.ClusterID, options.IncludeClosed)
	if err != nil {
		return ClusterDetail{}, err
	}
	limit := options.MemberLimit
	if limit <= 0 {
		limit = 20
	}
	where := `cm.cluster_id = ?`
	args := []any{options.ClusterID}
	if !options.IncludeClosed {
		where += ` and cm.state = 'active' and t.closed_at_local is null`
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		select cm.role, cm.state, cm.score_to_representative,
			t.id, t.repo_id, t.github_id, t.number, t.kind, t.state, t.title, t.body, t.author_login, t.author_type,
			t.html_url, t.labels_json, t.assignees_json, t.raw_json, t.content_hash, t.is_draft,
			t.created_at_gh, t.updated_at_gh, t.closed_at_gh, t.merged_at_gh,
			t.first_pulled_at, t.last_pulled_at, t.updated_at, t.closed_at_local, t.close_reason_local
		from cluster_memberships cm
		join threads t on t.id = cm.thread_id
		where `+where+`
		order by case cm.role when 'canonical' then 0 when 'representative' then 1 else 2 end,
			coalesce(cm.score_to_representative, 0) desc,
			t.number asc
		limit ?
	`, args...)
	if err != nil {
		return ClusterDetail{}, fmt.Errorf("list cluster members: %w", err)
	}
	defer rows.Close()

	members := make([]ClusterMemberDetail, 0, limit)
	threadIDs := make([]int64, 0, limit)
	for rows.Next() {
		member, err := scanClusterMemberDetail(rows, options.BodyChars)
		if err != nil {
			return ClusterDetail{}, err
		}
		threadIDs = append(threadIDs, member.Thread.ID)
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return ClusterDetail{}, fmt.Errorf("iterate cluster members: %w", err)
	}
	summaries, err := s.summariesByThreadIDs(ctx, threadIDs)
	if err != nil {
		return ClusterDetail{}, err
	}
	for index := range members {
		if summaryMap := summaries[members[index].Thread.ID]; len(summaryMap) > 0 {
			members[index].Summaries = summaryMap
		}
	}
	return ClusterDetail{Cluster: summary, Members: members}, nil
}

func (s *Store) ClusterIDForThreadNumber(ctx context.Context, repoID int64, number int, includeClosed bool) (int64, error) {
	where := `t.repo_id = ? and t.number = ?`
	args := []any{repoID, number}
	if !includeClosed {
		where += ` and t.closed_at_local is null and cm.state = 'active' and cg.status = 'active' and cg.closed_at is null`
	}
	row := s.db.QueryRowContext(ctx, `
		select cg.id
		from threads t
		join cluster_memberships cm on cm.thread_id = t.id
		join cluster_groups cg on cg.id = cm.cluster_id
		where `+where+`
		order by case cm.state when 'active' then 0 else 1 end,
			case cg.status when 'active' then 0 else 1 end,
			coalesce(cg.updated_at, '') desc,
			cg.id desc
		limit 1
	`, args...)
	var clusterID int64
	if err := row.Scan(&clusterID); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("thread #%d is not in a cluster", number)
		}
		return 0, fmt.Errorf("find thread cluster: %w", err)
	}
	return clusterID, nil
}

func (s *Store) CloseClusterLocally(ctx context.Context, repoID, clusterID int64, reason string) error {
	if repoID <= 0 {
		return fmt.Errorf("repo id must be positive")
	}
	if clusterID <= 0 {
		return fmt.Errorf("cluster id must be positive")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "local close"
	}
	now := time.Now().UTC().Format(timeLayout)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin close cluster: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		update cluster_groups
		set status = 'closed', closed_at = ?, updated_at = ?
		where repo_id = ? and id = ?
	`, now, now, repoID, clusterID)
	if err != nil {
		return fmt.Errorf("close cluster locally: %w", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("cluster %d was not found", clusterID)
	}
	if _, err := tx.ExecContext(ctx, `
		insert into cluster_closures(cluster_id, reason, actor_kind, created_at, updated_at)
		values(?, ?, 'local', ?, ?)
		on conflict(cluster_id) do update set
			reason = excluded.reason,
			actor_kind = excluded.actor_kind,
			updated_at = excluded.updated_at
	`, clusterID, reason, now, now); err != nil {
		return fmt.Errorf("record cluster closure: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit close cluster: %w", err)
	}
	return nil
}

func (s *Store) ReopenClusterLocally(ctx context.Context, repoID, clusterID int64) error {
	if repoID <= 0 {
		return fmt.Errorf("repo id must be positive")
	}
	if clusterID <= 0 {
		return fmt.Errorf("cluster id must be positive")
	}
	now := time.Now().UTC().Format(timeLayout)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reopen cluster: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		update cluster_groups
		set status = 'active', closed_at = null, updated_at = ?
		where repo_id = ? and id = ?
	`, now, repoID, clusterID)
	if err != nil {
		return fmt.Errorf("reopen cluster locally: %w", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("cluster %d was not found", clusterID)
	}
	if _, err := tx.ExecContext(ctx, `delete from cluster_closures where cluster_id = ?`, clusterID); err != nil {
		return fmt.Errorf("clear cluster closure: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reopen cluster: %w", err)
	}
	return nil
}

func (s *Store) SaveDurableClusters(ctx context.Context, repoID int64, inputs []DurableClusterInput) (SaveDurableClustersResult, error) {
	if repoID <= 0 {
		return SaveDurableClustersResult{}, fmt.Errorf("repo id must be positive")
	}
	now := time.Now().UTC().Format(timeLayout)
	result := SaveDurableClustersResult{ClusterCount: len(inputs)}
	err := s.WithTx(ctx, func(tx *Store) error {
		runID, err := tx.insertClusterRun(ctx, repoID, now)
		if err != nil {
			return err
		}
		result.RunID = runID
		for _, input := range inputs {
			clusterID, err := tx.upsertDurableCluster(ctx, repoID, runID, input, now)
			if err != nil {
				return err
			}
			memberIDs := make([]int64, 0, len(input.Members))
			for _, member := range input.Members {
				if member.ThreadID <= 0 {
					return fmt.Errorf("cluster %q has invalid member thread id", input.StableKey)
				}
				role := strings.TrimSpace(member.Role)
				if role == "" {
					role = "member"
				}
				if _, err := tx.q().ExecContext(ctx, `
					insert into cluster_memberships(
						cluster_id, thread_id, role, state, score_to_representative,
						first_seen_run_id, last_seen_run_id, added_by, added_reason_json, created_at, updated_at
					)
					values(?, ?, ?, 'active', ?, ?, ?, 'cluster', '{}', ?, ?)
					on conflict(cluster_id, thread_id) do update set
						role = excluded.role,
						state = 'active',
						score_to_representative = excluded.score_to_representative,
						last_seen_run_id = excluded.last_seen_run_id,
						removed_by = null,
						removed_reason_json = null,
						removed_at = null,
						updated_at = excluded.updated_at
				`, clusterID, member.ThreadID, role, nullableFloat(member.ScoreToRepresentative), runID, runID, now, now); err != nil {
					return fmt.Errorf("upsert durable cluster member: %w", err)
				}
				memberIDs = append(memberIDs, member.ThreadID)
				result.MemberCount++
			}
			if err := tx.markMissingClusterMembersRemoved(ctx, clusterID, memberIDs, now); err != nil {
				return err
			}
			if err := tx.applyClusterOverrides(ctx, repoID, clusterID, now); err != nil {
				return err
			}
		}
		if _, err := tx.q().ExecContext(ctx, `
			update cluster_runs
			set finished_at = ?, stats_json = ?
			where id = ?
		`, now, fmt.Sprintf(`{"cluster_count":%d,"member_count":%d}`, result.ClusterCount, result.MemberCount), runID); err != nil {
			return fmt.Errorf("finish cluster run: %w", err)
		}
		return nil
	})
	if err != nil {
		return SaveDurableClustersResult{}, err
	}
	return result, nil
}

func (s *Store) ExcludeClusterMemberLocally(ctx context.Context, repoID, clusterID int64, number int, reason string) (ClusterMemberOverride, error) {
	if repoID <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("repo id must be positive")
	}
	if clusterID <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("cluster id must be positive")
	}
	if number <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("thread number must be positive")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "local exclude"
	}
	var result ClusterMemberOverride
	err := s.WithTx(ctx, func(tx *Store) error {
		threadID, err := tx.clusterMemberThreadID(ctx, repoID, clusterID, number, false)
		if err != nil {
			return err
		}
		now := time.Now().UTC().Format(timeLayout)
		reasonJSON, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			return fmt.Errorf("encode override reason: %w", err)
		}
		if _, err := tx.q().ExecContext(ctx, `
			update cluster_memberships
			set state = 'excluded', removed_by = 'local', removed_reason_json = ?, removed_at = ?, updated_at = ?
			where cluster_id = ? and thread_id = ?
		`, string(reasonJSON), now, now, clusterID, threadID); err != nil {
			return fmt.Errorf("exclude cluster member: %w", err)
		}
		if _, err := tx.q().ExecContext(ctx, `delete from cluster_overrides where cluster_id = ? and thread_id = ? and action in ('include', 'canonical')`, clusterID, threadID); err != nil {
			return fmt.Errorf("clear stale member overrides: %w", err)
		}
		if err := tx.upsertClusterOverride(ctx, repoID, clusterID, threadID, "exclude", reason, now); err != nil {
			return err
		}
		if err := tx.ensureActiveClusterRepresentative(ctx, repoID, clusterID, now); err != nil {
			return err
		}
		result = ClusterMemberOverride{ClusterID: clusterID, ThreadID: threadID, Number: number, Action: "exclude", Reason: reason}
		return nil
	})
	if err != nil {
		return ClusterMemberOverride{}, err
	}
	return result, nil
}

func (s *Store) IncludeClusterMemberLocally(ctx context.Context, repoID, clusterID int64, number int, reason string) (ClusterMemberOverride, error) {
	if repoID <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("repo id must be positive")
	}
	if clusterID <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("cluster id must be positive")
	}
	if number <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("thread number must be positive")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "local include"
	}
	var result ClusterMemberOverride
	err := s.WithTx(ctx, func(tx *Store) error {
		threadID, err := tx.clusterMemberThreadID(ctx, repoID, clusterID, number, false)
		if err != nil {
			return err
		}
		now := time.Now().UTC().Format(timeLayout)
		update, err := tx.q().ExecContext(ctx, `
			update cluster_memberships
			set state = 'active', removed_by = null, removed_reason_json = null, removed_at = null, updated_at = ?
			where cluster_id = ? and thread_id = ?
		`, now, clusterID, threadID)
		if err != nil {
			return fmt.Errorf("include cluster member: %w", err)
		}
		if affected, err := update.RowsAffected(); err == nil && affected == 0 {
			return fmt.Errorf("thread #%d is not in cluster %d", number, clusterID)
		}
		if _, err := tx.q().ExecContext(ctx, `delete from cluster_overrides where cluster_id = ? and thread_id = ? and action = 'exclude'`, clusterID, threadID); err != nil {
			return fmt.Errorf("clear exclude override: %w", err)
		}
		if err := tx.upsertClusterOverride(ctx, repoID, clusterID, threadID, "include", reason, now); err != nil {
			return err
		}
		if err := tx.ensureActiveClusterRepresentative(ctx, repoID, clusterID, now); err != nil {
			return err
		}
		result = ClusterMemberOverride{ClusterID: clusterID, ThreadID: threadID, Number: number, Action: "include", Reason: reason}
		return nil
	})
	if err != nil {
		return ClusterMemberOverride{}, err
	}
	return result, nil
}

func (s *Store) SetClusterCanonicalLocally(ctx context.Context, repoID, clusterID int64, number int, reason string) (ClusterMemberOverride, error) {
	if repoID <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("repo id must be positive")
	}
	if clusterID <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("cluster id must be positive")
	}
	if number <= 0 {
		return ClusterMemberOverride{}, fmt.Errorf("thread number must be positive")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "local canonical"
	}
	var result ClusterMemberOverride
	err := s.WithTx(ctx, func(tx *Store) error {
		threadID, err := tx.clusterMemberThreadID(ctx, repoID, clusterID, number, true)
		if err != nil {
			return err
		}
		now := time.Now().UTC().Format(timeLayout)
		if _, err := tx.q().ExecContext(ctx, `
			update cluster_memberships
			set role = case when thread_id = ? then 'canonical' else 'member' end,
				updated_at = ?
			where cluster_id = ? and state = 'active'
		`, threadID, now, clusterID); err != nil {
			return fmt.Errorf("set canonical member roles: %w", err)
		}
		update, err := tx.q().ExecContext(ctx, `
			update cluster_groups
			set representative_thread_id = ?, updated_at = ?
			where repo_id = ? and id = ?
		`, threadID, now, repoID, clusterID)
		if err != nil {
			return fmt.Errorf("set cluster canonical: %w", err)
		}
		if affected, err := update.RowsAffected(); err == nil && affected == 0 {
			return fmt.Errorf("cluster %d was not found", clusterID)
		}
		if _, err := tx.q().ExecContext(ctx, `delete from cluster_overrides where cluster_id = ? and action = 'canonical'`, clusterID); err != nil {
			return fmt.Errorf("clear canonical overrides: %w", err)
		}
		if _, err := tx.q().ExecContext(ctx, `delete from cluster_overrides where cluster_id = ? and thread_id = ? and action = 'exclude'`, clusterID, threadID); err != nil {
			return fmt.Errorf("clear exclude override: %w", err)
		}
		if err := tx.upsertClusterOverride(ctx, repoID, clusterID, threadID, "canonical", reason, now); err != nil {
			return err
		}
		result = ClusterMemberOverride{ClusterID: clusterID, ThreadID: threadID, Number: number, Action: "canonical", Reason: reason}
		return nil
	})
	if err != nil {
		return ClusterMemberOverride{}, err
	}
	return result, nil
}

func (s *Store) clusterMemberThreadID(ctx context.Context, repoID, clusterID int64, number int, requireActive bool) (int64, error) {
	where := `cg.repo_id = ? and cg.id = ? and t.repo_id = ? and t.number = ?`
	if requireActive {
		where += ` and cm.state = 'active'`
	}
	row := s.q().QueryRowContext(ctx, `
		select t.id
		from cluster_groups cg
		join cluster_memberships cm on cm.cluster_id = cg.id
		join threads t on t.id = cm.thread_id
		where `+where+`
		limit 1
	`, repoID, clusterID, repoID, number)
	var threadID int64
	if err := row.Scan(&threadID); err != nil {
		if err == sql.ErrNoRows {
			if requireActive {
				return 0, fmt.Errorf("active thread #%d is not in cluster %d", number, clusterID)
			}
			return 0, fmt.Errorf("thread #%d is not in cluster %d", number, clusterID)
		}
		return 0, fmt.Errorf("find cluster member: %w", err)
	}
	return threadID, nil
}

func (s *Store) insertClusterRun(ctx context.Context, repoID int64, now string) (int64, error) {
	var runID int64
	if err := s.q().QueryRowContext(ctx, `
		insert into cluster_runs(repo_id, scope, status, started_at)
		values(?, 'durable', 'success', ?)
		returning id
	`, repoID, now).Scan(&runID); err != nil {
		return 0, fmt.Errorf("insert cluster run: %w", err)
	}
	return runID, nil
}

func (s *Store) upsertDurableCluster(ctx context.Context, repoID, runID int64, input DurableClusterInput, now string) (int64, error) {
	stableKey := strings.TrimSpace(input.StableKey)
	if stableKey == "" {
		return 0, fmt.Errorf("durable cluster stable key is required")
	}
	stableSlug := strings.TrimSpace(input.StableSlug)
	if stableSlug == "" {
		stableSlug = stableKey
	}
	var clusterID int64
	if err := s.q().QueryRowContext(ctx, `
		insert into cluster_groups(
			repo_id, stable_key, stable_slug, status, cluster_type, representative_thread_id, title, created_at, updated_at
		)
		values(?, ?, ?, 'active', 'similarity', ?, ?, ?, ?)
		on conflict(repo_id, stable_key) do update set
			stable_slug = excluded.stable_slug,
			cluster_type = excluded.cluster_type,
			representative_thread_id = case
				when cluster_groups.status = 'closed' then cluster_groups.representative_thread_id
				else excluded.representative_thread_id
			end,
			title = excluded.title,
			updated_at = excluded.updated_at
		returning id
	`, repoID, stableKey, stableSlug, nullInt(input.RepresentativeThreadID), nullString(input.Title), now, now).Scan(&clusterID); err != nil {
		return 0, fmt.Errorf("upsert durable cluster: %w", err)
	}
	if _, err := s.q().ExecContext(ctx, `
		insert into cluster_events(cluster_id, run_id, event_type, actor_kind, payload_json, created_at)
		values(?, ?, 'seen', 'cluster', '{}', ?)
	`, clusterID, runID, now); err != nil {
		return 0, fmt.Errorf("record durable cluster event: %w", err)
	}
	return clusterID, nil
}

func (s *Store) markMissingClusterMembersRemoved(ctx context.Context, clusterID int64, memberIDs []int64, now string) error {
	if len(memberIDs) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(memberIDs))
	args := []any{`{"reason":"not seen in latest cluster run"}`, now, now, clusterID}
	for _, id := range memberIDs {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	if _, err := s.q().ExecContext(ctx, `
		update cluster_memberships
		set state = 'removed',
			removed_by = 'cluster',
			removed_reason_json = ?,
			removed_at = ?,
			updated_at = ?
		where cluster_id = ?
			and thread_id not in (`+strings.Join(placeholders, ",")+`)
			and state = 'active'
	`, args...); err != nil {
		return fmt.Errorf("mark missing cluster members removed: %w", err)
	}
	return nil
}

func (s *Store) upsertClusterOverride(ctx context.Context, repoID, clusterID, threadID int64, action, reason, now string) error {
	if _, err := s.q().ExecContext(ctx, `
		insert into cluster_overrides(repo_id, cluster_id, thread_id, action, reason, created_at)
		values(?, ?, ?, ?, ?, ?)
		on conflict(cluster_id, thread_id, action) do update set
			reason = excluded.reason,
			created_at = excluded.created_at
	`, repoID, clusterID, threadID, action, reason, now); err != nil {
		return fmt.Errorf("record cluster override: %w", err)
	}
	return nil
}

func (s *Store) applyClusterOverrides(ctx context.Context, repoID, clusterID int64, now string) error {
	if _, err := s.q().ExecContext(ctx, `
		update cluster_memberships
		set state = 'excluded',
			removed_by = 'local',
			removed_reason_json = coalesce(removed_reason_json, '{"reason":"local override"}'),
			removed_at = coalesce(removed_at, ?),
			updated_at = ?
		where cluster_id = ?
			and thread_id in (select thread_id from cluster_overrides where repo_id = ? and cluster_id = ? and action = 'exclude')
	`, now, now, clusterID, repoID, clusterID); err != nil {
		return fmt.Errorf("apply exclude overrides: %w", err)
	}
	if _, err := s.q().ExecContext(ctx, `
		update cluster_memberships
		set state = 'active',
			removed_by = null,
			removed_reason_json = null,
			removed_at = null,
			updated_at = ?
		where cluster_id = ?
			and thread_id in (select thread_id from cluster_overrides where repo_id = ? and cluster_id = ? and action = 'include')
	`, now, clusterID, repoID, clusterID); err != nil {
		return fmt.Errorf("apply include overrides: %w", err)
	}
	var canonicalThreadID sql.NullInt64
	err := s.q().QueryRowContext(ctx, `
		select thread_id
		from cluster_overrides
		where repo_id = ? and cluster_id = ? and action = 'canonical'
		order by created_at desc, id desc
		limit 1
	`, repoID, clusterID).Scan(&canonicalThreadID)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("find canonical override: %w", err)
	}
	if canonicalThreadID.Valid {
		if _, err := s.q().ExecContext(ctx, `
			update cluster_memberships
			set role = case when thread_id = ? then 'canonical' else 'member' end,
				updated_at = ?
			where cluster_id = ? and state = 'active'
		`, canonicalThreadID.Int64, now, clusterID); err != nil {
			return fmt.Errorf("apply canonical override role: %w", err)
		}
		if _, err := s.q().ExecContext(ctx, `
			update cluster_groups
			set representative_thread_id = ?, updated_at = ?
			where repo_id = ? and id = ?
		`, canonicalThreadID.Int64, now, repoID, clusterID); err != nil {
			return fmt.Errorf("apply canonical override representative: %w", err)
		}
		return nil
	}
	return s.ensureActiveClusterRepresentative(ctx, repoID, clusterID, now)
}

func (s *Store) ensureActiveClusterRepresentative(ctx context.Context, repoID, clusterID int64, now string) error {
	if _, err := s.q().ExecContext(ctx, `
		update cluster_groups
		set representative_thread_id = (
				select cm.thread_id
				from cluster_memberships cm
				join threads t on t.id = cm.thread_id
				where cm.cluster_id = cluster_groups.id and cm.state = 'active'
				order by case cm.role when 'canonical' then 0 when 'representative' then 1 else 2 end,
					coalesce(cm.score_to_representative, 0) desc,
					t.number asc
				limit 1
			),
			updated_at = ?
		where repo_id = ? and id = ?
			and (
				representative_thread_id is null
				or representative_thread_id not in (
					select thread_id from cluster_memberships where cluster_id = ? and state = 'active'
				)
			)
	`, now, repoID, clusterID, clusterID); err != nil {
		return fmt.Errorf("refresh cluster representative: %w", err)
	}
	return nil
}

func nullInt(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: value != 0}
}

func nullableFloat(value *float64) sql.NullFloat64 {
	if value == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *value, Valid: true}
}

func (s *Store) clusterSummaryByID(ctx context.Context, repoID, clusterID int64, includeClosed bool) (ClusterSummary, error) {
	where := `cg.repo_id = ? and cg.id = ?`
	args := []any{repoID, clusterID}
	if !includeClosed {
		where += ` and cg.status = 'active' and cg.closed_at is null`
	}
	row := s.db.QueryRowContext(ctx, `
		select cg.id, cg.stable_slug, cg.status, cg.title, cg.representative_thread_id,
			rt.number, rt.kind, rt.title,
			count(cm.thread_id) as member_count,
			cg.updated_at, cg.closed_at
		from cluster_groups cg
		left join cluster_memberships cm on cm.cluster_id = cg.id and cm.state = 'active'
		left join threads rt on rt.id = cg.representative_thread_id
		where `+where+`
		group by cg.id
	`, args...)
	var summary ClusterSummary
	var title, closedAt, repKind, repTitle sql.NullString
	var repThreadID sql.NullInt64
	var repNumber sql.NullInt64
	if err := row.Scan(&summary.ID, &summary.StableSlug, &summary.Status, &title, &repThreadID, &repNumber, &repKind, &repTitle, &summary.MemberCount, &summary.UpdatedAt, &closedAt); err != nil {
		if err == sql.ErrNoRows {
			return ClusterSummary{}, fmt.Errorf("cluster %d was not found", clusterID)
		}
		return ClusterSummary{}, fmt.Errorf("scan cluster summary: %w", err)
	}
	summary.Title = title.String
	summary.ClosedAt = closedAt.String
	summary.RepresentativeThreadID = repThreadID.Int64
	summary.RepresentativeNumber = int(repNumber.Int64)
	summary.RepresentativeKind = repKind.String
	summary.RepresentativeTitle = repTitle.String
	return summary, nil
}

func scanClusterMemberDetail(row interface {
	Scan(dest ...any) error
}, bodyChars int) (ClusterMemberDetail, error) {
	var member ClusterMemberDetail
	var score sql.NullFloat64
	var body, authorLogin, authorType, rawJSON, createdAt, updatedAtGH, closedAt, mergedAt, firstPulled, lastPulled, closedLocal, closeReason sql.NullString
	var isDraft int
	if err := row.Scan(&member.Role, &member.State, &score,
		&member.Thread.ID, &member.Thread.RepoID, &member.Thread.GitHubID, &member.Thread.Number, &member.Thread.Kind, &member.Thread.State, &member.Thread.Title,
		&body, &authorLogin, &authorType, &member.Thread.HTMLURL, &member.Thread.LabelsJSON, &member.Thread.AssigneesJSON, &rawJSON,
		&member.Thread.ContentHash, &isDraft, &createdAt, &updatedAtGH, &closedAt, &mergedAt, &firstPulled, &lastPulled, &member.Thread.UpdatedAt,
		&closedLocal, &closeReason); err != nil {
		return ClusterMemberDetail{}, fmt.Errorf("scan cluster member: %w", err)
	}
	if score.Valid {
		value := score.Float64
		member.ScoreToRepresentative = &value
	}
	member.Thread.Body = ""
	member.Thread.AuthorLogin = authorLogin.String
	member.Thread.AuthorType = authorType.String
	member.Thread.CreatedAtGitHub = createdAt.String
	member.Thread.UpdatedAtGitHub = updatedAtGH.String
	member.Thread.ClosedAtGitHub = closedAt.String
	member.Thread.MergedAtGitHub = mergedAt.String
	member.Thread.FirstPulledAt = firstPulled.String
	member.Thread.LastPulledAt = lastPulled.String
	member.Thread.ClosedAtLocal = closedLocal.String
	member.Thread.CloseReasonLocal = closeReason.String
	member.Thread.RawJSON = rawJSON.String
	member.Thread.IsDraft = isDraft != 0
	member.BodySnippet = snippetRunes(body.String, bodyChars)
	return member, nil
}

func (s *Store) summariesByThreadIDs(ctx context.Context, threadIDs []int64) (map[int64]map[string]string, error) {
	if len(threadIDs) == 0 {
		return map[int64]map[string]string{}, nil
	}
	placeholders := make([]string, 0, len(threadIDs))
	args := make([]any, 0, len(threadIDs))
	for _, threadID := range threadIDs {
		placeholders = append(placeholders, "?")
		args = append(args, threadID)
	}
	rows, err := s.db.QueryContext(ctx, `
		select thread_id, summary_kind, summary_text
		from document_summaries
		where thread_id in (`+strings.Join(placeholders, ",")+`)
		order by thread_id, summary_kind, updated_at desc
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("select document summaries: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]map[string]string)
	for rows.Next() {
		var threadID int64
		var kind, text string
		if err := rows.Scan(&threadID, &kind, &text); err != nil {
			return nil, fmt.Errorf("scan document summary: %w", err)
		}
		if out[threadID] == nil {
			out[threadID] = map[string]string{}
		}
		if _, exists := out[threadID][kind]; !exists {
			out[threadID][kind] = text
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate document summaries: %w", err)
	}
	return out, nil
}

func snippetRunes(value string, limit int) string {
	if limit <= 0 || value == "" {
		return ""
	}
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit])
}
