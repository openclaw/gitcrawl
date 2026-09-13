package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type ClusterMemberOverride struct {
	ClusterID int64  `json:"cluster_id"`
	ThreadID  int64  `json:"thread_id"`
	Number    int    `json:"number"`
	Action    string `json:"action"`
	Reason    string `json:"reason,omitempty"`
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
			set representative_thread_id = null, updated_at = ?
			where repo_id = ? and id = ?
				and status = 'active'
				and closed_at is null
		`, now, repoID, clusterID); err != nil {
			return fmt.Errorf("apply canonical override representative: %w", err)
		}
	}
	return s.ensureActiveClusterRepresentative(ctx, repoID, clusterID, now)
}

func (s *Store) ensureActiveClusterRepresentative(ctx context.Context, repoID, clusterID int64, now string) error {
	visible := durableVisibleMemberPredicate("cluster_groups", "cm", "t")
	if _, err := s.q().ExecContext(ctx, `
		update cluster_groups
		set representative_thread_id = (
				select cm.thread_id
				from cluster_memberships cm
				join threads t on t.id = cm.thread_id
				where cm.cluster_id = cluster_groups.id and cm.state = 'active'
					and `+visible+`
				order by case cm.role when 'canonical' then 0 when 'representative' then 1 else 2 end,
					coalesce(cm.score_to_representative, 0) desc,
					t.number asc
				limit 1
			),
			updated_at = ?
		where repo_id = ? and id = ?
			and status = 'active'
			and closed_at is null
			and (
				representative_thread_id is null
				or representative_thread_id not in (
					select cm.thread_id
					from cluster_memberships cm
					join threads t on t.id = cm.thread_id
					where cm.cluster_id = ? and cm.state = 'active'
						and `+visible+`
				)
			)
	`, now, repoID, clusterID, clusterID); err != nil {
		return fmt.Errorf("refresh cluster representative: %w", err)
	}
	return nil
}
