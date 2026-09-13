package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type DurableClusterInput struct {
	StableKey              string
	StableSlug             string
	ClusterType            string
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

func (s *Store) SaveDurableClusters(ctx context.Context, repoID int64, inputs []DurableClusterInput) (SaveDurableClustersResult, error) {
	return s.saveDurableClusters(ctx, repoID, inputs, true, false)
}

func (s *Store) SavePartialDurableClusters(ctx context.Context, repoID int64, inputs []DurableClusterInput) (SaveDurableClustersResult, error) {
	return s.saveDurableClusters(ctx, repoID, inputs, false, false)
}

func (s *Store) SaveCompleteDurableClusters(ctx context.Context, repoID int64, inputs []DurableClusterInput) (SaveDurableClustersResult, error) {
	return s.saveDurableClusters(ctx, repoID, inputs, true, true)
}

func (s *Store) saveDurableClusters(ctx context.Context, repoID int64, inputs []DurableClusterInput, pruneMissingMembers, retireMissing bool) (SaveDurableClustersResult, error) {
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
		seenClusterIDs := make([]int64, 0, len(inputs))
		for _, input := range inputs {
			if len(input.Members) == 0 {
				return fmt.Errorf("durable cluster %q has no members", strings.TrimSpace(input.StableKey))
			}
			if !pruneMissingMembers && !retireMissing {
				input, err = tx.reconcilePartialDurableClusterInput(ctx, repoID, input)
				if err != nil {
					return err
				}
			}
			clusterID, err := tx.upsertDurableCluster(ctx, repoID, runID, input, now)
			if err != nil {
				return err
			}
			seenClusterIDs = append(seenClusterIDs, clusterID)
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
			if pruneMissingMembers {
				if err := tx.markMissingClusterMembersRemoved(ctx, clusterID, memberIDs, now); err != nil {
					return err
				}
			}
			if err := tx.applyClusterOverrides(ctx, repoID, clusterID, now); err != nil {
				return err
			}
		}
		if retireMissing {
			if err := tx.markMissingDurableClustersRetired(ctx, repoID, runID, seenClusterIDs, now); err != nil {
				return err
			}
		}
		if len(inputs) > 0 {
			if _, err := tx.q().ExecContext(ctx, `
					delete from cluster_groups
				where repo_id = ?
				  and cluster_type = 'similarity'
			`, repoID); err != nil {
				return fmt.Errorf("delete legacy similarity clusters: %w", err)
			}
			for _, clusterID := range seenClusterIDs {
				if err := tx.ensureActiveClusterRepresentative(ctx, repoID, clusterID, now); err != nil {
					return err
				}
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

func (s *Store) reconcilePartialDurableClusterInput(ctx context.Context, repoID int64, input DurableClusterInput) (DurableClusterInput, error) {
	placeholders := make([]string, 0, len(input.Members))
	args := []any{repoID}
	for _, member := range input.Members {
		if member.ThreadID <= 0 {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, member.ThreadID)
	}
	if len(placeholders) == 0 {
		return input, nil
	}
	rows, err := s.q().QueryContext(ctx, `
		select distinct cg.id, cg.stable_key, cg.stable_slug, coalesce(cg.cluster_type, ''),
			coalesce(cg.representative_thread_id, 0), coalesce(cg.title, '')
		from cluster_groups cg
		join cluster_memberships cm on cm.cluster_id = cg.id
		where cg.repo_id = ?
			and cg.status = 'active'
			and cg.closed_at is null
			and coalesce(cg.cluster_type, '') <> 'similarity'
			and cm.state = 'active'
			and cm.thread_id in (`+strings.Join(placeholders, ",")+`)
		order by cg.id
	`, args...)
	if err != nil {
		return DurableClusterInput{}, fmt.Errorf("find partial durable cluster identity: %w", err)
	}
	defer rows.Close()

	type clusterIdentity struct {
		id                     int64
		stableKey              string
		stableSlug             string
		clusterType            string
		representativeThreadID int64
		title                  string
	}
	var identities []clusterIdentity
	for rows.Next() {
		var identity clusterIdentity
		if err := rows.Scan(
			&identity.id,
			&identity.stableKey,
			&identity.stableSlug,
			&identity.clusterType,
			&identity.representativeThreadID,
			&identity.title,
		); err != nil {
			return DurableClusterInput{}, fmt.Errorf("scan partial durable cluster identity: %w", err)
		}
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return DurableClusterInput{}, fmt.Errorf("iterate partial durable cluster identities: %w", err)
	}
	if len(identities) == 0 {
		return input, nil
	}
	if len(identities) > 1 {
		return DurableClusterInput{}, fmt.Errorf(
			"partial durable cluster %q overlaps multiple active identities (%d and %d)",
			strings.TrimSpace(input.StableKey),
			identities[0].id,
			identities[1].id,
		)
	}

	identity := identities[0]
	representativeChanged := identity.representativeThreadID != 0 &&
		identity.representativeThreadID != input.RepresentativeThreadID
	input.StableKey = identity.stableKey
	input.StableSlug = identity.stableSlug
	input.ClusterType = identity.clusterType
	input.Title = identity.title
	if identity.representativeThreadID != 0 {
		input.RepresentativeThreadID = identity.representativeThreadID
	}
	input.Members = append([]DurableClusterMemberInput(nil), input.Members...)
	for index := range input.Members {
		member := &input.Members[index]
		if member.ThreadID == input.RepresentativeThreadID {
			member.Role = "canonical"
			score := 1.0
			member.ScoreToRepresentative = &score
			continue
		}
		if representativeChanged {
			if member.Role == "canonical" || member.Role == "representative" {
				member.Role = "related"
			}
			member.ScoreToRepresentative = nil
		}
	}
	return input, nil
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
	clusterType := strings.TrimSpace(input.ClusterType)
	if clusterType == "" {
		clusterType = "duplicate_candidate"
	}
	var clusterID int64
	if err := s.q().QueryRowContext(ctx, `
		insert into cluster_groups(
			repo_id, stable_key, stable_slug, status, cluster_type, representative_thread_id, title, created_at, updated_at
		)
		values(?, ?, ?, 'active', ?, ?, ?, ?, ?)
			on conflict(repo_id, stable_key) do update set
				status = case
					when exists(select 1 from cluster_closures where cluster_id = cluster_groups.id and actor_kind = 'local') then cluster_groups.status
					else 'active'
				end,
				stable_slug = excluded.stable_slug,
				cluster_type = excluded.cluster_type,
				representative_thread_id = case
					when exists(select 1 from cluster_closures where cluster_id = cluster_groups.id and actor_kind = 'local') then cluster_groups.representative_thread_id
					else excluded.representative_thread_id
				end,
				title = excluded.title,
				closed_at = case
					when exists(select 1 from cluster_closures where cluster_id = cluster_groups.id and actor_kind = 'local') then cluster_groups.closed_at
					else null
				end,
				updated_at = excluded.updated_at
			returning id
		`, repoID, stableKey, stableSlug, clusterType, nullInt(input.RepresentativeThreadID), nullString(input.Title), now, now).Scan(&clusterID); err != nil {
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

func (s *Store) markMissingDurableClustersRetired(ctx context.Context, repoID, runID int64, seenClusterIDs []int64, now string) error {
	where := `repo_id = ? and status = 'active' and closed_at is null`
	args := []any{now, now, repoID}
	if len(seenClusterIDs) > 0 {
		placeholders := make([]string, 0, len(seenClusterIDs))
		for _, id := range seenClusterIDs {
			placeholders = append(placeholders, "?")
			args = append(args, id)
		}
		where += ` and id not in (` + strings.Join(placeholders, ",") + `)`
	}
	rows, err := s.q().QueryContext(ctx, `
		update cluster_groups
		set status = 'closed',
			closed_at = ?,
			updated_at = ?
		where `+where+`
		returning id
	`, args...)
	if err != nil {
		return fmt.Errorf("retire missing durable clusters: %w", err)
	}
	defer rows.Close()

	var retired []int64
	for rows.Next() {
		var clusterID int64
		if err := rows.Scan(&clusterID); err != nil {
			return fmt.Errorf("scan retired durable cluster: %w", err)
		}
		retired = append(retired, clusterID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate retired durable clusters: %w", err)
	}
	for _, clusterID := range retired {
		if _, err := s.q().ExecContext(ctx, `
			insert into cluster_events(cluster_id, run_id, event_type, actor_kind, payload_json, created_at)
			values(?, ?, 'retired', 'cluster', '{"reason":"not seen in latest cluster run"}', ?)
		`, clusterID, runID, now); err != nil {
			return fmt.Errorf("record retired durable cluster event: %w", err)
		}
	}
	return nil
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

func nullInt(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: value != 0}
}

func nullableFloat(value *float64) sql.NullFloat64 {
	if value == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *value, Valid: true}
}
