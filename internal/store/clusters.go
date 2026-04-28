package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

type ClusterSummary struct {
	ID                     int64  `json:"id"`
	Source                 string `json:"source,omitempty"`
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

const (
	ClusterSourceRun     = "run_cluster"
	ClusterSourceDurable = "durable_cluster"
)

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

func (s *Store) ListClusterSummaries(ctx context.Context, options ClusterSummaryOptions) ([]ClusterSummary, error) {
	return s.listDurableClusterSummaries(ctx, options)
}

func (s *Store) ListDisplayClusterSummaries(ctx context.Context, options ClusterSummaryOptions) ([]ClusterSummary, error) {
	durable, err := s.listDurableClusterSummaries(ctx, options)
	if err != nil {
		return nil, err
	}
	if len(durable) > 0 {
		return durable, nil
	}
	return s.ListRunClusterSummaries(ctx, options)
}

func (s *Store) ListRunClusterSummaries(ctx context.Context, options ClusterSummaryOptions) ([]ClusterSummary, error) {
	runID, ok, err := s.latestRawClusterRunID(ctx, options.RepoID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []ClusterSummary{}, nil
	}
	orderBy := `latest_updated_at desc, c.id desc`
	if options.Sort == "size" {
		orderBy = `c.member_count desc, c.id asc`
	}
	limit := options.Limit
	if limit <= 0 {
		limit = 20
	}
	minSize := options.MinSize
	if minSize <= 0 {
		minSize = 1
	}
	where := `c.repo_id = ? and c.cluster_run_id = ?`
	args := []any{options.RepoID, runID, minSize}
	memberCountExpr := `c.member_count`
	updatedAtExpr := `coalesce(max(coalesce(t.updated_at_gh, t.updated_at)), c.created_at)`
	having := memberCountExpr + ` >= ?`
	if !options.IncludeClosed {
		memberCountExpr = `sum(case when t.state = 'open' and t.closed_at_local is null then 1 else 0 end)`
		updatedAtExpr = `coalesce(max(case when t.state = 'open' and t.closed_at_local is null then coalesce(t.updated_at_gh, t.updated_at) end), c.created_at)`
		having = memberCountExpr + ` >= ? and c.close_reason_local is null`
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		select c.id, c.representative_thread_id,
			rt.number, rt.kind, rt.title,
			`+memberCountExpr+` as member_count,
			`+updatedAtExpr+` as latest_updated_at,
			c.closed_at_local, c.close_reason_local,
			sum(case when t.closed_at_local is not null or t.state <> 'open' then 1 else 0 end) as closed_member_count
		from clusters c
		left join threads rt on rt.id = c.representative_thread_id
		join cluster_members cm on cm.cluster_id = c.id
		join threads t on t.id = cm.thread_id
		where `+where+`
		group by c.id, c.representative_thread_id, rt.number, rt.kind, rt.title, c.member_count, c.created_at, c.closed_at_local, c.close_reason_local
		having `+having+`
		order by `+orderBy+`
		limit ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list run cluster summaries: %w", err)
	}
	defer rows.Close()

	var out []ClusterSummary
	for rows.Next() {
		var summary ClusterSummary
		var repThreadID sql.NullInt64
		var repNumber sql.NullInt64
		var repKind, repTitle, updatedAt, closedAt, closeReason sql.NullString
		var closedMemberCount int
		if err := rows.Scan(&summary.ID, &repThreadID, &repNumber, &repKind, &repTitle, &summary.MemberCount, &updatedAt, &closedAt, &closeReason, &closedMemberCount); err != nil {
			return nil, fmt.Errorf("scan run cluster summary: %w", err)
		}
		summary.Source = ClusterSourceRun
		summary.StableSlug = clusterHumanName(options.RepoID, repThreadID.Int64, summary.ID)
		summary.Status = "active"
		if closeReason.Valid || closedMemberCount >= summary.MemberCount {
			summary.Status = "closed"
		}
		summary.UpdatedAt = updatedAt.String
		summary.ClosedAt = closedAt.String
		summary.RepresentativeThreadID = repThreadID.Int64
		summary.RepresentativeNumber = int(repNumber.Int64)
		summary.RepresentativeKind = repKind.String
		summary.RepresentativeTitle = repTitle.String
		out = append(out, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run cluster summaries: %w", err)
	}
	return out, nil
}

func (s *Store) listDurableClusterSummaries(ctx context.Context, options ClusterSummaryOptions) ([]ClusterSummary, error) {
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
		memberThreadJoin += ` and ` + durableVisibleMemberPredicate("cg", "cm", "mt")
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
		summary.Source = ClusterSourceDurable
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

func (s *Store) listClosedDurableClusterSummaries(ctx context.Context, options ClusterSummaryOptions, representedThreadIDs map[int64]bool) ([]ClusterSummary, error) {
	minSize := options.MinSize
	if minSize <= 0 {
		minSize = 1
	}
	rows, err := s.db.QueryContext(ctx, `
		select cg.id, cg.stable_slug, cg.status, cg.title, cg.representative_thread_id,
			rt.number, rt.kind, rt.title,
			count(cm.thread_id) as member_count,
			max(coalesce(t.updated_at_gh, t.updated_at)) as latest_updated_at,
			coalesce(cc.updated_at, cg.closed_at) as closed_at,
			sum(case when t.closed_at_local is not null or t.state <> 'open' then 1 else 0 end) as closed_member_count,
			group_concat(t.id, ',') as member_thread_ids
		from cluster_groups cg
		left join cluster_closures cc on cc.cluster_id = cg.id
		left join threads rt on rt.id = cg.representative_thread_id
		join cluster_memberships cm on cm.cluster_id = cg.id and cm.state <> 'removed_by_user'
		join threads t on t.id = cm.thread_id
		where cg.repo_id = ?
		group by cg.id, cg.stable_slug, cg.status, cg.title, cg.representative_thread_id,
			rt.number, rt.kind, rt.title, cg.closed_at, cc.updated_at, cc.reason
		having member_count >= ?
		   and (cc.cluster_id is not null
		    or cg.status in ('closed', 'merged', 'split')
		    or closed_member_count >= member_count)
	`, options.RepoID, minSize)
	if err != nil {
		return nil, fmt.Errorf("list closed durable cluster summaries: %w", err)
	}
	defer rows.Close()

	type closedDurableSummary struct {
		summary   ClusterSummary
		memberIDs map[int64]bool
	}
	var candidates []closedDurableSummary
	for rows.Next() {
		var summary ClusterSummary
		var title, closedAt, updatedAt, repKind, repTitle, memberThreadIDs sql.NullString
		var repThreadID sql.NullInt64
		var repNumber sql.NullInt64
		var closedMemberCount int
		if err := rows.Scan(&summary.ID, &summary.StableSlug, &summary.Status, &title, &repThreadID, &repNumber, &repKind, &repTitle, &summary.MemberCount, &updatedAt, &closedAt, &closedMemberCount, &memberThreadIDs); err != nil {
			return nil, fmt.Errorf("scan closed durable cluster summary: %w", err)
		}
		if repThreadID.Valid && representedThreadIDs[repThreadID.Int64] {
			continue
		}
		summary.Source = ClusterSourceDurable
		summary.Status = "closed"
		summary.Title = title.String
		summary.UpdatedAt = updatedAt.String
		summary.ClosedAt = closedAt.String
		summary.RepresentativeThreadID = repThreadID.Int64
		summary.RepresentativeNumber = int(repNumber.Int64)
		summary.RepresentativeKind = repKind.String
		summary.RepresentativeTitle = repTitle.String
		candidates = append(candidates, closedDurableSummary{summary: summary, memberIDs: parseIDSet(memberThreadIDs.String)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate closed durable cluster summaries: %w", err)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i].summary
		right := candidates[j].summary
		if left.MemberCount != right.MemberCount {
			return left.MemberCount > right.MemberCount
		}
		if left.UpdatedAt != right.UpdatedAt {
			return left.UpdatedAt > right.UpdatedAt
		}
		return left.ID < right.ID
	})
	selected := make([]closedDurableSummary, 0, len(candidates))
	for _, candidate := range candidates {
		duplicate := false
		for _, existing := range selected {
			if idSetOverlapRatio(candidate.memberIDs, existing.memberIDs) >= 0.8 {
				duplicate = true
				break
			}
		}
		if !duplicate {
			selected = append(selected, candidate)
		}
	}
	out := make([]ClusterSummary, 0, len(selected))
	for _, candidate := range selected {
		out = append(out, candidate.summary)
	}
	return out, nil
}

func sortClusterSummaries(clusters []ClusterSummary, sortMode string) {
	sort.SliceStable(clusters, func(i, j int) bool {
		left := clusters[i]
		right := clusters[j]
		if sortMode == "size" {
			if left.MemberCount != right.MemberCount {
				return left.MemberCount > right.MemberCount
			}
			if left.UpdatedAt != right.UpdatedAt {
				return left.UpdatedAt > right.UpdatedAt
			}
			return left.ID < right.ID
		}
		if left.UpdatedAt != right.UpdatedAt {
			return left.UpdatedAt > right.UpdatedAt
		}
		if left.MemberCount != right.MemberCount {
			return left.MemberCount > right.MemberCount
		}
		return left.ID < right.ID
	})
}

func parseIDSet(value string) map[int64]bool {
	out := map[int64]bool{}
	for _, part := range strings.Split(value, ",") {
		var id int64
		if _, err := fmt.Sscanf(strings.TrimSpace(part), "%d", &id); err == nil && id > 0 {
			out[id] = true
		}
	}
	return out
}

func durableVisibleMemberPredicate(clusterAlias, membershipAlias, threadAlias string) string {
	return threadAlias + `.state = 'open'
		and ` + threadAlias + `.closed_at_local is null
		and (
			` + membershipAlias + `.role in ('canonical', 'representative')
			or not exists (
				select 1
				from cluster_memberships visible_cm
				join cluster_groups visible_cg on visible_cg.id = visible_cm.cluster_id
				where visible_cm.thread_id = ` + membershipAlias + `.thread_id
				  and visible_cm.cluster_id <> ` + membershipAlias + `.cluster_id
				  and visible_cm.state = 'active'
				  and visible_cm.role in ('canonical', 'representative')
				  and visible_cg.repo_id = ` + clusterAlias + `.repo_id
				  and visible_cg.status = 'active'
				  and visible_cg.closed_at is null
			)
		)`
}

func idSetOverlapRatio(left, right map[int64]bool) float64 {
	smaller := len(left)
	if len(right) < smaller {
		smaller = len(right)
	}
	if smaller == 0 {
		return 0
	}
	overlap := 0
	for id := range left {
		if right[id] {
			overlap++
		}
	}
	return float64(overlap) / float64(smaller)
}

func (s *Store) ClusterDetail(ctx context.Context, options ClusterDetailOptions) (ClusterDetail, error) {
	detail, err := s.RunClusterDetail(ctx, options)
	if err == nil {
		return detail, nil
	}
	if !strings.Contains(err.Error(), "was not found") {
		return ClusterDetail{}, err
	}
	return s.DurableClusterDetail(ctx, options)
}

func (s *Store) DurableClusterDetail(ctx context.Context, options ClusterDetailOptions) (ClusterDetail, error) {
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
		where += ` and cm.state = 'active' and ` + durableVisibleMemberPredicate("cg", "cm", "t")
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		select cm.role, cm.state, cm.score_to_representative,
			`+s.threadSelectColumns(ctx, "t")+`
		from cluster_memberships cm
		join cluster_groups cg on cg.id = cm.cluster_id
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

func (s *Store) RunClusterDetail(ctx context.Context, options ClusterDetailOptions) (ClusterDetail, error) {
	summary, runID, err := s.runClusterSummaryByID(ctx, options.RepoID, options.ClusterID, options.IncludeClosed)
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
		where += ` and t.state = 'open' and t.closed_at_local is null`
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		select case when t.id = c.representative_thread_id then 'representative' else 'member' end as role,
			'active' as state,
			cm.score_to_representative,
			`+s.threadSelectColumns(ctx, "t")+`
		from cluster_members cm
		join clusters c on c.id = cm.cluster_id and c.cluster_run_id = ?
		join threads t on t.id = cm.thread_id
		where `+where+`
		order by case when t.id = c.representative_thread_id then 0 else 1 end,
			coalesce(cm.score_to_representative, 0) desc,
			case t.kind when 'issue' then 0 else 1 end asc,
			coalesce(t.updated_at_gh, t.updated_at) desc,
			t.number desc
		limit ?
	`, append([]any{runID}, args...)...)
	if err != nil {
		return ClusterDetail{}, fmt.Errorf("list run cluster members: %w", err)
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
		return ClusterDetail{}, fmt.Errorf("iterate run cluster members: %w", err)
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
		where += ` and cm.state = 'active' and cg.status = 'active' and cg.closed_at is null and ` + durableVisibleMemberPredicate("cg", "cm", "t")
	}
	row := s.db.QueryRowContext(ctx, `
		select cg.id
		from threads t
		join cluster_memberships cm on cm.thread_id = t.id
		join cluster_groups cg on cg.id = cm.cluster_id
		where `+where+`
		order by case cm.state when 'active' then 0 else 1 end,
			case cg.status when 'active' then 0 else 1 end,
			case cm.role when 'canonical' then 0 when 'representative' then 1 else 2 end,
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
		if len(inputs) > 0 {
			if _, err := tx.q().ExecContext(ctx, `
				delete from cluster_groups
				where repo_id = ?
				  and cluster_type = 'similarity'
			`, repoID); err != nil {
				return fmt.Errorf("delete legacy similarity clusters: %w", err)
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
			stable_slug = excluded.stable_slug,
			cluster_type = excluded.cluster_type,
			representative_thread_id = case
				when cluster_groups.status = 'closed' then cluster_groups.representative_thread_id
				else excluded.representative_thread_id
			end,
			title = excluded.title,
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
	memberCountExpr := `count(cm.thread_id)`
	closedMemberCountExpr := `sum(case when t.closed_at_local is not null or t.state <> 'open' then 1 else 0 end)`
	memberThreadJoin := ``
	if !includeClosed {
		where += ` and cg.status = 'active' and cg.closed_at is null`
		memberCountExpr = `count(mt.id)`
		closedMemberCountExpr = `0`
		memberThreadJoin = `
		left join threads mt on mt.id = cm.thread_id
			and (` + durableVisibleMemberPredicate("cg", "cm", "mt") + `)`
	}
	row := s.db.QueryRowContext(ctx, `
		select cg.id, cg.stable_slug, cg.status, cg.title, cg.representative_thread_id,
			rt.number, rt.kind, rt.title,
			`+memberCountExpr+` as member_count,
			cg.updated_at, coalesce(cc.updated_at, cg.closed_at) as closed_at,
			`+closedMemberCountExpr+` as closed_member_count
		from cluster_groups cg
		left join cluster_closures cc on cc.cluster_id = cg.id
		left join cluster_memberships cm on cm.cluster_id = cg.id and cm.state = 'active'
		left join threads t on t.id = cm.thread_id
		`+memberThreadJoin+`
		left join threads rt on rt.id = cg.representative_thread_id
		where `+where+`
		group by cg.id
	`, args...)
	var summary ClusterSummary
	var title, closedAt, repKind, repTitle sql.NullString
	var repThreadID sql.NullInt64
	var repNumber sql.NullInt64
	var closedMemberCount int
	if err := row.Scan(&summary.ID, &summary.StableSlug, &summary.Status, &title, &repThreadID, &repNumber, &repKind, &repTitle, &summary.MemberCount, &summary.UpdatedAt, &closedAt, &closedMemberCount); err != nil {
		if err == sql.ErrNoRows {
			return ClusterSummary{}, fmt.Errorf("cluster %d was not found", clusterID)
		}
		return ClusterSummary{}, fmt.Errorf("scan cluster summary: %w", err)
	}
	summary.Source = ClusterSourceDurable
	if summary.Status == "active" && summary.MemberCount > 0 && closedMemberCount >= summary.MemberCount {
		summary.Status = "closed"
	}
	summary.Title = title.String
	summary.ClosedAt = closedAt.String
	summary.RepresentativeThreadID = repThreadID.Int64
	summary.RepresentativeNumber = int(repNumber.Int64)
	summary.RepresentativeKind = repKind.String
	summary.RepresentativeTitle = repTitle.String
	return summary, nil
}

func (s *Store) latestRawClusterRunID(ctx context.Context, repoID int64) (int64, bool, error) {
	if !s.hasTable(ctx, "cluster_runs") || !s.hasTable(ctx, "clusters") {
		return 0, false, nil
	}
	row := s.db.QueryRowContext(ctx, `
		select cr.id
		from cluster_runs cr
		where cr.repo_id = ?
		  and cr.status in ('completed', 'success')
		  and exists (
		    select 1
		    from clusters c
		    where c.repo_id = cr.repo_id and c.cluster_run_id = cr.id
		  )
		order by coalesce(cr.finished_at, cr.started_at) desc, cr.id desc
		limit 1
	`, repoID)
	var runID int64
	if err := row.Scan(&runID); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read latest raw cluster run: %w", err)
	}
	return runID, true, nil
}

func (s *Store) runClusterSummaryByID(ctx context.Context, repoID, clusterID int64, includeClosed bool) (ClusterSummary, int64, error) {
	runID, ok, err := s.latestRawClusterRunID(ctx, repoID)
	if err != nil {
		return ClusterSummary{}, 0, err
	}
	if !ok {
		return ClusterSummary{}, 0, fmt.Errorf("cluster %d was not found", clusterID)
	}
	having := `1 = 1`
	memberCountExpr := `c.member_count`
	updatedAtExpr := `coalesce(max(coalesce(t.updated_at_gh, t.updated_at)), c.created_at)`
	if !includeClosed {
		memberCountExpr = `sum(case when t.state = 'open' and t.closed_at_local is null then 1 else 0 end)`
		updatedAtExpr = `coalesce(max(case when t.state = 'open' and t.closed_at_local is null then coalesce(t.updated_at_gh, t.updated_at) end), c.created_at)`
		having = memberCountExpr + ` > 0 and c.close_reason_local is null`
	}
	row := s.db.QueryRowContext(ctx, `
		select c.id, c.representative_thread_id,
			rt.number, rt.kind, rt.title,
			`+memberCountExpr+` as member_count,
			`+updatedAtExpr+` as latest_updated_at,
			c.closed_at_local, c.close_reason_local,
			sum(case when t.closed_at_local is not null or t.state <> 'open' then 1 else 0 end) as closed_member_count
		from clusters c
		left join threads rt on rt.id = c.representative_thread_id
		join cluster_members cm on cm.cluster_id = c.id
		join threads t on t.id = cm.thread_id
		where c.repo_id = ? and c.cluster_run_id = ? and c.id = ?
		group by c.id, c.representative_thread_id, rt.number, rt.kind, rt.title, c.member_count, c.created_at, c.closed_at_local, c.close_reason_local
		having `+having+`
	`, repoID, runID, clusterID)
	var summary ClusterSummary
	var repThreadID sql.NullInt64
	var repNumber sql.NullInt64
	var repKind, repTitle, updatedAt, closedAt, closeReason sql.NullString
	var closedMemberCount int
	if err := row.Scan(&summary.ID, &repThreadID, &repNumber, &repKind, &repTitle, &summary.MemberCount, &updatedAt, &closedAt, &closeReason, &closedMemberCount); err != nil {
		if err == sql.ErrNoRows {
			return ClusterSummary{}, 0, fmt.Errorf("cluster %d was not found", clusterID)
		}
		return ClusterSummary{}, 0, fmt.Errorf("scan run cluster summary: %w", err)
	}
	summary.Source = ClusterSourceRun
	summary.StableSlug = clusterHumanName(repoID, repThreadID.Int64, summary.ID)
	summary.Status = "active"
	if closeReason.Valid || closedMemberCount >= summary.MemberCount {
		summary.Status = "closed"
	}
	summary.UpdatedAt = updatedAt.String
	summary.ClosedAt = closedAt.String
	summary.RepresentativeThreadID = repThreadID.Int64
	summary.RepresentativeNumber = int(repNumber.Int64)
	summary.RepresentativeKind = repKind.String
	summary.RepresentativeTitle = repTitle.String
	return summary, runID, nil
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
	out := make(map[int64]map[string]string)
	if s.hasTable(ctx, "document_summaries") {
		rows, err := s.db.QueryContext(ctx, `
			select thread_id, summary_kind, summary_text
			from document_summaries
			where thread_id in (`+strings.Join(placeholders, ",")+`)
			order by thread_id, summary_kind, updated_at desc
		`, args...)
		if err != nil {
			return nil, fmt.Errorf("select document summaries: %w", err)
		}
		if err := scanSummaryRows(rows, out, "document summary"); err != nil {
			return nil, err
		}
	}
	if s.hasTable(ctx, "thread_key_summaries") && s.hasTable(ctx, "thread_revisions") {
		rows, err := s.db.QueryContext(ctx, `
			select tr.thread_id, tks.summary_kind, tks.key_text
			from thread_key_summaries tks
			join thread_revisions tr on tr.id = tks.thread_revision_id
			where tr.thread_id in (`+strings.Join(placeholders, ",")+`)
			order by tr.thread_id, tks.summary_kind, tks.created_at desc
		`, args...)
		if err != nil {
			return nil, fmt.Errorf("select thread key summaries: %w", err)
		}
		if err := scanSummaryRows(rows, out, "thread key summary"); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scanSummaryRows(rows *sql.Rows, out map[int64]map[string]string, source string) error {
	defer rows.Close()
	for rows.Next() {
		var threadID int64
		var kind, text string
		if err := rows.Scan(&threadID, &kind, &text); err != nil {
			return fmt.Errorf("scan %s: %w", source, err)
		}
		if out[threadID] == nil {
			out[threadID] = map[string]string{}
		}
		if _, exists := out[threadID][kind]; !exists {
			out[threadID][kind] = text
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s rows: %w", source, err)
	}
	return nil
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
