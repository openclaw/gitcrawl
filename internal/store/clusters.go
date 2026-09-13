package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
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
	Source        string
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

func (s *Store) ListClusterSummaries(ctx context.Context, options ClusterSummaryOptions) ([]ClusterSummary, error) {
	return s.listDurableClusterSummaries(ctx, options)
}

func (s *Store) ListDisplayClusterSummaries(ctx context.Context, options ClusterSummaryOptions) ([]ClusterSummary, error) {
	raw, err := s.ListRunClusterSummaries(ctx, options)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		if options.IncludeClosed {
			represented := make(map[int64]bool, len(raw))
			for _, cluster := range raw {
				if cluster.RepresentativeThreadID != 0 {
					represented[cluster.RepresentativeThreadID] = true
				}
			}
			closed, err := s.listClosedDurableClusterSummaries(ctx, options, represented)
			if err != nil {
				return nil, err
			}
			raw = append(raw, closed...)
			sortClusterSummaries(raw, options.Sort)
			if options.Limit > 0 && len(raw) > options.Limit {
				raw = raw[:options.Limit]
			}
		}
		return raw, nil
	}
	return s.listDurableClusterSummaries(ctx, options)
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
	} else if options.Sort == "oldest" {
		orderBy = `latest_updated_at asc, c.id asc`
	}
	limit := options.Limit
	if limit <= 0 {
		limit = -1
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
		having = memberCountExpr + ` >= ? and c.close_reason_local is null and sum(case when t.closed_at_local is not null or t.state <> 'open' then 1 else 0 end) < c.member_count`
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
	} else if options.Sort == "oldest" {
		orderBy = `coalesce(cg.updated_at, '') asc, cg.id asc`
	}
	limit := options.Limit
	if limit <= 0 {
		limit = -1
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
		if sortMode == "oldest" {
			if left.UpdatedAt != right.UpdatedAt {
				return left.UpdatedAt < right.UpdatedAt
			}
			if left.MemberCount != right.MemberCount {
				return left.MemberCount > right.MemberCount
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
	source, err := normalizeClusterDetailSource(options.Source)
	if err != nil {
		return ClusterDetail{}, err
	}
	switch source {
	case ClusterSourceRun:
		return s.RunClusterDetail(ctx, options)
	case ClusterSourceDurable:
		return s.DurableClusterDetail(ctx, options)
	}
	detail, err := s.RunClusterDetail(ctx, options)
	if err == nil {
		return detail, nil
	}
	if !strings.Contains(err.Error(), "was not found") {
		return ClusterDetail{}, err
	}
	return s.DurableClusterDetail(ctx, options)
}

func normalizeClusterDetailSource(source string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "", "auto":
		return "", nil
	case "run", "raw", ClusterSourceRun:
		return ClusterSourceRun, nil
	case "durable", ClusterSourceDurable:
		return ClusterSourceDurable, nil
	default:
		return "", fmt.Errorf("unsupported cluster source %q", source)
	}
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
	var body, authorLogin, authorType, authorAssociation, rawJSON, createdAt, updatedAtGH, closedAt, mergedAt, firstPulled, lastPulled, closedLocal, closeReason sql.NullString
	var isDraft int
	if err := row.Scan(&member.Role, &member.State, &score,
		&member.Thread.ID, &member.Thread.RepoID, &member.Thread.GitHubID, &member.Thread.Number, &member.Thread.Kind, &member.Thread.State, &member.Thread.Title,
		&body, &authorLogin, &authorType, &authorAssociation, &member.Thread.HTMLURL, &member.Thread.LabelsJSON, &member.Thread.AssigneesJSON, &rawJSON,
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
	member.Thread.AuthorAssociation = authorAssociation.String
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
		revisionOrder := s.latestThreadRevisionConsumerOrder(ctx, "latest", "t")
		revisionFresh := s.threadRevisionFreshnessPredicate(ctx, "tr", "t")
		rows, err := s.db.QueryContext(ctx, `
			select tr.thread_id, tks.summary_kind, tks.key_text
			from thread_key_summaries tks
			join thread_revisions tr on tr.id = tks.thread_revision_id
			join threads t on t.id = tr.thread_id
			where tr.thread_id in (`+strings.Join(placeholders, ",")+`)
				and tr.id = (
					select latest.id
						from thread_revisions latest
						where latest.thread_id = tr.thread_id
						order by `+revisionOrder+`
					limit 1
				)
				and `+revisionFresh+`
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
