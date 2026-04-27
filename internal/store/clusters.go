package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

	rows, err := s.db.QueryContext(ctx, `
		select cg.id, cg.stable_slug, cg.status, cg.title, cg.representative_thread_id,
			rt.number, rt.kind, rt.title,
			count(cm.thread_id) as member_count,
			cg.updated_at, cg.closed_at
		from cluster_groups cg
		left join cluster_memberships cm on cm.cluster_id = cg.id and cm.state = 'active'
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
