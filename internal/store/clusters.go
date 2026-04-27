package store

import (
	"context"
	"database/sql"
	"fmt"
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
