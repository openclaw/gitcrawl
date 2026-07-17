package store

import (
	"context"
	"fmt"
	"strings"
)

type MissingPullRequestDetailOptions struct {
	Limit int
	Order string
}

func (s *Store) MissingPullRequestDetailNumbers(ctx context.Context, repoID int64, options MissingPullRequestDetailOptions) ([]int, error) {
	orderClause, err := missingPullRequestDetailOrderClause(options.Order)
	if err != nil {
		return nil, err
	}
	limit := options.Limit
	if limit <= 0 {
		limit = -1
	}
	rows, err := s.q().QueryContext(ctx, `
select t.number
from threads t
left join pull_request_details d on d.thread_id = t.id
where t.repo_id = ?
  and t.kind = 'pull_request'
  and d.thread_id is null
order by `+orderClause+`
limit ?`, repoID, limit)
	if err != nil {
		return nil, fmt.Errorf("list missing pull request details: %w", err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var number int
		if err := rows.Scan(&number); err != nil {
			return nil, fmt.Errorf("scan missing pull request detail number: %w", err)
		}
		out = append(out, number)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan missing pull request detail numbers: %w", err)
	}
	return out, nil
}

func missingPullRequestDetailOrderClause(order string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(order)) {
	case "", "newest-first":
		return "coalesce(t.updated_at_gh, t.updated_at, '') desc, t.number desc", nil
	case "oldest-first":
		return "coalesce(t.updated_at_gh, t.updated_at, '') asc, t.number asc", nil
	case "open-first":
		return "case when t.state = 'open' then 0 else 1 end, coalesce(t.updated_at_gh, t.updated_at, '') desc, t.number desc", nil
	default:
		return "", fmt.Errorf("unsupported fill-pr-details order %q", order)
	}
}
