package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runRemoteThreads(ctx context.Context, cfg config.Config, repository string, includeClosed bool, numbers []int, limit int) error {
	client, err := a.remoteClient(cfg)
	if err != nil {
		return err
	}
	// Large explicit selections use bounded requests at one publication pin.
	numbers = slices.Clone(numbers)
	slices.Sort(numbers)
	numbers = slices.Compact(numbers)
	groups := [][]int{nil}
	if len(numbers) > 0 {
		groups = nil
		for start := 0; start < len(numbers); start += 500 {
			groups = append(groups, numbers[start:min(start+500, len(numbers))])
		}
	}
	threads := []store.Thread{}
	pin := ""
	for _, group := range groups {
		args := map[string]any{"repository": repository, "includeClosed": includeClosed}
		if len(group) > 0 {
			selected := make([]string, len(group))
			for i, n := range group {
				selected[i] = strconv.Itoa(n)
			}
			args["numbers"] = selected
		}
		cursor := ""
		for {
			pageLimit := 500
			if limit > 0 {
				pageLimit = min(pageLimit, limit-len(threads))
			}
			result, err := client.Query(ctx, "gitcrawl", cfg.Remote.Archive, remote.QueryRequest{Name: "gitcrawl.threads.list", Args: args, Limit: pageLimit, Cursor: cursor, SnapshotID: pin})
			if err != nil {
				return err
			}
			if result.Stats.SnapshotID == "" || pin != "" && pin != result.Stats.SnapshotID {
				return fmt.Errorf("remote threads changed publication pin")
			}
			pin = result.Stats.SnapshotID
			rows, err := remoteFullThreads(result)
			if err != nil {
				return err
			}
			threads = append(threads, rows...)
			if result.Cursor == "" || limit > 0 && len(threads) >= limit {
				break
			}
			if len(rows) == 0 || result.Cursor == cursor {
				return fmt.Errorf("remote threads cursor made no progress")
			}
			cursor = result.Cursor
		}
		if limit > 0 && len(threads) >= limit {
			break
		}
	}
	slices.SortFunc(threads, func(a, b store.Thread) int {
		if a.Number < b.Number {
			return -1
		}
		if a.Number > b.Number {
			return 1
		}
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	if limit > 0 && len(threads) > limit {
		threads = threads[:limit]
	}
	return a.writeOutput("threads", map[string]any{"repository": repository, "threads": threads}, true)
}

// Current native rows carry decimal integers as strings. Decode every public
// Thread field explicitly; the older search projection intentionally omits some.
func remoteFullThreads(result remote.QueryResult) ([]store.Thread, error) {
	required := strings.Fields("id repo_id github_id number kind state title body author_login author_type author_association html_url labels_json assignees_json content_hash is_draft created_at_gh updated_at_gh closed_at_gh merged_at_gh first_pulled_at last_pulled_at updated_at closed_at_local close_reason_local")
	positions := make(map[string]int, len(result.Columns))
	for i, col := range result.Columns {
		if _, ok := positions[col]; ok {
			return nil, fmt.Errorf("remote threads duplicate column %s", col)
		}
		positions[col] = i
	}
	for _, col := range required {
		if _, ok := positions[col]; !ok {
			return nil, fmt.Errorf("remote threads missing column %s", col)
		}
	}
	out := make([]store.Thread, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) != len(result.Columns) {
			return nil, fmt.Errorf("remote threads invalid row width")
		}
		value := make(map[string]any, len(required))
		for _, col := range required {
			v := row[positions[col]]
			if col == "id" || col == "repo_id" || col == "number" || col == "is_draft" {
				text, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("remote threads invalid %s", col)
				}
				n, err := strconv.ParseInt(text, 10, 64)
				if err != nil || strconv.FormatInt(n, 10) != text {
					return nil, fmt.Errorf("remote threads invalid %s", col)
				}
				if col == "is_draft" {
					if n != 0 && n != 1 {
						return nil, fmt.Errorf("remote threads invalid draft flag")
					}
					value[col] = n == 1
				} else {
					if n <= 0 {
						return nil, fmt.Errorf("remote threads invalid %s", col)
					}
					value[col] = n
				}
			} else {
				if v != nil {
					if _, ok := v.(string); !ok {
						return nil, fmt.Errorf("remote threads invalid %s", col)
					}
				}
				value[col] = v
			}
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		var thread store.Thread
		if err = json.Unmarshal(raw, &thread); err != nil {
			return nil, err
		}
		out = append(out, thread)
	}
	return out, nil
}
