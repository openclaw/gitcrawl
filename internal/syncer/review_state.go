package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (s *Syncer) syncReviewState(ctx context.Context, options Options, started string) (Stats, error) {
	stats := Stats{Repository: options.Owner + "/" + options.Repo, Numbers: uniquePositiveNumbers(options.Numbers), StartedAt: started, ReviewStateOnly: true}
	client, ok := s.client.(interface {
		FetchGraphQLReviewState(context.Context, string, string, []int, gh.Reporter) ([]gh.ReviewStateItem, error)
	})
	if !ok {
		return stats, fmt.Errorf("client does not support targeted review state")
	}
	// Reserve an observation sequence before fetching, as in normal capture.
	sequence, err := s.store.NextThreadObservationSequence(ctx, started)
	if err != nil {
		return stats, err
	}
	fetched := time.Now()
	items, err := client.FetchGraphQLReviewState(ctx, options.Owner, options.Repo, stats.Numbers, options.Reporter)
	stats.FetchMillis = time.Since(fetched).Milliseconds()
	if err != nil {
		return stats, err
	}
	if len(items) != len(stats.Numbers) {
		return stats, fmt.Errorf("incomplete review-state batch")
	}
	persist := time.Now()
	err = s.store.WithTx(ctx, func(tx *store.Store) error {
		applied, threads := 0, 0
		for i, item := range items {
			if item.Number != stats.Numbers[i] {
				return fmt.Errorf("review-state selection mismatch")
			}
			t, err := tx.ReviewStateParent(ctx, stats.Repository, item.Number, item.RepositoryID, item.RepositoryNodeID)
			if err != nil {
				return err
			}
			var raw map[string]any
			if err := json.Unmarshal([]byte(t.RawJSON), &raw); err != nil {
				return err
			}
			if known := stringValue(raw["node_id"]); known != "" && known != item.NodeID {
				return fmt.Errorf("review-state archived node identity mismatch")
			}
			reserved, err := tx.ReserveThreadChildObservation(ctx, t.ID, store.ThreadChildReviewThreads, item.UpdatedAt, sequence)
			if err != nil {
				return err
			}
			if !reserved {
				continue
			}
			count, err := s.persistPullReviewThreads(ctx, tx, t, item.Threads, started)
			if err != nil {
				return err
			}
			applied++
			threads += count
		}
		stats.ThreadsSynced = applied
		stats.PullRequestsSynced = applied
		stats.ReviewThreadsSynced = threads
		return nil
	})
	stats.PersistMillis = time.Since(persist).Milliseconds()
	stats.FinishedAt = s.now().Format(time.RFC3339Nano)
	return stats, err
}
