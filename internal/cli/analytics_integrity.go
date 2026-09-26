package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

var errAnalyticsIncomplete = errors.New("analytics coverage remains incomplete")

func (a *App) analyticsUpdateLog(err error) {
	event := "github_update_complete"
	fields := map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano)}
	if err != nil {
		event = "github_update_failed"
		if errors.Is(err, errAnalyticsIncomplete) {
			event = "github_coverage_pending"
			fields["error"] = err.Error()
		} else {
			class, message, _ := gh.HistoryFailureDetails(err)
			fields["error_class"] = class
			fields["error"] = message
		}
	}
	fields["event"] = event
	encoded, _ := json.Marshal(fields)
	fmt.Fprintln(a.Stderr, string(encoded))
}

func migrateAnalyticsLanes(ctx context.Context, s *store.Store, repository string) error {
	// runAnalytics initializes coverage from a verified phase=complete discovery
	// receipt before any cycle. A partial checkpoint alone cannot certify a
	// historical baseline; missing baseline evidence must remain an error.
	marker := "independent_lanes:" + repository
	value, err := s.AnalyticsState(ctx, marker)
	if err != nil || value != "" {
		return err
	}
	value, err = s.AnalyticsState(ctx, "updates:"+repository)
	if err != nil {
		return err
	}
	var legacy updateCheckpoint
	if value != "" {
		if err = json.Unmarshal([]byte(value), &legacy); err != nil {
			return err
		}
	}
	through, err := s.AnalyticsState(ctx, "through:"+repository)
	if err != nil {
		return err
	}
	if through == "" {
		if err = s.DB().QueryRowContext(ctx, "SELECT through FROM analytics_coverage WHERE repository=?", repository).Scan(&through); err != nil {
			return fmt.Errorf("verified historical coverage watermark required: %w", err)
		}
	}
	var verifiedThrough string
	if err = s.DB().QueryRowContext(ctx, "SELECT through FROM analytics_coverage WHERE repository=?", repository).Scan(&verifiedThrough); err != nil {
		return err
	}
	if _, err = time.Parse(time.RFC3339Nano, verifiedThrough); err != nil {
		return fmt.Errorf("invalid verified historical watermark: %w", err)
	}
	return s.WithTx(ctx, func(tx *store.Store) error {
		// through retains the previous verified baseline even when a transient
		// post-upgrade failure temporarily clears complete before migration.
		if err := tx.SetAnalyticsState(ctx, "core_baseline_verified:"+repository, "1"); err != nil {
			return err
		}
		for i, kind := range []string{"issues", "pullRequests"} {
			cp := updateCheckpoint{}
			laneThrough := through
			if legacy.Started != "" {
				if legacy.Kind > i {
					laneThrough = legacy.Started
					total := legacy.Issues
					if i == 1 {
						total = legacy.PRs
					}
					encodedTotal, _ := json.Marshal(total)
					if err := tx.SetAnalyticsState(ctx, "total:"+repository+":"+kind, string(encodedTotal)); err != nil {
						return err
					}
				} else {
					cp = legacy
					cp.Kind = i
					if legacy.Kind < i {
						cp.Cursor = ""
					}
				}
			}
			encoded := ""
			if cp.Started != "" {
				b, _ := json.Marshal(cp)
				encoded = string(b)
			}
			if err := tx.SetAnalyticsState(ctx, "updates:"+repository+":"+kind, encoded); err != nil {
				return err
			}
			if err := tx.SetAnalyticsState(ctx, "through:"+repository+":"+kind, laneThrough); err != nil {
				return err
			}
		}
		// The original receipt/checkpoint is retained for historical evidence.
		return tx.SetAnalyticsState(ctx, marker, time.Now().UTC().Format(time.RFC3339Nano))
	})
}

func (a *App) analyticsCycle(ctx context.Context, s *store.Store, c *gh.Client, owner, repo string) error {
	repository := owner + "/" + repo
	if err := migrateAnalyticsLanes(ctx, s, repository); err != nil {
		return err
	}
	recovery, err := s.SeedReviewStateRecovery(ctx, repository, 500)
	if err != nil {
		return err
	}
	// Bounded retry work never replaces either independent discovery lane.
	for _, operation := range []string{"graphql_history", "review_state"} {
		due, e := s.DueAnalyticsRetries(ctx, repository, time.Now().UTC().Format(time.RFC3339Nano), 8, operation)
		if e != nil {
			return e
		}
		if e = a.analyticsNumbers(ctx, s, owner, repo, due, false, operation); e != nil {
			return e
		}
	}
	var failures []error
	for i, kind := range []string{"issues", "pullRequests"} {
		if err = a.analyticsLane(ctx, s, c, owner, repo, kind, i); err != nil {
			failures = append(failures, err)
		}
		if ctx.Err() != nil {
			return errors.Join(append(failures, ctx.Err())...)
		}
	}
	var through string
	baseline, err := s.AnalyticsState(ctx, "core_baseline_verified:"+repository)
	if err != nil {
		return err
	}
	complete := baseline == "1" && len(failures) == 0
	var totals [2]int
	// Preserve last verified totals until this lane obtains a new provider count.
	_ = s.DB().QueryRowContext(ctx, "SELECT issues,pull_requests FROM analytics_coverage WHERE repository=?", repository).Scan(&totals[0], &totals[1])
	for i, kind := range []string{"issues", "pullRequests"} {
		at, e := s.AnalyticsState(ctx, "through:"+repository+":"+kind)
		if e != nil {
			return e
		}
		if through == "" || at < through {
			through = at
		}
		value, e := s.AnalyticsState(ctx, "total:"+repository+":"+kind)
		if e != nil {
			return e
		}
		if value != "" {
			if e = json.Unmarshal([]byte(value), &totals[i]); e != nil {
				return e
			}
		}
	}
	outstanding, err := s.AnalyticsCoreOutstanding(ctx, repository)
	if err != nil {
		return err
	}
	complete = complete && outstanding == 0
	if err = s.WithTx(ctx, func(tx *store.Store) error {
		if e := tx.SaveAnalyticsCoverage(ctx, repository, through, totals[0], totals[1]); e != nil {
			return e
		}
		if e := tx.SetAnalyticsCoverageComplete(ctx, repository, complete); e != nil {
			return e
		}
		return tx.SetAnalyticsState(ctx, "through:"+repository, through)
	}); err != nil {
		return err
	}
	if err = s.SaveReviewStateCoverage(ctx, repository, recovery); err != nil {
		return err
	}
	totalPending, err := s.AnalyticsOutstanding(ctx, repository)
	if err != nil {
		return err
	}
	progress, _ := json.Marshal(map[string]any{"event": "review_state_progress", "at": time.Now().UTC().Format(time.RFC3339Nano), "scanned": recovery.Scanned, "ceiling": recovery.Ceiling, "queued": recovery.Queued, "pending_items": totalPending - outstanding, "scan_complete": recovery.Done, "complete": recovery.Done && totalPending == outstanding})
	fmt.Fprintln(a.Stderr, string(progress))
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if !complete {
		return fmt.Errorf("%w: core_unresolved=%d", errAnalyticsIncomplete, outstanding)
	}
	return nil
}

func (a *App) analyticsNumbers(ctx context.Context, s *store.Store, owner, repo string, numbers []int, discovery bool, operation string) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	slots := make(chan struct{}, 8)
	var failures []error
	var mu sync.Mutex
	for i := 0; i < len(numbers); i += 2 {
		var part []int
		for _, n := range numbers[i:min(i+2, len(numbers))] {
			if discovery {
				// Review-only recovery must not defer a newly discovered core edit.
				// If this core attempt fails, it creates graphql_history retry state
				// and every later overlap skips it until the bounded scheduler retries.
				deferred, e := s.AnalyticsItemQueued(ctx, owner+"/"+repo, n)
				if e != nil {
					return e
				}
				if deferred {
					continue
				}
			}
			part = append(part, n)
		}
		if len(part) == 0 {
			continue
		}
		slots <- struct{}{}
		wg.Add(1)
		go func(part []int) {
			defer wg.Done()
			defer func() { <-slots }()
			if e := a.analyticsIsolatedBatch(ctx, s, owner, repo, part, operation); e != nil {
				mu.Lock()
				failures = append(failures, e)
				mu.Unlock()
			}
		}(part)
	}
	wg.Wait()
	return errors.Join(failures...)
}

func (a *App) analyticsIsolatedBatch(ctx context.Context, s *store.Store, owner, repo string, numbers []int, operation string) error {
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
	err := a.syncAnalyticsBatch(bounded, s, owner, repo, numbers, operation)
	cancel()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return err
	}
	// Persisted failure receipts allow splitting all rejected batches, including
	// partial/invalid connections. A poison item cannot block its healthy peer.
	if len(numbers) > 1 {
		var failures []error
		for _, n := range numbers {
			if e := a.analyticsIsolatedBatch(ctx, s, owner, repo, []int{n}, operation); e != nil {
				failures = append(failures, e)
			}
		}
		return errors.Join(failures...)
	}
	queued, e := s.AnalyticsItemQueued(ctx, owner+"/"+repo, numbers[0], operation)
	if e != nil {
		return errors.Join(err, e)
	}
	if !queued {
		return err
	} // Never advance past an unrecorded failure.
	return nil
}

func (a *App) analyticsLane(ctx context.Context, s *store.Store, c *gh.Client, owner, repo, kind string, index int) error {
	repository := owner + "/" + repo
	key := "updates:" + repository + ":" + kind
	value, err := s.AnalyticsState(ctx, key)
	if err != nil {
		return err
	}
	var cp updateCheckpoint
	if value != "" {
		if err = json.Unmarshal([]byte(value), &cp); err != nil {
			return err
		}
	}
	if cp.Started == "" {
		through, e := s.AnalyticsState(ctx, "through:"+repository+":"+kind)
		if e != nil {
			return e
		}
		at, e := time.Parse(time.RFC3339Nano, through)
		if e != nil {
			return e
		}
		cp = updateCheckpoint{Started: time.Now().UTC().Format(time.RFC3339Nano), Since: at.Add(-5 * time.Minute).Format(time.RFC3339Nano), Kind: index}
	}
	since, err := time.Parse(time.RFC3339Nano, cp.Since)
	if err != nil {
		return err
	}
	for pages := 0; pages < 2; pages++ {
		started := time.Now().UTC().Format(time.RFC3339Nano)
		page, e := c.UpdatedNumbers(ctx, owner, repo, kind, cp.Cursor, since)
		attempt := store.AnalyticsAttempt{Repository: repository, Operation: "discover_" + kind, StartedAt: started, FinishedAt: time.Now().UTC().Format(time.RFC3339Nano), Status: "success", Evidence: json.RawMessage(`{}`)}
		if e != nil {
			attempt.Status = "failed"
			attempt.ErrorClass, attempt.ErrorText, attempt.Evidence = gh.HistoryFailureDetails(e)
		}
		receiptCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		writeErr := s.RecordAnalyticsAttempt(receiptCtx, attempt)
		cancel()
		if e != nil || writeErr != nil {
			return errors.Join(e, writeErr)
		}
		if e = a.analyticsNumbers(ctx, s, owner, repo, page.Numbers, true, "graphql_history"); e != nil {
			return e
		}
		done := !page.More || (!page.Oldest.IsZero() && page.Oldest.Before(since))
		if !done {
			cp.Cursor = page.Cursor
		}
		if e = s.WithTx(ctx, func(tx *store.Store) error {
			count, _ := json.Marshal(page.Total)
			if e := tx.SetAnalyticsState(ctx, "total:"+repository+":"+kind, string(count)); e != nil {
				return e
			}
			if done {
				if e := tx.SetAnalyticsState(ctx, "through:"+repository+":"+kind, cp.Started); e != nil {
					return e
				}
				return tx.SetAnalyticsState(ctx, key, "")
			}
			encoded, _ := json.Marshal(cp)
			return tx.SetAnalyticsState(ctx, key, string(encoded))
		}); e != nil {
			return e
		}
		fmt.Fprintf(a.Stderr, "{\"event\":\"github_update_page\",\"at\":%q,\"kind\":%q,\"threads\":%d}\n", time.Now().UTC().Format(time.RFC3339Nano), kind, len(page.Numbers))
		if done {
			return nil
		}
	}
	return nil
}
