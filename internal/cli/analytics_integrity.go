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

const (
	analyticsPollInterval = 2 * time.Minute
	analyticsCoreReserve  = 1500
	// Keep another 1500 points available to ordinary capture above its floor.
	analyticsReviewReserve = 3000
)

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
	nextCore := time.Now().Add(analyticsPollInterval)
	repository := owner + "/" + repo
	if err := migrateAnalyticsLanes(ctx, s, repository); err != nil {
		return err
	}
	// Ordinary capture and its retry obligations always go first. Historical
	// review enrichment uses only the remaining time before the next core poll.
	due, err := s.DueAnalyticsRetries(ctx, repository, time.Now().UTC().Format(time.RFC3339Nano), 8, "graphql_history")
	if err != nil {
		return err
	}
	if err = a.analyticsNumbers(ctx, s, owner, repo, due, false, "graphql_history"); err != nil {
		return err
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
	reviewErr := a.analyticsReviewRecovery(ctx, s, c, owner, repo, nextCore.Add(-5*time.Second))
	if len(failures) > 0 {
		return errors.Join(append(failures, reviewErr)...)
	}
	if !complete {
		return errors.Join(fmt.Errorf("%w: core_unresolved=%d", errAnalyticsIncomplete, outstanding), reviewErr)
	}
	return reviewErr
}

// Fill the time formerly spent idle with bounded, quota-checked waves through
// the existing executor. Each scan chunk and item receipt remains durable.
func (a *App) analyticsReviewRecovery(ctx context.Context, s *store.Store, c *gh.Client, owner, repo string, deadline time.Time) (resultErr error) {
	window, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	yield := func(err error) error {
		if ctx.Err() == nil && window.Err() == context.DeadlineExceeded && analyticsCancellationOnly(err) {
			fmt.Fprintf(a.Stderr, "{\"event\":\"review_state_yield\",\"at\":%q}\n", time.Now().UTC().Format(time.RFC3339Nano))
			return nil
		}
		return err
	}
	repository := owner + "/" + repo
	var progress store.ReviewStateRecovery
	haveProgress, quotaBlocked := false, false
	defer func() {
		if !haveProgress {
			return
		}
		// Cancellation cannot erase the last committed scan or completed items.
		receiptCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := s.SaveReviewStateCoverage(receiptCtx, repository, progress); err != nil {
			resultErr = errors.Join(resultErr, err)
			return
		}
		var pending int
		if err := s.DB().QueryRowContext(receiptCtx, "SELECT pending_items FROM analytics_review_state_coverage WHERE repository=?", repository).Scan(&pending); err != nil {
			resultErr = errors.Join(resultErr, err)
			return
		}
		encoded, _ := json.Marshal(map[string]any{"event": "review_state_progress", "at": time.Now().UTC().Format(time.RFC3339Nano), "scanned": progress.Scanned, "ceiling": progress.Ceiling, "queued": progress.Queued, "pending_items": pending, "scan_complete": progress.Done, "complete": progress.Done && pending == 0})
		fmt.Fprintln(a.Stderr, string(encoded))
	}()
	for time.Until(deadline) > 5*time.Second {
		if err := window.Err(); err != nil {
			return yield(err)
		}
		next, err := s.SeedReviewStateRecovery(window, repository, 5000)
		if err != nil {
			return yield(err)
		}
		progress, haveProgress = next, true
		// Persist progress even if quota is exhausted or the process is stopped.
		if err = s.SaveReviewStateCoverage(window, repository, progress); err != nil {
			return yield(err)
		}
		if quotaBlocked {
			if progress.Done {
				return nil
			}
			continue
		}
		due, err := s.DueAnalyticsRetries(window, repository, time.Now().UTC().Format(time.RFC3339Nano), 16, "review_state")
		if err != nil {
			return yield(err)
		}
		if len(due) == 0 {
			if progress.Done {
				return nil
			}
			continue
		}
		observedQuota, rawQuota, err := c.AnalyticsRateLimit(window)
		if err != nil {
			if window.Err() != nil {
				return yield(err)
			}
			class, message, _ := gh.HistoryFailureDetails(err)
			fmt.Fprintf(a.Stderr, "{\"event\":\"review_state_quota_deferred\",\"at\":%q,\"error_class\":%q,\"error\":%q}\n", time.Now().UTC().Format(time.RFC3339Nano), class, message)
			quotaBlocked = true
			continue
		}
		budget, quota, err := analyticsReviewBudget([]gh.RateLimitSnapshot{observedQuota}, time.Now())
		if err != nil {
			fmt.Fprintf(a.Stderr, "{\"event\":\"review_state_quota_deferred\",\"at\":%q,\"error\":%q}\n", time.Now().UTC().Format(time.RFC3339Nano), err.Error())
			quotaBlocked = true
			continue
		}
		encoded, _ := json.Marshal(map[string]any{"event": "review_state_quota", "at": time.Now().UTC().Format(time.RFC3339Nano), "limit": quota.Limit, "remaining": quota.Remaining, "reset_at": quota.ResetAt, "provider_remaining": rawQuota.Remaining, "provider_reset_at": rawQuota.ResetAt, "reserve": analyticsReviewReserve, "wave_items": min(len(due), budget)})
		fmt.Fprintln(a.Stderr, string(encoded))
		if budget == 0 {
			quotaBlocked = true
			continue
		}
		// Item-level reserve failures are durably queued and absorbed by
		// analyticsIsolatedBatch; the next wave reprobes quota and keeps scanning.
		// Remaining errors include unrecorded storage failures, not safe deferrals.
		if err = a.analyticsNumbers(window, s, owner, repo, due[:min(len(due), budget)], false, "review_state"); err != nil {
			return yield(err)
		}
	}
	return ctx.Err()
}

// Joined storage/receipt errors must never disappear behind a window timeout.
func analyticsCancellationOnly(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if !analyticsCancellationOnly(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return analyticsCancellationOnly(wrapped.Unwrap())
	}
	return err == context.Canceled || err == context.DeadlineExceeded
}

func analyticsReviewBudget(limits []gh.RateLimitSnapshot, now time.Time) (int, gh.RateLimitSnapshot, error) {
	for _, quota := range limits {
		if quota.Resource != "graphql" {
			continue
		}
		if quota.Limit <= 0 || quota.Remaining < 0 || !quota.ResetAt.After(now) {
			return 0, quota, fmt.Errorf("fresh GraphQL quota required for review recovery")
		}
		// A conservative admission margin limits in-flight overshoot. Native
		// request guards recheck actual remaining quota before every request.
		return min(16, max(0, quota.Remaining-analyticsReviewReserve)/32), quota, nil
	}
	return 0, gh.RateLimitSnapshot{}, fmt.Errorf("GraphQL quota unavailable for review recovery")
}

func (a *App) analyticsNumbers(ctx context.Context, s *store.Store, owner, repo string, numbers []int, discovery bool, operation string) (resultErr error) {
	var wg sync.WaitGroup
	var failures []error
	// Every return, including cancelled admission, waits for durable receipts.
	defer func() {
		wg.Wait()
		resultErr = errors.Join(append(failures, resultErr)...)
	}()
	slots := make(chan struct{}, 8)
	var mu sync.Mutex
	for i := 0; i < len(numbers); i += 2 {
		if err := ctx.Err(); err != nil {
			return err
		}
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
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			<-slots
			return err
		}
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
	return nil
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
