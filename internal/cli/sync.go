package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

func (a *App) runSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	since := fs.String("since", "", "GitHub since timestamp")
	state := fs.String("state", "", "GitHub issue state: open|closed|all; default open")
	numbersRaw := fs.String("numbers", "", "comma-separated issue or pull request numbers")
	limitRaw := fs.String("limit", "", "maximum issue/PR rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	includeComments := fs.Bool("include-comments", false, "hydrate issue comments, PR reviews, and PR review comments")
	includePRDetails := fs.Bool("include-pr-details", false, "hydrate PR files, commits, checks, and workflow runs")
	withRaw := fs.String("with", "", "extra hydration: pr-metadata, pr-details")
	progressFile := fs.String("progress-file", "", "write an atomic sanitized sync progress snapshot")
	fs.Bool("include-code", false, "accepted for compatibility; code hydration is not implemented yet")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"numbers": true, "since": true, "state": true, "limit": true, "with": true, "progress-file": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("sync requires owner/repo"))
	}
	owner, repo, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	numbers, err := parseOptionalThreadNumberList(*numbersRaw)
	if err != nil {
		return usageErr(err)
	}
	with, err := parseSyncWith(*withRaw)
	if err != nil {
		return usageErr(err)
	}
	progress, err := newSyncProgressWriter(
		*progressFile,
		owner+"/"+repo,
	)
	if err != nil {
		return usageErr(err)
	}
	if err := progress.report(syncer.SyncProgress{
		Stage: syncer.SyncProgressConnecting,
	}); err != nil {
		return err
	}

	stats, target, err := a.syncRepository(ctx, owner, repo, syncOptions{
		Since:             strings.TrimSpace(*since),
		State:             strings.TrimSpace(*state),
		Limit:             limit,
		Numbers:           numbers,
		IncludeComments:   *includeComments,
		IncludePRMetadata: with["pr-metadata"],
		IncludePRDetails:  *includePRDetails || with["pr-details"],
		Progress:          progress.report,
	})
	if err != nil {
		if progressErr := progress.finish(syncProgressFailed); progressErr != nil {
			return progressErr
		}
		return err
	}
	if err := progress.finish(syncProgressSucceeded); err != nil {
		return err
	}
	result := struct {
		syncer.Stats
		dbTargetInfo
	}{Stats: stats, dbTargetInfo: target}
	return a.writeOutput("sync", result, true)
}

type syncOptions struct {
	Since             string
	State             string
	Limit             int
	Numbers           []int
	IncludeComments   bool
	IncludePRMetadata bool
	IncludePRDetails  bool
	Quiet             bool
	RateLimitReserve  int
	Progress          syncer.SyncProgressReporter
}

type fillPRDetailsResult struct {
	Repository       string               `json:"repository"`
	Selected         int                  `json:"selected"`
	Filled           int                  `json:"filled"`
	Remaining        int                  `json:"remaining"`
	Numbers          []int                `json:"numbers,omitempty"`
	Order            string               `json:"order"`
	Limit            int                  `json:"limit,omitempty"`
	BatchSize        int                  `json:"batch_size"`
	StoppedReason    string               `json:"stopped_reason,omitempty"`
	RateLimit        *fillRateLimitResult `json:"rate_limit,omitempty"`
	StartedAt        string               `json:"started_at"`
	FinishedAt       string               `json:"finished_at"`
	Batches          []fillPRDetailsBatch `json:"batches,omitempty"`
	IncludeComments  bool                 `json:"include_comments,omitempty"`
	JSONProgress     bool                 `json:"json_progress,omitempty"`
	ReserveRateLimit int                  `json:"reserve_rate_limit,omitempty"`
	dbTargetInfo
}

type fillPRDetailsBatch struct {
	Index              int                  `json:"index"`
	Numbers            []int                `json:"numbers"`
	PullRequestsSynced int                  `json:"pull_requests_synced"`
	PRDetailsSynced    int                  `json:"pr_details_synced"`
	RateLimit          *fillRateLimitResult `json:"rate_limit,omitempty"`
}

type fillRateLimitResult struct {
	Host      string `json:"host,omitempty"`
	Resource  string `json:"resource,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Remaining int    `json:"remaining"`
	ResetAt   string `json:"reset_at,omitempty"`
	Low       bool   `json:"low,omitempty"`
}

func (a *App) runFillPRDetails(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fill-pr-details", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limitRaw := fs.String("limit", "", "maximum missing PR details to hydrate")
	order := fs.String("order", "newest-first", "missing PR order: newest-first|oldest-first|open-first")
	batchSizeRaw := fs.String("batch-size", "50", "PRs to hydrate per sync batch")
	reserveRaw := fs.String("reserve-rate-limit", "1500", "keep this best-effort observed floor for shared-token GitHub quota")
	jsonProgress := fs.Bool("json-progress", false, "write newline JSON progress events to stderr")
	includeComments := fs.Bool("include-comments", false, "also hydrate issue comments and PR reviews while filling details")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"limit": true, "order": true, "batch-size": true, "reserve-rate-limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("fill-pr-details requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	batchSize, err := parseOptionalPositiveInt(*batchSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	reserve, err := parseOptionalPositiveInt(*reserveRaw)
	if err != nil {
		return usageErr(err)
	}
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		_ = rt.Store.Close()
		return fmt.Errorf("repository %s/%s is not in the local archive; run gitcrawl sync first: %w", owner, repoName, err)
	}
	numbers, err := rt.Store.MissingPullRequestDetailNumbers(ctx, repo.ID, store.MissingPullRequestDetailOptions{Limit: limit, Order: *order})
	if closeErr := rt.Store.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	result := fillPRDetailsResult{
		Repository:       repo.FullName,
		Selected:         len(numbers),
		Numbers:          numbers,
		Order:            strings.TrimSpace(*order),
		Limit:            limit,
		BatchSize:        batchSize,
		StartedAt:        startedAt,
		IncludeComments:  *includeComments,
		JSONProgress:     *jsonProgress,
		ReserveRateLimit: reserve,
		dbTargetInfo:     rt.dbTarget(),
	}
	for i := 0; i < len(numbers); i += batchSize {
		end := i + batchSize
		if end > len(numbers) {
			end = len(numbers)
		}
		batchNumbers := append([]int(nil), numbers[i:end]...)
		if *jsonProgress {
			a.writeFillPRDetailsProgress(fillPRDetailsProgressEvent{
				Event:      "batch_start",
				Repository: repo.FullName,
				Batch:      len(result.Batches) + 1,
				Numbers:    batchNumbers,
			})
		}
		stats, _, err := a.syncRepository(ctx, owner, repoName, syncOptions{
			Numbers:          batchNumbers,
			IncludeComments:  *includeComments,
			IncludePRDetails: true,
			Quiet:            *jsonProgress,
			RateLimitReserve: reserve,
		})
		if err != nil {
			var reserveErr *gh.RateLimitReserveError
			if errors.As(err, &reserveErr) {
				rate := fillRateLimitResultFromSnapshot(reserveErr.RateLimit, reserve)
				result.StoppedReason = "rate-limit-reserve"
				result.RateLimit = &rate
				break
			}
			return err
		}
		rate, hasRate := a.currentFillRateLimit(ctx, reserve)
		batch := fillPRDetailsBatch{
			Index:              len(result.Batches) + 1,
			Numbers:            batchNumbers,
			PullRequestsSynced: stats.PullRequestsSynced,
			PRDetailsSynced:    stats.PRDetailsSynced,
		}
		if hasRate {
			batch.RateLimit = &rate
			result.RateLimit = &rate
		}
		result.Batches = append(result.Batches, batch)
		result.Filled += stats.PRDetailsSynced
		if *jsonProgress {
			a.writeFillPRDetailsProgress(fillPRDetailsProgressEvent{
				Event:      "batch_done",
				Repository: repo.FullName,
				Batch:      batch.Index,
				Numbers:    batchNumbers,
				Filled:     result.Filled,
				RateLimit:  batch.RateLimit,
			})
		}
	}
	result.Remaining = result.Selected - result.Filled
	if result.Remaining < 0 {
		result.Remaining = 0
	}
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return a.writeOutput("fill-pr-details", result, true)
}

type fillPRDetailsProgressEvent struct {
	Event      string               `json:"event"`
	Repository string               `json:"repository"`
	Batch      int                  `json:"batch"`
	Numbers    []int                `json:"numbers"`
	Filled     int                  `json:"filled,omitempty"`
	RateLimit  *fillRateLimitResult `json:"rate_limit,omitempty"`
}

func (a *App) writeFillPRDetailsProgress(event fillPRDetailsProgressEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	fmt.Fprintln(a.Stderr, string(data))
}

func (a *App) currentFillRateLimit(ctx context.Context, reserve int) (fillRateLimitResult, bool) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return fillRateLimitResult{}, false
	}
	token := a.resolveGitHubToken(ctx, cfg)
	if token.Value == "" {
		return fillRateLimitResult{}, false
	}
	host := ghRateLimitHostForAPIBaseURL(githubBaseURL())
	if host == "" {
		host = "github.com"
	}
	state, ok := a.sharedRateLimitStateForTokenHost(token.Value, host)
	if !ok {
		return fillRateLimitResult{}, false
	}
	if !fillRateLimitStateFresh(state, time.Now().UTC()) {
		return fillRateLimitResult{}, false
	}
	result := fillRateLimitResult{
		Host:      state.Host,
		Resource:  state.Resource,
		Limit:     state.Limit,
		Remaining: state.Remaining,
		Low:       state.Remaining <= reserve,
	}
	if !state.ResetAt.IsZero() {
		result.ResetAt = state.ResetAt.Format(time.RFC3339)
	}
	return result, true
}

func fillRateLimitResultFromSnapshot(snapshot gh.RateLimitSnapshot, reserve int) fillRateLimitResult {
	result := fillRateLimitResult{
		Host:      snapshot.Host,
		Resource:  snapshot.Resource,
		Limit:     snapshot.Limit,
		Remaining: snapshot.Remaining,
		Low:       snapshot.Remaining <= reserve,
	}
	if !snapshot.ResetAt.IsZero() {
		result.ResetAt = snapshot.ResetAt.Format(time.RFC3339)
	}
	return result
}

func fillRateLimitStateFresh(state ghSharedRateLimitState, now time.Time) bool {
	if !state.ResetAt.IsZero() && !now.Before(state.ResetAt) {
		return false
	}
	if !state.UpdatedAt.IsZero() && now.Sub(state.UpdatedAt) > ghRateLimitStateMaxAge() {
		return false
	}
	return true
}

func parseSyncWith(value string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, part := range strings.Split(value, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		switch name {
		case "pr-metadata", "pr-details":
			out[name] = true
		default:
			return nil, fmt.Errorf("unsupported --with value %q", name)
		}
	}
	return out, nil
}

func (a *App) syncRepository(ctx context.Context, owner, repo string, options syncOptions) (syncer.Stats, dbTargetInfo, error) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return syncer.Stats{}, dbTargetInfo{}, err
	}
	token := a.resolveGitHubToken(ctx, cfg)
	var provider func(context.Context) (string, error)
	if a.githubTokenCommand != nil {
		var command func(context.Context) (string, error)
		command, err = githubTokenProvider(*a.githubTokenCommand)
		if err != nil {
			return syncer.Stats{}, dbTargetInfo{}, err
		}
		provider = func(ctx context.Context) (string, error) {
			a.githubTokenMu.Lock()
			a.observedGitHubToken = ""
			a.githubTokenMu.Unlock()
			return command(ctx)
		}
	}
	if provider == nil && token.Value == "" {
		return syncer.Stats{}, dbTargetInfo{}, fmt.Errorf("missing GitHub token: set %s or authenticate gh", cfg.GitHub.TokenEnv)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return syncer.Stats{}, dbTargetInfo{}, err
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return syncer.Stats{}, dbTargetInfo{}, err
	}
	defer rt.Store.Close()
	target := rt.dbTarget()

	var reporter gh.Reporter
	var logger *slog.Logger
	if !options.Quiet {
		reporter = func(message string) {
			fmt.Fprintln(a.Stderr, message)
		}
		logger = progressLogger(a.Stderr)
	}
	baseURL := githubBaseURL()
	client := gh.New(gh.Options{
		Token:            token.Value,
		TokenProvider:    provider,
		BaseURL:          baseURL,
		RateLimit:        a.observeGitHubRateLimit(ctx),
		RateLimitReserve: options.RateLimitReserve,
	})
	service := syncer.New(client, rt.Store)
	stats, err := service.Sync(ctx, syncer.Options{
		Owner:             owner,
		Repo:              repo,
		State:             strings.TrimSpace(options.State),
		Since:             strings.TrimSpace(options.Since),
		Limit:             options.Limit,
		Numbers:           options.Numbers,
		IncludeComments:   options.IncludeComments,
		IncludePRMetadata: options.IncludePRMetadata,
		IncludePRDetails:  options.IncludePRDetails,
		Reporter:          reporter,
		Logger:            logger,
		Progress:          options.Progress,
	})
	if err != nil {
		return syncer.Stats{}, target, err
	}
	return stats, target, nil
}
