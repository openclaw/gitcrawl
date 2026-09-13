package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runRuns(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("runs", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	kind := fs.String("kind", "sync", "run kind: sync|summary|embedding|cluster")
	limitRaw := fs.String("limit", "", "maximum run rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"kind": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("runs requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	runs, err := rt.Store.ListRuns(ctx, repo.ID, strings.TrimSpace(*kind), limit)
	if err != nil {
		return err
	}
	return a.writeOutput("runs", map[string]any{
		"repository": repo.FullName,
		"kind":       strings.TrimSpace(*kind),
		"runs":       runs,
	}, true)
}

func (a *App) runSyncFailures(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync-failures", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	includeResolved := fs.Bool("include-resolved", false, "include resolved failure rows")
	limitRaw := fs.String("limit", "", "maximum failure rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("sync-failures requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	failures, err := rt.Store.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{
		RepoID:          repo.ID,
		IncludeResolved: *includeResolved,
		Limit:           limit,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("sync-failures", map[string]any{
		"repository":       repo.FullName,
		"include_resolved": *includeResolved,
		"failures":         failures,
	}, true)
}

func (a *App) runCoverage(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("coverage", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	reposRaw := fs.String("repos", "", "comma-separated owner/repo filters")
	minMissingRaw := fs.String("min-missing-pr-details", "", "only show repositories with at least this many PRs missing detail rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"repos": true, "min-missing-pr-details": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() > 1 {
		return usageErr(fmt.Errorf("coverage accepts at most one owner/repo filter"))
	}
	if fs.NArg() == 1 && strings.TrimSpace(*reposRaw) != "" {
		return usageErr(fmt.Errorf("coverage accepts either a positional owner/repo or --repos, not both"))
	}
	minMissing, err := parseOptionalNonNegativeInt(*minMissingRaw)
	if err != nil {
		return usageErr(fmt.Errorf("min-missing-pr-details: %w", err))
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repositoryFilters := make([]string, 0)
	if fs.NArg() == 1 {
		repositoryFilters = append(repositoryFilters, fs.Arg(0))
	} else if strings.TrimSpace(*reposRaw) != "" {
		for _, value := range strings.Split(*reposRaw, ",") {
			value = strings.TrimSpace(value)
			if value == "" {
				return usageErr(fmt.Errorf("repos: expected comma-separated owner/repo values"))
			}
			repositoryFilters = append(repositoryFilters, value)
		}
	}
	opts := store.ArchiveCoverageOptions{MinMissingPRDetails: minMissing}
	resolvedFilters := make([]string, 0, len(repositoryFilters))
	seenRepoIDs := make(map[int64]struct{}, len(repositoryFilters))
	for _, value := range repositoryFilters {
		owner, repoName, err := parseOwnerRepo(value)
		if err != nil {
			return usageErr(err)
		}
		repo, err := rt.repository(ctx, owner, repoName)
		if err != nil {
			return err
		}
		if _, seen := seenRepoIDs[repo.ID]; seen {
			continue
		}
		seenRepoIDs[repo.ID] = struct{}{}
		opts.RepoIDs = append(opts.RepoIDs, repo.ID)
		resolvedFilters = append(resolvedFilters, repo.FullName)
	}
	coverage, err := rt.Store.ArchiveCoverage(ctx, opts)
	if err != nil {
		return err
	}
	payload := map[string]any{
		"repository_filters":           resolvedFilters,
		"min_missing_pr_details":       minMissing,
		"hydration_failures_available": coverage.Totals.HydrationFailuresSupported,
		"repositories":                 coverage.Rows,
		"totals":                       coverage.Totals,
	}
	if a.format == FormatJSON {
		return a.writeOutput("coverage", payload, true)
	}
	return a.writeCoverageTable(coverage)
}

func (a *App) writeCoverageTable(coverage store.ArchiveCoverage) error {
	tw := tabwriter.NewWriter(a.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "REPOSITORY\tISSUES\tPRS\tCOMMENTS\tPR_REVIEWS\tPRS_WITH_DETAILS\tMISSING_PR_DETAILS\tFILES\tCOMMITS\tCHECKS\tREVIEW_THREADS\tWORKFLOW_RUNS\tLAST_SYNC"); err != nil {
		return err
	}
	for _, row := range coverage.Rows {
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
			row.Repository,
			row.Issues,
			row.PullRequests,
			row.Comments,
			row.PRReviews,
			row.PullRequestsWithDetails,
			row.MissingPRDetails,
			row.PRFiles,
			row.PRCommits,
			row.PRChecks,
			row.PRReviewThreads,
			row.WorkflowRuns,
			row.LastSyncAt,
		); err != nil {
			return err
		}
	}
	if len(coverage.Rows) > 1 {
		row := coverage.Totals
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
			row.Repository,
			row.Issues,
			row.PullRequests,
			row.Comments,
			row.PRReviews,
			row.PullRequestsWithDetails,
			row.MissingPRDetails,
			row.PRFiles,
			row.PRCommits,
			row.PRChecks,
			row.PRReviewThreads,
			row.WorkflowRuns,
			row.LastSyncAt,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func (a *App) runThreads(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("threads", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	includeClosed := fs.Bool("include-closed", false, "include locally closed rows")
	numbersRaw := fs.String("numbers", "", "comma-separated issue or pull request numbers")
	limitRaw := fs.String("limit", "", "maximum thread rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"numbers": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("threads requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	numbers, err := parseOptionalThreadNumberList(*numbersRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	threads, err := rt.Store.ListThreadsFiltered(ctx, store.ThreadListOptions{
		RepoID:        repo.ID,
		IncludeClosed: *includeClosed,
		Numbers:       numbers,
		Limit:         limit,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("threads", map[string]any{
		"repository": repo.FullName,
		"threads":    threads,
	}, true)
}
