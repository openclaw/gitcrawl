package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/openclaw/gitcrawl/internal/store"
)

// runClusterCandidates reads the selected (or an explicit) computed candidate
// generation. Candidate IDs are run-scoped and never durable identities.
// Rollback is a local owner action and is unavailable in the cluster reader.
func (a *App) runClusterCandidates(ctx context.Context, args []string, readerOnly bool) error {
	fs := flag.NewFlagSet("cluster-candidates", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	generationRaw := fs.String("generation", "", "generation id (default: selected)")
	limitRaw := fs.String("limit", "", "maximum candidates")
	members := fs.Bool("members", false, "include member thread ids")
	listGenerations := fs.Bool("generations", false, "list recent generations instead of candidates")
	rollback := fs.Bool("rollback", false, "select the previous generation")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"generation": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("cluster-candidates requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	generationID, err := parseOptionalPositiveInt(*generationRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	if *rollback && (readerOnly || generationID != 0 || *listGenerations) {
		return usageErr(fmt.Errorf("--rollback is a local owner action without --generation or --generations"))
	}
	if *rollback {
		rt, err := a.openLocalRuntime(ctx)
		if err != nil {
			return err
		}
		defer rt.Store.Close()
		repo, err := rt.repository(ctx, owner, repoName)
		if err != nil {
			return err
		}
		generation, err := rt.Store.RollbackClusterCandidateGeneration(ctx, repo.ID)
		if err != nil {
			return err
		}
		return a.writeOutput("cluster-candidates", map[string]any{"repository": repo.FullName, "selected_generation": generation}, false)
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
	// One read transaction gives each response a single database snapshot,
	// so selection flags, counts and members cannot straddle a rollback or an
	// owner purge.
	var generations []store.ClusterCandidateGeneration
	var generation store.ClusterCandidateGeneration
	var candidates []store.ClusterCandidate
	err = rt.Store.WithTx(ctx, func(tx *store.Store) error {
		var err error
		if *listGenerations {
			generations, err = tx.ListClusterCandidateGenerations(ctx, repo.ID, limit)
			return err
		}
		if generationID != 0 {
			generation, err = tx.ClusterCandidateGenerationByID(ctx, repo.ID, int64(generationID))
		} else {
			generation, err = tx.SelectedClusterCandidateGeneration(ctx, repo.ID)
		}
		if err != nil {
			return err
		}
		candidates, err = tx.ClusterCandidates(ctx, repo.ID, generation.ID, limit, *members)
		return err
	})
	if errors.Is(err, store.ErrNoClusterCandidateGeneration) {
		return fmt.Errorf("%w for %s", err, repo.FullName)
	}
	if err != nil {
		return err
	}
	if *listGenerations {
		return a.writeOutput("cluster-candidates", map[string]any{"repository": repo.FullName, "format": store.ClusterCandidateFormatV3, "generations": generations}, false)
	}
	return a.writeOutput("cluster-candidates", map[string]any{
		"repository": repo.FullName,
		"generation": generation,
		// Computed IDs are scoped to this generation. Overlaps are observed
		// shared members with durable groups, not equivalence.
		"identity_domain":                  "computed-run-scoped",
		"identity_reconciliation_complete": false,
		"candidates":                       candidates,
	}, false)
}
