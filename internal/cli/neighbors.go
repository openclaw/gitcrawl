package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/vector"
)

func (a *App) runNeighbors(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("neighbors", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "issue or pull request number")
	limitRaw := fs.String("limit", "", "maximum neighbor rows")
	thresholdRaw := fs.String("threshold", "", "minimum cosine score")
	includeClosed := fs.Bool("include-closed", false, "include closed issue and pull request vectors")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "limit": true, "threshold": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("neighbors requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseRequiredThreadNumber("number", *numberRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	threshold, err := parseOptionalFloat(*thresholdRaw)
	if err != nil {
		return usageErr(err)
	}
	if limit <= 0 {
		limit = 10
	}
	if threshold <= 0 {
		threshold = 0.2
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
	targetThread, targetVector, err := configuredNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
	if err != nil {
		if isMissingThreadVectorErr(err, number) {
			if store.SupportsEmbeddingBasis(rt.Config.EmbeddingBasis) {
				_ = rt.Store.Close()
				if embedded, embedErr := a.backfillNeighborEmbedding(ctx, owner, repoName, number, *includeClosed); embedErr != nil {
					return embedErr
				} else if embedded {
					rt, err = a.openLocalRuntimeReadOnly(ctx)
					if err != nil {
						return err
					}
					defer rt.Store.Close()
					repo, err = rt.repository(ctx, owner, repoName)
					if err != nil {
						return err
					}
					targetThread, targetVector, err = configuredNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
				}
				if err != nil {
					return neighborEmbeddingRecoveryError(owner, repoName, number, *includeClosed, err)
				}
			} else {
				targetThread, targetVector, err = fallbackNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
				if err != nil {
					return err
				}
			}
		} else {
			return err
		}
	}
	stale, err := neighborEmbeddingNeedsRefresh(ctx, rt.Store, repo.ID, targetVector, number, *includeClosed)
	if err != nil {
		return err
	}
	if stale {
		_ = rt.Store.Close()
		if embedded, embedErr := a.backfillNeighborEmbedding(ctx, owner, repoName, number, *includeClosed); embedErr != nil {
			return embedErr
		} else if !embedded {
			return neighborEmbeddingRecoveryError(owner, repoName, number, *includeClosed, fmt.Errorf("thread #%d has a stale embedding", number))
		}
		rt, err = a.openLocalRuntimeReadOnly(ctx)
		if err != nil {
			return err
		}
		defer rt.Store.Close()
		repo, err = rt.repository(ctx, owner, repoName)
		if err != nil {
			return err
		}
		targetThread, targetVector, err = configuredNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
		if err != nil {
			return err
		}
	}
	vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, store.ThreadVectorQuery{
		RepoID:        repo.ID,
		Model:         targetVector.Model,
		Basis:         targetVector.Basis,
		Dimensions:    targetVector.Dimensions,
		IncludeClosed: *includeClosed,
	})
	if err != nil {
		return err
	}
	items := make([]vector.Item, 0, len(vectors))
	for _, stored := range vectors {
		items = append(items, vector.Item{ThreadID: stored.ThreadID, Vector: stored.Vector})
	}
	candidates, err := vector.QueryWithOptions(ctx, items, targetVector.Vector, vector.QueryOptions{
		Backend:         rt.Config.VectorBackend,
		Limit:           limit * 2,
		ExcludeThreadID: targetThread.ID,
	})
	if err != nil {
		return err
	}
	filtered := make([]vector.Neighbor, 0, limit)
	for _, candidate := range candidates {
		if candidate.Score < threshold {
			continue
		}
		filtered = append(filtered, candidate)
		if len(filtered) >= limit {
			break
		}
	}
	ids := make([]int64, 0, len(filtered))
	for _, candidate := range filtered {
		ids = append(ids, candidate.ThreadID)
	}
	threads, err := rt.Store.ThreadsByIDs(ctx, repo.ID, ids)
	if err != nil {
		return err
	}
	neighbors := make([]map[string]any, 0, len(filtered))
	for _, candidate := range filtered {
		thread, ok := threads[candidate.ThreadID]
		if !ok {
			continue
		}
		neighbors = append(neighbors, map[string]any{
			"thread_id": candidate.ThreadID,
			"number":    thread.Number,
			"kind":      thread.Kind,
			"title":     thread.Title,
			"score":     candidate.Score,
		})
	}
	return a.writeOutput("neighbors", map[string]any{
		"repository": repo.FullName,
		"thread":     targetThread,
		"neighbors":  neighbors,
	}, true)
}

func configuredNeighborTargetVector(ctx context.Context, rt localRuntime, repoID int64, number int, includeClosed bool) (store.Thread, store.ThreadVector, error) {
	query := store.ThreadVectorQuery{
		RepoID:        repoID,
		Model:         rt.Config.OpenAI.EmbedModel,
		Basis:         rt.Config.EmbeddingBasis,
		IncludeClosed: includeClosed,
	}
	targetThread, targetVector, err := rt.Store.ThreadVectorByNumber(ctx, query, number)
	if err == nil {
		return targetThread, targetVector, nil
	}
	return store.Thread{}, store.ThreadVector{}, err
}

func fallbackNeighborTargetVector(ctx context.Context, rt localRuntime, repoID int64, number int, includeClosed bool) (store.Thread, store.ThreadVector, error) {
	fallbackQuery := store.ThreadVectorQuery{RepoID: repoID, IncludeClosed: includeClosed}
	fallbackThread, fallbackVector, fallbackErr := rt.Store.ThreadVectorByNumber(ctx, fallbackQuery, number)
	if fallbackErr != nil {
		return store.Thread{}, store.ThreadVector{}, fallbackErr
	}
	return fallbackThread, fallbackVector, nil
}

func isMissingThreadVectorErr(err error, number int) bool {
	return err != nil && strings.Contains(err.Error(), fmt.Sprintf("thread #%d was not found with an embedding", number))
}

func (a *App) backfillNeighborEmbedding(ctx context.Context, owner, repoName string, number int, includeClosed bool) (bool, error) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return false, err
	}
	if token := config.ResolveOpenAIKey(cfg); token.Value == "" {
		return false, nil
	}
	result, err := a.embedRepository(ctx, owner, repoName, embedOptions{
		Number:        number,
		Limit:         1,
		IncludeClosed: includeClosed,
	})
	if err != nil {
		return false, fmt.Errorf("backfill neighbor embedding for #%d: %w", number, err)
	}
	return result.Embedded > 0, nil
}

func neighborEmbeddingNeedsRefresh(ctx context.Context, st *store.Store, repoID int64, vector store.ThreadVector, number int, includeClosed bool) (bool, error) {
	if strings.TrimSpace(vector.Basis) == "" || strings.TrimSpace(vector.Model) == "" {
		return false, nil
	}
	if !store.SupportsEmbeddingBasis(vector.Basis) {
		return false, nil
	}
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID:        repoID,
		Basis:         vector.Basis,
		Model:         vector.Model,
		Number:        number,
		Limit:         1,
		IncludeClosed: includeClosed,
	})
	if err != nil {
		return false, err
	}
	return len(tasks) > 0, nil
}

func neighborEmbeddingRecoveryError(owner, repoName string, number int, includeClosed bool, cause error) error {
	cmd := fmt.Sprintf("gitcrawl embed %s/%s --number %d --limit 1", owner, repoName, number)
	if includeClosed {
		cmd += " --include-closed"
	}
	return fmt.Errorf("%w; run `%s` and retry neighbors", cause, cmd)
}
