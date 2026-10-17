package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

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
	openAtRaw := fs.String("open-at", "", "only rows open at this time")
	createdAfterRaw := fs.String("created-after", "", "only rows created after this time or thread")
	mergedAfterRaw := fs.String("merged-after", "", "only pull requests merged after this time")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "limit": true, "threshold": true, "open-at": true, "created-after": true, "merged-after": true})); err != nil {
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
	number, err := parseRequiredThreadNumber("number", *numberRaw, owner+"/"+repoName)
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
	openAt, err := parseNeighborTime("open-at", *openAtRaw)
	if err != nil {
		return usageErr(err)
	}
	createdAfter, createdAfterNumber, err := parseNeighborCreatedAfter(*createdAfterRaw, owner+"/"+repoName)
	if err != nil {
		return usageErr(err)
	}
	mergedAfter, err := parseNeighborTime("merged-after", *mergedAfterRaw)
	if err != nil {
		return usageErr(err)
	}
	// Rows open at a past time or merged since, and often the source, may be closed now.
	if !openAt.IsZero() || !mergedAfter.IsZero() {
		*includeClosed = true
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
		RepoID:             repo.ID,
		Model:              targetVector.Model,
		Basis:              targetVector.Basis,
		Dimensions:         targetVector.Dimensions,
		IncludeClosed:      *includeClosed,
		OpenAt:             openAt,
		CreatedAfter:       createdAfter,
		CreatedAfterNumber: createdAfterNumber,
		MergedAfter:        mergedAfter,
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
		neighbors = append(neighbors, neighborRow(thread, candidate.Score))
	}
	stateAsOf, err := rt.Store.StateAsOf(ctx, repo.ID)
	if err != nil {
		return err
	}
	payload := map[string]any{
		"repository": repo.FullName,
		"thread":     targetThread,
		"neighbors":  neighbors,
	}
	if !stateAsOf.IsZero() {
		payload["state_as_of"] = stateAsOf.UTC().Format(time.RFC3339Nano)
	}
	return a.writeOutput("neighbors", payload, true)
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

func neighborRow(thread store.Thread, score float64) map[string]any {
	row := map[string]any{
		"thread_id": thread.ID,
		"number":    thread.Number,
		"kind":      thread.Kind,
		"state":     thread.State,
		"title":     thread.Title,
		"html_url":  thread.HTMLURL,
		"is_draft":  thread.IsDraft,
		"score":     score,
	}
	for key, value := range map[string]string{
		"author_login":       thread.AuthorLogin,
		"author_association": thread.AuthorAssociation,
		"created_at_gh":      thread.CreatedAtGitHub,
		"closed_at_gh":       thread.ClosedAtGitHub,
		"merged_at_gh":       thread.MergedAtGitHub,
		"closed_at_local":    thread.ClosedAtLocal,
	} {
		if value != "" {
			row[key] = value
		}
	}
	return row
}

func parseNeighborTime(name, value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.DateOnly} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("--%s expects an RFC 3339 time or YYYY-MM-DD date, got %q", name, value)
}

// parseNeighborCreatedAfter takes a time, or an issue or pull request
// reference meaning "created after that thread".
func parseNeighborCreatedAfter(value, repository string) (time.Time, int, error) {
	at, timeErr := parseNeighborTime("created-after", value)
	if timeErr == nil {
		return at, 0, nil
	}
	if _, ok := parseThreadReference(value); ok {
		number, err := parseOptionalThreadNumber(value, repository)
		if err != nil {
			return time.Time{}, 0, fmt.Errorf("--created-after: %w", err)
		}
		return time.Time{}, number, nil
	}
	return time.Time{}, 0, fmt.Errorf("--created-after expects an RFC 3339 time, YYYY-MM-DD date, or issue or pull request number, got %q", strings.TrimSpace(value))
}

func neighborEmbeddingRecoveryError(owner, repoName string, number int, includeClosed bool, cause error) error {
	cmd := fmt.Sprintf("gitcrawl embed %s/%s --number %d --limit 1", owner, repoName, number)
	if includeClosed {
		cmd += " --include-closed"
	}
	return fmt.Errorf("%w; run `%s` and retry neighbors", cause, cmd)
}
