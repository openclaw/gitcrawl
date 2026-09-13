package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cluster", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	thresholdRaw := fs.String("threshold", fmt.Sprintf("%.2f", defaultClusterThreshold), "minimum cosine score")
	minSizeRaw := fs.String("min-size", "1", "minimum cluster member count")
	maxClusterSizeRaw := fs.String("max-cluster-size", strconv.Itoa(defaultClusterMaxSize), "maximum members per generated cluster")
	fanoutRaw := fs.String("k", strconv.Itoa(defaultClusterFanout), "nearest-neighbor fanout per thread")
	crossKindThresholdRaw := fs.String("cross-kind-threshold", fmt.Sprintf("%.2f", defaultCrossKindMinScore), "minimum score for issue/pull request edges")
	limitRaw := fs.String("limit", "", "maximum vector rows to cluster")
	model := fs.String("model", "", "embedding model")
	basis := fs.String("basis", "", "embedding basis")
	includeClosed := fs.Bool("include-closed", false, "include closed issue and pull request vectors")
	strictVectors := fs.Bool("strict-vectors", false, "require complete fresh configured vectors before clustering")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"threshold": true, "min-size": true, "max-cluster-size": true, "k": true, "cross-kind-threshold": true, "limit": true, "model": true, "basis": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("cluster requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	threshold, err := parseOptionalFloat(*thresholdRaw)
	if err != nil {
		return usageErr(err)
	}
	if threshold <= 0 || threshold > 1 {
		return usageErr(fmt.Errorf("cluster requires --threshold between 0 and 1"))
	}
	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	if minSize <= 0 {
		minSize = 2
	}
	maxClusterSize, fanout, crossKindThreshold, err := parseClusterShapeOptions("cluster", *maxClusterSizeRaw, *fanoutRaw, *crossKindThresholdRaw)
	if err != nil {
		return err
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	query := store.ThreadVectorQuery{
		RepoID:        repo.ID,
		Model:         firstNonEmpty(strings.TrimSpace(*model), rt.Config.OpenAI.EmbedModel),
		Basis:         firstNonEmpty(strings.TrimSpace(*basis), rt.Config.EmbeddingBasis),
		IncludeClosed: *includeClosed,
	}
	vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		return err
	}
	coverage, freshVectors, err := evaluateClusterVectorCoverage(ctx, rt.Store, query, vectors)
	if err != nil {
		return err
	}
	partial := limit > 0
	if *strictVectors {
		if err := requireCompleteClusterVectorCoverage(owner, repoName, query, coverage, len(freshVectors), partial); err != nil {
			return err
		}
	}
	vectors = freshVectors
	if len(vectors) == 0 && !*strictVectors && strings.TrimSpace(*model) == "" && strings.TrimSpace(*basis) == "" {
		fallbackQuery := store.ThreadVectorQuery{RepoID: repo.ID, IncludeClosed: *includeClosed}
		fallbackVectors, err := rt.Store.ListThreadVectorsFiltered(ctx, fallbackQuery)
		if err != nil {
			return err
		}
		if len(fallbackVectors) > 0 {
			query = fallbackQuery
			vectors, err = freshFallbackClusterVectors(ctx, rt.Store, fallbackQuery, fallbackVectors)
			if err != nil {
				return err
			}
			coverage.Fallback = true
		}
	}
	availableFresh := len(vectors)
	if limit > 0 && len(vectors) > limit {
		vectors = vectors[:limit]
	}
	coverage.Processed = len(vectors)
	coverage.Partial = coverage.Fallback || !coverage.Complete || len(vectors) < availableFresh
	if coverage.Partial {
		coverage.Complete = false
	}
	retireMissing := minSize <= 1 && limit == 0 && strings.TrimSpace(*model) == "" && strings.TrimSpace(*basis) == "" && !*includeClosed && coverage.Complete
	clusterResult, err := clusterRepository(ctx, rt.Store, repo.ID, vectors, clusterBuildOptions{
		Threshold:          threshold,
		MinSize:            minSize,
		MaxClusterSize:     maxClusterSize,
		Fanout:             fanout,
		CrossKindThreshold: crossKindThreshold,
		RetireMissing:      retireMissing,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("cluster", map[string]any{
		"repository":      repo.FullName,
		"threshold":       threshold,
		"cross_kind":      crossKindThreshold,
		"min_size":        minSize,
		"max_size":        maxClusterSize,
		"k":               fanout,
		"vector_count":    len(vectors),
		"edge_count":      clusterResult.EdgeCount,
		"cluster_count":   clusterResult.ClusterCount,
		"member_count":    clusterResult.MemberCount,
		"run_id":          clusterResult.RunID,
		"vector_coverage": coverage,
	}, true)
}
