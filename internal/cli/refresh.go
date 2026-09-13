package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

type refreshResult struct {
	Repository string          `json:"repository"`
	Selected   map[string]bool `json:"selected"`
	Sync       *syncer.Stats   `json:"sync,omitempty"`
	Embed      *embedResult    `json:"embed,omitempty"`
	Cluster    map[string]any  `json:"cluster,omitempty"`
	dbTargetInfo
}

func (a *App) runRefresh(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("refresh", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	noSync := fs.Bool("no-sync", false, "skip GitHub sync stage")
	noEmbed := fs.Bool("no-embed", false, "skip embedding stage")
	noCluster := fs.Bool("no-cluster", false, "skip clustering stage")
	includeComments := fs.Bool("include-comments", false, "hydrate comments during sync")
	includePRDetails := fs.Bool("include-pr-details", false, "hydrate PR files, commits, checks, workflow runs, and review threads")
	withRaw := fs.String("with", "", "additional sync hydration: pr-metadata, pr-details")
	fs.Bool("include-code", false, "accepted for compatibility; code hydration is not implemented yet")
	since := fs.String("since", "", "GitHub since timestamp")
	state := fs.String("state", "", "GitHub issue state: open|closed|all; default open")
	limitRaw := fs.String("limit", "", "maximum sync or embedding rows")
	thresholdRaw := fs.String("threshold", fmt.Sprintf("%.2f", defaultClusterThreshold), "minimum cluster cosine score")
	minSizeRaw := fs.String("min-size", "1", "minimum cluster member count")
	maxClusterSizeRaw := fs.String("max-cluster-size", strconv.Itoa(defaultClusterMaxSize), "maximum members per generated cluster")
	fanoutRaw := fs.String("k", strconv.Itoa(defaultClusterFanout), "nearest-neighbor fanout per thread")
	crossKindThresholdRaw := fs.String("cross-kind-threshold", fmt.Sprintf("%.2f", defaultCrossKindMinScore), "minimum score for issue/pull request edges")
	strictVectors := fs.Bool("strict-vectors", false, "require complete fresh configured vectors before clustering")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"since": true, "state": true, "limit": true, "threshold": true, "min-size": true, "max-cluster-size": true, "k": true, "cross-kind-threshold": true, "with": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("refresh requires owner/repo"))
	}
	if *noSync && *noEmbed && *noCluster {
		return usageErr(fmt.Errorf("refresh requires at least one selected stage"))
	}
	with, err := parseSyncWith(*withRaw)
	if err != nil {
		return usageErr(err)
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
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
	if threshold <= 0 || threshold > 1 {
		return usageErr(fmt.Errorf("refresh requires --threshold between 0 and 1"))
	}
	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	if minSize <= 0 {
		minSize = 2
	}
	maxClusterSize, fanout, crossKindThreshold, err := parseClusterShapeOptions("refresh", *maxClusterSizeRaw, *fanoutRaw, *crossKindThresholdRaw)
	if err != nil {
		return err
	}

	result := refreshResult{
		Repository: owner + "/" + repoName,
		Selected: map[string]bool{
			"sync":    !*noSync,
			"embed":   !*noEmbed,
			"cluster": !*noCluster,
		},
	}
	if !*noSync {
		fmt.Fprintln(a.Stderr, "[refresh] sync")
		stats, target, err := a.syncRepository(ctx, owner, repoName, syncOptions{
			Since:             strings.TrimSpace(*since),
			State:             strings.TrimSpace(*state),
			Limit:             limit,
			IncludeComments:   *includeComments,
			IncludePRMetadata: with["pr-metadata"],
			IncludePRDetails:  *includePRDetails || with["pr-details"],
		})
		if err != nil {
			return err
		}
		result.Repository = stats.Repository
		result.Sync = &stats
		result.dbTargetInfo = target
	}
	if !*noEmbed {
		fmt.Fprintln(a.Stderr, "[refresh] embed")
		embed, err := a.embedRepository(ctx, owner, repoName, embedOptions{Limit: limit, IncludeClosed: stateIncludesClosed(*state)})
		if err != nil {
			return err
		}
		result.Repository = embed.Repository
		result.Embed = &embed
		if result.DBTarget == "" {
			result.dbTargetInfo = embed.dbTarget
		}
	}
	if !*noCluster {
		fmt.Fprintln(a.Stderr, "[refresh] cluster")
		rt, err := a.openLocalRuntime(ctx)
		if err != nil {
			return err
		}
		if result.DBTarget == "" {
			result.dbTargetInfo = rt.dbTarget()
		}
		repo, err := rt.repository(ctx, owner, repoName)
		if err != nil {
			_ = rt.Store.Close()
			return err
		}
		query := store.ThreadVectorQuery{RepoID: repo.ID, Model: rt.Config.OpenAI.EmbedModel, Basis: rt.Config.EmbeddingBasis}
		query.IncludeClosed = stateIncludesClosed(*state)
		vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, query)
		if err != nil {
			_ = rt.Store.Close()
			return err
		}
		coverage, freshVectors, err := evaluateClusterVectorCoverage(ctx, rt.Store, query, vectors)
		if err != nil {
			_ = rt.Store.Close()
			return err
		}
		if *strictVectors {
			if err := requireCompleteClusterVectorCoverage(owner, repoName, query, coverage, len(freshVectors), false); err != nil {
				_ = rt.Store.Close()
				return err
			}
		}
		vectors = freshVectors
		if len(vectors) == 0 && !*strictVectors {
			fallbackQuery := store.ThreadVectorQuery{RepoID: repo.ID, IncludeClosed: stateIncludesClosed(*state)}
			fallbackVectors, err := rt.Store.ListThreadVectorsFiltered(ctx, fallbackQuery)
			if err != nil {
				_ = rt.Store.Close()
				return err
			}
			if len(fallbackVectors) > 0 {
				query = fallbackQuery
				vectors, err = freshFallbackClusterVectors(ctx, rt.Store, fallbackQuery, fallbackVectors)
				if err != nil {
					_ = rt.Store.Close()
					return err
				}
				coverage.Fallback = true
			}
		}
		coverage.Processed = len(vectors)
		coverage.Partial = coverage.Fallback || !coverage.Complete
		if coverage.Partial {
			coverage.Complete = false
		}
		retireMissing := minSize <= 1 && !stateIncludesClosed(*state) && coverage.Complete
		clusterResult, err := clusterRepository(ctx, rt.Store, repo.ID, vectors, clusterBuildOptions{
			Threshold:          threshold,
			MinSize:            minSize,
			MaxClusterSize:     maxClusterSize,
			Fanout:             fanout,
			CrossKindThreshold: crossKindThreshold,
			RetireMissing:      retireMissing,
		})
		_ = rt.Store.Close()
		if err != nil {
			return err
		}
		result.Repository = repo.FullName
		result.Cluster = map[string]any{
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
		}
	}
	return a.writeOutput("refresh", result, true)
}
