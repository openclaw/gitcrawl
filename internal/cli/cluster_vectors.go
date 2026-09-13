package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

type clusterRepositoryResult struct {
	EdgeCount    int
	ClusterCount int
	MemberCount  int
	RunID        int64
}

type clusterVectorCoverage struct {
	Supported    bool `json:"supported"`
	Fallback     bool `json:"fallback"`
	Eligible     int  `json:"eligible"`
	Covered      int  `json:"covered"`
	Fresh        int  `json:"fresh"`
	Processed    int  `json:"processed"`
	Missing      int  `json:"missing"`
	MissingInput int  `json:"missing_input"`
	Stale        int  `json:"stale"`
	Partial      bool `json:"partial"`
	Complete     bool `json:"complete"`
}

func evaluateClusterVectorCoverage(ctx context.Context, st *store.Store, query store.ThreadVectorQuery, storedVectors []store.ThreadVector) (clusterVectorCoverage, []store.ThreadVector, error) {
	coverage := clusterVectorCoverage{
		Covered:   len(storedVectors),
		Fresh:     len(storedVectors),
		Processed: len(storedVectors),
	}
	if strings.TrimSpace(query.Model) == "" || strings.TrimSpace(query.Basis) == "" || !store.SupportsEmbeddingBasis(query.Basis) {
		return coverage, storedVectors, nil
	}
	coverage.Supported = true
	eligible, err := st.CountThreadVectorScope(ctx, query)
	if err != nil {
		return clusterVectorCoverage{}, nil, err
	}
	coverage.Eligible = eligible
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID:        query.RepoID,
		Basis:         query.Basis,
		Model:         query.Model,
		Force:         true,
		IncludeClosed: query.IncludeClosed,
	})
	if err != nil {
		return clusterVectorCoverage{}, nil, err
	}
	hashByThread := make(map[int64]string, len(tasks))
	for _, task := range tasks {
		hashByThread[task.ThreadID] = task.ContentHash
	}
	coverage.MissingInput = max(0, coverage.Eligible-len(hashByThread))
	fresh := make([]store.ThreadVector, 0, len(storedVectors))
	for _, vector := range storedVectors {
		if hashByThread[vector.ThreadID] == vector.ContentHash && vector.ContentHash != "" {
			fresh = append(fresh, vector)
			continue
		}
		coverage.Stale++
	}
	coverage.Fresh = len(fresh)
	coverage.Processed = len(fresh)
	coverage.Missing = max(0, coverage.Eligible-coverage.Covered)
	coverage.Complete = coverage.Fresh == coverage.Eligible && coverage.Missing == 0 && coverage.MissingInput == 0 && coverage.Stale == 0
	return coverage, fresh, nil
}

func freshFallbackClusterVectors(ctx context.Context, st *store.Store, query store.ThreadVectorQuery, vectors []store.ThreadVector) ([]store.ThreadVector, error) {
	type vectorGroup struct {
		query   store.ThreadVectorQuery
		vectors []store.ThreadVector
	}
	groups := map[string]*vectorGroup{}
	groupOrder := make([]string, 0)
	passthrough := make([]store.ThreadVector, 0, len(vectors))
	for _, vector := range vectors {
		if strings.TrimSpace(vector.Model) == "" || strings.TrimSpace(vector.Basis) == "" || !store.SupportsEmbeddingBasis(vector.Basis) {
			passthrough = append(passthrough, vector)
			continue
		}
		key := vector.Model + "\x00" + vector.Basis
		group := groups[key]
		if group == nil {
			group = &vectorGroup{
				query: store.ThreadVectorQuery{
					RepoID:        query.RepoID,
					Model:         vector.Model,
					Basis:         vector.Basis,
					IncludeClosed: query.IncludeClosed,
				},
			}
			groups[key] = group
			groupOrder = append(groupOrder, key)
		}
		group.vectors = append(group.vectors, vector)
	}
	for _, key := range groupOrder {
		group := groups[key]
		_, fresh, err := evaluateClusterVectorCoverage(ctx, st, group.query, group.vectors)
		if err != nil {
			return nil, err
		}
		passthrough = append(passthrough, fresh...)
	}
	return dedupeThreadVectorsByThread(passthrough), nil
}

func requireCompleteClusterVectorCoverage(owner, repoName string, query store.ThreadVectorQuery, coverage clusterVectorCoverage, freshVectorCount int, allowPartial bool) error {
	if !coverage.Supported {
		return clusterVectorCoverageError(owner, repoName, query, coverage, "vector coverage cannot be verified")
	}
	if coverage.Eligible == 0 && coverage.Supported {
		return nil
	}
	if freshVectorCount == 0 {
		return clusterVectorCoverageError(owner, repoName, query, coverage, "no fresh vectors are available")
	}
	if coverage.Supported && !coverage.Complete && !allowPartial {
		return clusterVectorCoverageError(owner, repoName, query, coverage, "vector coverage is incomplete")
	}
	return nil
}

func clusterVectorCoverageError(owner, repoName string, query store.ThreadVectorQuery, coverage clusterVectorCoverage, reason string) error {
	cmd := fmt.Sprintf("gitcrawl embed %s/%s", owner, repoName)
	if query.IncludeClosed {
		cmd += " --include-closed"
	}
	recovery := fmt.Sprintf("run `%s` and retry", cmd)
	if !coverage.Supported {
		recovery = fmt.Sprintf("populate exact vectors for model %q basis %q and retry", query.Model, query.Basis)
	} else if query.Basis == "llm_key_summary" && coverage.MissingInput > 0 {
		recovery = "populate missing key summaries, then " + recovery
	}
	return fmt.Errorf("%s for model %q basis %q (eligible=%d fresh=%d missing=%d missing_input=%d stale=%d); %s",
		reason,
		query.Model,
		query.Basis,
		coverage.Eligible,
		coverage.Fresh,
		coverage.Missing,
		coverage.MissingInput,
		coverage.Stale,
		recovery,
	)
}

func clusterRepository(ctx context.Context, st *store.Store, repoID int64, storedVectors []store.ThreadVector, options clusterBuildOptions) (clusterRepositoryResult, error) {
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, storedVectors, options)
	if err != nil {
		return clusterRepositoryResult{}, err
	}
	var saveResult store.SaveDurableClustersResult
	if options.RetireMissing {
		saveResult, err = st.SaveCompleteDurableClusters(ctx, repoID, inputs)
	} else {
		saveResult, err = st.SavePartialDurableClusters(ctx, repoID, inputs)
	}
	if err != nil {
		return clusterRepositoryResult{}, err
	}
	return clusterRepositoryResult{
		EdgeCount:    edgeCount,
		ClusterCount: saveResult.ClusterCount,
		MemberCount:  saveResult.MemberCount,
		RunID:        saveResult.RunID,
	}, nil
}

func threadIDPairKey(left, right int64) string {
	if left > right {
		left, right = right, left
	}
	return strconv.FormatInt(left, 10) + ":" + strconv.FormatInt(right, 10)
}
