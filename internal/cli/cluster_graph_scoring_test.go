package cli

import (
	clusterer "github.com/openclaw/gitcrawl/internal/cluster"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/vector"
	"math"
	"testing"
)

func TestPreparedClusterEdgesMatchOriginal(t *testing.T) {
	nodes, threads, vectors := clusterBenchmarkData(96)
	vectors[1] = []float64{math.NaN()}
	vectors[2] = nil
	vectors[3] = []float64{0}
	vectors[4] = []float64{1, 2}
	vectors[5] = []float64{math.MaxFloat64, math.MaxFloat64}
	vectors[6] = []float64{math.SmallestNonzeroFloat64, math.SmallestNonzeroFloat64}
	var maxDeviation float64
	for _, threshold := range []float64{-1, 0, 0.3, 0.9, 1} {
		opts := clusterBuildOptions{Threshold: threshold, CrossKindThreshold: defaultCrossKindMinScore}
		got, want := scoreClusterEdges(nodes, threads, vectors, opts), originalClusterEdges(nodes, threads, vectors, opts)
		if len(got) != len(want) {
			t.Fatalf("threshold %g: %d edges want %d", threshold, len(got), len(want))
		}
		for pair, expected := range want {
			actual, ok := got[pair]
			deviation := math.Abs(actual.Score - expected.Score)
			maxDeviation = max(maxDeviation, deviation)
			if !ok || actual.LeftThreadID != expected.LeftThreadID || actual.RightThreadID != expected.RightThreadID || deviation > 1e-12 {
				t.Fatalf("threshold %g pair %s: %+v want %+v", threshold, pair, actual, expected)
			}
		}
	}
	t.Logf("max cluster-edge score deviation: %.17g", maxDeviation)
}

func originalClusterEdges(nodes []clusterer.Node, threads map[int64]store.Thread, vectorByThreadID map[int64][]float64, options clusterBuildOptions) map[string]clusterer.Edge {
	candidateByPair := map[string]clusterer.Edge{}
	for left := 0; left < len(nodes); left++ {
		for right := left + 1; right < len(nodes); right++ {
			leftID := nodes[left].ThreadID
			rightID := nodes[right].ThreadID
			score := vector.Cosine(vectorByThreadID[leftID], vectorByThreadID[rightID])
			if score < options.Threshold {
				continue
			}
			if score < highConfidenceEdgeScore && titleTokenOverlap(threads[leftID].Title, threads[rightID].Title) < weakEdgeMinTitleOverlap {
				continue
			}
			if threads[leftID].Kind != threads[rightID].Kind && score < options.CrossKindThreshold {
				continue
			}
			upsertClusterEdge(candidateByPair, leftID, rightID, score)
		}
	}
	return candidateByPair
}
