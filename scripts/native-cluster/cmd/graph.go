//go:build native_cluster_overlay

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	clusterer "github.com/openclaw/gitcrawl/internal/cluster"
	"github.com/openclaw/gitcrawl/internal/store"
)

type clusterBuildOptions struct {
	Threshold          float64
	MinSize            int
	MaxClusterSize     int
	Fanout             int
	CrossKindThreshold float64
	RetireMissing      bool
}

type similarityCache struct {
	recomputed   int64
	reused       int64
	fingerprints map[int64]string
	edges        map[string]clusterer.Edge
}

func buildDurableClusterInputs(ctx context.Context, repoID int64, storedVectors []store.ThreadVector, threads map[int64]store.Thread, repoFullName string, options clusterBuildOptions) ([]store.DurableClusterInput, int, error) {
	return buildWithCache(ctx, repoID, storedVectors, threads, repoFullName, options, &similarityCache{})
}
func buildWithCache(ctx context.Context, repoID int64, storedVectors []store.ThreadVector, threads map[int64]store.Thread, repoFullName string, options clusterBuildOptions, cache *similarityCache) ([]store.DurableClusterInput, int, error) {
	if options.MinSize <= 0 {
		options.MinSize = 1
	}
	if options.MaxClusterSize <= 0 {
		options.MaxClusterSize = defaultClusterMaxSize
	}
	if options.Fanout <= 0 {
		options.Fanout = defaultClusterFanout
	}
	if options.CrossKindThreshold <= 0 {
		options.CrossKindThreshold = defaultCrossKindMinScore
	}
	threadIDs := make([]int64, 0, len(storedVectors))
	vectorByThreadID := make(map[int64][]float64, len(storedVectors))
	for _, stored := range storedVectors {
		threadIDs = append(threadIDs, stored.ThreadID)
		vectorByThreadID[stored.ThreadID] = stored.Vector
	}

	nodes := make([]clusterer.Node, 0, len(storedVectors))
	for _, stored := range storedVectors {
		thread, ok := threads[stored.ThreadID]
		if !ok {
			continue
		}
		nodes = append(nodes, clusterer.Node{ThreadID: stored.ThreadID, Number: thread.Number, Title: thread.Title})
	}
	prepared := map[int64]preparedVector{}
	for id, vector := range vectorByThreadID {
		prepared[id] = prepareVector(vector)
	}

	fingerprints := map[int64]string{}
	changed := map[int64]bool{}
	for _, v := range storedVectors {
		t := threads[v.ThreadID]
		raw, err := json.Marshal([]any{v.ThreadID, v.ContentHash, v.Model, v.Basis, v.Dimensions, v.Vector, t.GitHubID, t.Number, t.Kind, t.Title, options.Threshold, options.CrossKindThreshold})
		if err != nil {
			return nil, 0, err
		}
		sum := sha256.Sum256(raw)
		fingerprints[v.ThreadID] = hex.EncodeToString(sum[:])
		changed[v.ThreadID] = cache.fingerprints[v.ThreadID] != fingerprints[v.ThreadID]
	}
	candidateByPair := map[string]clusterer.Edge{}
	for key, edge := range cache.edges {
		_, left := fingerprints[edge.LeftThreadID]
		_, right := fingerprints[edge.RightThreadID]
		if left && right && !changed[edge.LeftThreadID] && !changed[edge.RightThreadID] {
			candidateByPair[key] = edge
		}
	}

	var recomputed, reused int64
	for left := 0; left < len(nodes); left++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		for right := left + 1; right < len(nodes); right++ {
			leftID := nodes[left].ThreadID
			rightID := nodes[right].ThreadID
			if !changed[leftID] && !changed[rightID] {
				reused++
				continue
			}
			recomputed++
			score := preparedCosine(prepared[leftID], prepared[rightID])
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

	cache.recomputed = recomputed
	cache.reused = reused
	cache.fingerprints = fingerprints
	cache.edges = map[string]clusterer.Edge{}
	for key, edge := range candidateByPair {
		cache.edges[key] = edge
	}
	addDeterministicReferenceEdges(candidateByPair, nodes, threads, repoFullName)
	candidates := make([]clusterer.Edge, 0, len(candidateByPair))
	for _, edge := range candidateByPair {
		candidates = append(candidates, edge)
	}
	edges := keepTopEdges(candidates, options.Fanout)
	pairScores := map[string]float64{}
	for _, edge := range edges {
		pairScores[threadIDPairKey(edge.LeftThreadID, edge.RightThreadID)] = edge.Score
	}
	built := clusterer.BuildWithOptions(nodes, edges, clusterer.Options{MaxSize: options.MaxClusterSize})
	inputs := make([]store.DurableClusterInput, 0, len(built))
	for _, builtCluster := range built {
		if len(builtCluster.Members) < options.MinSize {
			continue
		}
		sort.Slice(builtCluster.Members, func(i, j int) bool {
			left := threads[builtCluster.Members[i]]
			right := threads[builtCluster.Members[j]]
			return left.Number < right.Number
		})
		identity := store.HumanKeyForValue(fmt.Sprintf("repo:%d:cluster-representative:%d", repoID, builtCluster.RepresentativeThreadID))
		clusterType := "duplicate_candidate"
		if len(builtCluster.Members) == 1 {
			clusterType = "singleton_orphan"
		}
		input := store.DurableClusterInput{
			StableKey:              identity.Hash,
			StableSlug:             store.HumanKeyStableSlug(identity),
			ClusterType:            clusterType,
			RepresentativeThreadID: builtCluster.RepresentativeThreadID,
			Title:                  "Cluster " + identity.Slug,
			Members:                make([]store.DurableClusterMemberInput, 0, len(builtCluster.Members)),
		}
		for _, threadID := range builtCluster.Members {
			role := "related"
			var scorePtr *float64
			if threadID == builtCluster.RepresentativeThreadID {
				role = "canonical"
				scoreCopy := 1.0
				scorePtr = &scoreCopy
			} else if score, ok := pairScores[threadIDPairKey(threadID, builtCluster.RepresentativeThreadID)]; ok {
				scoreCopy := score
				scorePtr = &scoreCopy
			}
			input.Members = append(input.Members, store.DurableClusterMemberInput{ThreadID: threadID, Role: role, ScoreToRepresentative: scorePtr})
		}
		inputs = append(inputs, input)
	}
	return inputs, len(edges), nil
}

func upsertClusterEdge(edges map[string]clusterer.Edge, leftID, rightID int64, score float64) {
	if leftID == rightID {
		return
	}
	key := threadIDPairKey(leftID, rightID)
	if existing, ok := edges[key]; ok && existing.Score >= score {
		return
	}
	if leftID > rightID {
		leftID, rightID = rightID, leftID
	}
	edges[key] = clusterer.Edge{LeftThreadID: leftID, RightThreadID: rightID, Score: score}
}

func addDeterministicReferenceEdges(edges map[string]clusterer.Edge, nodes []clusterer.Node, threads map[int64]store.Thread, repoFullName string) {
	threadIDByNumber := make(map[int]int64, len(nodes))
	for _, node := range nodes {
		thread := threads[node.ThreadID]
		threadIDByNumber[thread.Number] = node.ThreadID
	}
	refIDsByThreadID := make(map[int64]map[int64]bool, len(nodes))
	for _, node := range nodes {
		thread := threads[node.ThreadID]
		refNumbers := referencedThreadNumbersByLocation(thread, repoFullName)
		refIDs := map[int64]bool{}
		for number, evidence := range refNumbers {
			if referencedID, ok := threadIDByNumber[number]; ok && referencedID != node.ThreadID {
				referencedThread := threads[referencedID]
				if evidence.Title || evidence.EarlyBody || titleTokenOverlap(thread.Title, referencedThread.Title) >= weakEdgeMinTitleOverlap {
					refIDs[referencedID] = true
				}
			}
		}
		refIDsByThreadID[node.ThreadID] = refIDs
	}
	for threadID, refIDs := range refIDsByThreadID {
		for referencedID := range refIDs {
			upsertClusterEdge(edges, threadID, referencedID, deterministicRefScore)
		}
	}
}

func referencedThreadNumbersByLocation(thread store.Thread, repoFullName string) map[int]referenceEvidence {
	refs := map[int]referenceEvidence{}
	collectReferencedThreadNumbers(refs, thread.Number, thread.Body, false, repoFullName)
	collectReferencedThreadNumbers(refs, thread.Number, thread.Title, true, repoFullName)
	return refs
}

func collectReferencedThreadNumbers(refs map[int]referenceEvidence, threadNumber int, value string, titleRef bool, repoFullName string) {
	for _, match := range threadReferencePattern.FindAllStringSubmatchIndex(value, -1) {
		numberText := ""
		if match[2] >= 0 {
			refRepo := value[match[2]:match[3]]
			if !strings.EqualFold(refRepo, repoFullName) {
				continue
			}
			numberText = value[match[4]:match[5]]
		} else if match[8] >= 0 {
			if match[6] >= 0 {
				refRepo := value[match[6]:match[7]]
				if !strings.EqualFold(refRepo, repoFullName) {
					continue
				}
			}
			numberText = value[match[8]:match[9]]
		} else if match[10] >= 0 {
			numberText = value[match[10]:match[11]]
		}
		number, err := strconv.Atoi(numberText)
		if err != nil || number <= 0 || number == threadNumber {
			continue
		}
		evidence := refs[number]
		if titleRef {
			evidence.Title = true
		} else if match[0] <= bodyRefEvidencePrefixChars {
			evidence.EarlyBody = true
		}
		refs[number] = evidence
	}
}

func titleTokenOverlap(left, right string) float64 {
	leftTokens := titleTokenSet(left)
	rightTokens := titleTokenSet(right)
	if len(leftTokens) == 0 || len(rightTokens) == 0 {
		return 0
	}
	overlap := 0
	for token := range leftTokens {
		if rightTokens[token] {
			overlap++
		}
	}
	base := len(leftTokens)
	if len(rightTokens) < base {
		base = len(rightTokens)
	}
	return float64(overlap) / float64(base)
}

func titleTokenSet(value string) map[string]bool {
	out := map[string]bool{}
	for _, token := range titleTokenPattern.FindAllString(strings.ToLower(value), -1) {
		out[token] = true
	}
	return out
}

func keepTopEdges(edges []clusterer.Edge, fanout int) []clusterer.Edge {
	if fanout <= 0 || len(edges) == 0 {
		return edges
	}
	neighbors := map[int64][]clusterer.Edge{}
	for _, edge := range edges {
		neighbors[edge.LeftThreadID] = append(neighbors[edge.LeftThreadID], edge)
		neighbors[edge.RightThreadID] = append(neighbors[edge.RightThreadID], edge)
	}
	top := map[int64]map[int64]bool{}
	for threadID, list := range neighbors {
		sort.SliceStable(list, func(i, j int) bool {
			if list[i].Score == list[j].Score {
				return edgeOtherThreadID(list[i], threadID) < edgeOtherThreadID(list[j], threadID)
			}
			return list[i].Score > list[j].Score
		})
		if len(list) > fanout {
			list = list[:fanout]
		}
		seen := make(map[int64]bool, len(list))
		for _, edge := range list {
			seen[edgeOtherThreadID(edge, threadID)] = true
		}
		top[threadID] = seen
	}
	out := make([]clusterer.Edge, 0, len(edges))
	for _, edge := range edges {
		if top[edge.LeftThreadID][edge.RightThreadID] || top[edge.RightThreadID][edge.LeftThreadID] {
			out = append(out, edge)
		}
	}
	return out
}

func edgeOtherThreadID(edge clusterer.Edge, threadID int64) int64 {
	if edge.LeftThreadID == threadID {
		return edge.RightThreadID
	}
	return edge.LeftThreadID
}

var threadReferencePattern = regexp.MustCompile(`(?i)(?:\b([\w.-]+/[\w.-]+)#(\d+)|(?:\b([\w.-]+/[\w.-]+)/)?(?:issues|pull)/(\d+)|#(\d{2,}))`)

var titleTokenPattern = regexp.MustCompile(`[A-Za-z0-9]{4,}`)

type referenceEvidence struct {
	Title     bool
	EarlyBody bool
}

const (
	defaultClusterMaxSize      = 40
	defaultClusterFanout       = 16
	defaultCrossKindMinScore   = .93
	highConfidenceEdgeScore    = .90
	weakEdgeMinTitleOverlap    = .18
	deterministicRefScore      = .94
	bodyRefEvidencePrefixChars = 240
)

func threadIDPairKey(left, right int64) string {
	if left > right {
		left, right = right, left
	}
	return strconv.FormatInt(left, 10) + ":" + strconv.FormatInt(right, 10)
}
