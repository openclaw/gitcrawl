package cli

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	clusterer "github.com/openclaw/gitcrawl/internal/cluster"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/vector"
)

type clusterBuildOptions struct {
	Threshold          float64
	MinSize            int
	MaxClusterSize     int
	Fanout             int
	CrossKindThreshold float64
	RetireMissing      bool
}

func parseClusterShapeOptions(command, maxClusterSizeRaw, fanoutRaw, crossKindThresholdRaw string) (int, int, float64, error) {
	maxClusterSize, err := parseOptionalPositiveInt(maxClusterSizeRaw)
	if err != nil {
		return 0, 0, 0, err
	}
	fanout, err := parseOptionalPositiveInt(fanoutRaw)
	if err != nil {
		return 0, 0, 0, err
	}
	crossKindThreshold, err := parseOptionalFloat(crossKindThresholdRaw)
	if err != nil {
		return 0, 0, 0, err
	}
	if maxClusterSize == 0 {
		maxClusterSize = defaultClusterMaxSize
	}
	if fanout == 0 {
		fanout = defaultClusterFanout
	}
	if crossKindThreshold == 0 {
		crossKindThreshold = defaultCrossKindMinScore
	}
	if crossKindThreshold < 0 || crossKindThreshold > 1 {
		return 0, 0, 0, fmt.Errorf("%s requires --cross-kind-threshold between 0 and 1", command)
	}
	return maxClusterSize, fanout, crossKindThreshold, nil
}

func buildDurableClusterInputs(ctx context.Context, st *store.Store, repoID int64, storedVectors []store.ThreadVector, options clusterBuildOptions) ([]store.DurableClusterInput, int, error) {
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
	threads, err := st.ThreadsByIDs(ctx, repoID, threadIDs)
	if err != nil {
		return nil, 0, err
	}
	nodes := make([]clusterer.Node, 0, len(storedVectors))
	for _, stored := range storedVectors {
		thread, ok := threads[stored.ThreadID]
		if !ok {
			continue
		}
		nodes = append(nodes, clusterer.Node{ThreadID: stored.ThreadID, Number: thread.Number, Title: thread.Title})
	}
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
	repoFullName, err := repositoryFullNameByID(ctx, st, repoID)
	if err != nil {
		return nil, 0, err
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

func repositoryFullNameByID(ctx context.Context, st *store.Store, repoID int64) (string, error) {
	repositories, err := st.ListRepositories(ctx)
	if err != nil {
		return "", err
	}
	for _, repo := range repositories {
		if repo.ID == repoID {
			return repo.FullName, nil
		}
	}
	return "", fmt.Errorf("repository id %d not found", repoID)
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
