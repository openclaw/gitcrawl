package vector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	crawlvector "github.com/openclaw/crawlkit/vector"
)

type Item struct {
	ThreadID int64
	Vector   []float64
}

type Neighbor struct {
	ThreadID int64   `json:"thread_id"`
	Score    float64 `json:"score"`
}

type QueryOptions struct {
	Backend         string
	Limit           int
	ExcludeThreadID int64
	TurboVec        crawlvector.TurboVecOptions
}

const maxTurboVecTieCandidates = 16 * 1024

func Query(items []Item, query []float64, limit int, excludeThreadID int64) []Neighbor {
	neighbors, _ := queryExact(context.Background(), items, query, limit, excludeThreadID)
	return neighbors
}

func QueryWithOptions(ctx context.Context, items []Item, query []float64, opts QueryOptions) ([]Neighbor, error) {
	backend := crawlvector.SearchBackend(strings.ToLower(strings.TrimSpace(opts.Backend)))
	if backend == "" {
		backend = crawlvector.BackendExact
	}
	switch backend {
	case crawlvector.BackendExact:
		return queryExact(ctx, items, query, opts.Limit, opts.ExcludeThreadID)
	case crawlvector.BackendTurboVec:
		return queryTurboVec(ctx, items, query, opts)
	default:
		return nil, fmt.Errorf("unsupported vector backend %q", opts.Backend)
	}
}

func queryExact(ctx context.Context, items []Item, query []float64, limit int, excludeThreadID int64) ([]Neighbor, error) {
	if limit <= 0 {
		limit = 20
	}
	if err := validateExactQuery(query); err != nil {
		return nil, err
	}
	scored := make([]crawlvector.Scored[Neighbor], 0, len(items))
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if item.ThreadID == excludeThreadID {
			continue
		}
		score := Cosine(query, item.Vector)
		if math.IsNaN(score) || math.IsInf(score, 0) || score <= 0 {
			continue
		}
		neighbor := Neighbor{ThreadID: item.ThreadID, Score: score}
		scored = append(scored, crawlvector.Scored[Neighbor]{Item: neighbor, Score: score})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	top := crawlvector.TopK(scored, limit, func(left, right Neighbor) bool {
		return left.ThreadID < right.ThreadID
	})
	out := make([]Neighbor, len(top))
	for i, item := range top {
		out[i] = item.Item
	}
	return out, nil
}

func queryTurboVec(ctx context.Context, items []Item, query []float64, opts QueryOptions) ([]Neighbor, error) {
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	candidateCount := 0
	for _, item := range items {
		if item.ThreadID == opts.ExcludeThreadID {
			continue
		}
		candidateCount++
	}
	if shouldFallbackToExact(len(query), candidateCount, opts.TurboVec.MaxInputBytes) {
		return queryExact(ctx, items, query, opts.Limit, opts.ExcludeThreadID)
	}
	candidates := make([]crawlvector.SearchCandidate[Neighbor], 0, len(items))
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if item.ThreadID == opts.ExcludeThreadID {
			continue
		}
		candidates = append(candidates, crawlvector.SearchCandidate[Neighbor]{
			Item:   Neighbor{ThreadID: item.ThreadID},
			Vector: float64To32(item.Vector),
		})
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	results, err := crawlvector.Search(ctx, float64To32(query), candidates, crawlvector.SearchOptions[Neighbor]{
		Backend:       crawlvector.BackendTurboVec,
		Limit:         opts.Limit,
		InvalidVector: crawlvector.InvalidVectorSkip,
		TurboVec:      opts.TurboVec,
		TieLess: func(left, right Neighbor) bool {
			return left.ThreadID < right.ThreadID
		},
	})
	if err != nil {
		return nil, err
	}
	out := make([]Neighbor, 0, len(results))
	for _, result := range results {
		if math.IsNaN(result.Score) || math.IsInf(result.Score, 0) || result.Score <= 0 {
			continue
		}
		neighbor := result.Item
		neighbor.Score = result.Score
		out = append(out, neighbor)
	}
	return out, nil
}

func float64To32(values []float64) []float32 {
	out := make([]float32, len(values))
	for i, value := range values {
		out[i] = float32(value)
	}
	return out
}

func validateExactQuery(query []float64) error {
	if len(query) == 0 {
		return errors.New("query vector is empty")
	}
	var maxAbs float64
	for i, value := range query {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("query vector contains non-finite value at index %d", i)
		}
		maxAbs = max(maxAbs, math.Abs(value))
	}
	if maxAbs == 0 {
		return errors.New("query vector is zero")
	}
	var magnitude float64
	for _, value := range query {
		scaled := value / maxAbs
		magnitude += scaled * scaled
	}
	if magnitude == 0 {
		return errors.New("query vector is zero")
	}
	return nil
}

func shouldFallbackToExact(dimensions, candidateCount int, maxInputBytes int64) bool {
	if candidateCount > maxTurboVecTieCandidates {
		return true
	}
	effectiveMaxInput := maxInputBytes
	if effectiveMaxInput == 0 {
		effectiveMaxInput = crawlvector.DefaultTurboVecMaxInputSize
	}
	if effectiveMaxInput <= 0 || dimensions <= 0 || candidateCount <= 0 {
		return false
	}
	const baseJSONBudget = int64(256)
	const floatJSONBudget = int64(16)
	estimate := baseJSONBudget + int64(dimensions)*int64(candidateCount+1)*floatJSONBudget + int64(candidateCount*4)
	return estimate > effectiveMaxInput
}

func Cosine(left, right []float64) float64 {
	if len(left) == 0 || len(left) != len(right) {
		return 0
	}
	var leftMaxAbs, rightMaxAbs float64
	for index := range left {
		leftValue := left[index]
		rightValue := right[index]
		if math.IsNaN(leftValue) || math.IsNaN(rightValue) || math.IsInf(leftValue, 0) || math.IsInf(rightValue, 0) {
			return 0
		}
		leftMaxAbs = max(leftMaxAbs, math.Abs(leftValue))
		rightMaxAbs = max(rightMaxAbs, math.Abs(rightValue))
	}
	if leftMaxAbs == 0 || rightMaxAbs == 0 {
		return 0
	}
	var dot, leftMag, rightMag float64
	for index := range left {
		leftValue := left[index] / leftMaxAbs
		rightValue := right[index] / rightMaxAbs
		dot += leftValue * rightValue
		leftMag += leftValue * leftValue
		rightMag += rightValue * rightValue
	}
	if leftMag == 0 || rightMag == 0 {
		return 0
	}
	score := dot / (math.Sqrt(leftMag) * math.Sqrt(rightMag))
	if score > 1 {
		return 1
	}
	if score < -1 {
		return -1
	}
	return score
}
