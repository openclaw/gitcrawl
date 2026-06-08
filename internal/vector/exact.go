package vector

import (
	"context"
	"math"

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

func Query(items []Item, query []float64, limit int, excludeThreadID int64) []Neighbor {
	neighbors, _ := QueryWithOptions(context.Background(), items, query, QueryOptions{
		Backend:         crawlvector.BackendExact,
		Limit:           limit,
		ExcludeThreadID: excludeThreadID,
	})
	return neighbors
}

func QueryWithOptions(ctx context.Context, items []Item, query []float64, opts QueryOptions) ([]Neighbor, error) {
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	candidates := make([]crawlvector.SearchCandidate[Neighbor], 0, len(items))
	for _, item := range items {
		if item.ThreadID == opts.ExcludeThreadID {
			continue
		}
		candidates = append(candidates, crawlvector.SearchCandidate[Neighbor]{
			Item:   Neighbor{ThreadID: item.ThreadID},
			Vector: crawlvector.Float64To32(item.Vector),
		})
	}
	results, err := crawlvector.Search(ctx, crawlvector.Float64To32(query), candidates, crawlvector.SearchOptions[Neighbor]{
		Backend:       opts.Backend,
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
