package vector

import (
	"math"
	"sort"
)

type Item struct {
	ThreadID int64
	Vector   []float64
}

type Neighbor struct {
	ThreadID int64   `json:"thread_id"`
	Score    float64 `json:"score"`
}

func Query(items []Item, query []float64, limit int, excludeThreadID int64) []Neighbor {
	if limit <= 0 {
		limit = 20
	}
	out := make([]Neighbor, 0, len(items))
	for _, item := range items {
		if item.ThreadID == excludeThreadID {
			continue
		}
		score := Cosine(query, item.Vector)
		if score <= 0 {
			continue
		}
		out = append(out, Neighbor{ThreadID: item.ThreadID, Score: score})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].ThreadID < out[j].ThreadID
		}
		return out[i].Score > out[j].Score
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func Cosine(left, right []float64) float64 {
	if len(left) == 0 || len(left) != len(right) {
		return 0
	}
	var dot, leftMag, rightMag float64
	for index := range left {
		dot += left[index] * right[index]
		leftMag += left[index] * left[index]
		rightMag += right[index] * right[index]
	}
	if leftMag == 0 || rightMag == 0 {
		return 0
	}
	return dot / (math.Sqrt(leftMag) * math.Sqrt(rightMag))
}
