//go:build native_cluster_overlay

package main

import "math"

// Hoist the native per-vector maximum/divisions/squared norm out of the pair
// loop. Dot-product ordering, denominator and final clamping remain identical.
type preparedVector struct {
	Values    []float64
	Magnitude float64
}

func prepareVector(v []float64) preparedVector {
	var maxAbs float64
	for _, x := range v {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return preparedVector{}
		}
		maxAbs = max(maxAbs, math.Abs(x))
	}
	if maxAbs == 0 {
		return preparedVector{}
	}
	p := preparedVector{Values: make([]float64, len(v))}
	var mag float64
	for i, x := range v {
		n := x / maxAbs
		p.Values[i] = n
		mag += n * n
	}
	p.Magnitude = math.Sqrt(mag)
	return p
}
func preparedCosine(left, right preparedVector) float64 {
	if len(left.Values) == 0 || len(left.Values) != len(right.Values) {
		return 0
	}
	var dot float64
	for i, x := range left.Values {
		dot += x * right.Values[i]
	}
	score := dot / (left.Magnitude * right.Magnitude)
	if score > 1 {
		return 1
	}
	if score < -1 {
		return -1
	}
	return score
}
