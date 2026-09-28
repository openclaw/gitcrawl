package vector

import "math"

// Prepared holds an immutable, validated copy of a vector for repeated scoring.
// Its zero value scores zero against every vector.
type Prepared struct {
	values    []float64
	magnitude float64
}

// Prepare scales by max-abs before computing the magnitude, so even very large
// or subnormal finite inputs are safe. Empty, zero and non-finite inputs are invalid.
func Prepare(values []float64) Prepared {
	return prepareInto(make([]float64, len(values)), values)
}

func prepareScalar(dst, values []float64) Prepared {
	var maxAbs float64
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return Prepared{}
		}
		maxAbs = max(maxAbs, math.Abs(value))
	}
	if maxAbs == 0 {
		return Prepared{}
	}
	var magnitude float64
	for i, value := range values {
		scaled := value / maxAbs
		dst[i] = scaled
		magnitude += scaled * scaled
	}
	return Prepared{values: dst[:len(values)], magnitude: math.Sqrt(magnitude)}
}

// Cosine returns zero for invalid or mismatched vectors and clamps to [-1, 1].
func (left Prepared) Cosine(right Prepared) float64 {
	if len(left.values) == 0 || len(left.values) != len(right.values) {
		return 0
	}
	return max(-1, min(1, preparedDot(left.values, right.values)/(left.magnitude*right.magnitude)))
}

func dotScalar(left, right []float64) float64 {
	var dot float64
	for i, value := range left {
		dot += value * right[i]
	}
	return dot
}
