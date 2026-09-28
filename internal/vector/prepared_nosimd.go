//go:build !goexperiment.simd

package vector

func preparedDot(left, right []float64) float64 {
	return dotScalar(left, right)
}

func prepareInto(dst, values []float64) Prepared {
	return prepareScalar(dst, values)
}
