//go:build goexperiment.simd

package vector

import (
	"math"
	"simd"
)

// Emulation is slower than the scalar kernels.
var emulatedSIMD = simd.Emulated()

// Lane buffers cover 2048-bit vectors (arm64 SVE is planned for Go 1.28), since
// release builds enable this experiment and Store panics on a short slice.
const maxFloat64Lanes = 32

func preparedDot(left, right []float64) float64 {
	if emulatedSIMD {
		return dotScalar(left, right)
	}
	var a, b, c, d simd.Float64s
	lanes := a.Len()
	i := 0
	for ; i+4*lanes <= len(left); i += 4 * lanes {
		a = simd.LoadFloat64s(left[i:]).MulAdd(simd.LoadFloat64s(right[i:]), a)
		b = simd.LoadFloat64s(left[i+lanes:]).MulAdd(simd.LoadFloat64s(right[i+lanes:]), b)
		c = simd.LoadFloat64s(left[i+2*lanes:]).MulAdd(simd.LoadFloat64s(right[i+2*lanes:]), c)
		d = simd.LoadFloat64s(left[i+3*lanes:]).MulAdd(simd.LoadFloat64s(right[i+3*lanes:]), d)
	}
	for ; i+lanes <= len(left); i += lanes {
		a = simd.LoadFloat64s(left[i:]).MulAdd(simd.LoadFloat64s(right[i:]), a)
	}
	// Reduce once, outside the loop.
	var sums [maxFloat64Lanes]float64
	a.Add(b).Add(c.Add(d)).Store(sums[:])
	var dot float64
	for _, value := range sums[:lanes] {
		dot += value
	}
	for ; i < len(left); i++ {
		dot += left[i] * right[i]
	}
	return dot
}

func prepareInto(dst, values []float64) Prepared {
	if emulatedSIMD {
		return prepareScalar(dst, values)
	}
	var maxima simd.Float64s
	var invalid simd.Mask64s
	finiteLimit := simd.BroadcastFloat64s(math.MaxFloat64)
	lanes := maxima.Len()
	i := 0
	for ; i+lanes <= len(values); i += lanes {
		v := simd.LoadFloat64s(values[i:])
		abs := v.Abs()
		invalid = invalid.Or(v.NotEqual(v)).Or(abs.Greater(finiteLimit))
		maxima = maxima.Max(abs)
	}
	var maxValues [maxFloat64Lanes]float64
	var bad [maxFloat64Lanes]int64
	maxima.Store(maxValues[:])
	invalid.ToInt64s().Store(bad[:])
	var maxAbs float64
	for lane := 0; lane < lanes; lane++ {
		if bad[lane] != 0 {
			return Prepared{}
		}
		maxAbs = max(maxAbs, maxValues[lane])
	}
	for _, value := range values[i:] {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return Prepared{}
		}
		maxAbs = max(maxAbs, math.Abs(value))
	}
	if maxAbs == 0 {
		return Prepared{}
	}
	divisor := simd.BroadcastFloat64s(maxAbs)
	var a, b, c, d simd.Float64s
	i = 0
	for ; i+4*lanes <= len(values); i += 4 * lanes {
		v0 := simd.LoadFloat64s(values[i:]).Div(divisor)
		v1 := simd.LoadFloat64s(values[i+lanes:]).Div(divisor)
		v2 := simd.LoadFloat64s(values[i+2*lanes:]).Div(divisor)
		v3 := simd.LoadFloat64s(values[i+3*lanes:]).Div(divisor)
		v0.Store(dst[i:])
		v1.Store(dst[i+lanes:])
		v2.Store(dst[i+2*lanes:])
		v3.Store(dst[i+3*lanes:])
		a = v0.MulAdd(v0, a)
		b = v1.MulAdd(v1, b)
		c = v2.MulAdd(v2, c)
		d = v3.MulAdd(v3, d)
	}
	for ; i+lanes <= len(values); i += lanes {
		v := simd.LoadFloat64s(values[i:]).Div(divisor)
		v.Store(dst[i:])
		a = v.MulAdd(v, a)
	}
	var sums [maxFloat64Lanes]float64
	a.Add(b).Add(c.Add(d)).Store(sums[:])
	var magnitude float64
	for _, value := range sums[:lanes] {
		magnitude += value
	}
	for ; i < len(values); i++ {
		scaled := values[i] / maxAbs
		dst[i] = scaled
		magnitude += scaled * scaled
	}
	return Prepared{values: dst[:len(values)], magnitude: math.Sqrt(magnitude)}
}
