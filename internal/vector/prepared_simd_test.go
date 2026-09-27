//go:build goexperiment.simd

package vector

import (
	"simd"
	"testing"
)

func TestSIMDMode(t *testing.T) {
	var lanes simd.Float64s
	t.Logf("float64 lanes=%d emulated=%t", lanes.Len(), emulatedSIMD)
	if !emulatedSIMD {
		return
	}
	items := benchmarkItems(2, 1024)
	left := Prepare(items[0].Vector)
	right := Prepare(items[1].Vector)
	if got, want := left.Cosine(right), Cosine(items[0].Vector, items[1].Vector); got != want {
		t.Fatalf("emulated fallback: got %.17g want scalar %.17g", got, want)
	}
}
