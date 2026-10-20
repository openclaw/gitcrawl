//go:build native_cluster_overlay

package main

import (
	"encoding/json"
	"github.com/openclaw/gitcrawl/internal/vector"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestPreparedCosineExact(t *testing.T) {
	r := rand.New(rand.NewSource(73))
	vectors := [][]float64{nil, {0, 0}, {1}, {1, 2}, {1, math.NaN()}, {1, math.Inf(1)}, {math.MaxFloat64, math.SmallestNonzeroFloat64}, {1e-250, -1e250}}
	for i := 0; i < 120; i++ {
		a := make([]float64, 1536)
		scale := math.Pow10(i%601 - 300)
		for j := range a {
			a[j] = (r.Float64()*2 - 1) * scale
		}
		vectors = append(vectors, a)
	}
	for _, a := range vectors {
		for _, b := range vectors {
			want := vector.Cosine(a, b)
			got := preparedCosine(prepareVector(a), prepareVector(b))
			if math.Float64bits(want) != math.Float64bits(got) {
				t.Fatalf("cosine differs %.18g %.18g", want, got)
			}
		}
	}
}
func TestPreparedCPU(t *testing.T) {
	if os.Getenv("GITCRAWL_CLUSTER_CPU") != "1" {
		t.Skip("bounded explicit synthetic benchmark")
	}
	r := rand.New(rand.NewSource(91))
	v := make([]preparedVector, 9389)
	for i := range v {
		a := make([]float64, 1536)
		for j := range a {
			a[j] = r.Float64()*2 - 1
		}
		v[i] = prepareVector(a)
	}
	started := time.Now()
	n := 0
	sum := 0.0
	for i := range v {
		for j := i + 1; j < len(v); j++ {
			sum += preparedCosine(v[i], v[j])
			n++
			if n == 524288 {
				goto done
			}
		}
	}
done:
	b, _ := json.Marshal(map[string]any{"synthetic_only": true, "nodes": len(v), "dimensions": 1536, "measured_pairs": n, "seconds": time.Since(started).Seconds(), "projected_full_pair_seconds": time.Since(started).Seconds() * 44071966 / float64(n), "checksum": sum})
	if e := os.WriteFile(os.Getenv("GITCRAWL_CLUSTER_CPU_RECEIPT"), b, 0600); e != nil {
		t.Fatal(e)
	}
}
