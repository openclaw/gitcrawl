package vector

import (
	"context"
	"math"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/openclaw/gitcrawl/internal/config"
)

// Lane reduction and FMA change rounding; normalized scores must agree within 1e-12.
func TestPreparedCosine(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 29))
	lengths := []int{1024, 1536, 3072}
	// Cover three times the maximum float32 lane count, including odd tails.
	for n := 0; n <= 3*16+7; n++ {
		lengths = append(lengths, n)
	}
	var maxDeviation, maxDotDeviation float64
	for _, n := range lengths {
		left, right := make([]float64, n), make([]float64, n)
		for trial := 0; trial < 24; trial++ {
			for i := range left {
				left[i], right[i] = rng.NormFloat64(), rng.NormFloat64()
				if trial%3 == 0 {
					right[i] = left[i] + right[i]*0.01
				}
				if trial%3 == 1 {
					right[i] = -left[i]
				}
				if trial%4 == 0 {
					left[i] *= 1e300
					right[i] *= 1e-300
				}
			}
			l, r := Prepare(left), Prepare(right)
			want, got := Cosine(left, right), l.Cosine(r)
			deviation := math.Abs(want - got)
			maxDeviation = max(maxDeviation, deviation)
			if math.IsNaN(got) || deviation > 1e-12 {
				t.Fatalf("n=%d trial=%d: got %.17g want %.17g", n, trial, got, want)
			}
			if n > 0 {
				scalar := dotScalar(l.values, r.values) / (l.magnitude * r.magnitude)
				actual := preparedDot(l.values, r.values) / (l.magnitude * r.magnitude)
				maxDotDeviation = max(maxDotDeviation, math.Abs(scalar-actual))
				if math.Abs(scalar-actual) > 1e-12 {
					t.Fatalf("dot n=%d: got %.17g want %.17g", n, actual, scalar)
				}
			}
		}
	}
	t.Logf("max cosine deviation from original: %.17g; dispatch-vs-scalar normalized dot: %.17g", maxDeviation, maxDotDeviation)
}

func TestPreparedSpecialValues(t *testing.T) {
	cases := [][]float64{nil, {}, {0}, {0, 0}, {1}, {-1}, {1, -1}, {math.MaxFloat64, -math.MaxFloat64}, {math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64}, {math.NaN(), 1}, {1, math.Inf(1)}, {math.Inf(-1), 1}}
	for _, left := range cases {
		for _, right := range cases {
			want, got := Cosine(left, right), Prepare(left).Cosine(Prepare(right))
			if math.IsNaN(got) || math.Abs(want-got) > 1e-12 {
				t.Fatalf("%v / %v: got %.17g want %.17g", left, right, got, want)
			}
		}
	}
	for n := 1; n <= 3*16+7; n++ {
		for i := 0; i < n; i++ {
			for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
				values := make([]float64, n)
				values[0], values[i] = 1, bad
				if got := Prepare(values).Cosine(Prepare(values)); got != 0 {
					t.Fatalf("n=%d index=%d: %g", n, i, got)
				}
			}
		}
	}
	values := []float64{1, 2, 3}
	prepared := Prepare(values)
	values[0] = math.NaN()
	if got := prepared.Cosine(Prepare([]float64{1, 2, 3})); math.Abs(got-1) > 1e-12 {
		t.Fatalf("Prepare aliases input: %g", got)
	}
	if got := preparedDot(nil, nil); got != 0 {
		t.Fatalf("empty dot = %g", got)
	}
	if got := preparedDot(make([]float64, 55), make([]float64, 55)); got != 0 {
		t.Fatalf("zero dot = %g", got)
	}
}

func TestPreparedQueryMatchesOriginal(t *testing.T) {
	items := benchmarkItems(256, 1024)
	query := items[0].Vector
	var want []Neighbor
	for _, item := range items[1:] {
		score := Cosine(query, item.Vector)
		if score > 0 {
			want = append(want, Neighbor{ThreadID: item.ThreadID, Score: score})
		}
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].Score == want[j].Score {
			return want[i].ThreadID < want[j].ThreadID
		}
		return want[i].Score > want[j].Score
	})
	want = want[:20]
	got := Query(items, query, 20, items[0].ThreadID)
	if len(got) != len(want) {
		t.Fatalf("neighbors: %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ThreadID != want[i].ThreadID || math.Abs(got[i].Score-want[i].Score) > 1e-12 {
			t.Fatalf("neighbor %d: %+v want %+v", i, got[i], want[i])
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queryExact(ctx, items, query, 20, 0); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
}

func benchmarkItems(count, dims int) []Item {
	rng := rand.New(rand.NewPCG(7, 11))
	items := make([]Item, count)
	for i := range items {
		values := make([]float64, dims)
		for j := range values {
			values[j] = rng.NormFloat64()
		}
		items[i] = Item{ThreadID: int64(i + 1), Vector: values}
	}
	return items
}

var benchmarkScore float64
var benchmarkNeighbors []Neighbor

func BenchmarkCosine(b *testing.B) {
	items := benchmarkItems(2, config.Default().OpenAI.EmbedDimensions)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkScore = Cosine(items[0].Vector, items[1].Vector)
	}
}

func BenchmarkPreparedCosine(b *testing.B) {
	items := benchmarkItems(2, config.Default().OpenAI.EmbedDimensions)
	left, right := Prepare(items[0].Vector), Prepare(items[1].Vector)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkScore = left.Cosine(right)
	}
}

func BenchmarkQueryExact20000(b *testing.B) {
	items := benchmarkItems(20000, config.Default().OpenAI.EmbedDimensions)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkNeighbors = Query(items, items[0].Vector, 20, items[0].ThreadID)
	}
}
