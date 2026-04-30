package vector

import "testing"

func TestCosine(t *testing.T) {
	if got := Cosine([]float64{1, 0}, []float64{1, 0}); got != 1 {
		t.Fatalf("cosine same: got %f want 1", got)
	}
	if got := Cosine([]float64{1, 0}, []float64{0, 1}); got != 0 {
		t.Fatalf("cosine orthogonal: got %f want 0", got)
	}
}

func TestQuerySortsByScore(t *testing.T) {
	got := Query([]Item{
		{ThreadID: 1, Vector: []float64{1, 0}},
		{ThreadID: 2, Vector: []float64{0.5, 0.5}},
		{ThreadID: 3, Vector: []float64{0, 1}},
	}, []float64{1, 0}, 2, 0)
	if len(got) != 2 {
		t.Fatalf("neighbors: got %d want 2", len(got))
	}
	if got[0].ThreadID != 1 || got[1].ThreadID != 2 {
		t.Fatalf("order: %#v", got)
	}
}

func TestQueryAndCosineEdgeBranches(t *testing.T) {
	got := Query([]Item{
		{ThreadID: 2, Vector: []float64{1, 0}},
		{ThreadID: 1, Vector: []float64{1, 0}},
		{ThreadID: 3, Vector: []float64{-1, 0}},
	}, []float64{1, 0}, 0, 2)
	if len(got) != 1 || got[0].ThreadID != 1 {
		t.Fatalf("default limit/exclude/nonpositive score result = %#v", got)
	}
	if got := Cosine(nil, []float64{1}); got != 0 {
		t.Fatalf("nil cosine = %f", got)
	}
	if got := Cosine([]float64{1}, []float64{1, 2}); got != 0 {
		t.Fatalf("mismatch cosine = %f", got)
	}
	if got := Cosine([]float64{0}, []float64{1}); got != 0 {
		t.Fatalf("zero magnitude cosine = %f", got)
	}
}
