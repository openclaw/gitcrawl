package vector

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"

	crawlvector "github.com/openclaw/crawlkit/vector"
)

func TestCosine(t *testing.T) {
	if got := Cosine([]float64{1, 0}, []float64{1, 0}); got != 1 {
		t.Fatalf("cosine same: got %f want 1", got)
	}
	if got := Cosine([]float64{1, 0}, []float64{0, 1}); got != 0 {
		t.Fatalf("cosine orthogonal: got %f want 0", got)
	}
}

func TestCosineLargeFiniteVectors(t *testing.T) {
	large := math.MaxFloat64 / 4
	if got := Cosine([]float64{large, large}, []float64{large, large}); math.Abs(got-1) > 1e-12 {
		t.Fatalf("large same cosine = %.17g, want 1", got)
	}
	if got := Cosine([]float64{large, -large}, []float64{-large, large}); math.Abs(got+1) > 1e-12 {
		t.Fatalf("large opposite cosine = %.17g, want -1", got)
	}
	if got := Cosine([]float64{large, 0}, []float64{0, large}); got != 0 {
		t.Fatalf("large orthogonal cosine = %.17g, want 0", got)
	}
	if got := Cosine([]float64{1e-100}, []float64{1e150}); math.Abs(got-1) > 1e-12 {
		t.Fatalf("mixed-scale cosine = %.17g, want 1", got)
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

func TestQueryFiltersNonFiniteScores(t *testing.T) {
	got := Query([]Item{
		{ThreadID: 1, Vector: []float64{math.NaN()}},
		{ThreadID: 2, Vector: []float64{math.Inf(1)}},
		{ThreadID: 3, Vector: []float64{1}},
	}, []float64{1}, 10, 0)
	if len(got) != 1 || got[0].ThreadID != 3 || math.IsNaN(got[0].Score) || math.IsInf(got[0].Score, 0) {
		t.Fatalf("neighbors = %#v, want only finite thread 3", got)
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

func TestQueryWithTurboVecBackend(t *testing.T) {
	t.Setenv("CRAWLKIT_TEST_TURBOVEC_HELPER", "1")
	got, err := QueryWithOptions(context.Background(), []Item{
		{ThreadID: 1, Vector: []float64{1, 0, 0, 0, 0, 0, 0, 0}},
		{ThreadID: 2, Vector: []float64{0.8, 0.2, 0, 0, 0, 0, 0, 0}},
	}, []float64{1, 0, 0, 0, 0, 0, 0, 0}, QueryOptions{
		Backend: "turbovec",
		Limit:   2,
		TurboVec: crawlvector.TurboVecOptions{
			Command: []string{os.Args[0], "-test.run=TestTurboVecHelperProcess", "--"},
		},
	})
	if err != nil {
		t.Fatalf("query turbovec: %v", err)
	}
	if len(got) != 2 || got[0].ThreadID != 2 || got[0].Score != 0.9 || got[1].ThreadID != 1 {
		t.Fatalf("neighbors = %#v", got)
	}
}

func TestQueryWithOptionsExactRejectsInvalidQuery(t *testing.T) {
	_, err := QueryWithOptions(context.Background(), []Item{{ThreadID: 1, Vector: []float64{1, 0}}}, []float64{0, 0}, QueryOptions{Backend: "exact"})
	if err == nil || !strings.Contains(err.Error(), "query vector is zero") {
		t.Fatalf("zero query err = %v", err)
	}
}

func TestQueryWithTurboVecFallsBackToExactWhenRequestIsTooLarge(t *testing.T) {
	got, err := QueryWithOptions(context.Background(), []Item{
		{ThreadID: 1, Vector: []float64{1, 0, 0, 0, 0, 0, 0, 0}},
		{ThreadID: 2, Vector: []float64{0.8, 0.2, 0, 0, 0, 0, 0, 0}},
	}, []float64{1, 0, 0, 0, 0, 0, 0, 0}, QueryOptions{
		Backend: "turbovec",
		Limit:   2,
		TurboVec: crawlvector.TurboVecOptions{
			MaxInputBytes: 600,
		},
	})
	if err != nil {
		t.Fatalf("query fallback: %v", err)
	}
	if len(got) != 2 || got[0].ThreadID != 1 || got[1].ThreadID != 2 {
		t.Fatalf("neighbors = %#v", got)
	}
}

func TestQueryWithTurboVecFallsBackToExactWhenCandidateCountExceedsTieCap(t *testing.T) {
	items := make([]Item, 0, maxTurboVecTieCandidates+1)
	for i := 1; i <= maxTurboVecTieCandidates+1; i++ {
		items = append(items, Item{ThreadID: int64(i), Vector: []float64{1, 0, 0, 0, 0, 0, 0, 0}})
	}
	got, err := QueryWithOptions(context.Background(), items, []float64{1, 0, 0, 0, 0, 0, 0, 0}, QueryOptions{
		Backend: "turbovec",
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("query fallback tie cap: %v", err)
	}
	if len(got) != 1 || got[0].ThreadID != 1 {
		t.Fatalf("neighbors = %#v", got)
	}
}

func TestTurboVecHelperProcess(t *testing.T) {
	if os.Getenv("CRAWLKIT_TEST_TURBOVEC_HELPER") != "1" {
		return
	}
	defer os.Exit(0)

	var request struct {
		Dimensions int         `json:"dimensions"`
		Limit      int         `json:"limit"`
		Vectors    [][]float32 `json:"vectors"`
	}
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		panic(err)
	}
	if request.Dimensions != 8 || request.Limit != 2 {
		panic("unexpected request")
	}
	response := struct {
		Results []struct {
			Index int     `json:"index"`
			Score float64 `json:"score"`
		} `json:"results"`
	}{
		Results: []struct {
			Index int     `json:"index"`
			Score float64 `json:"score"`
		}{
			{Index: 1, Score: 0.9},
			{Index: 0, Score: 0.8},
		},
	}
	if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
		panic(err)
	}
}
