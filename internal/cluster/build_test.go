package cluster

import "testing"

func TestBuildConnectedComponents(t *testing.T) {
	clusters := Build([]Node{
		{ThreadID: 1, Number: 10},
		{ThreadID: 2, Number: 11},
		{ThreadID: 3, Number: 1},
	}, []Edge{
		{LeftThreadID: 1, RightThreadID: 2, Score: 0.9},
	})
	if len(clusters) != 2 {
		t.Fatalf("clusters: got %d want 2", len(clusters))
	}
	if len(clusters[0].Members) != 2 {
		t.Fatalf("first cluster members: %#v", clusters[0].Members)
	}
	if clusters[0].RepresentativeThreadID != 1 {
		t.Fatalf("representative: got %d want 1", clusters[0].RepresentativeThreadID)
	}
}

func TestBuildWithOptionsKeepsStrongBoundedComponents(t *testing.T) {
	clusters := BuildWithOptions([]Node{
		{ThreadID: 1, Number: 10},
		{ThreadID: 2, Number: 11},
		{ThreadID: 3, Number: 12},
		{ThreadID: 4, Number: 13},
		{ThreadID: 5, Number: 14},
		{ThreadID: 6, Number: 15},
	}, []Edge{
		{LeftThreadID: 1, RightThreadID: 2, Score: 0.95},
		{LeftThreadID: 2, RightThreadID: 3, Score: 0.94},
		{LeftThreadID: 3, RightThreadID: 4, Score: 0.82},
		{LeftThreadID: 4, RightThreadID: 5, Score: 0.81},
		{LeftThreadID: 5, RightThreadID: 6, Score: 0.80},
	}, Options{MaxSize: 3})
	if len(clusters) != 2 {
		t.Fatalf("clusters: got %d want 2: %#v", len(clusters), clusters)
	}
	if got := clusters[0].Members; len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("first cluster members: %#v", got)
	}
	if got := clusters[1].Members; len(got) != 3 || got[0] != 4 || got[1] != 5 || got[2] != 6 {
		t.Fatalf("second cluster members: %#v", got)
	}
}
