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
