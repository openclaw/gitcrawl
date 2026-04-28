package store

import "testing"

func TestHumanKeyForValueMatchesGhcrawlRepresentativeIdentity(t *testing.T) {
	key := HumanKeyForValue("repo:1:cluster-representative:546")
	if key.Hash != "e77f18999d72cc6d27c5d3d0aa2c02cdc8cad3c1be077feb70062bc55eae98fd" {
		t.Fatalf("hash = %q", key.Hash)
	}
	if HumanKeyStableSlug(key) != "usage-matrix-binary-zrzm" {
		t.Fatalf("stable slug = %q", HumanKeyStableSlug(key))
	}
}
