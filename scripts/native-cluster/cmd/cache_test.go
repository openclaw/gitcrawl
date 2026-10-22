//go:build native_cluster_overlay

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/openclaw/gitcrawl/internal/store"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func cacheFixture() store.NativeClusterSnapshot {
	return store.NativeClusterSnapshot{Source: store.NativeSourceBinding{SourceCommit: "source", ArchivePath: "/synthetic", Device: "1", Inode: "2", SchemaVersion: 16, RepoID: "1", ProviderRepoID: "provider", OwnerExclusionsSHA256: "owners", ConsumerExclusionsSHA256: "consumer"}, RepoID: 1, InputSHA256: store.NativeSHA256([]byte("snapshot")), Threads: map[int64]store.Thread{1: {ID: 1, Number: 101, Kind: "issue", Title: "native cache", Body: "#102"}, 2: {ID: 2, Number: 102, Kind: "issue", Title: "native cache"}, 3: {ID: 3, Number: 103, Kind: "issue", Title: "another"}}, Vectors: []store.ThreadVector{{ThreadID: 1, Vector: []float64{1, 0, 0}}, {ThreadID: 2, Vector: []float64{1, .1, 0}}, {ThreadID: 3, Vector: []float64{0, 0, 1}}}}
}
func TestPairCacheCompleteLineageAndReconstruction(t *testing.T) {
	s := cacheFixture()
	dir := t.TempDir()
	c := &similarityCache{}
	opts := clusterBuildOptions{Threshold: .8, MinSize: 1, MaxClusterSize: 40, Fanout: 16, CrossKindThreshold: .93}
	want, we, e := buildWithCache(context.Background(), s.RepoID, s.Vectors, s.Threads, "openclaw/openclaw", opts, c)
	if e != nil {
		t.Fatal(e)
	}
	id, e := savePairs(dir, s, c, "")
	if e != nil {
		t.Fatal(e)
	}
	loaded, parent, e := loadPairs(dir, s)
	if e != nil || parent != id {
		t.Fatal("load", e)
	}
	got, ge, e := buildWithCache(context.Background(), s.RepoID, s.Vectors, s.Threads, "openclaw/openclaw", opts, loaded)
	if e != nil || we != ge || !reflect.DeepEqual(want, got) {
		t.Fatal("reused graph differs")
	}
	if loaded.recomputed != 0 || loaded.reused != 3 {
		t.Fatal("unchanged pass repeated pair dot products")
	}
	before, _ := json.Marshal([]any{loaded.fingerprints, loaded.edges})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	s.Vectors[0].Vector[0] = .5
	if _, _, e = buildWithCache(cancelled, s.RepoID, s.Vectors, s.Threads, "openclaw/openclaw", opts, loaded); e == nil {
		t.Fatal("interruption ignored")
	}
	after, _ := json.Marshal([]any{loaded.fingerprints, loaded.edges})
	if string(before) != string(after) {
		t.Fatal("partial pass replaced complete cache")
	}
}
func TestPairCacheRejectsShapeAndInvalidatesIdentity(t *testing.T) {
	s := cacheFixture()
	dir := t.TempDir()
	c := &similarityCache{}
	_, _, e := buildWithCache(context.Background(), 1, s.Vectors, s.Threads, "openclaw/openclaw", clusterBuildOptions{Threshold: .8}, c)
	if e != nil {
		t.Fatal(e)
	}
	id, e := savePairs(dir, s, c, "")
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, id+".json"))
	for _, bad := range []string{"partial", "pair_count", "endpoint", "score"} {
		var f pairCacheFile
		json.Unmarshal(raw, &f)
		switch bad {
		case "partial":
			f.Complete = false
		case "pair_count":
			f.Pairs = 0
		case "endpoint":
			for key, edge := range f.Edges {
				edge.RightThreadID = 99
				f.Edges[key] = edge
				break
			}
		case "score":
			for key, edge := range f.Edges {
				edge.Score = 2
				f.Edges[key] = edge
				break
			}
		}
		b, _ := json.Marshal(f)
		sum := sha256.Sum256(b)
		hash := hex.EncodeToString(sum[:])
		os.WriteFile(filepath.Join(dir, hash+".json"), b, 0600)
		pointer, _ := json.Marshal(hash)
		os.WriteFile(filepath.Join(dir, "current.json"), pointer, 0600)
		if _, _, e = loadPairs(dir, s); e == nil {
			t.Fatal("accepted malformed cache", bad)
		}
	}
	pointer, _ := json.Marshal(id)
	os.WriteFile(filepath.Join(dir, "current.json"), pointer, 0600)
	s.Source.ConsumerExclusionsSHA256 = "changed"
	fresh, _, e := loadPairs(dir, s)
	if e != nil || len(fresh.fingerprints) != 0 {
		t.Fatal("new exclusion reused old cache")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("exclusive old cache capability retained")
	}
}

func TestReviewedParentCacheReusePreservesProvenance(t *testing.T) {
	s := cacheFixture()
	dir := t.TempDir()
	c := &similarityCache{}
	opts := clusterBuildOptions{Threshold: .8, MinSize: 1, MaxClusterSize: 40, Fanout: 16, CrossKindThreshold: .93}
	want, _, e := buildWithCache(context.Background(), 1, s.Vectors, s.Threads, "openclaw/openclaw", opts, c)
	if e != nil {
		t.Fatal(e)
	}
	id, e := savePairs(dir, s, c, "")
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, id+".json"))
	var f pairCacheFile
	json.Unmarshal(raw, &f)
	f.Producer = reviewedSubsetCacheProducer
	if !reviewedCacheProducer(reviewedQuarantineCacheProducer) || reviewedCacheProducer(store.NativeSHA256([]byte("unreviewed"))) {
		t.Fatal("reviewed producer set changed")
	}
	raw, _ = json.Marshal(f)
	parent := store.NativeSHA256(raw)
	os.WriteFile(filepath.Join(dir, parent+".json"), raw, 0600)
	pointer, _ := json.Marshal(parent)
	os.WriteFile(filepath.Join(dir, "current.json"), pointer, 0600)
	loaded, bound, e := loadPairs(dir, s)
	if e != nil || bound != parent {
		t.Fatal(e)
	}
	got, _, e := buildWithCache(context.Background(), 1, s.Vectors, s.Threads, "openclaw/openclaw", opts, loaded)
	if e != nil || !reflect.DeepEqual(got, want) || loaded.recomputed != 0 || loaded.reused != 3 {
		t.Fatal("reviewed parent changed graph/recomputed", e)
	}
	if _, e = savePairs(dir, s, loaded, parent); e != nil {
		t.Fatal(e)
	}
	still, _ := os.ReadFile(filepath.Join(dir, parent+".json"))
	if string(still) != string(raw) {
		t.Fatal("parent provenance changed")
	}
	f.Producer = store.NativeSHA256([]byte("unreviewed"))
	bad, _ := json.Marshal(f)
	badID := store.NativeSHA256(bad)
	os.WriteFile(filepath.Join(dir, badID+".json"), bad, 0600)
	pointer, _ = json.Marshal(badID)
	os.WriteFile(filepath.Join(dir, "current.json"), pointer, 0600)
	if _, _, e = loadPairs(dir, s); e == nil {
		t.Fatal("unreviewed parent admitted")
	}
	if _, e = os.Stat(filepath.Join(dir, badID+".json")); e != nil {
		t.Fatal("failed proof deleted")
	}
}
