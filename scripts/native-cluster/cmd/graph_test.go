//go:build native_cluster_overlay

package main

import (
	"context"
	"github.com/openclaw/gitcrawl/internal/cli"
	"github.com/openclaw/gitcrawl/internal/store"
	"math/rand"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestOriginalGraphExact(t *testing.T) {
	ctx := context.Background()
	s, e := store.Open(ctx, filepath.Join(t.TempDir(), "oracle.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	repo, e := s.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	r := rand.New(rand.NewSource(100))
	var vectors []store.ThreadVector
	var ids []int64
	var base []float64
	for i := 0; i < 85; i++ {
		kind := "issue"
		if i%3 == 0 {
			kind = "pull_request"
		}
		title := "native graph oracle group"
		if i%2 == 0 {
			title = "different title"
		}
		thread := store.Thread{RepoID: repo, GitHubID: strconv.Itoa(i), Number: i + 100, Kind: kind, State: "open", Title: title, Body: "native reference #102", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"}
		id, e := s.UpsertThread(ctx, thread)
		if e != nil {
			t.Fatal(e)
		}
		ids = append(ids, id)
		a := make([]float64, 1536)
		for j := range a {
			a[j] = r.Float64()*2 - 1
		}
		if i == 0 {
			base = a
		}
		if i%7 != 0 {
			for j := range a {
				a[j] = base[j] + .15*a[j]
			}
		}
		vectors = append(vectors, store.ThreadVector{ThreadID: id, Vector: a})
	}
	threads, e := s.ThreadsByIDs(ctx, repo, ids)
	if e != nil {
		t.Fatal(e)
	}
	want, we, e := cli.NativeOriginalGraphOracle(ctx, s, repo, vectors)
	if e != nil {
		t.Fatal(e)
	}
	got, ge, e := buildDurableClusterInputs(ctx, repo, vectors, threads, "openclaw/openclaw", clusterBuildOptions{Threshold: .80, MinSize: 1, MaxClusterSize: 40, Fanout: 16, CrossKindThreshold: .93, RetireMissing: true})
	if e != nil || we != ge || !reflect.DeepEqual(want, got) {
		t.Fatalf("graph differs: edges %d/%d error %v", we, ge, e)
	}
	cache := &similarityCache{}
	_, _, e = buildWithCache(ctx, repo, vectors, threads, "openclaw/openclaw", clusterBuildOptions{Threshold: .80, MinSize: 1, MaxClusterSize: 40, Fanout: 16, CrossKindThreshold: .93}, cache)
	if e != nil {
		t.Fatal(e)
	}
	// Same-node vector/title/kind edits, changed references, deletion and re-entry
	// must produce the original complete graph, not a stale cached neighborhood.
	for step := 0; step < 4; step++ {
		if step == 0 {
			vectors[4].Vector[0] = .9
			th := threads[vectors[4].ThreadID]
			th.Title = "edited native"
			th.Kind = "pull_request"
			th.Body = "reference #140"
			threads[th.ID] = th
			s.DB().Exec("update threads set title=?,kind=?,body=? where id=?", th.Title, th.Kind, th.Body, th.ID)
		}
		use := vectors
		if step == 1 {
			use = vectors[1:]
		}
		if step == 2 {
			th := threads[vectors[2].ThreadID]
			th.Body = "no reference"
			threads[th.ID] = th
			s.DB().Exec("update threads set body=? where id=?", th.Body, th.ID)
		}
		want, we, e = cli.NativeOriginalGraphOracle(ctx, s, repo, use)
		if e != nil {
			t.Fatal(e)
		}
		got, ge, e = buildWithCache(ctx, repo, use, threads, "openclaw/openclaw", clusterBuildOptions{Threshold: .80, MinSize: 1, MaxClusterSize: 40, Fanout: 16, CrossKindThreshold: .93}, cache)
		if e != nil || we != ge || !reflect.DeepEqual(want, got) {
			t.Fatalf("incremental graph step%d differs", step)
		}
	}
	stop, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, e = buildDurableClusterInputs(stop, repo, vectors, threads, "openclaw/openclaw", clusterBuildOptions{}); e == nil {
		t.Fatal("cancel ignored")
	}
}
