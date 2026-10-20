package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestClusterPersistenceReusesOuterTransaction(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "outer-transaction.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	tid, e := s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: "1", Number: 1, Kind: "issue", State: "open", Title: "fixture", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	s.DB().SetMaxOpenConns(1)
	inputs := []DurableClusterInput{{StableKey: "fixture", StableSlug: "fixture", RepresentativeThreadID: tid, Members: []DurableClusterMemberInput{{ThreadID: tid, Role: "canonical"}}}}
	rollback := errors.New("outer rollback")
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	e = s.WithTx(bounded, func(tx *Store) error {
		if _, e := tx.SaveAvailableSubsetClusters(bounded, repo, inputs); e != nil {
			return e
		}
		return rollback
	})
	cancel()
	if !errors.Is(e, rollback) {
		t.Fatalf("save failed to reuse one-connection outer transaction: %v", e)
	}
	for _, table := range []string{"cluster_runs", "cluster_groups", "cluster_memberships", "cluster_events"} {
		var count int
		if e = s.DB().QueryRow("select count(*) from " + table).Scan(&count); e != nil || count != 0 {
			t.Fatalf("%s escaped outer rollback: %d %v", table, count, e)
		}
	}
	bounded, cancel = context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if e = s.WithTx(bounded, func(tx *Store) error { _, e := tx.SaveCompleteScopedClusters(bounded, repo, inputs); return e }); e != nil {
		t.Fatalf("committed outer transaction: %v", e)
	}
	var count int
	if e = s.DB().QueryRow("select count(*) from cluster_memberships").Scan(&count); e != nil || count != 1 {
		t.Fatalf("committed membership absent: %d %v", count, e)
	}
}
