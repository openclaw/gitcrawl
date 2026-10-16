package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestThreadsByIDsBeyondSQLiteParameterLimit(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	otherRepoID, err := st.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "other", FullName: "fixture/other"})
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := st.UpsertThread(ctx, Thread{RepoID: otherRepoID, GitHubID: "other", Number: 1, Kind: "issue", State: "open", Title: "other repository", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 32768)
	for i := range ids {
		ids[i] = int64(i + 100000)
	}
	ids[0] = threadIDs[0]
	ids[len(ids)/2] = otherID
	ids[len(ids)-2] = threadIDs[0]
	ids[len(ids)-1] = threadIDs[1]
	threads, err := st.ThreadsByIDs(ctx, repoID, ids)
	if err != nil {
		t.Fatalf("large thread lookup: %v", err)
	}
	if len(threads) != 2 || threads[threadIDs[0]].Number != 301 || threads[threadIDs[1]].Number != 302 {
		t.Fatalf("expected only the two requested repository threads, got %+v", threads)
	}
}
