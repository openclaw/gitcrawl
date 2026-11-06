package store

import (
	"context"
	"fmt"
	"testing"
)

func TestUpsertRepositoryAndThread(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t, ctx)
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:        "openclaw",
		Name:         "gitcrawl",
		FullName:     "openclaw/gitcrawl",
		GitHubRepoID: "123",
		RawJSON:      "{}",
		UpdatedAt:    "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID:        repoID,
		GitHubID:      "456",
		Number:        1,
		Kind:          "issue",
		State:         "open",
		Title:         "download stalls",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash",
		UpdatedAt:     "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	if threadID == 0 {
		t.Fatal("expected thread id")
	}

	rows, err := st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(rows) != 1 || rows[0].Title != "download stalls" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
	for _, number := range []int{2, 3} {
		if _, err := st.UpsertThread(ctx, Thread{
			RepoID: repoID, GitHubID: fmt.Sprint(number), Number: number,
			Kind: "issue", State: "open", LabelsJSON: "[]", AssigneesJSON: "[]",
			RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z",
		}); err != nil {
			t.Fatalf("seed competing thread %d: %v", number, err)
		}
	}
	for _, test := range []struct {
		name    string
		numbers []int
		limit   int
		want    int
	}{
		{name: "original selection", numbers: []int{1}, limit: 1, want: 1},
		{name: "numbers only", numbers: []int{2}, want: 2},
		{name: "numbers and limit", numbers: []int{2, 3}, limit: 1, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			filtered, err := st.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repoID, Numbers: test.numbers, Limit: test.limit})
			if err != nil {
				t.Fatalf("filtered threads: %v", err)
			}
			if len(filtered) != 1 || filtered[0].Number != test.want {
				t.Fatalf("unexpected filtered rows: %#v, want number %d", filtered, test.want)
			}
		})
	}
}
