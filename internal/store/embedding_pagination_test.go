package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestEmbeddingLimitSkipsCurrentVectorsAcrossPages(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}",
		UpdatedAt: "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	for number := 1; number <= embeddingCandidatePageSize+2; number++ {
		_, err := st.UpsertThread(ctx, Thread{
			RepoID: repoID, GitHubID: fmt.Sprint(number), Number: number,
			Kind: "issue", State: "open", Title: fmt.Sprintf("Thread %d", number),
			Body: "body", RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]",
			ContentHash: "fixture", UpdatedAt: "2026-01-01T00:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	opts := EmbeddingTaskOptions{RepoID: repoID, Model: "fixture", Force: true}
	tasks, err := st.ListEmbeddingTasks(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != embeddingCandidatePageSize+2 {
		t.Fatalf("tasks=%d", len(tasks))
	}
	for _, task := range tasks {
		if task.Number <= 2 {
			continue
		}
		_, err := st.DB().ExecContext(ctx, `
			insert into thread_vectors(thread_id, basis, model, dimensions, vector_json, vector_backend, content_hash, created_at, updated_at)
			values(?, 'title_original', 'fixture', 1, '[1]', 'json', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		`, task.ThreadID, task.ContentHash)
		if err != nil {
			t.Fatal(err)
		}
	}
	opts.Force, opts.Limit = false, 1
	tasks, err = st.ListEmbeddingTasks(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Number != 2 {
		t.Fatalf("limited tasks=%+v, want older missing vector #2", tasks)
	}
	opts.Limit = 0
	tasks, err = st.ListEmbeddingTasks(ctx, opts)
	if err != nil || len(tasks) != 2 || tasks[0].Number != 2 || tasks[1].Number != 1 {
		t.Fatalf("unbounded pending tasks=%+v err=%v", tasks, err)
	}
	opts.Force, opts.Limit = true, 1
	tasks, err = st.ListEmbeddingTasks(ctx, opts)
	if err != nil || len(tasks) != 1 || tasks[0].Number != embeddingCandidatePageSize+2 {
		t.Fatalf("forced tasks=%+v err=%v", tasks, err)
	}
	opts.Force, opts.Number = false, 1
	tasks, err = st.ListEmbeddingTasks(ctx, opts)
	if err != nil || len(tasks) != 1 || tasks[0].Number != 1 {
		t.Fatalf("targeted tasks=%+v err=%v", tasks, err)
	}
}
