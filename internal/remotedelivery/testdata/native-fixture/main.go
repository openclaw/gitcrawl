// This synthetic Store-owned fixture is built explicitly for cross-process
// archive qualification. It has no provider client or credential access.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func run() error {
	if len(os.Args) != 3 {
		return errors.New("expected seed, mutate or remove and fixture directory")
	}
	mode, dir := os.Args[1], os.Args[2]
	if mode != "seed" && mode != "mutate" && mode != "remove" {
		return errors.New("unknown fixture operation")
	}
	path := filepath.Join(dir, "native.sqlite")
	st, err := os.Stat(path)
	if mode == "seed" {
		if !errors.Is(err, os.ErrNotExist) {
			return errors.New("fixture destination must not exist")
		}
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	} else if err != nil || !st.Mode().IsRegular() {
		return errors.New("existing native fixture is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := store.Open(ctx, path)
	if err != nil {
		return err
	}
	defer s.Close()
	switch mode {
	case "seed":
		repo, err := s.UpsertRepository(ctx, store.Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", GitHubRepoID: "9007199254740993", RawJSON: "{}", UpdatedAt: "2026-01-01"})
		if err != nil {
			return err
		}
		thread, err := s.UpsertThread(ctx, store.Thread{RepoID: repo, GitHubID: "thread-1", Number: 42, Kind: "issue", State: "open", Title: "retained", Body: "thread body", HTMLURL: "https://github.com/fixture/repo/issues/42", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"node_id":"THREAD_1"}`, ContentHash: "initial", UpdatedAt: "2026-01-01"})
		if err != nil {
			return err
		}
		if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: thread, GitHubID: "seed", CommentType: "issue_comment", Body: "before\x00snow☃", RawJSON: `{"node_id":"COMMENT_SEED"}`}); err != nil {
			return err
		}
		if _, err := s.EnableChangeContract(ctx, store.ChangeContractEnableOptions{}); err != nil {
			return err
		}
		return s.PrepareNativeReportIndexes(ctx)
	case "mutate":
		return s.WithTx(ctx, func(tx *store.Store) error {
			if _, err := tx.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "seed", CommentType: "issue_comment", Body: "after\x00snow☃", RawJSON: `{"node_id":"COMMENT_SEED"}`}); err != nil {
				return err
			}
			for i := 0; i < 500; i++ {
				if _, err := tx.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: fmt.Sprintf("new-%d", i), CommentType: "issue_comment", Body: "new child", RawJSON: fmt.Sprintf(`{"node_id":"COMMENT_%d"}`, i)}); err != nil {
					return err
				}
			}
			return nil
		})
	case "remove":
		_, err := s.DB().ExecContext(ctx, `DELETE FROM comments WHERE github_id='seed'`)
		return err
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "native fixture failed")
		os.Exit(1)
	}
}
