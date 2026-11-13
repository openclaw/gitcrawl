// This synthetic Store-owned fixture is built explicitly for cross-process
// archive qualification. It has no provider client or credential access.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func run() error {
	if len(os.Args) != 3 {
		return errors.New("expected seed, cluster-seed, mutate or remove and fixture directory")
	}
	mode, dir := os.Args[1], os.Args[2]
	if mode != "seed" && mode != "cluster-seed" && mode != "mutate" && mode != "remove" {
		return errors.New("unknown fixture operation")
	}
	path := filepath.Join(dir, "native.sqlite")
	st, err := os.Stat(path)
	if mode == "seed" || mode == "cluster-seed" {
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
	case "cluster-seed":
		return seedClusterFixture(ctx, s)
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

func seedClusterFixture(ctx context.Context, s *store.Store) error {
	repo, err := s.UpsertRepository(ctx, store.Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", GitHubRepoID: "9007199254740993", RawJSON: `{"private":false,"visibility":"public"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		return err
	}
	var ids []int64
	for i, kind := range []string{"issue", "issue", "pull_request"} {
		number := 42 + i
		id, err := s.UpsertThread(ctx, store.Thread{RepoID: repo, GitHubID: fmt.Sprintf("thread-%d", number), Number: number, Kind: kind, State: "open", Title: fmt.Sprintf("authorization boundary %d", number), Body: "synthetic credential scope\x00snow☃", HTMLURL: fmt.Sprintf("https://github.com/fixture/repo/issues/%d", number), LabelsJSON: `["security"]`, AssigneesJSON: "[]", RawJSON: fmt.Sprintf(`{"node_id":"THREAD_%d"}`, number), ContentHash: fmt.Sprintf("fixture-%d", number), UpdatedAtGitHub: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z"})
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}
	runID := "1"
	scope := store.ClusterScope{Format: store.ClusterScopeFormat, Mode: "full-native", RunID: runID, Repository: "fixture/repo", PolicyVersion: 1, PolicyDigest: strings.Repeat("a", 64), SourceAsOf: store.ClusterScopeClock{Through: "2026-01-01T00:00:00Z", ObservedAt: "2026-01-01T00:00:00Z"}, ProviderComplete: true, FullNativeScopeComplete: true, SelectedInputComplete: true, Population: store.ClusterScopePopulation{NativeEligible: 3, Selected: 3}, RefreshedMemberships: 3, LastFullRunID: &runID}
	raw, err := json.Marshal(map[string]any{"native_producer": map[string]any{"scope": scope}})
	if err != nil {
		return err
	}
	if _, err = s.DB().ExecContext(ctx, `insert into cluster_runs(id,repo_id,scope,status,started_at,finished_at,stats_json) values(1,?,'durable','success','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z',?)`, repo, string(raw)); err != nil {
		return err
	}
	if _, err = s.DB().ExecContext(ctx, `insert into cluster_groups(id,repo_id,stable_key,stable_slug,status,representative_thread_id,created_at,updated_at) values(99,?,'fixture-cluster','fixture-cluster','active',?,'2026-01-01T00:00:00Z','2026-01-01T00:00:01Z')`, repo, ids[0]); err != nil {
		return err
	}
	for i, id := range ids {
		role := "member"
		if i == 0 {
			role = "canonical"
		}
		if _, err = s.DB().ExecContext(ctx, `insert into cluster_memberships(cluster_id,thread_id,role,state,last_seen_run_id,added_by,added_reason_json,created_at,updated_at) values(99,?,?,'active',1,'fixture','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z')`, id, role); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "native fixture failed")
		os.Exit(1)
	}
}
