package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

const ghPRDetailFreshness = 90 * time.Second

func (a *App) ensureFreshGHPullRequestCache(ctx context.Context, repoValue string, number int) (store.PullRequestCache, error) {
	return a.loadGHPullRequestCache(ctx, repoValue, number, true)
}

func (a *App) loadGHPullRequestCache(ctx context.Context, repoValue string, number int, requireFresh bool) (store.PullRequestCache, error) {
	cache, err := a.localGHPullRequestCache(ctx, repoValue, number)
	if err == nil && (!requireFresh || ghPullRequestCacheFresh(cache)) {
		return cache, nil
	}
	if !a.shouldAutoHydrateGHPRDetails(err) {
		return cache, err
	}
	owner, repoName, parseErr := parseOwnerRepo(repoValue)
	if parseErr != nil {
		return store.PullRequestCache{}, parseErr
	}
	if _, syncErr := a.syncRepository(ctx, owner, repoName, syncOptions{
		Numbers:          []int{number},
		IncludePRDetails: true,
		Quiet:            true,
	}); syncErr != nil {
		return store.PullRequestCache{}, localGHUnsupported(syncErr)
	}
	return a.localGHPullRequestCache(ctx, repoValue, number)
}

func ghPRFieldsNeedFresh(fields []string) bool {
	for _, field := range fields {
		switch field {
		case "statusCheckRollup", "mergeStateStatus", "mergeable", "reviewDecision", "reviews", "latestReviews":
			return true
		}
	}
	return false
}

func (a *App) shouldAutoHydrateGHPRDetails(err error) bool {
	return a.shouldAutoHydrateGHThread(err)
}

func (a *App) shouldAutoHydrateGHThread(err error) bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("GITCRAWL_GH_AUTO_HYDRATE")), "0") {
		return false
	}
	if err == nil {
		return true
	}
	return isMissingLocalPRCache(err) || errors.Is(err, errLocalGHUnsupported)
}

func ghPullRequestCacheFresh(cache store.PullRequestCache) bool {
	if rawHead := ghPRHeadSHAFromRawJSON(cache.Detail.RawJSON); rawHead != "" && !strings.EqualFold(cache.Detail.HeadSHA, rawHead) {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, cache.Detail.FetchedAt)
	if err != nil {
		return false
	}
	return time.Since(parsed) <= ghPRDetailFreshness
}

func isMissingLocalPRCache(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, sql.ErrNoRows) ||
		strings.Contains(err.Error(), "pull request detail") ||
		strings.Contains(err.Error(), "was not found")
}

func (a *App) findGHPullRequestNumberByBranch(ctx context.Context, repoValue, branch string) (int, error) {
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return 0, err
	}
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return 0, localGHUnsupported(err)
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return 0, localGHUnsupported(err)
	}
	threads, err := rt.Store.SearchThreads(ctx, store.ThreadSearchOptions{
		RepoID:               repo.ID,
		Kind:                 "pull_request",
		State:                "open",
		IncludeLocallyClosed: true,
		Limit:                100,
	})
	if err != nil {
		return 0, err
	}
	for _, thread := range threads {
		if branch == ghPRHeadRefFromRawJSON(thread.RawJSON) {
			return thread.Number, nil
		}
		if cache, cacheErr := rt.Store.PullRequestCache(ctx, repo.ID, thread.Number); cacheErr == nil && branch == cache.Detail.HeadRef {
			return thread.Number, nil
		}
	}
	return 0, localGHUnsupported(fmt.Errorf("cached PR branch %q was not found", branch))
}

func ghPRHeadRefFromRawJSON(raw string) string {
	var payload struct {
		Head struct {
			Ref string `json:"ref"`
		} `json:"head"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Head.Ref)
}
