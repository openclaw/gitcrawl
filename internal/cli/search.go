package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/openai"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/vector"
)

func (a *App) runSearch(ctx context.Context, args []string) error {
	if len(args) > 0 && isGHSearchKind(args[0]) {
		return a.runGHSearch(ctx, args)
	}

	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	query := fs.String("query", "", "search query")
	limitRaw := fs.String("limit", "", "maximum hit rows")
	mode := fs.String("mode", "keyword", "search mode: keyword|semantic|hybrid")
	scope := fs.String("scope", "threads", "search scope: threads|code|all")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"query": true, "limit": true, "mode": true, "scope": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("search requires owner/repo"))
	}
	if strings.TrimSpace(*query) == "" {
		return usageErr(fmt.Errorf("search requires --query"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	searchMode := strings.TrimSpace(*mode)
	if searchMode == "" {
		searchMode = "keyword"
	}
	if searchMode != "keyword" && searchMode != "semantic" && searchMode != "hybrid" {
		return usageErr(fmt.Errorf("unsupported search mode %q", searchMode))
	}
	searchScope := strings.TrimSpace(*scope)
	if searchScope == "" {
		searchScope = "threads"
	}
	if searchScope != "threads" && searchScope != "code" && searchScope != "all" {
		return usageErr(fmt.Errorf("unsupported search scope %q", searchScope))
	}
	if searchScope == "code" && searchMode != "keyword" {
		return usageErr(fmt.Errorf("code search currently supports --mode keyword only"))
	}
	if cfg, err := config.LoadRuntime(a.configPath); err == nil && cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		if searchScope != "threads" {
			return usageErr(fmt.Errorf("code search requires a local gitcrawl database"))
		}
		return a.runRemoteSearch(ctx, cfg, owner, repoName, strings.TrimSpace(*query), limit, searchMode)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	queryText := strings.TrimSpace(*query)
	if searchScope == "code" {
		hits, err := rt.Store.SearchCodeDocuments(ctx, repo.ID, queryText, limit)
		if err != nil {
			return err
		}
		return a.writeOutput("search", map[string]any{
			"repository": repo.FullName,
			"query":      queryText,
			"mode":       "keyword",
			"scope":      "code",
			"hits":       hits,
		}, true)
	}
	hits, effectiveMode, err := a.searchDocuments(ctx, rt, repo.ID, queryText, limit, searchMode)
	if err != nil {
		return err
	}
	if searchScope == "all" {
		codeHits, err := rt.Store.SearchCodeDocuments(ctx, repo.ID, queryText, limit)
		if err != nil {
			return err
		}
		payload := map[string]any{
			"repository": repo.FullName,
			"query":      queryText,
			"mode":       effectiveMode,
			"code_mode":  "keyword",
			"scope":      "all",
			"hits":       mergeScopedSearchHits(hits, codeHits, limit),
		}
		if effectiveMode != searchMode {
			payload["requested_mode"] = searchMode
		}
		return a.writeOutput("search", payload, true)
	}
	payload := map[string]any{
		"repository": repo.FullName,
		"query":      queryText,
		"mode":       effectiveMode,
		"hits":       hits,
	}
	if effectiveMode != searchMode {
		payload["requested_mode"] = searchMode
	}
	return a.writeOutput("search", payload, true)
}

func (a *App) searchDocuments(ctx context.Context, rt localRuntime, repoID int64, query string, limit int, mode string) ([]store.SearchHit, string, error) {
	switch mode {
	case "keyword":
		hits, err := rt.Store.SearchDocuments(ctx, repoID, query, limit)
		return hits, "keyword", err
	case "semantic":
		hits, err := a.semanticSearchDocuments(ctx, rt, repoID, query, limit)
		if errors.Is(err, errNoSemanticVectors) {
			return nil, "semantic", nil
		}
		return hits, "semantic", err
	case "hybrid":
		keywordHits, err := rt.Store.SearchDocuments(ctx, repoID, query, limit)
		if err != nil {
			return nil, "", err
		}
		semanticHits, err := a.semanticSearchDocuments(ctx, rt, repoID, query, limit)
		if err != nil {
			if !canFallbackFromSemanticSearch(err) {
				return nil, "", err
			}
			return keywordHits, "keyword", nil
		}
		return mergeHybridSearchHits(keywordHits, semanticHits, limit), "hybrid", nil
	default:
		return nil, "", fmt.Errorf("unsupported search mode %q", mode)
	}
}

func canFallbackFromSemanticSearch(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func (a *App) semanticSearchDocuments(ctx context.Context, rt localRuntime, repoID int64, query string, limit int) ([]store.SearchHit, error) {
	if limit <= 0 {
		limit = 20
	}
	vectorQuery := store.ThreadVectorQuery{
		RepoID:        repoID,
		Model:         rt.Config.OpenAI.EmbedModel,
		Basis:         rt.Config.EmbeddingBasis,
		IncludeClosed: true,
	}
	storedVectors, err := semanticSearchVectors(ctx, rt.Store, vectorQuery)
	if err != nil {
		return nil, err
	}
	if len(storedVectors) == 0 {
		return nil, errNoSemanticVectors
	}
	token := config.ResolveOpenAIKey(rt.Config)
	if token.Value == "" {
		return nil, fmt.Errorf("semantic search requires OpenAI API key: set %s", rt.Config.OpenAI.APIKeyEnv)
	}
	client := openai.New(openai.Options{APIKey: token.Value, BaseURL: embedBaseURL(rt.Config), Dimensions: rt.Config.OpenAI.EmbedDimensions, Retry: embedRetryOverride()})
	queryVectors, err := client.Embed(ctx, rt.Config.OpenAI.EmbedModel, []string{query})
	if err != nil {
		return nil, err
	}
	if len(queryVectors) == 0 {
		return nil, nil
	}
	storedVectors = threadVectorsWithDimensions(storedVectors, len(queryVectors[0]))
	if len(storedVectors) == 0 {
		return nil, nil
	}
	items := make([]vector.Item, 0, len(storedVectors))
	for _, stored := range storedVectors {
		items = append(items, vector.Item{ThreadID: stored.ThreadID, Vector: stored.Vector})
	}
	neighbors, err := vector.QueryWithOptions(ctx, items, queryVectors[0], vector.QueryOptions{
		Backend: rt.Config.VectorBackend,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(neighbors))
	scoreByThreadID := make(map[int64]float64, len(neighbors))
	for _, neighbor := range neighbors {
		ids = append(ids, neighbor.ThreadID)
		scoreByThreadID[neighbor.ThreadID] = neighbor.Score
	}
	threads, err := rt.Store.ThreadsByIDs(ctx, repoID, ids)
	if err != nil {
		return nil, err
	}
	hits := make([]store.SearchHit, 0, len(neighbors))
	for _, neighbor := range neighbors {
		thread, ok := threads[neighbor.ThreadID]
		if !ok {
			continue
		}
		hits = append(hits, store.SearchHit{
			ThreadID:    thread.ID,
			Number:      thread.Number,
			Kind:        thread.Kind,
			State:       thread.State,
			Title:       thread.Title,
			HTMLURL:     thread.HTMLURL,
			AuthorLogin: thread.AuthorLogin,
			Snippet:     thread.Title,
			Score:       scoreByThreadID[neighbor.ThreadID],
		})
	}
	return hits, nil
}

func semanticSearchVectors(ctx context.Context, st *store.Store, query store.ThreadVectorQuery) ([]store.ThreadVector, error) {
	storedVectors, err := st.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(storedVectors) > 0 || strings.TrimSpace(query.Basis) == "" {
		return storedVectors, nil
	}
	query.Basis = ""
	fallbackVectors, err := st.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		return nil, err
	}
	return dedupeThreadVectorsByThread(fallbackVectors), nil
}

func dedupeThreadVectorsByThread(vectors []store.ThreadVector) []store.ThreadVector {
	out := make([]store.ThreadVector, 0, len(vectors))
	indexByThreadID := make(map[int64]int, len(vectors))
	for _, candidate := range vectors {
		index, ok := indexByThreadID[candidate.ThreadID]
		if !ok {
			indexByThreadID[candidate.ThreadID] = len(out)
			out = append(out, candidate)
			continue
		}
		if threadVectorPreferred(candidate, out[index]) {
			out[index] = candidate
		}
	}
	return out
}

func threadVectorPreferred(candidate, current store.ThreadVector) bool {
	if candidate.UpdatedAt != current.UpdatedAt {
		return threadVectorTimestampAfter(candidate.UpdatedAt, current.UpdatedAt)
	}
	if candidate.CreatedAt != current.CreatedAt {
		return threadVectorTimestampAfter(candidate.CreatedAt, current.CreatedAt)
	}
	if candidate.Basis != current.Basis {
		return candidate.Basis < current.Basis
	}
	return candidate.Model < current.Model
}

func threadVectorTimestampAfter(candidate, current string) bool {
	candidateTime, candidateErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(candidate))
	currentTime, currentErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(current))
	if candidateErr == nil && currentErr == nil {
		return candidateTime.After(currentTime)
	}
	return candidate > current
}

func threadVectorsWithDimensions(vectors []store.ThreadVector, dimensions int) []store.ThreadVector {
	out := vectors[:0]
	for _, stored := range vectors {
		if stored.Dimensions == dimensions && len(stored.Vector) == dimensions {
			out = append(out, stored)
		}
	}
	return out
}

func mergeHybridSearchHits(keywordHits, semanticHits []store.SearchHit, limit int) []store.SearchHit {
	if limit <= 0 {
		limit = 20
	}
	seen := make(map[int64]bool, len(keywordHits)+len(semanticHits))
	out := make([]store.SearchHit, 0, limit)
	maxLen := len(keywordHits)
	if len(semanticHits) > maxLen {
		maxLen = len(semanticHits)
	}
	for index := 0; index < maxLen; index++ {
		if index < len(keywordHits) {
			out = appendSearchHit(out, seen, keywordHits[index], limit)
		}
		if len(out) >= limit {
			return out
		}
		if index < len(semanticHits) {
			out = appendSearchHit(out, seen, semanticHits[index], limit)
		}
		if len(out) >= limit {
			return out
		}
	}
	return out
}

func appendSearchHit(out []store.SearchHit, seen map[int64]bool, hit store.SearchHit, limit int) []store.SearchHit {
	if seen[hit.ThreadID] || len(out) >= limit {
		return out
	}
	seen[hit.ThreadID] = true
	return append(out, hit)
}

var errNoSemanticVectors = errors.New("no semantic vectors")
