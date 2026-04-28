package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	clusterer "github.com/openclaw/gitcrawl/internal/cluster"
	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/openai"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
	"github.com/openclaw/gitcrawl/internal/vector"
)

const (
	defaultTUIMinSize          = 5
	defaultTUIWorkingSetLimit  = 500
	defaultClusterMaxSize      = 40
	defaultClusterFanout       = 16
	defaultClusterThreshold    = 0.80
	defaultCrossKindMinScore   = 0.93
	highConfidenceEdgeScore    = 0.90
	weakEdgeMinTitleOverlap    = 0.18
	deterministicRefScore      = 0.94
	bodyRefEvidencePrefixChars = 240
)

var threadReferencePattern = regexp.MustCompile(`(?i)(?:\b[\w.-]+/[\w.-]+#(\d+)|(?:issues|pull)/(\d+)|#(\d{2,}))`)
var titleTokenPattern = regexp.MustCompile(`[A-Za-z0-9]{4,}`)

type referenceEvidence struct {
	Title     bool
	EarlyBody bool
}

type App struct {
	Stdout io.Writer
	Stderr io.Writer

	configPath string
	format     OutputFormat
}

type initResult struct {
	ConfigPath       string `json:"config_path"`
	DBPath           string `json:"db_path"`
	CacheDir         string `json:"cache_dir"`
	VectorDir        string `json:"vector_dir"`
	PortableStoreURL string `json:"portable_store_url,omitempty"`
	PortableStoreDir string `json:"portable_store_dir,omitempty"`
	PortableStore    string `json:"portable_store,omitempty"`
}

type OutputFormat string

const (
	FormatText OutputFormat = "text"
	FormatJSON OutputFormat = "json"
	FormatLog  OutputFormat = "log"
)

var version = "dev"

func New() *App {
	return &App{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		format: FormatText,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	global := flag.NewFlagSet("gitcrawl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	configPath := global.String("config", "", "config path")
	format := global.String("format", string(FormatText), "output format: text|json|log")
	jsonOut := global.Bool("json", false, "write JSON output")
	versionFlag := global.Bool("version", false, "print version")
	global.Bool("no-color", false, "disable color output")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			a.printUsage()
			return nil
		}
		return usageErr(err)
	}

	resolvedFormat, err := resolveOutputFormat(*format, *jsonOut)
	if err != nil {
		return usageErr(err)
	}
	a.configPath = strings.TrimSpace(*configPath)
	a.format = resolvedFormat

	rest := global.Args()
	if *versionFlag {
		return a.writeOutput("version", map[string]string{"version": version}, false)
	}
	if len(rest) == 0 || rest[0] == "--help" || rest[0] == "-h" {
		a.printUsage()
		return nil
	}
	if rest[0] == "help" {
		if len(rest) > 1 {
			return a.printCommandUsage(rest[1])
		}
		a.printUsage()
		return nil
	}

	switch rest[0] {
	case "version":
		return a.writeOutput("version", map[string]string{"version": version}, false)
	case "serve":
		return usageErr(fmt.Errorf("serve is not supported in gitcrawl"))
	case "init":
		return a.runInit(ctx, rest[1:])
	case "doctor":
		return a.runDoctor(ctx, rest[1:])
	case "sync":
		return a.runSync(ctx, rest[1:])
	case "threads":
		return a.runThreads(ctx, rest[1:])
	case "close-thread":
		return a.runCloseThread(ctx, rest[1:])
	case "reopen-thread":
		return a.runReopenThread(ctx, rest[1:])
	case "close-cluster":
		return a.runCloseCluster(ctx, rest[1:])
	case "reopen-cluster":
		return a.runReopenCluster(ctx, rest[1:])
	case "exclude-cluster-member":
		return a.runExcludeClusterMember(ctx, rest[1:])
	case "include-cluster-member":
		return a.runIncludeClusterMember(ctx, rest[1:])
	case "set-cluster-canonical":
		return a.runSetClusterCanonical(ctx, rest[1:])
	case "runs":
		return a.runRuns(ctx, rest[1:])
	case "search":
		return a.runSearch(ctx, rest[1:])
	case "configure":
		return a.runConfigure(rest[1:])
	case "refresh":
		return a.runRefresh(ctx, rest[1:])
	case "embed":
		return a.runEmbed(ctx, rest[1:])
	case "clusters":
		return a.runClusters(ctx, rest[1:])
	case "durable-clusters":
		return a.runDurableClusters(ctx, rest[1:])
	case "cluster-detail":
		return a.runClusterDetail(ctx, rest[1:])
	case "cluster-explain":
		return a.runClusterDetail(ctx, rest[1:])
	case "neighbors":
		return a.runNeighbors(ctx, rest[1:])
	case "cluster":
		return a.runCluster(ctx, rest[1:])
	case "portable":
		return a.runPortable(ctx, rest[1:])
	case "tui":
		return a.runTUI(ctx, rest[1:])
	case "summarize", "key-summaries", "cluster-experiment", "merge-clusters", "split-cluster", "export-sync", "import-sync", "validate-sync", "portable-size", "sync-status", "optimize", "completion":
		_ = ctx
		return notImplemented(rest[0])
	default:
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
}

func (a *App) runConfigure(args []string) error {
	fs := flag.NewFlagSet("configure", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	summaryModel := fs.String("summary-model", "", "summary model")
	embedModel := fs.String("embed-model", "", "embedding model")
	embeddingBasis := fs.String("embedding-basis", "", "embedding basis")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"summary-model": true, "embed-model": true, "embedding-basis": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)

	cfg, err := config.Load(a.configPath)
	configExists := true
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		configExists = false
		cfg = config.Default()
	}
	updated := false
	if strings.TrimSpace(*summaryModel) != "" {
		cfg.OpenAI.SummaryModel = strings.TrimSpace(*summaryModel)
		updated = true
	}
	if strings.TrimSpace(*embedModel) != "" {
		cfg.OpenAI.EmbedModel = strings.TrimSpace(*embedModel)
		updated = true
	}
	if strings.TrimSpace(*embeddingBasis) != "" {
		cfg.EmbeddingBasis = strings.TrimSpace(*embeddingBasis)
		updated = true
	}
	if updated || !configExists {
		if err := config.Save(a.configPath, cfg); err != nil {
			return err
		}
	}
	return a.writeOutput("configure", map[string]any{
		"config_path":     config.ResolvePath(a.configPath),
		"updated":         updated || !configExists,
		"summary_model":   cfg.OpenAI.SummaryModel,
		"embed_model":     cfg.OpenAI.EmbedModel,
		"embedding_basis": cfg.EmbeddingBasis,
	}, true)
}

type refreshResult struct {
	Repository string          `json:"repository"`
	Selected   map[string]bool `json:"selected"`
	Sync       *syncer.Stats   `json:"sync,omitempty"`
	Embed      *embedResult    `json:"embed,omitempty"`
	Cluster    map[string]any  `json:"cluster,omitempty"`
}

func (a *App) runRefresh(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("refresh", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	noSync := fs.Bool("no-sync", false, "skip GitHub sync stage")
	noEmbed := fs.Bool("no-embed", false, "skip embedding stage")
	noCluster := fs.Bool("no-cluster", false, "skip clustering stage")
	includeComments := fs.Bool("include-comments", false, "hydrate comments during sync")
	fs.Bool("include-code", false, "accepted for compatibility; code hydration is not implemented yet")
	since := fs.String("since", "", "GitHub since timestamp")
	state := fs.String("state", "", "GitHub issue state: open|closed|all; default open")
	limitRaw := fs.String("limit", "", "maximum sync or embedding rows")
	thresholdRaw := fs.String("threshold", fmt.Sprintf("%.2f", defaultClusterThreshold), "minimum cluster cosine score")
	minSizeRaw := fs.String("min-size", "1", "minimum cluster member count")
	maxClusterSizeRaw := fs.String("max-cluster-size", strconv.Itoa(defaultClusterMaxSize), "maximum members per generated cluster")
	fanoutRaw := fs.String("k", strconv.Itoa(defaultClusterFanout), "nearest-neighbor fanout per thread")
	crossKindThresholdRaw := fs.String("cross-kind-threshold", fmt.Sprintf("%.2f", defaultCrossKindMinScore), "minimum score for issue/pull request edges")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"since": true, "state": true, "limit": true, "threshold": true, "min-size": true, "max-cluster-size": true, "k": true, "cross-kind-threshold": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("refresh requires owner/repo"))
	}
	if *noSync && *noEmbed && *noCluster {
		return usageErr(fmt.Errorf("refresh requires at least one selected stage"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	threshold, err := parseOptionalFloat(*thresholdRaw)
	if err != nil {
		return usageErr(err)
	}
	if threshold <= 0 || threshold > 1 {
		return usageErr(fmt.Errorf("refresh requires --threshold between 0 and 1"))
	}
	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	if minSize <= 0 {
		minSize = 2
	}
	maxClusterSize, fanout, crossKindThreshold, err := parseClusterShapeOptions("refresh", *maxClusterSizeRaw, *fanoutRaw, *crossKindThresholdRaw)
	if err != nil {
		return err
	}

	result := refreshResult{
		Repository: owner + "/" + repoName,
		Selected: map[string]bool{
			"sync":    !*noSync,
			"embed":   !*noEmbed,
			"cluster": !*noCluster,
		},
	}
	if !*noSync {
		fmt.Fprintln(a.Stderr, "[refresh] sync")
		stats, err := a.syncRepository(ctx, owner, repoName, syncOptions{
			Since:           strings.TrimSpace(*since),
			State:           strings.TrimSpace(*state),
			Limit:           limit,
			IncludeComments: *includeComments,
		})
		if err != nil {
			return err
		}
		result.Repository = stats.Repository
		result.Sync = &stats
	}
	if !*noEmbed {
		fmt.Fprintln(a.Stderr, "[refresh] embed")
		embed, err := a.embedRepository(ctx, owner, repoName, embedOptions{Limit: limit})
		if err != nil {
			return err
		}
		result.Repository = embed.Repository
		result.Embed = &embed
	}
	if !*noCluster {
		fmt.Fprintln(a.Stderr, "[refresh] cluster")
		rt, err := a.openLocalRuntime(ctx)
		if err != nil {
			return err
		}
		repo, err := rt.repository(ctx, owner, repoName)
		if err != nil {
			_ = rt.Store.Close()
			return err
		}
		query := store.ThreadVectorQuery{RepoID: repo.ID, Model: rt.Config.OpenAI.EmbedModel, Basis: rt.Config.EmbeddingBasis}
		vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, query)
		if err != nil {
			_ = rt.Store.Close()
			return err
		}
		if len(vectors) == 0 {
			vectors, err = rt.Store.ListThreadVectorsFiltered(ctx, store.ThreadVectorQuery{RepoID: repo.ID})
			if err != nil {
				_ = rt.Store.Close()
				return err
			}
		}
		clusterResult, err := clusterRepository(ctx, rt.Store, repo.ID, vectors, clusterBuildOptions{
			Threshold:          threshold,
			MinSize:            minSize,
			MaxClusterSize:     maxClusterSize,
			Fanout:             fanout,
			CrossKindThreshold: crossKindThreshold,
		})
		_ = rt.Store.Close()
		if err != nil {
			return err
		}
		result.Repository = repo.FullName
		result.Cluster = map[string]any{
			"threshold":     threshold,
			"cross_kind":    crossKindThreshold,
			"min_size":      minSize,
			"max_size":      maxClusterSize,
			"k":             fanout,
			"vector_count":  len(vectors),
			"edge_count":    clusterResult.EdgeCount,
			"cluster_count": clusterResult.ClusterCount,
			"member_count":  clusterResult.MemberCount,
			"run_id":        clusterResult.RunID,
		}
	}
	return a.writeOutput("refresh", result, true)
}

func (a *App) runSearch(ctx context.Context, args []string) error {
	if len(args) > 0 && isGHSearchKind(args[0]) {
		return a.runGHSearch(ctx, args)
	}

	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	query := fs.String("query", "", "search query")
	limitRaw := fs.String("limit", "", "maximum hit rows")
	mode := fs.String("mode", "keyword", "search mode: keyword|semantic|hybrid")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"query": true, "limit": true, "mode": true})); err != nil {
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

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	hits, err := rt.Store.SearchDocuments(ctx, repo.ID, strings.TrimSpace(*query), limit)
	if err != nil {
		return err
	}
	return a.writeOutput("search", map[string]any{
		"repository": repo.FullName,
		"query":      strings.TrimSpace(*query),
		"mode":       searchMode,
		"hits":       hits,
	}, true)
}

func (a *App) runNeighbors(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("neighbors", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "issue or pull request number")
	limitRaw := fs.String("limit", "", "maximum neighbor rows")
	thresholdRaw := fs.String("threshold", "", "minimum cosine score")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "limit": true, "threshold": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("neighbors requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseRequiredPositiveInt("number", *numberRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	threshold, err := parseOptionalFloat(*thresholdRaw)
	if err != nil {
		return usageErr(err)
	}
	if limit <= 0 {
		limit = 10
	}
	if threshold <= 0 {
		threshold = 0.2
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
	targetThread, targetVector, err := rt.Store.ThreadVectorByNumber(ctx, store.ThreadVectorQuery{
		RepoID: repo.ID,
		Model:  rt.Config.OpenAI.EmbedModel,
		Basis:  rt.Config.EmbeddingBasis,
	}, number)
	if err != nil {
		var fallbackErr error
		targetThread, targetVector, fallbackErr = rt.Store.ThreadVectorByNumber(ctx, store.ThreadVectorQuery{RepoID: repo.ID}, number)
		if fallbackErr != nil {
			return err
		}
	}
	vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, store.ThreadVectorQuery{
		RepoID:     repo.ID,
		Model:      targetVector.Model,
		Basis:      targetVector.Basis,
		Dimensions: targetVector.Dimensions,
	})
	if err != nil {
		return err
	}
	items := make([]vector.Item, 0, len(vectors))
	for _, stored := range vectors {
		items = append(items, vector.Item{ThreadID: stored.ThreadID, Vector: stored.Vector})
	}
	candidates := vector.Query(items, targetVector.Vector, limit*2, targetThread.ID)
	filtered := make([]vector.Neighbor, 0, limit)
	for _, candidate := range candidates {
		if candidate.Score < threshold {
			continue
		}
		filtered = append(filtered, candidate)
		if len(filtered) >= limit {
			break
		}
	}
	ids := make([]int64, 0, len(filtered))
	for _, candidate := range filtered {
		ids = append(ids, candidate.ThreadID)
	}
	threads, err := rt.Store.ThreadsByIDs(ctx, repo.ID, ids)
	if err != nil {
		return err
	}
	neighbors := make([]map[string]any, 0, len(filtered))
	for _, candidate := range filtered {
		thread, ok := threads[candidate.ThreadID]
		if !ok {
			continue
		}
		neighbors = append(neighbors, map[string]any{
			"thread_id": candidate.ThreadID,
			"number":    thread.Number,
			"kind":      thread.Kind,
			"title":     thread.Title,
			"score":     candidate.Score,
		})
	}
	return a.writeOutput("neighbors", map[string]any{
		"repository": repo.FullName,
		"thread":     targetThread,
		"neighbors":  neighbors,
	}, true)
}

func (a *App) runCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cluster", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	thresholdRaw := fs.String("threshold", fmt.Sprintf("%.2f", defaultClusterThreshold), "minimum cosine score")
	minSizeRaw := fs.String("min-size", "1", "minimum cluster member count")
	maxClusterSizeRaw := fs.String("max-cluster-size", strconv.Itoa(defaultClusterMaxSize), "maximum members per generated cluster")
	fanoutRaw := fs.String("k", strconv.Itoa(defaultClusterFanout), "nearest-neighbor fanout per thread")
	crossKindThresholdRaw := fs.String("cross-kind-threshold", fmt.Sprintf("%.2f", defaultCrossKindMinScore), "minimum score for issue/pull request edges")
	limitRaw := fs.String("limit", "", "maximum vector rows to cluster")
	model := fs.String("model", "", "embedding model")
	basis := fs.String("basis", "", "embedding basis")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"threshold": true, "min-size": true, "max-cluster-size": true, "k": true, "cross-kind-threshold": true, "limit": true, "model": true, "basis": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("cluster requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	threshold, err := parseOptionalFloat(*thresholdRaw)
	if err != nil {
		return usageErr(err)
	}
	if threshold <= 0 || threshold > 1 {
		return usageErr(fmt.Errorf("cluster requires --threshold between 0 and 1"))
	}
	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	if minSize <= 0 {
		minSize = 2
	}
	maxClusterSize, fanout, crossKindThreshold, err := parseClusterShapeOptions("cluster", *maxClusterSizeRaw, *fanoutRaw, *crossKindThresholdRaw)
	if err != nil {
		return err
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	query := store.ThreadVectorQuery{
		RepoID: repo.ID,
		Model:  firstNonEmpty(strings.TrimSpace(*model), rt.Config.OpenAI.EmbedModel),
		Basis:  firstNonEmpty(strings.TrimSpace(*basis), rt.Config.EmbeddingBasis),
	}
	vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		return err
	}
	if len(vectors) == 0 && strings.TrimSpace(*model) == "" && strings.TrimSpace(*basis) == "" {
		vectors, err = rt.Store.ListThreadVectorsFiltered(ctx, store.ThreadVectorQuery{RepoID: repo.ID})
		if err != nil {
			return err
		}
	}
	if limit > 0 && len(vectors) > limit {
		vectors = vectors[:limit]
	}
	clusterResult, err := clusterRepository(ctx, rt.Store, repo.ID, vectors, clusterBuildOptions{
		Threshold:          threshold,
		MinSize:            minSize,
		MaxClusterSize:     maxClusterSize,
		Fanout:             fanout,
		CrossKindThreshold: crossKindThreshold,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("cluster", map[string]any{
		"repository":    repo.FullName,
		"threshold":     threshold,
		"cross_kind":    crossKindThreshold,
		"min_size":      minSize,
		"max_size":      maxClusterSize,
		"k":             fanout,
		"vector_count":  len(vectors),
		"edge_count":    clusterResult.EdgeCount,
		"cluster_count": clusterResult.ClusterCount,
		"member_count":  clusterResult.MemberCount,
		"run_id":        clusterResult.RunID,
	}, true)
}

type embedResult struct {
	Repository string `json:"repository"`
	Model      string `json:"model"`
	Basis      string `json:"basis"`
	Selected   int    `json:"selected"`
	Embedded   int    `json:"embedded"`
	Skipped    int    `json:"skipped"`
	RunID      int64  `json:"run_id"`
}

func (a *App) runEmbed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("embed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "embed one issue or pull request number")
	limitRaw := fs.String("limit", "", "maximum rows to embed")
	force := fs.Bool("force", false, "re-embed even when content hash is unchanged")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("embed requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseOptionalPositiveInt(*numberRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	result, err := a.embedRepository(ctx, owner, repoName, embedOptions{
		Number: number,
		Limit:  limit,
		Force:  *force,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("embed", result, true)
}

type embedOptions struct {
	Number int
	Limit  int
	Force  bool
}

func (a *App) embedRepository(ctx context.Context, owner, repoName string, options embedOptions) (embedResult, error) {
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return embedResult{}, err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return embedResult{}, err
	}
	if rt.Config.EmbeddingBasis == "title_summary" {
		return embedResult{}, fmt.Errorf("embedding basis %q needs summarize support, which is not implemented yet; use `gitcrawl configure --embedding-basis title_original`", rt.Config.EmbeddingBasis)
	}
	token := config.ResolveOpenAIKey(rt.Config)
	if token.Value == "" {
		return embedResult{}, fmt.Errorf("missing OpenAI API key: set %s", rt.Config.OpenAI.APIKeyEnv)
	}
	tasks, err := rt.Store.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID: repo.ID,
		Basis:  rt.Config.EmbeddingBasis,
		Model:  rt.Config.OpenAI.EmbedModel,
		Number: options.Number,
		Limit:  options.Limit,
		Force:  options.Force,
	})
	if err != nil {
		return embedResult{}, err
	}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	embedded := 0
	batchSize := rt.Config.OpenAI.BatchSize
	if batchSize <= 0 {
		batchSize = 64
	}
	client := openai.New(openai.Options{APIKey: token.Value, BaseURL: openAIBaseURL(), Dimensions: rt.Config.OpenAI.EmbedDimensions})
	for start := 0; start < len(tasks); start += batchSize {
		end := start + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		batch := tasks[start:end]
		texts := make([]string, 0, len(batch))
		for _, task := range batch {
			texts = append(texts, task.Text)
		}
		fmt.Fprintf(a.Stderr, "[embed] embedding %d-%d of %d\n", start+1, end, len(tasks))
		vectors, err := client.Embed(ctx, rt.Config.OpenAI.EmbedModel, texts)
		if err != nil {
			_, _ = rt.Store.RecordRun(ctx, store.RunRecord{
				RepoID:     repo.ID,
				Kind:       "embedding",
				Scope:      "repo",
				Status:     "error",
				StartedAt:  started,
				FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
				ErrorText:  err.Error(),
			})
			return embedResult{}, err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for index, vector := range vectors {
			task := batch[index]
			if err := rt.Store.UpsertThreadVector(ctx, store.ThreadVector{
				ThreadID:    task.ThreadID,
				Basis:       rt.Config.EmbeddingBasis,
				Model:       rt.Config.OpenAI.EmbedModel,
				Dimensions:  len(vector),
				ContentHash: task.ContentHash,
				Vector:      vector,
				Backend:     "openai",
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				return embedResult{}, err
			}
			embedded++
		}
	}
	result := embedResult{
		Repository: repo.FullName,
		Model:      rt.Config.OpenAI.EmbedModel,
		Basis:      rt.Config.EmbeddingBasis,
		Selected:   len(tasks),
		Embedded:   embedded,
		RunID:      0,
	}
	statsJSON, _ := json.Marshal(result)
	runID, err := rt.Store.RecordRun(ctx, store.RunRecord{
		RepoID:     repo.ID,
		Kind:       "embedding",
		Scope:      "repo",
		Status:     "success",
		StartedAt:  started,
		FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
		StatsJSON:  string(statsJSON),
	})
	if err != nil {
		return embedResult{}, err
	}
	result.RunID = runID
	return result, nil
}

func openAIBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("GITCRAWL_OPENAI_BASE_URL")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("OPENAI_BASE_URL"))
}

func (a *App) runClusters(ctx context.Context, args []string) error {
	return a.runClusterList(ctx, "clusters", args, false)
}

func (a *App) runDurableClusters(ctx context.Context, args []string) error {
	return a.runClusterList(ctx, "durable-clusters", args, true)
}

func clusterListIncludesClosed(durable bool, includeClosed bool, hideClosed bool) bool {
	if hideClosed {
		return false
	}
	if durable {
		return includeClosed
	}
	return true
}

func (a *App) runClusterList(ctx context.Context, command string, args []string, durable bool) error {
	fs := flag.NewFlagSet("clusters", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "", "minimum active member count")
	limitRaw := fs.String("limit", "", "maximum cluster rows")
	sortMode := fs.String("sort", "size", "sort mode: recent|size")
	includeClosed := fs.Bool("include-closed", false, "deprecated; clusters include closed rows by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "sort": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("%s requires owner/repo", command))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	sort := strings.TrimSpace(*sortMode)
	if sort != "recent" && sort != "size" {
		return usageErr(fmt.Errorf("unsupported sort %q", sort))
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
	options := store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: clusterListIncludesClosed(durable, *includeClosed, *hideClosed),
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	}
	var clusters []store.ClusterSummary
	if durable {
		clusters, err = rt.Store.ListClusterSummaries(ctx, options)
	} else {
		clusters, err = rt.Store.ListDisplayClusterSummaries(ctx, options)
	}
	if err != nil {
		return err
	}
	return a.writeOutput(command, map[string]any{
		"repository": repo.FullName,
		"clusters":   clusters,
	}, true)
}

func (a *App) runTUI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "", "minimum active member count")
	limitRaw := fs.String("limit", "", "maximum cluster rows")
	sortMode := fs.String("sort", "", "sort mode: recent|size")
	includeClosed := fs.Bool("include-closed", false, "deprecated; closed clusters are shown by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "sort": true})); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("tui")
		}
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() > 1 {
		return usageErr(fmt.Errorf("tui accepts at most one owner/repo"))
	}

	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	if strings.TrimSpace(*minSizeRaw) == "" {
		minSize = defaultTUIMinSize
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}

	interactive := a.format == FormatText && a.canRunInteractiveTUI()
	var rt localRuntime
	if interactive {
		rt, err = a.openLocalRuntime(ctx)
	} else {
		rt, err = a.openLocalRuntimeReadOnly(ctx)
	}
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, inferred, err := a.resolveOptionalRepository(ctx, rt, fs.Args())
	if err != nil {
		return err
	}
	sort := strings.TrimSpace(*sortMode)
	if sort == "" {
		sort = strings.TrimSpace(rt.Config.TUI.DefaultSort)
	}
	if sort == "" {
		sort = "size"
	}
	if sort != "recent" && sort != "size" {
		return usageErr(fmt.Errorf("unsupported sort %q", sort))
	}
	showClosed := !*hideClosed || *includeClosed

	clusters, err := rt.Store.ListDisplayClusterSummaries(ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: showClosed,
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	})
	if err != nil {
		return err
	}
	if interactive {
		workingSet, err := rt.Store.ListDisplayClusterSummaries(ctx, store.ClusterSummaryOptions{
			RepoID:        repo.ID,
			IncludeClosed: showClosed,
			MinSize:       1,
			Limit:         maxInt(defaultTUIWorkingSetLimit, limit),
			Sort:          sort,
		})
		if err != nil {
			return err
		}
		clusters = mergeClusterSummaries(clusters, workingSet)
	}
	if clusters == nil {
		clusters = []store.ClusterSummary{}
	}
	payload := clusterBrowserPayload{
		Repository:         repo.FullName,
		InferredRepository: inferred,
		Mode:               "cluster-browser",
		Sort:               sort,
		MinSize:            minSize,
		Limit:              limit,
		HideClosed:         !showClosed,
		EmbedModel:         rt.Config.OpenAI.EmbedModel,
		EmbeddingBasis:     rt.Config.EmbeddingBasis,
		Clusters:           clusters,
	}
	if !interactive {
		if a.format == FormatText {
			return usageErr(fmt.Errorf("tui requires an interactive terminal; run it from a TTY or pass --json for machine-readable cluster data"))
		}
		return a.writeOutput("tui", payload, true)
	}
	return a.runInteractiveTUI(ctx, rt.Store, repo.ID, payload)
}

func (a *App) resolveOptionalRepository(ctx context.Context, rt localRuntime, args []string) (store.Repository, bool, error) {
	if len(args) == 0 {
		repo, err := rt.defaultRepository(ctx)
		if err != nil {
			return store.Repository{}, false, usageErr(fmt.Errorf("tui could not infer a repository: %w; run gitcrawl sync owner/repo or pass owner/repo explicitly", err))
		}
		return repo, true, nil
	}
	owner, repoName, err := parseOwnerRepo(args[0])
	if err != nil {
		return store.Repository{}, false, usageErr(err)
	}
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return store.Repository{}, false, err
	}
	return repo, false, nil
}

func mergeClusterSummaries(primary, secondary []store.ClusterSummary) []store.ClusterSummary {
	if len(primary) == 0 {
		return append([]store.ClusterSummary(nil), secondary...)
	}
	out := append([]store.ClusterSummary(nil), primary...)
	seen := make(map[int64]bool, len(out)+len(secondary))
	for _, cluster := range out {
		seen[cluster.ID] = true
	}
	for _, cluster := range secondary {
		if !seen[cluster.ID] {
			out = append(out, cluster)
			seen[cluster.ID] = true
		}
	}
	return out
}

func (a *App) runClusterDetail(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cluster-detail", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	clusterIDRaw := fs.String("id", "", "cluster id")
	memberLimitRaw := fs.String("member-limit", "", "maximum member rows")
	bodyCharsRaw := fs.String("body-chars", "", "maximum body snippet characters")
	includeClosed := fs.Bool("include-closed", false, "include closed clusters and members")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "member-limit": true, "body-chars": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("cluster-detail requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, err := parseRequiredPositiveInt("id", *clusterIDRaw)
	if err != nil {
		return usageErr(err)
	}
	memberLimit, err := parseOptionalPositiveInt(*memberLimitRaw)
	if err != nil {
		return usageErr(err)
	}
	bodyChars, err := parseOptionalPositiveInt(*bodyCharsRaw)
	if err != nil {
		return usageErr(err)
	}
	if bodyChars <= 0 {
		bodyChars = 280
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
	detail, err := rt.Store.ClusterDetail(ctx, store.ClusterDetailOptions{
		RepoID:        repo.ID,
		ClusterID:     int64(clusterID),
		IncludeClosed: *includeClosed,
		MemberLimit:   memberLimit,
		BodyChars:     bodyChars,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("cluster-detail", map[string]any{
		"repository": repo.FullName,
		"cluster":    detail.Cluster,
		"members":    detail.Members,
	}, true)
}

func (a *App) runRuns(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("runs", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	kind := fs.String("kind", "sync", "run kind: sync|summary|embedding|cluster")
	limitRaw := fs.String("limit", "", "maximum run rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"kind": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("runs requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
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
	runs, err := rt.Store.ListRuns(ctx, repo.ID, strings.TrimSpace(*kind), limit)
	if err != nil {
		return err
	}
	return a.writeOutput("runs", map[string]any{
		"repository": repo.FullName,
		"kind":       strings.TrimSpace(*kind),
		"runs":       runs,
	}, true)
}

func (a *App) runThreads(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("threads", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	includeClosed := fs.Bool("include-closed", false, "include locally closed rows")
	numbersRaw := fs.String("numbers", "", "comma-separated issue or pull request numbers")
	limitRaw := fs.String("limit", "", "maximum thread rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"numbers": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("threads requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	numbers, err := parseOptionalPositiveIntList(*numbersRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
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
	threads, err := rt.Store.ListThreadsFiltered(ctx, store.ThreadListOptions{
		RepoID:        repo.ID,
		IncludeClosed: *includeClosed,
		Numbers:       numbers,
		Limit:         limit,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("threads", map[string]any{
		"repository": repo.FullName,
		"threads":    threads,
	}, true)
}

func (a *App) runCloseThread(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("close-thread", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "issue or pull request number")
	reason := fs.String("reason", "CLI manual close", "local close reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("close-thread requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseOptionalPositiveInt(*numberRaw)
	if err != nil {
		return usageErr(err)
	}
	if number == 0 {
		return usageErr(fmt.Errorf("close-thread requires --number"))
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	if err := rt.Store.CloseThreadLocally(ctx, repo.ID, number, *reason); err != nil {
		return err
	}
	return a.writeOutput("close-thread", map[string]any{
		"repository": repo.FullName,
		"number":     number,
		"reason":     strings.TrimSpace(*reason),
		"closed":     true,
	}, true)
}

func (a *App) runReopenThread(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reopen-thread", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "issue or pull request number")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("reopen-thread requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseOptionalPositiveInt(*numberRaw)
	if err != nil {
		return usageErr(err)
	}
	if number == 0 {
		return usageErr(fmt.Errorf("reopen-thread requires --number"))
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	if err := rt.Store.ReopenThreadLocally(ctx, repo.ID, number); err != nil {
		return err
	}
	return a.writeOutput("reopen-thread", map[string]any{
		"repository": repo.FullName,
		"number":     number,
		"reopened":   true,
	}, true)
}

func (a *App) runCloseCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("close-cluster", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	reason := fs.String("reason", "CLI manual close", "local close reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("close-cluster requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, err := parseOptionalPositiveInt(*idRaw)
	if err != nil {
		return usageErr(err)
	}
	if clusterID == 0 {
		return usageErr(fmt.Errorf("close-cluster requires --id"))
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	if err := rt.Store.CloseClusterLocally(ctx, repo.ID, int64(clusterID), *reason); err != nil {
		return err
	}
	return a.writeOutput("close-cluster", map[string]any{
		"repository": repo.FullName,
		"id":         clusterID,
		"reason":     strings.TrimSpace(*reason),
		"closed":     true,
	}, true)
}

func (a *App) runReopenCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reopen-cluster", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("reopen-cluster requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, err := parseOptionalPositiveInt(*idRaw)
	if err != nil {
		return usageErr(err)
	}
	if clusterID == 0 {
		return usageErr(fmt.Errorf("reopen-cluster requires --id"))
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	if err := rt.Store.ReopenClusterLocally(ctx, repo.ID, int64(clusterID)); err != nil {
		return err
	}
	return a.writeOutput("reopen-cluster", map[string]any{
		"repository": repo.FullName,
		"id":         clusterID,
		"reopened":   true,
	}, true)
}

func (a *App) runExcludeClusterMember(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("exclude-cluster-member", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	numberRaw := fs.String("number", "", "issue or pull request number")
	reason := fs.String("reason", "CLI manual exclude", "local override reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "number": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("exclude-cluster-member requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, number, err := parseClusterMemberCommandIDs("exclude-cluster-member", *idRaw, *numberRaw)
	if err != nil {
		return usageErr(err)
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	override, err := rt.Store.ExcludeClusterMemberLocally(ctx, repo.ID, int64(clusterID), number, *reason)
	if err != nil {
		return err
	}
	return a.writeOutput("exclude-cluster-member", map[string]any{
		"repository": repo.FullName,
		"override":   override,
		"excluded":   true,
	}, true)
}

func (a *App) runIncludeClusterMember(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("include-cluster-member", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	numberRaw := fs.String("number", "", "issue or pull request number")
	reason := fs.String("reason", "CLI manual include", "local override reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "number": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("include-cluster-member requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, number, err := parseClusterMemberCommandIDs("include-cluster-member", *idRaw, *numberRaw)
	if err != nil {
		return usageErr(err)
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	override, err := rt.Store.IncludeClusterMemberLocally(ctx, repo.ID, int64(clusterID), number, *reason)
	if err != nil {
		return err
	}
	return a.writeOutput("include-cluster-member", map[string]any{
		"repository": repo.FullName,
		"override":   override,
		"included":   true,
	}, true)
}

func (a *App) runSetClusterCanonical(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("set-cluster-canonical", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	numberRaw := fs.String("number", "", "issue or pull request number")
	reason := fs.String("reason", "CLI manual canonical", "local override reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "number": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("set-cluster-canonical requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, number, err := parseClusterMemberCommandIDs("set-cluster-canonical", *idRaw, *numberRaw)
	if err != nil {
		return usageErr(err)
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	override, err := rt.Store.SetClusterCanonicalLocally(ctx, repo.ID, int64(clusterID), number, *reason)
	if err != nil {
		return err
	}
	return a.writeOutput("set-cluster-canonical", map[string]any{
		"repository": repo.FullName,
		"override":   override,
		"canonical":  true,
	}, true)
}

func (a *App) runSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	since := fs.String("since", "", "GitHub since timestamp")
	state := fs.String("state", "", "GitHub issue state: open|closed|all; default open")
	limitRaw := fs.String("limit", "", "maximum issue/PR rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	includeComments := fs.Bool("include-comments", false, "hydrate issue comments, PR reviews, and PR review comments")
	fs.Bool("include-code", false, "accepted for compatibility; code hydration is not implemented yet")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"since": true, "state": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("sync requires owner/repo"))
	}
	owner, repo, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}

	stats, err := a.syncRepository(ctx, owner, repo, syncOptions{
		Since:           strings.TrimSpace(*since),
		State:           strings.TrimSpace(*state),
		Limit:           limit,
		IncludeComments: *includeComments,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("sync", stats, true)
}

type syncOptions struct {
	Since           string
	State           string
	Limit           int
	IncludeComments bool
}

func (a *App) syncRepository(ctx context.Context, owner, repo string, options syncOptions) (syncer.Stats, error) {
	cfg, err := config.Load(a.configPath)
	if err != nil {
		return syncer.Stats{}, err
	}
	token := config.ResolveGitHubToken(cfg)
	if token.Value == "" {
		return syncer.Stats{}, fmt.Errorf("missing GitHub token: set %s", cfg.GitHub.TokenEnv)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return syncer.Stats{}, err
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return syncer.Stats{}, err
	}
	defer st.Close()

	client := gh.New(gh.Options{Token: token.Value})
	service := syncer.New(client, st)
	stats, err := service.Sync(ctx, syncer.Options{
		Owner:           owner,
		Repo:            repo,
		State:           strings.TrimSpace(options.State),
		Since:           strings.TrimSpace(options.Since),
		Limit:           options.Limit,
		IncludeComments: options.IncludeComments,
		Reporter: func(message string) {
			fmt.Fprintln(a.Stderr, message)
		},
	})
	if err != nil {
		return syncer.Stats{}, err
	}
	return stats, nil
}

func (a *App) runInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "", "database path")
	portableStore := fs.String("portable-store", "", "HTTPS git URL for a portable gitcrawl store")
	portableDB := fs.String("portable-db", "data/openclaw__openclaw.sync.db", "database path inside portable store")
	storeDir := fs.String("store-dir", "", "local portable store checkout directory")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"db": true, "portable-store": true, "portable-db": true, "store-dir": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if strings.TrimSpace(*dbPath) != "" && strings.TrimSpace(*portableStore) != "" {
		return usageErr(fmt.Errorf("use either --db or --portable-store, not both"))
	}

	cfg := config.Default()
	portableStoreURL := strings.TrimSpace(*portableStore)
	portableStoreDir := ""
	portableStoreAction := ""
	if portableStoreURL != "" {
		portableStoreDir = strings.TrimSpace(*storeDir)
		if portableStoreDir == "" {
			portableStoreDir = defaultPortableStoreDir(config.ResolvePath(a.configPath), portableStoreURL)
		}
		action, err := syncPortableStore(ctx, portableStoreURL, portableStoreDir)
		if err != nil {
			return err
		}
		portableStoreAction = action
		relativeDB := filepath.Clean(filepath.FromSlash(strings.TrimLeft(strings.TrimSpace(*portableDB), "/")))
		if relativeDB == "." || filepath.IsAbs(relativeDB) || strings.HasPrefix(relativeDB, ".."+string(os.PathSeparator)) || relativeDB == ".." {
			return usageErr(fmt.Errorf("invalid --portable-db %q", *portableDB))
		}
		cfg.DBPath = filepath.Join(portableStoreDir, relativeDB)
		if _, err := os.Stat(cfg.DBPath); err != nil {
			return fmt.Errorf("portable database not found at %s: %w", cfg.DBPath, err)
		}
	}
	if strings.TrimSpace(*dbPath) != "" {
		cfg.DBPath = strings.TrimSpace(*dbPath)
	}
	if err := config.Save(a.configPath, cfg); err != nil {
		return err
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}
	return a.writeInitOutput(initResult{
		ConfigPath:       config.ResolvePath(a.configPath),
		DBPath:           cfg.DBPath,
		CacheDir:         cfg.CacheDir,
		VectorDir:        cfg.VectorDir,
		PortableStoreURL: portableStoreURL,
		PortableStoreDir: portableStoreDir,
		PortableStore:    portableStoreAction,
	})
}

func (a *App) runPortable(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("portable requires a subcommand"))
	}
	switch args[0] {
	case "prune":
		return a.runPortablePrune(ctx, args[1:])
	default:
		return usageErr(fmt.Errorf("unknown portable subcommand %q", args[0]))
	}
}

func (a *App) runPortablePrune(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("portable prune", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	bodyCharsRaw := fs.String("body-chars", "256", "maximum thread body characters to keep")
	noVacuum := fs.Bool("no-vacuum", false, "skip SQLite vacuum after pruning")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"body-chars": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("portable prune does not take positional arguments"))
	}
	bodyChars, err := parseOptionalPositiveInt(*bodyCharsRaw)
	if err != nil {
		return usageErr(err)
	}
	if bodyChars == 0 {
		bodyChars = 256
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	stats, err := rt.Store.PrunePortablePayloads(ctx, store.PortablePruneOptions{
		BodyChars: bodyChars,
		Vacuum:    !*noVacuum,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("portable prune", stats, true)
}

func defaultPortableStoreDir(configPath, remoteURL string) string {
	base := filepath.Join(filepath.Dir(configPath), "stores")
	name := strings.TrimSuffix(remoteURL, ".git")
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = safePathName(name)
	if name == "" {
		name = "portable-store"
	}
	return filepath.Join(base, name)
}

func safePathName(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(value) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-.")
}

func syncPortableStore(ctx context.Context, remoteURL, dir string) (string, error) {
	if strings.TrimSpace(remoteURL) == "" {
		return "", fmt.Errorf("portable store URL is required")
	}
	if strings.TrimSpace(dir) == "" {
		return "", fmt.Errorf("portable store directory is required")
	}
	gitDir := filepath.Join(dir, ".git")
	if info, err := os.Stat(gitDir); err == nil && info.IsDir() {
		if !gitWorktreeClean(ctx, dir) {
			if resetErr := runGit(ctx, "", "-C", dir, "reset", "--hard", "HEAD"); resetErr != nil {
				return "", resetErr
			}
			if retryErr := runGit(ctx, "", "-C", dir, "pull", "--ff-only"); retryErr != nil {
				return "", retryErr
			}
			return "reset-pulled", nil
		}
		if err := runGit(ctx, "", "-C", dir, "pull", "--ff-only"); err != nil {
			if !isDirtyPortablePullError(err) {
				return "", err
			}
			if resetErr := runGit(ctx, "", "-C", dir, "reset", "--hard", "HEAD"); resetErr != nil {
				return "", err
			}
			if retryErr := runGit(ctx, "", "-C", dir, "pull", "--ff-only"); retryErr != nil {
				return "", retryErr
			}
			return "reset-pulled", nil
		}
		return "pulled", nil
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return "", fmt.Errorf("portable store directory %s exists but is not a git checkout", dir)
	} else if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read portable store directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", fmt.Errorf("create portable store parent: %w", err)
	}
	if err := runGit(ctx, "", "clone", "--depth", "1", remoteURL, dir); err != nil {
		return "", err
	}
	return "cloned", nil
}

func isDirtyPortablePullError(err error) bool {
	message := err.Error()
	return strings.Contains(message, "Your local changes") || strings.Contains(message, "would be overwritten by merge")
}

func runGit(ctx context.Context, workdir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = workdir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (a *App) runDoctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	_ = ctx

	cfg, err := config.Load(a.configPath)
	configExists := true
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		configExists = false
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return err
		}
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}
	storeStatus := store.Status{DBPath: cfg.DBPath}
	st, err := store.OpenReadOnly(ctx, cfg.DBPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		defer st.Close()
		storeStatus, err = st.Status(ctx)
		if err != nil {
			return err
		}
	}

	githubToken := config.ResolveGitHubToken(cfg)
	openAIKey := config.ResolveOpenAIKey(cfg)
	return a.writeOutput("doctor", map[string]any{
		"version":              version,
		"config_path":          config.ResolvePath(a.configPath),
		"config_exists":        configExists,
		"db_path":              cfg.DBPath,
		"github_token_present": githubToken.Value != "",
		"github_token_source":  githubToken.Source,
		"openai_key_present":   openAIKey.Value != "",
		"openai_key_source":    openAIKey.Source,
		"repository_count":     storeStatus.RepositoryCount,
		"thread_count":         storeStatus.ThreadCount,
		"open_thread_count":    storeStatus.OpenThreadCount,
		"cluster_count":        storeStatus.ClusterCount,
		"last_sync_at":         formatOptionalTime(storeStatus.LastSyncAt),
		"summary_model":        cfg.OpenAI.SummaryModel,
		"embed_model":          cfg.OpenAI.EmbedModel,
		"embedding_basis":      cfg.EmbeddingBasis,
		"api_supported":        false,
	}, true)
}

func (a *App) applyCommandJSON(enabled bool) {
	if enabled {
		a.format = FormatJSON
	}
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func resolveOutputFormat(value string, jsonOut bool) (OutputFormat, error) {
	if jsonOut {
		return FormatJSON, nil
	}
	switch OutputFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", FormatText:
		return FormatText, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatLog:
		return FormatLog, nil
	default:
		return "", fmt.Errorf("unsupported format %q: use text, json, or log", value)
	}
}

func parseOwnerRepo(value string) (string, string, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("expected owner/repo, got %q", value)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func parseOptionalPositiveInt(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("expected positive integer, got %q", value)
	}
	return parsed, nil
}

func parseRequiredPositiveInt(name, value string) (int, error) {
	parsed, err := parseOptionalPositiveInt(value)
	if err != nil {
		return 0, err
	}
	if parsed == 0 {
		return 0, fmt.Errorf("missing --%s", name)
	}
	return parsed, nil
}

func parseClusterMemberCommandIDs(command, clusterIDRaw, numberRaw string) (int, int, error) {
	clusterID, err := parseOptionalPositiveInt(clusterIDRaw)
	if err != nil {
		return 0, 0, err
	}
	if clusterID == 0 {
		return 0, 0, fmt.Errorf("%s requires --id", command)
	}
	number, err := parseOptionalPositiveInt(numberRaw)
	if err != nil {
		return 0, 0, err
	}
	if number == 0 {
		return 0, 0, fmt.Errorf("%s requires --number", command)
	}
	return clusterID, number, nil
}

type clusterBuildOptions struct {
	Threshold          float64
	MinSize            int
	MaxClusterSize     int
	Fanout             int
	CrossKindThreshold float64
}

func parseClusterShapeOptions(command, maxClusterSizeRaw, fanoutRaw, crossKindThresholdRaw string) (int, int, float64, error) {
	maxClusterSize, err := parseOptionalPositiveInt(maxClusterSizeRaw)
	if err != nil {
		return 0, 0, 0, err
	}
	fanout, err := parseOptionalPositiveInt(fanoutRaw)
	if err != nil {
		return 0, 0, 0, err
	}
	crossKindThreshold, err := parseOptionalFloat(crossKindThresholdRaw)
	if err != nil {
		return 0, 0, 0, err
	}
	if maxClusterSize == 0 {
		maxClusterSize = defaultClusterMaxSize
	}
	if fanout == 0 {
		fanout = defaultClusterFanout
	}
	if crossKindThreshold == 0 {
		crossKindThreshold = defaultCrossKindMinScore
	}
	if crossKindThreshold < 0 || crossKindThreshold > 1 {
		return 0, 0, 0, fmt.Errorf("%s requires --cross-kind-threshold between 0 and 1", command)
	}
	return maxClusterSize, fanout, crossKindThreshold, nil
}

func buildDurableClusterInputs(ctx context.Context, st *store.Store, repoID int64, storedVectors []store.ThreadVector, options clusterBuildOptions) ([]store.DurableClusterInput, int, error) {
	if options.MinSize <= 0 {
		options.MinSize = 1
	}
	if options.MaxClusterSize <= 0 {
		options.MaxClusterSize = defaultClusterMaxSize
	}
	if options.Fanout <= 0 {
		options.Fanout = defaultClusterFanout
	}
	if options.CrossKindThreshold <= 0 {
		options.CrossKindThreshold = defaultCrossKindMinScore
	}
	threadIDs := make([]int64, 0, len(storedVectors))
	vectorByThreadID := make(map[int64][]float64, len(storedVectors))
	for _, stored := range storedVectors {
		threadIDs = append(threadIDs, stored.ThreadID)
		vectorByThreadID[stored.ThreadID] = stored.Vector
	}
	threads, err := st.ThreadsByIDs(ctx, repoID, threadIDs)
	if err != nil {
		return nil, 0, err
	}
	nodes := make([]clusterer.Node, 0, len(storedVectors))
	for _, stored := range storedVectors {
		thread, ok := threads[stored.ThreadID]
		if !ok {
			continue
		}
		nodes = append(nodes, clusterer.Node{ThreadID: stored.ThreadID, Number: thread.Number, Title: thread.Title})
	}
	candidateByPair := map[string]clusterer.Edge{}
	for left := 0; left < len(nodes); left++ {
		for right := left + 1; right < len(nodes); right++ {
			leftID := nodes[left].ThreadID
			rightID := nodes[right].ThreadID
			score := vector.Cosine(vectorByThreadID[leftID], vectorByThreadID[rightID])
			if score < options.Threshold {
				continue
			}
			if score < highConfidenceEdgeScore && titleTokenOverlap(threads[leftID].Title, threads[rightID].Title) < weakEdgeMinTitleOverlap {
				continue
			}
			if threads[leftID].Kind != threads[rightID].Kind && score < options.CrossKindThreshold {
				continue
			}
			upsertClusterEdge(candidateByPair, leftID, rightID, score)
		}
	}
	addDeterministicReferenceEdges(candidateByPair, nodes, threads)
	candidates := make([]clusterer.Edge, 0, len(candidateByPair))
	for _, edge := range candidateByPair {
		candidates = append(candidates, edge)
	}
	edges := keepTopEdges(candidates, options.Fanout)
	pairScores := map[string]float64{}
	for _, edge := range edges {
		pairScores[threadIDPairKey(edge.LeftThreadID, edge.RightThreadID)] = edge.Score
	}
	built := clusterer.BuildWithOptions(nodes, edges, clusterer.Options{MaxSize: options.MaxClusterSize})
	inputs := make([]store.DurableClusterInput, 0, len(built))
	for _, builtCluster := range built {
		if len(builtCluster.Members) < options.MinSize {
			continue
		}
		sort.Slice(builtCluster.Members, func(i, j int) bool {
			left := threads[builtCluster.Members[i]]
			right := threads[builtCluster.Members[j]]
			return left.Number < right.Number
		})
		identity := store.HumanKeyForValue(fmt.Sprintf("repo:%d:cluster-representative:%d", repoID, builtCluster.RepresentativeThreadID))
		clusterType := "duplicate_candidate"
		if len(builtCluster.Members) == 1 {
			clusterType = "singleton_orphan"
		}
		input := store.DurableClusterInput{
			StableKey:              identity.Hash,
			StableSlug:             store.HumanKeyStableSlug(identity),
			ClusterType:            clusterType,
			RepresentativeThreadID: builtCluster.RepresentativeThreadID,
			Title:                  "Cluster " + identity.Slug,
			Members:                make([]store.DurableClusterMemberInput, 0, len(builtCluster.Members)),
		}
		for _, threadID := range builtCluster.Members {
			role := "related"
			var scorePtr *float64
			if threadID == builtCluster.RepresentativeThreadID {
				role = "canonical"
				scoreCopy := 1.0
				scorePtr = &scoreCopy
			} else if score, ok := pairScores[threadIDPairKey(threadID, builtCluster.RepresentativeThreadID)]; ok {
				scoreCopy := score
				scorePtr = &scoreCopy
			}
			input.Members = append(input.Members, store.DurableClusterMemberInput{ThreadID: threadID, Role: role, ScoreToRepresentative: scorePtr})
		}
		inputs = append(inputs, input)
	}
	return inputs, len(edges), nil
}

func upsertClusterEdge(edges map[string]clusterer.Edge, leftID, rightID int64, score float64) {
	if leftID == rightID {
		return
	}
	key := threadIDPairKey(leftID, rightID)
	if existing, ok := edges[key]; ok && existing.Score >= score {
		return
	}
	if leftID > rightID {
		leftID, rightID = rightID, leftID
	}
	edges[key] = clusterer.Edge{LeftThreadID: leftID, RightThreadID: rightID, Score: score}
}

func addDeterministicReferenceEdges(edges map[string]clusterer.Edge, nodes []clusterer.Node, threads map[int64]store.Thread) {
	threadIDByNumber := make(map[int]int64, len(nodes))
	for _, node := range nodes {
		thread := threads[node.ThreadID]
		threadIDByNumber[thread.Number] = node.ThreadID
	}
	refIDsByThreadID := make(map[int64]map[int64]bool, len(nodes))
	for _, node := range nodes {
		thread := threads[node.ThreadID]
		refNumbers := referencedThreadNumbersByLocation(thread)
		refIDs := map[int64]bool{}
		for number, evidence := range refNumbers {
			if referencedID, ok := threadIDByNumber[number]; ok && referencedID != node.ThreadID {
				referencedThread := threads[referencedID]
				if evidence.Title || evidence.EarlyBody || titleTokenOverlap(thread.Title, referencedThread.Title) >= weakEdgeMinTitleOverlap {
					refIDs[referencedID] = true
				}
			}
		}
		refIDsByThreadID[node.ThreadID] = refIDs
	}
	for threadID, refIDs := range refIDsByThreadID {
		for referencedID := range refIDs {
			upsertClusterEdge(edges, threadID, referencedID, deterministicRefScore)
		}
	}
}

func referencedThreadNumbersByLocation(thread store.Thread) map[int]referenceEvidence {
	refs := map[int]referenceEvidence{}
	collectReferencedThreadNumbers(refs, thread.Number, thread.Body, false)
	collectReferencedThreadNumbers(refs, thread.Number, thread.Title, true)
	return refs
}

func collectReferencedThreadNumbers(refs map[int]referenceEvidence, threadNumber int, value string, titleRef bool) {
	for _, match := range threadReferencePattern.FindAllStringSubmatchIndex(value, -1) {
		numberText := ""
		for index := 2; index+1 < len(match); index += 2 {
			if match[index] >= 0 {
				numberText = value[match[index]:match[index+1]]
				break
			}
		}
		number, err := strconv.Atoi(numberText)
		if err != nil || number <= 0 || number == threadNumber {
			continue
		}
		evidence := refs[number]
		if titleRef {
			evidence.Title = true
		} else if match[0] <= bodyRefEvidencePrefixChars {
			evidence.EarlyBody = true
		}
		refs[number] = evidence
	}
}

func titleTokenOverlap(left, right string) float64 {
	leftTokens := titleTokenSet(left)
	rightTokens := titleTokenSet(right)
	if len(leftTokens) == 0 || len(rightTokens) == 0 {
		return 0
	}
	overlap := 0
	for token := range leftTokens {
		if rightTokens[token] {
			overlap++
		}
	}
	base := len(leftTokens)
	if len(rightTokens) < base {
		base = len(rightTokens)
	}
	return float64(overlap) / float64(base)
}

func titleTokenSet(value string) map[string]bool {
	out := map[string]bool{}
	for _, token := range titleTokenPattern.FindAllString(strings.ToLower(value), -1) {
		out[token] = true
	}
	return out
}

func keepTopEdges(edges []clusterer.Edge, fanout int) []clusterer.Edge {
	if fanout <= 0 || len(edges) == 0 {
		return edges
	}
	neighbors := map[int64][]clusterer.Edge{}
	for _, edge := range edges {
		neighbors[edge.LeftThreadID] = append(neighbors[edge.LeftThreadID], edge)
		neighbors[edge.RightThreadID] = append(neighbors[edge.RightThreadID], edge)
	}
	top := map[int64]map[int64]bool{}
	for threadID, list := range neighbors {
		sort.SliceStable(list, func(i, j int) bool {
			if list[i].Score == list[j].Score {
				return edgeOtherThreadID(list[i], threadID) < edgeOtherThreadID(list[j], threadID)
			}
			return list[i].Score > list[j].Score
		})
		if len(list) > fanout {
			list = list[:fanout]
		}
		seen := make(map[int64]bool, len(list))
		for _, edge := range list {
			seen[edgeOtherThreadID(edge, threadID)] = true
		}
		top[threadID] = seen
	}
	out := make([]clusterer.Edge, 0, len(edges))
	for _, edge := range edges {
		if top[edge.LeftThreadID][edge.RightThreadID] || top[edge.RightThreadID][edge.LeftThreadID] {
			out = append(out, edge)
		}
	}
	return out
}

func edgeOtherThreadID(edge clusterer.Edge, threadID int64) int64 {
	if edge.LeftThreadID == threadID {
		return edge.RightThreadID
	}
	return edge.LeftThreadID
}

type clusterRepositoryResult struct {
	EdgeCount    int
	ClusterCount int
	MemberCount  int
	RunID        int64
}

func clusterRepository(ctx context.Context, st *store.Store, repoID int64, storedVectors []store.ThreadVector, options clusterBuildOptions) (clusterRepositoryResult, error) {
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, storedVectors, options)
	if err != nil {
		return clusterRepositoryResult{}, err
	}
	saveResult, err := st.SaveDurableClusters(ctx, repoID, inputs)
	if err != nil {
		return clusterRepositoryResult{}, err
	}
	return clusterRepositoryResult{
		EdgeCount:    edgeCount,
		ClusterCount: saveResult.ClusterCount,
		MemberCount:  saveResult.MemberCount,
		RunID:        saveResult.RunID,
	}, nil
}

func threadIDPairKey(left, right int64) string {
	if left > right {
		left, right = right, left
	}
	return strconv.FormatInt(left, 10) + ":" + strconv.FormatInt(right, 10)
}

func parseOptionalFloat(value string) (float64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("expected number, got %q", value)
	}
	return parsed, nil
}

func parseOptionalPositiveIntList(value string) ([]int, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := parseOptionalPositiveInt(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func (a *App) writeOutput(title string, payload any, allowLog bool) error {
	switch a.format {
	case FormatJSON:
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.Stdout, "%s\n", data)
		return err
	case FormatLog:
		if allowLog {
			_, err := fmt.Fprintf(a.Stdout, "%s=%v\n", title, payload)
			return err
		}
		fallthrough
	default:
		if versionPayload, ok := payload.(map[string]string); ok && title == "version" {
			_, err := fmt.Fprintln(a.Stdout, versionPayload["version"])
			return err
		}
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.Stdout, "%s\n%s\n", title, data)
		return err
	}
}

func (a *App) writeInitOutput(result initResult) error {
	switch a.format {
	case FormatJSON:
		return a.writeOutput("init", result, true)
	case FormatLog:
		_, err := fmt.Fprintf(a.Stdout, "init config_path=%s db_path=%s portable_store=%s\n", result.ConfigPath, result.DBPath, result.PortableStore)
		return err
	default:
		lines := []string{
			"gitcrawl init",
			"config path: " + result.ConfigPath,
			"db path: " + result.DBPath,
			"cache dir: " + result.CacheDir,
			"vector dir: " + result.VectorDir,
		}
		if result.PortableStoreURL != "" {
			lines = append(lines,
				"",
				"Portable store",
				"  url: "+result.PortableStoreURL,
				"  checkout: "+result.PortableStoreDir,
				"  state: "+firstNonEmpty(result.PortableStore, "ready"),
			)
		}
		_, err := fmt.Fprintln(a.Stdout, strings.Join(lines, "\n"))
		return err
	}
}

func (a *App) printUsage() {
	fmt.Fprint(a.Stdout, usageText)
}

func (a *App) printCommandUsage(command string) error {
	switch command {
	case "tui":
		fmt.Fprint(a.Stdout, tuiUsageText)
		return nil
	default:
		return usageErr(fmt.Errorf("unknown help topic %q", command))
	}
}

const usageText = `gitcrawl mirrors GitHub issues and pull requests into local SQLite for maintainer triage.

Usage:
  gitcrawl [global flags] <command> [command flags]
  gitcrawl help <command>

Global flags:
  --config <path>       config path
  --format <mode>      output format: text|json|log
  --json               write JSON output
  --version            print version

Core commands:
  init                 create config, optionally from a portable store
  doctor               check config, token, and database readiness
  sync                 sync GitHub issue and pull request metadata
  refresh              run sync, enrichment, embedding, and clustering pipeline
  threads              list local issue and pull request rows
  cluster              build durable clusters from local thread vectors
  close-thread         locally hide one issue or pull request row
  reopen-thread        clear a local hide for one issue or pull request row
  close-cluster        locally hide one durable cluster
  reopen-cluster       clear a local hide for one durable cluster
  exclude-cluster-member
                       locally remove one row from a durable cluster
  include-cluster-member
                       restore one row to a durable cluster
  set-cluster-canonical
                       set the canonical row for a durable cluster
  clusters             list latest run cluster summaries, with durable fallback
  durable-clusters     list durable cluster groups
  cluster-detail       dump one latest run cluster, with durable fallback
  cluster-explain      alias for cluster-detail
  neighbors            list vector-nearest local issue and pull request rows
  search               search local thread documents; also supports search issues|prs gh syntax
  portable prune       prune volatile payloads from a portable store
  tui [owner/repo]     browse clusters in the terminal UI; repo is inferred when omitted

No API server is provided. There is intentionally no serve command.
`

const tuiUsageText = `gitcrawl tui opens the local terminal cluster browser.

Usage:
  gitcrawl tui [owner/repo] [--limit N] [--min-size N] [--sort recent|size] [--hide-closed]

If owner/repo is omitted, gitcrawl uses the most recently updated repository in the local database.
The TUI starts with ghcrawl-style cluster display defaults: --min-size 5, --sort size, and closed historical clusters visible. Pass --min-size 1 for singleton clusters or --hide-closed to focus open-only.
Mouse is supported: click rows, wheel panes, right-click for actions, and use the menu for copy/sort/filter/jump/member triage controls.
Press a to open the same action menu from the keyboard.
Press # to jump directly to an issue or PR number.
Press p to switch between repositories already present in the local store.
Press n to load neighbors for the selected issue or PR.
Enter from the members pane also loads neighbors before opening detail.
The TUI quietly refreshes from the local store every 15 seconds and leaves the current status alone when nothing changed.
`
