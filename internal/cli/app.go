package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/openclaw/crawlkit/control"
	"github.com/openclaw/crawlkit/mirror"
	crawlremote "github.com/openclaw/crawlkit/remote"
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

var errNoSemanticVectors = errors.New("no semantic vectors")

var threadReferencePattern = regexp.MustCompile(`(?i)(?:\b([\w.-]+/[\w.-]+)#(\d+)|(?:issues|pull)/(\d+)|#(\d{2,}))`)
var githubThreadURLPattern = regexp.MustCompile(`(?i)^https?://github\.com/([\w.-]+)/([\w.-]+)/(?:issues|pull)/(\d+)(?:[/?#].*)?$`)
var ownerRepoThreadPattern = regexp.MustCompile(`(?i)^([\w.-]+)/([\w.-]+)#(\d+)$`)
var pathThreadPattern = regexp.MustCompile(`(?i)(?:^|/)(?:issues|pull)/(\d+)(?:[/?#].*)?$`)
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
	RuntimeDir       string `json:"runtime_dir,omitempty"`
	DBPath           string `json:"db_path"`
	CacheDir         string `json:"cache_dir"`
	VectorDir        string `json:"vector_dir"`
	LogDir           string `json:"log_dir"`
	PortableStoreURL string `json:"portable_store_url,omitempty"`
	PortableStoreDir string `json:"portable_store_dir,omitempty"`
	PortableStore    string `json:"portable_store,omitempty"`
	RemoteMode       string `json:"remote_mode,omitempty"`
	RemoteEndpoint   string `json:"remote_endpoint,omitempty"`
	RemoteArchive    string `json:"remote_archive,omitempty"`
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
	if len(args) == 0 || rootHelpRequested(args, "config", "format") {
		a.printUsage()
		return nil
	}
	var global gitcrawlRootArgs
	if err := parseKongArgs(&global, args, "gitcrawl", a.Stdout, a.Stderr); err != nil {
		return usageErr(err)
	}

	resolvedFormat, err := resolveOutputFormat(global.Format, global.JSON)
	if err != nil {
		return usageErr(err)
	}
	a.configPath = strings.TrimSpace(global.Config)
	a.format = resolvedFormat

	rest := global.Args
	if global.Version {
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
	a.maybeNotifyRelease(ctx, rest)

	switch rest[0] {
	case "version":
		return a.writeOutput("version", map[string]string{"version": version}, false)
	case "check-update":
		return a.runCheckUpdate(ctx, rest[1:])
	case "metadata":
		return a.runMetadata(rest[1:])
	case "whoami":
		return a.runRemoteWhoami(ctx, rest[1:])
	case "remote":
		return a.runRemote(ctx, rest[1:])
	case "cloud":
		return a.runCloud(ctx, rest[1:])
	case "serve":
		return usageErr(fmt.Errorf("serve is not supported in gitcrawl"))
	case "init":
		return a.runInit(ctx, rest[1:])
	case "doctor":
		return a.runDoctor(ctx, rest[1:])
	case "status":
		return a.runStatus(ctx, rest[1:])
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
	case "code":
		return a.runCode(ctx, rest[1:])
	case "gh":
		return a.runGHShim(ctx, rest[1:])
	case "configure":
		return a.runConfigure(rest[1:])
	case "refresh":
		return a.runRefresh(ctx, rest[1:])
	case "embed":
		return a.runEmbed(ctx, rest[1:])
	case "clusters":
		return a.runClusters(ctx, rest[1:])
	case "clusters-report":
		return a.runClustersReport(ctx, rest[1:])
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

type gitcrawlRootArgs struct {
	Config  string   `help:"Config path."`
	Format  string   `default:"text" help:"Output format: text, json, or log."`
	JSON    bool     `name:"json" help:"Write JSON output."`
	Version bool     `help:"Print version."`
	NoColor bool     `name:"no-color" help:"Disable color output."`
	Args    []string `arg:"" optional:"" passthrough:"partial" name:"command" help:"Command and arguments."`
}

func rootHelpRequested(args []string, valueFlags ...string) bool {
	valueFlagSet := make(map[string]struct{}, len(valueFlags))
	for _, flag := range valueFlags {
		valueFlagSet[flag] = struct{}{}
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--help" || arg == "-h" || (arg == "help" && i == len(args)-1) {
			return true
		}
		if !strings.HasPrefix(arg, "-") {
			return false
		}
		if name, ok := strings.CutPrefix(arg, "--"); ok {
			if strings.Contains(name, "=") {
				continue
			}
			if _, ok := valueFlagSet[name]; ok {
				i++
			}
		}
	}
	return false
}

func parseKongArgs(target any, args []string, name string, stdout, stderr io.Writer, options ...kong.Option) error {
	_, err := parseKongContext(target, args, name, stdout, stderr, options...)
	return err
}

func parseKongContext(target any, args []string, name string, stdout, stderr io.Writer, options ...kong.Option) (*kong.Context, error) {
	opts := []kong.Option{
		kong.Name(name),
		kong.NoDefaultHelp(),
		kong.Writers(stdout, stderr),
		kong.Exit(func(int) {}),
	}
	opts = append(opts, options...)
	parser, err := kong.New(target, opts...)
	if err != nil {
		return nil, err
	}
	return parser.Parse(args)
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
		embed, err := a.embedRepository(ctx, owner, repoName, embedOptions{Limit: limit, IncludeClosed: stateIncludesClosed(*state)})
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
		query.IncludeClosed = stateIncludesClosed(*state)
		vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, query)
		if err != nil {
			_ = rt.Store.Close()
			return err
		}
		if len(vectors) == 0 {
			fallbackQuery := store.ThreadVectorQuery{RepoID: repo.ID, IncludeClosed: stateIncludesClosed(*state)}
			fallbackVectors, err := rt.Store.ListThreadVectorsFiltered(ctx, fallbackQuery)
			if err != nil {
				_ = rt.Store.Close()
				return err
			}
			if len(fallbackVectors) > 0 {
				query = fallbackQuery
				vectors = dedupeThreadVectorsByThread(fallbackVectors)
			}
		}
		retireMissing := minSize <= 1 && !stateIncludesClosed(*state)
		if retireMissing {
			retireMissing, err = completeClusterVectorCoverage(ctx, rt.Store, query, vectors)
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
			RetireMissing:      retireMissing,
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
	client := openai.New(openai.Options{APIKey: token.Value, BaseURL: openAIBaseURL(), Dimensions: rt.Config.OpenAI.EmbedDimensions, Retry: embedRetryOverride()})
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

func (a *App) runNeighbors(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("neighbors", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "issue or pull request number")
	limitRaw := fs.String("limit", "", "maximum neighbor rows")
	thresholdRaw := fs.String("threshold", "", "minimum cosine score")
	includeClosed := fs.Bool("include-closed", false, "include closed issue and pull request vectors")
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
	number, err := parseRequiredThreadNumber("number", *numberRaw)
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
	targetThread, targetVector, err := configuredNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
	if err != nil {
		if isMissingThreadVectorErr(err, number) {
			if store.SupportsEmbeddingBasis(rt.Config.EmbeddingBasis) {
				_ = rt.Store.Close()
				if embedded, embedErr := a.backfillNeighborEmbedding(ctx, owner, repoName, number, *includeClosed); embedErr != nil {
					return embedErr
				} else if embedded {
					rt, err = a.openLocalRuntimeReadOnly(ctx)
					if err != nil {
						return err
					}
					defer rt.Store.Close()
					repo, err = rt.repository(ctx, owner, repoName)
					if err != nil {
						return err
					}
					targetThread, targetVector, err = configuredNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
				}
				if err != nil {
					return neighborEmbeddingRecoveryError(owner, repoName, number, *includeClosed, err)
				}
			} else {
				targetThread, targetVector, err = fallbackNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
				if err != nil {
					return err
				}
			}
		} else {
			return err
		}
	}
	stale, err := neighborEmbeddingNeedsRefresh(ctx, rt.Store, repo.ID, targetVector, number, *includeClosed)
	if err != nil {
		return err
	}
	if stale {
		_ = rt.Store.Close()
		if embedded, embedErr := a.backfillNeighborEmbedding(ctx, owner, repoName, number, *includeClosed); embedErr != nil {
			return embedErr
		} else if !embedded {
			return neighborEmbeddingRecoveryError(owner, repoName, number, *includeClosed, fmt.Errorf("thread #%d has a stale embedding", number))
		}
		rt, err = a.openLocalRuntimeReadOnly(ctx)
		if err != nil {
			return err
		}
		defer rt.Store.Close()
		repo, err = rt.repository(ctx, owner, repoName)
		if err != nil {
			return err
		}
		targetThread, targetVector, err = configuredNeighborTargetVector(ctx, rt, repo.ID, number, *includeClosed)
		if err != nil {
			return err
		}
	}
	vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, store.ThreadVectorQuery{
		RepoID:        repo.ID,
		Model:         targetVector.Model,
		Basis:         targetVector.Basis,
		Dimensions:    targetVector.Dimensions,
		IncludeClosed: *includeClosed,
	})
	if err != nil {
		return err
	}
	items := make([]vector.Item, 0, len(vectors))
	for _, stored := range vectors {
		items = append(items, vector.Item{ThreadID: stored.ThreadID, Vector: stored.Vector})
	}
	candidates, err := vector.QueryWithOptions(ctx, items, targetVector.Vector, vector.QueryOptions{
		Backend:         rt.Config.VectorBackend,
		Limit:           limit * 2,
		ExcludeThreadID: targetThread.ID,
	})
	if err != nil {
		return err
	}
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

func configuredNeighborTargetVector(ctx context.Context, rt localRuntime, repoID int64, number int, includeClosed bool) (store.Thread, store.ThreadVector, error) {
	query := store.ThreadVectorQuery{
		RepoID:        repoID,
		Model:         rt.Config.OpenAI.EmbedModel,
		Basis:         rt.Config.EmbeddingBasis,
		IncludeClosed: includeClosed,
	}
	targetThread, targetVector, err := rt.Store.ThreadVectorByNumber(ctx, query, number)
	if err == nil {
		return targetThread, targetVector, nil
	}
	return store.Thread{}, store.ThreadVector{}, err
}

func fallbackNeighborTargetVector(ctx context.Context, rt localRuntime, repoID int64, number int, includeClosed bool) (store.Thread, store.ThreadVector, error) {
	fallbackQuery := store.ThreadVectorQuery{RepoID: repoID, IncludeClosed: includeClosed}
	fallbackThread, fallbackVector, fallbackErr := rt.Store.ThreadVectorByNumber(ctx, fallbackQuery, number)
	if fallbackErr != nil {
		return store.Thread{}, store.ThreadVector{}, fallbackErr
	}
	return fallbackThread, fallbackVector, nil
}

func isMissingThreadVectorErr(err error, number int) bool {
	return err != nil && strings.Contains(err.Error(), fmt.Sprintf("thread #%d was not found with an embedding", number))
}

func (a *App) backfillNeighborEmbedding(ctx context.Context, owner, repoName string, number int, includeClosed bool) (bool, error) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return false, err
	}
	if token := config.ResolveOpenAIKey(cfg); token.Value == "" {
		return false, nil
	}
	result, err := a.embedRepository(ctx, owner, repoName, embedOptions{
		Number:        number,
		Limit:         1,
		IncludeClosed: includeClosed,
	})
	if err != nil {
		return false, fmt.Errorf("backfill neighbor embedding for #%d: %w", number, err)
	}
	return result.Embedded > 0, nil
}

func neighborEmbeddingNeedsRefresh(ctx context.Context, st *store.Store, repoID int64, vector store.ThreadVector, number int, includeClosed bool) (bool, error) {
	if strings.TrimSpace(vector.Basis) == "" || strings.TrimSpace(vector.Model) == "" {
		return false, nil
	}
	if !store.SupportsEmbeddingBasis(vector.Basis) {
		return false, nil
	}
	tasks, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID:        repoID,
		Basis:         vector.Basis,
		Model:         vector.Model,
		Number:        number,
		Limit:         1,
		IncludeClosed: includeClosed,
	})
	if err != nil {
		return false, err
	}
	return len(tasks) > 0, nil
}

func neighborEmbeddingRecoveryError(owner, repoName string, number int, includeClosed bool, cause error) error {
	cmd := fmt.Sprintf("gitcrawl embed %s/%s --number %d --limit 1", owner, repoName, number)
	if includeClosed {
		cmd += " --include-closed"
	}
	return fmt.Errorf("%w; run `%s` and retry neighbors", cause, cmd)
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
	includeClosed := fs.Bool("include-closed", false, "include closed issue and pull request vectors")
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
		RepoID:        repo.ID,
		Model:         firstNonEmpty(strings.TrimSpace(*model), rt.Config.OpenAI.EmbedModel),
		Basis:         firstNonEmpty(strings.TrimSpace(*basis), rt.Config.EmbeddingBasis),
		IncludeClosed: *includeClosed,
	}
	vectors, err := rt.Store.ListThreadVectorsFiltered(ctx, query)
	if err != nil {
		return err
	}
	if len(vectors) == 0 && strings.TrimSpace(*model) == "" && strings.TrimSpace(*basis) == "" {
		fallbackQuery := store.ThreadVectorQuery{RepoID: repo.ID, IncludeClosed: *includeClosed}
		fallbackVectors, err := rt.Store.ListThreadVectorsFiltered(ctx, fallbackQuery)
		if err != nil {
			return err
		}
		if len(fallbackVectors) > 0 {
			query = fallbackQuery
			vectors = dedupeThreadVectorsByThread(fallbackVectors)
		}
	}
	if limit > 0 && len(vectors) > limit {
		vectors = vectors[:limit]
	}
	retireMissing := minSize <= 1 && limit == 0 && strings.TrimSpace(*model) == "" && strings.TrimSpace(*basis) == "" && !*includeClosed
	if retireMissing {
		retireMissing, err = completeClusterVectorCoverage(ctx, rt.Store, query, vectors)
		if err != nil {
			return err
		}
	}
	clusterResult, err := clusterRepository(ctx, rt.Store, repo.ID, vectors, clusterBuildOptions{
		Threshold:          threshold,
		MinSize:            minSize,
		MaxClusterSize:     maxClusterSize,
		Fanout:             fanout,
		CrossKindThreshold: crossKindThreshold,
		RetireMissing:      retireMissing,
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
	Repository string             `json:"repository"`
	Model      string             `json:"model"`
	Basis      string             `json:"basis"`
	Selected   int                `json:"selected"`
	Embedded   int                `json:"embedded"`
	Skipped    int                `json:"skipped"`
	Failed     int                `json:"failed,omitempty"`
	Retries    int                `json:"retries,omitempty"`
	Status     string             `json:"status,omitempty"`
	Failures   []embedFailureStat `json:"failures,omitempty"`
	RunID      int64              `json:"run_id"`
}

type embedFailureStat struct {
	BatchStart int    `json:"batch_start"`
	BatchEnd   int    `json:"batch_end"`
	Attempts   int    `json:"attempts"`
	Status     int    `json:"status,omitempty"`
	Type       string `json:"type,omitempty"`
	Code       string `json:"code,omitempty"`
	Message    string `json:"message"`
}

func (a *App) runEmbed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("embed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "embed one issue or pull request number")
	limitRaw := fs.String("limit", "", "maximum rows to embed")
	force := fs.Bool("force", false, "re-embed even when content hash is unchanged")
	includeClosed := fs.Bool("include-closed", false, "include closed issue and pull request rows")
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
	number, err := parseOptionalThreadNumber(*numberRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	result, err := a.embedRepository(ctx, owner, repoName, embedOptions{
		Number:        number,
		Limit:         limit,
		Force:         *force,
		IncludeClosed: *includeClosed,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("embed", result, true)
}

type embedOptions struct {
	Number        int
	Limit         int
	Force         bool
	IncludeClosed bool
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
		RepoID:        repo.ID,
		Basis:         rt.Config.EmbeddingBasis,
		Model:         rt.Config.OpenAI.EmbedModel,
		Number:        options.Number,
		Limit:         options.Limit,
		Force:         options.Force,
		IncludeClosed: options.IncludeClosed,
	})
	if err != nil {
		return embedResult{}, err
	}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	batchSize := rt.Config.OpenAI.BatchSize
	if batchSize <= 0 {
		batchSize = 64
	}
	client := openai.New(openai.Options{APIKey: token.Value, BaseURL: openAIBaseURL(), Dimensions: rt.Config.OpenAI.EmbedDimensions, Retry: embedRetryOverride()})

	type pendingBatch struct {
		start, end int
		attempts   int
	}
	var queue []pendingBatch
	for start := 0; start < len(tasks); start += batchSize {
		end := start + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		queue = append(queue, pendingBatch{start: start, end: end})
	}

	embedded := 0
	totalRetries := 0
	var failures []embedFailureStat
	cancelled := false
	var cancelErr error

	const maxBatchAttempts = 2
	for len(queue) > 0 {
		batch := queue[0]
		queue = queue[1:]
		batch.attempts++
		slice := tasks[batch.start:batch.end]
		texts := make([]string, 0, len(slice))
		for _, task := range slice {
			texts = append(texts, task.Text)
		}
		fmt.Fprintf(a.Stderr, "[embed] embedding %d-%d of %d (attempt %d)\n", batch.start+1, batch.end, len(tasks), batch.attempts)
		if batch.attempts == 1 {
			if truncated := truncatedEmbeddingTaskCount(slice); truncated > 0 {
				fmt.Fprintf(a.Stderr, "[embed] truncated %d input(s) to embedding input budget (%d runes/%d bytes)\n", truncated, store.MaxEmbeddingTextRunes, store.MaxEmbeddingTextBytes)
			}
		}
		vectors, err := client.Embed(ctx, rt.Config.OpenAI.EmbedModel, texts)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				cancelled = true
				cancelErr = err
				break
			}
			retryable := true
			if apiErr := openai.AsAPIError(err); apiErr != nil {
				retryable = apiErr.Retryable()
			}
			if retryable && batch.attempts < maxBatchAttempts {
				totalRetries++
				fmt.Fprintf(a.Stderr, "[embed] batch %d-%d failed (%s), requeueing\n", batch.start+1, batch.end, summarizeEmbedErr(err))
				queue = append(queue, batch)
				continue
			}
			fmt.Fprintf(a.Stderr, "[embed] batch %d-%d failed permanently: %s\n", batch.start+1, batch.end, summarizeEmbedErr(err))
			failures = append(failures, makeEmbedFailureStat(batch.start, batch.end, batch.attempts, err))
			continue
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for index, vector := range vectors {
			task := slice[index]
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

	failedRows := 0
	for _, f := range failures {
		failedRows += f.BatchEnd - f.BatchStart
	}

	status := "success"
	switch {
	case cancelled:
		status = "cancelled"
	case len(failures) > 0 && embedded == 0:
		status = "error"
	case len(failures) > 0:
		status = "partial"
	}

	result := embedResult{
		Repository: repo.FullName,
		Model:      rt.Config.OpenAI.EmbedModel,
		Basis:      rt.Config.EmbeddingBasis,
		Selected:   len(tasks),
		Embedded:   embedded,
		Failed:     failedRows,
		Retries:    totalRetries,
		Status:     status,
		Failures:   failures,
	}
	statsJSON, _ := json.Marshal(result)
	runRecord := store.RunRecord{
		RepoID:     repo.ID,
		Kind:       "embedding",
		Scope:      "repo",
		Status:     status,
		StartedAt:  started,
		FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
		StatsJSON:  string(statsJSON),
	}
	if cancelled && cancelErr != nil {
		runRecord.ErrorText = cancelErr.Error()
	} else if status == "error" && len(failures) > 0 {
		runRecord.ErrorText = failures[0].Message
	}
	recordCtx := ctx
	if cancelled {
		var cancelRecord context.CancelFunc
		recordCtx, cancelRecord = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelRecord()
	}
	runID, recordErr := rt.Store.RecordRun(recordCtx, runRecord)
	if recordErr != nil && !cancelled {
		return embedResult{}, recordErr
	}
	result.RunID = runID

	if cancelled {
		return result, cancelErr
	}
	if status == "error" {
		return result, fmt.Errorf("openai embeddings failed: %s", failures[0].Message)
	}
	return result, nil
}

func summarizeEmbedErr(err error) string {
	if apiErr := openai.AsAPIError(err); apiErr != nil {
		parts := []string{fmt.Sprintf("status=%d", apiErr.Status)}
		if apiErr.Type != "" {
			parts = append(parts, "type="+apiErr.Type)
		}
		if apiErr.Code != "" {
			parts = append(parts, "code="+apiErr.Code)
		}
		return strings.Join(parts, " ")
	}
	return err.Error()
}

func makeEmbedFailureStat(start, end, attempts int, err error) embedFailureStat {
	stat := embedFailureStat{
		BatchStart: start,
		BatchEnd:   end,
		Attempts:   attempts,
		Message:    err.Error(),
	}
	if apiErr := openai.AsAPIError(err); apiErr != nil {
		stat.Status = apiErr.Status
		stat.Type = apiErr.Type
		stat.Code = apiErr.Code
		if apiErr.Message != "" {
			stat.Message = apiErr.Message
		}
	}
	return stat
}

func embedRetryOverride() *openai.RetryConfig {
	if strings.TrimSpace(os.Getenv("GITCRAWL_OPENAI_RETRY_DISABLED")) == "1" {
		cfg := openai.NoRetry()
		return &cfg
	}
	return nil
}

func truncatedEmbeddingTaskCount(tasks []store.EmbeddingTask) int {
	count := 0
	for _, task := range tasks {
		if task.TextTruncated {
			count++
		}
	}
	return count
}

func openAIBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("GITCRAWL_OPENAI_BASE_URL")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("OPENAI_BASE_URL"))
}

func githubBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("GITCRAWL_GITHUB_BASE_URL")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("GITHUB_BASE_URL"))
}

func (a *App) runClusters(ctx context.Context, args []string) error {
	return a.runClusterList(ctx, "clusters", args, false)
}

func (a *App) runDurableClusters(ctx context.Context, args []string) error {
	return a.runClusterList(ctx, "durable-clusters", args, true)
}

type clustersReport struct {
	Repository  string                `json:"repository"`
	GeneratedAt string                `json:"generated_at"`
	Sort        string                `json:"sort"`
	MinSize     int                   `json:"min_size"`
	Limit       int                   `json:"limit"`
	MemberLimit int                   `json:"member_limit"`
	HideClosed  bool                  `json:"hide_closed,omitempty"`
	Clusters    []store.ClusterDetail `json:"clusters"`
	Totals      clustersReportTotals  `json:"totals"`
}

type clustersReportTotals struct {
	ClusterCount int `json:"cluster_count"`
	MemberCount  int `json:"member_count"`
	OpenCount    int `json:"open_count"`
	ClosedCount  int `json:"closed_count"`
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
	sortMode := fs.String("sort", "size", "sort mode: recent|oldest|size")
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
	if sort != "recent" && sort != "oldest" && sort != "size" {
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

func (a *App) runClustersReport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("clusters-report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "5", "minimum active member count")
	limitRaw := fs.String("limit", "10", "maximum cluster rows")
	memberLimitRaw := fs.String("member-limit", "8", "maximum member rows per cluster")
	bodyCharsRaw := fs.String("body-chars", "240", "maximum body snippet characters")
	sortMode := fs.String("sort", "size", "sort mode: size|recent|oldest")
	includeClosed := fs.Bool("include-closed", false, "deprecated; clusters include closed rows by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "member-limit": true, "body-chars": true, "sort": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("clusters-report requires owner/repo"))
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
	memberLimit, err := parseOptionalPositiveInt(*memberLimitRaw)
	if err != nil {
		return usageErr(err)
	}
	bodyChars, err := parseOptionalPositiveInt(*bodyCharsRaw)
	if err != nil {
		return usageErr(err)
	}
	sort := strings.TrimSpace(*sortMode)
	if sort != "recent" && sort != "oldest" && sort != "size" {
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
	summaries, err := rt.Store.ListDisplayClusterSummaries(ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: clusterListIncludesClosed(false, *includeClosed, *hideClosed),
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	})
	if err != nil {
		return err
	}
	report := clustersReport{
		Repository:  repo.FullName,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Sort:        sort,
		MinSize:     minSize,
		Limit:       limit,
		MemberLimit: memberLimit,
		HideClosed:  *hideClosed,
		Clusters:    make([]store.ClusterDetail, 0, len(summaries)),
	}
	for _, summary := range summaries {
		detail, err := rt.Store.ClusterDetail(ctx, store.ClusterDetailOptions{
			RepoID:        repo.ID,
			ClusterID:     summary.ID,
			Source:        summary.Source,
			IncludeClosed: clusterListIncludesClosed(false, *includeClosed, *hideClosed),
			MemberLimit:   memberLimit,
			BodyChars:     bodyChars,
		})
		if err != nil {
			return err
		}
		report.Clusters = append(report.Clusters, detail)
		report.Totals.MemberCount += detail.Cluster.MemberCount
		if detail.Cluster.Status == "closed" || detail.Cluster.ClosedAt != "" {
			report.Totals.ClosedCount++
		} else {
			report.Totals.OpenCount++
		}
	}
	report.Totals.ClusterCount = len(report.Clusters)
	return a.writeClustersReport(report)
}

func (a *App) writeClustersReport(report clustersReport) error {
	if a.format == FormatJSON {
		return a.writeOutput("clusters-report", report, true)
	}
	_, err := fmt.Fprint(a.Stdout, renderClustersReportMarkdown(report))
	return err
}

func renderClustersReportMarkdown(report clustersReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Cluster Report: %s\n\n", report.Repository)
	fmt.Fprintf(&b, "Generated: %s UTC\n", report.GeneratedAt)
	fmt.Fprintf(&b, "View: top %d clusters, min size %d, sorted by %s", report.Limit, report.MinSize, report.Sort)
	if report.HideClosed {
		b.WriteString(", closed hidden")
	}
	b.WriteString("\n\n")
	fmt.Fprintf(&b, "**Totals:** %d clusters, %d total members, %d open, %d closed.\n\n", report.Totals.ClusterCount, report.Totals.MemberCount, report.Totals.OpenCount, report.Totals.ClosedCount)
	if len(report.Clusters) == 0 {
		b.WriteString("No clusters matched the selected filters.\n")
		return b.String()
	}
	b.WriteString("## At a Glance\n\n")
	b.WriteString("| Rank | Cluster | Members | Status | Representative | Updated |\n")
	b.WriteString("| ---: | --- | ---: | --- | --- | --- |\n")
	for index, detail := range report.Clusters {
		cluster := detail.Cluster
		fmt.Fprintf(&b, "| %d | `%s` | %d | %s | %s | %s |\n",
			index+1,
			markdownTableText(firstNonEmpty(cluster.StableSlug, fmt.Sprintf("cluster-%d", cluster.ID))),
			cluster.MemberCount,
			markdownTableText(firstNonEmpty(cluster.Status, "unknown")),
			markdownTableText(clusterRepresentativeMarkdown(cluster)),
			markdownTableText(shortTime(cluster.UpdatedAt)),
		)
	}
	for index, detail := range report.Clusters {
		cluster := detail.Cluster
		title := firstNonEmpty(cluster.RepresentativeTitle, cluster.Title, "Untitled cluster")
		fmt.Fprintf(&b, "\n## %d. %s\n\n", index+1, markdownHeadingText(title))
		fmt.Fprintf(&b, "- Cluster: `%s` (`%d`)\n", firstNonEmpty(cluster.StableSlug, fmt.Sprintf("cluster-%d", cluster.ID)), cluster.ID)
		fmt.Fprintf(&b, "- Status: %s\n", firstNonEmpty(cluster.Status, "unknown"))
		fmt.Fprintf(&b, "- Members: %d total, %d shown\n", cluster.MemberCount, len(detail.Members))
		fmt.Fprintf(&b, "- Representative: %s\n", clusterRepresentativeMarkdown(cluster))
		if cluster.UpdatedAt != "" {
			fmt.Fprintf(&b, "- Last updated: %s\n", shortTime(cluster.UpdatedAt))
		}
		if cluster.ClosedAt != "" {
			fmt.Fprintf(&b, "- Closed locally: %s\n", shortTime(cluster.ClosedAt))
		}
		b.WriteString("\n")
		renderClusterMemberTable(&b, detail.Members)
		renderClusterSnippetList(&b, detail.Members)
	}
	return b.String()
}

func renderClusterMemberTable(b *strings.Builder, members []store.ClusterMemberDetail) {
	if len(members) == 0 {
		b.WriteString("No visible members.\n")
		return
	}
	b.WriteString("| Type | Number | State | Score | Title | Labels |\n")
	b.WriteString("| --- | ---: | --- | ---: | --- | --- |\n")
	for _, member := range members {
		thread := member.Thread
		score := ""
		if member.ScoreToRepresentative != nil {
			score = fmt.Sprintf("%.3f", *member.ScoreToRepresentative)
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s |\n",
			markdownTableText(kindTitle(thread.Kind)),
			markdownTableText(threadMarkdownLink(thread)),
			markdownTableText(firstNonEmpty(thread.State, member.State, "unknown")),
			markdownTableText(score),
			markdownTableText(thread.Title),
			markdownTableText(strings.Join(cliLabelNames(thread.LabelsJSON), ", ")),
		)
	}
}

func renderClusterSnippetList(b *strings.Builder, members []store.ClusterMemberDetail) {
	wroteHeading := false
	count := 0
	for _, member := range members {
		snippet := strings.TrimSpace(member.BodySnippet)
		if snippet == "" {
			continue
		}
		if !wroteHeading {
			b.WriteString("\nKey snippets:\n\n")
			wroteHeading = true
		}
		fmt.Fprintf(b, "- %s: %s\n", threadMarkdownLink(member.Thread), markdownInlineText(snippet))
		count++
		if count >= 3 {
			break
		}
	}
}

func clusterRepresentativeMarkdown(cluster store.ClusterSummary) string {
	if cluster.RepresentativeNumber <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%s #%d", kindTitle(cluster.RepresentativeKind), cluster.RepresentativeNumber)
}

func threadMarkdownLink(thread store.Thread) string {
	label := fmt.Sprintf("#%d", thread.Number)
	if strings.TrimSpace(thread.HTMLURL) == "" {
		return label
	}
	return fmt.Sprintf("[%s](%s)", label, strings.TrimSpace(thread.HTMLURL))
}

func markdownHeadingText(value string) string {
	value = strings.ReplaceAll(strings.TrimSpace(value), "\n", " ")
	value = strings.ReplaceAll(value, "#", "\\#")
	return value
}

func markdownInlineText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func markdownTableText(value string) string {
	value = markdownInlineText(value)
	value = strings.ReplaceAll(value, "|", "\\|")
	return value
}

func shortTime(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t.Format("2006-01-02")
	}
	if len(value) >= len("2006-01-02") {
		return value[:len("2006-01-02")]
	}
	return value
}

func cliLabelNames(raw string) []string {
	var labels []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(raw), &labels); err == nil {
		out := make([]string, 0, len(labels))
		for _, label := range labels {
			if name := strings.TrimSpace(label.Name); name != "" {
				out = append(out, name)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		return nil
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if name = strings.TrimSpace(name); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func (a *App) runTUI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "", "minimum active member count")
	limitRaw := fs.String("limit", "", "maximum cluster rows")
	sortMode := fs.String("sort", "", "sort mode: recent|oldest|size")
	layoutMode := fs.String("layout", "", "layout mode: focus|columns|right-stack")
	includeClosed := fs.Bool("include-closed", false, "deprecated; closed clusters are shown by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "sort": true, "layout": true})); err != nil {
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
		if !interactive && errors.Is(err, os.ErrNotExist) {
			cfg := config.Default()
			if cfgErr := cfg.Normalize(); cfgErr != nil {
				return cfgErr
			}
			cfg.ApplyRuntimeEnv()
			sort, sortErr := resolveTUISort(*sortMode, cfg)
			if sortErr != nil {
				return sortErr
			}
			layout, layoutErr := resolveTUILayout(*layoutMode, cfg)
			if layoutErr != nil {
				return layoutErr
			}
			return a.writeOutput("tui", emptyClusterBrowserPayload(ctx, cfg, cfg.DBPath, sort, layout, minSize, limit, *hideClosed), true)
		}
		return err
	}
	defer rt.Store.Close()

	repo, inferred, err := a.resolveOptionalRepository(ctx, rt, fs.Args())
	if err != nil {
		if !interactive && len(fs.Args()) == 0 && strings.Contains(err.Error(), "no local repositories found") {
			sort, sortErr := resolveTUISort(*sortMode, rt.Config)
			if sortErr != nil {
				return sortErr
			}
			layout, layoutErr := resolveTUILayout(*layoutMode, rt.Config)
			if layoutErr != nil {
				return layoutErr
			}
			return a.writeOutput("tui", emptyClusterBrowserPayload(ctx, rt.Config, rt.SourceDBPath, sort, layout, minSize, limit, *hideClosed), true)
		}
		return err
	}
	sort, err := resolveTUISort(*sortMode, rt.Config)
	if err != nil {
		return err
	}
	layout, err := resolveTUILayout(*layoutMode, rt.Config)
	if err != nil {
		return err
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
		DBSource:           databaseSourceKind(rt.SourceDBPath),
		DBLocation:         databaseSourceLocation(ctx, rt.SourceDBPath),
		DBRefreshSource:    remoteRefreshSource(rt),
		DBRuntimePath:      remoteRuntimePath(rt),
		ConfigPath:         a.configPath,
		Sort:               sort,
		Layout:             layout,
		MinSize:            minSize,
		Limit:              limit,
		HideClosed:         !showClosed,
		EmbedModel:         rt.Config.OpenAI.EmbedModel,
		EmbeddingBasis:     rt.Config.EmbeddingBasis,
		VectorBackend:      rt.Config.VectorBackend,
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

func resolveTUISort(raw string, cfg config.Config) (string, error) {
	sort := strings.TrimSpace(raw)
	if sort == "" {
		sort = strings.TrimSpace(cfg.TUI.DefaultSort)
	}
	if sort == "" {
		sort = "size"
	}
	if sort != "recent" && sort != "oldest" && sort != "size" {
		return "", usageErr(fmt.Errorf("unsupported sort %q", sort))
	}
	return sort, nil
}

func resolveTUILayout(raw string, cfg config.Config) (string, error) {
	layout := strings.TrimSpace(raw)
	if layout == "" {
		layout = strings.TrimSpace(cfg.TUI.DefaultLayout)
	}
	if !isSupportedTUILayout(layout) {
		return "", usageErr(fmt.Errorf("unsupported layout %q", layout))
	}
	return string(normalizeTUILayout(layout)), nil
}

func emptyClusterBrowserPayload(ctx context.Context, cfg config.Config, sourceDBPath, sort, layout string, minSize, limit int, hideClosed bool) clusterBrowserPayload {
	if strings.TrimSpace(sourceDBPath) == "" {
		sourceDBPath = cfg.DBPath
	}
	return clusterBrowserPayload{
		Mode:           "cluster-browser",
		DBSource:       databaseSourceKind(sourceDBPath),
		DBLocation:     databaseSourceLocation(ctx, sourceDBPath),
		Sort:           sort,
		Layout:         layout,
		MinSize:        minSize,
		Limit:          limit,
		HideClosed:     hideClosed,
		EmbedModel:     cfg.OpenAI.EmbedModel,
		EmbeddingBasis: cfg.EmbeddingBasis,
		VectorBackend:  cfg.VectorBackend,
		Clusters:       []store.ClusterSummary{},
	}
}

func databaseSourceKind(dbPath string) string {
	if _, ok := portableStoreRoot(dbPath); ok {
		return "remote"
	}
	return "local"
}

func remoteRefreshSource(rt localRuntime) string {
	if rt.RemoteSource {
		return rt.SourceDBPath
	}
	return ""
}

func remoteRuntimePath(rt localRuntime) string {
	if rt.RemoteSource {
		return rt.Config.DBPath
	}
	return ""
}

func databaseSourceLocation(ctx context.Context, dbPath string) string {
	filename := filepath.Base(dbPath)
	root, ok := portableStoreRoot(dbPath)
	if !ok {
		return filename
	}
	if repo := githubRepoFromRemote(gitRemoteURL(ctx, root)); repo != "" {
		return repo + ":" + filename
	}
	return filepath.Base(root) + ":" + filename
}

func gitRemoteURL(ctx context.Context, dir string) string {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "remote", "get-url", "origin")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func githubRepoFromRemote(remote string) string {
	value := strings.TrimSuffix(strings.TrimSpace(remote), ".git")
	switch {
	case strings.HasPrefix(value, "git@github.com:"):
		value = strings.TrimPrefix(value, "git@github.com:")
	case strings.Contains(value, "github.com/"):
		idx := strings.Index(value, "github.com/")
		value = value[idx+len("github.com/"):]
	default:
		return ""
	}
	value = strings.Trim(value, "/")
	parts := strings.Split(value, "/")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-2] + "/" + parts[len(parts)-1]
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
	seen := make(map[string]bool, len(out)+len(secondary))
	for _, cluster := range out {
		seen[clusterSummaryKey(cluster)] = true
	}
	for _, cluster := range secondary {
		key := clusterSummaryKey(cluster)
		if !seen[key] {
			out = append(out, cluster)
			seen[key] = true
		}
	}
	return out
}

func clusterSummaryKey(cluster store.ClusterSummary) string {
	source := strings.TrimSpace(cluster.Source)
	if source == "" {
		source = "auto"
	}
	return source + ":" + strconv.FormatInt(cluster.ID, 10)
}

func sameClusterSummary(left, right store.ClusterSummary) bool {
	if left.ID == 0 || right.ID == 0 || left.ID != right.ID {
		return false
	}
	leftSource := strings.TrimSpace(left.Source)
	rightSource := strings.TrimSpace(right.Source)
	if leftSource == "" || rightSource == "" {
		return true
	}
	return leftSource == rightSource
}

func (a *App) runClusterDetail(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cluster-detail", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	clusterIDRaw := fs.String("id", "", "cluster id")
	sourceRaw := fs.String("source", "", "cluster source: auto|run|durable")
	memberLimitRaw := fs.String("member-limit", "", "maximum member rows")
	bodyCharsRaw := fs.String("body-chars", "", "maximum body snippet characters")
	includeClosed := fs.Bool("include-closed", false, "deprecated; closed cluster members are shown by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed members")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "source": true, "member-limit": true, "body-chars": true})); err != nil {
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
	source, err := parseClusterDetailSource(*sourceRaw)
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
	detail, err := rt.Store.ClusterDetail(ctx, store.ClusterDetailOptions{
		RepoID:        repo.ID,
		ClusterID:     int64(clusterID),
		Source:        source,
		IncludeClosed: *includeClosed || !*hideClosed,
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

func parseClusterDetailSource(source string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "", "auto":
		return "", nil
	case "run", "raw", store.ClusterSourceRun:
		return store.ClusterSourceRun, nil
	case "durable", store.ClusterSourceDurable:
		return store.ClusterSourceDurable, nil
	default:
		return "", fmt.Errorf("unsupported cluster source %q", source)
	}
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
	numbers, err := parseOptionalThreadNumberList(*numbersRaw)
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
	number, err := parseOptionalThreadNumber(*numberRaw)
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
	number, err := parseOptionalThreadNumber(*numberRaw)
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
	numbersRaw := fs.String("numbers", "", "comma-separated issue or pull request numbers")
	limitRaw := fs.String("limit", "", "maximum issue/PR rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	includeComments := fs.Bool("include-comments", false, "hydrate issue comments, PR reviews, and PR review comments")
	includePRDetails := fs.Bool("include-pr-details", false, "hydrate PR files, commits, checks, and workflow runs")
	withRaw := fs.String("with", "", "extra hydration: pr-details")
	fs.Bool("include-code", false, "accepted for compatibility; code hydration is not implemented yet")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"numbers": true, "since": true, "state": true, "limit": true, "with": true})); err != nil {
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
	numbers, err := parseOptionalThreadNumberList(*numbersRaw)
	if err != nil {
		return usageErr(err)
	}
	with, err := parseSyncWith(*withRaw)
	if err != nil {
		return usageErr(err)
	}

	stats, err := a.syncRepository(ctx, owner, repo, syncOptions{
		Since:            strings.TrimSpace(*since),
		State:            strings.TrimSpace(*state),
		Limit:            limit,
		Numbers:          numbers,
		IncludeComments:  *includeComments,
		IncludePRDetails: *includePRDetails || with["pr-details"],
	})
	if err != nil {
		return err
	}
	return a.writeOutput("sync", stats, true)
}

type syncOptions struct {
	Since            string
	State            string
	Limit            int
	Numbers          []int
	IncludeComments  bool
	IncludePRDetails bool
	Quiet            bool
}

func parseSyncWith(value string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, part := range strings.Split(value, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		switch name {
		case "pr-details":
			out[name] = true
		default:
			return nil, fmt.Errorf("unsupported --with value %q", name)
		}
	}
	return out, nil
}

func (a *App) syncRepository(ctx context.Context, owner, repo string, options syncOptions) (syncer.Stats, error) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return syncer.Stats{}, err
	}
	token := a.resolveGitHubToken(ctx, cfg)
	if token.Value == "" {
		return syncer.Stats{}, fmt.Errorf("missing GitHub token: set %s or authenticate gh", cfg.GitHub.TokenEnv)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return syncer.Stats{}, err
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return syncer.Stats{}, err
	}
	defer rt.Store.Close()

	client := gh.New(gh.Options{Token: token.Value, BaseURL: githubBaseURL(), RateLimit: a.observeGitHubRateLimit(ctx, token.Value)})
	service := syncer.New(client, rt.Store)
	var reporter gh.Reporter
	var logger *slog.Logger
	if !options.Quiet {
		reporter = func(message string) {
			fmt.Fprintln(a.Stderr, message)
		}
		logger = progressLogger(a.Stderr)
	}
	stats, err := service.Sync(ctx, syncer.Options{
		Owner:            owner,
		Repo:             repo,
		State:            strings.TrimSpace(options.State),
		Since:            strings.TrimSpace(options.Since),
		Limit:            options.Limit,
		Numbers:          options.Numbers,
		IncludeComments:  options.IncludeComments,
		IncludePRDetails: options.IncludePRDetails,
		Reporter:         reporter,
		Logger:           logger,
	})
	if err != nil {
		return syncer.Stats{}, err
	}
	return stats, nil
}

func progressLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))
}

func (a *App) runInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "", "database path")
	runtimeDir := fs.String("runtime-dir", "", "root for an isolated database, cache, vectors, and logs")
	portableStore := fs.String("portable-store", "", "HTTPS git URL for a portable gitcrawl store")
	portableDB := fs.String("portable-db", "data/openclaw__openclaw.sync.db", "database path inside portable store")
	storeDir := fs.String("store-dir", "", "local portable store checkout directory")
	remoteEndpoint := fs.String("remote", "", "remote archive endpoint")
	remoteArchive := fs.String("archive", "", "remote archive id")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"db": true, "runtime-dir": true, "portable-store": true, "portable-db": true, "store-dir": true, "remote": true, "archive": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	localDBPath := strings.TrimSpace(*dbPath)
	isolatedRuntimeDir := strings.TrimSpace(*runtimeDir)
	portableStoreURL := strings.TrimSpace(*portableStore)
	remoteEndpointValue := strings.TrimSpace(*remoteEndpoint)
	if nonEmptyCount(localDBPath, isolatedRuntimeDir, portableStoreURL, remoteEndpointValue) > 1 {
		return usageErr(fmt.Errorf("use only one of --db, --runtime-dir, --portable-store, or --remote"))
	}

	cfg := config.Default()
	portableStoreDir := ""
	portableStoreAction := ""
	if remoteEndpointValue != "" {
		archive := strings.TrimSpace(*remoteArchive)
		if archive == "" {
			return usageErr(fmt.Errorf("--remote requires --archive"))
		}
		cfg.Remote = crawlremote.Config{
			Mode:     crawlremote.ModeCloud,
			Endpoint: remoteEndpointValue,
			Archive:  archive,
			TokenEnv: crawlremote.DefaultTokenEnv,
		}
	} else if portableStoreURL != "" {
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
	if localDBPath != "" {
		cfg.DBPath = localDBPath
	}
	if isolatedRuntimeDir != "" {
		cfg.DBPath = filepath.Join(isolatedRuntimeDir, "gitcrawl.db")
		cfg.CacheDir = filepath.Join(isolatedRuntimeDir, "cache")
		cfg.VectorDir = filepath.Join(isolatedRuntimeDir, "vectors")
		cfg.LogDir = filepath.Join(isolatedRuntimeDir, "logs")
	}
	if err := cfg.Normalize(); err != nil {
		return err
	}
	if err := config.Save(a.configPath, cfg); err != nil {
		return err
	}
	if !(cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud) {
		if err := config.EnsureRuntimeDirs(cfg); err != nil {
			return err
		}
	}
	result := initResult{
		ConfigPath:       config.ResolvePath(a.configPath),
		RuntimeDir:       isolatedRuntimeDir,
		DBPath:           cfg.DBPath,
		CacheDir:         cfg.CacheDir,
		VectorDir:        cfg.VectorDir,
		LogDir:           cfg.LogDir,
		PortableStoreURL: portableStoreURL,
		PortableStoreDir: portableStoreDir,
		PortableStore:    portableStoreAction,
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		result.RemoteMode = cfg.Remote.Mode
		result.RemoteEndpoint = cfg.Remote.Endpoint
		result.RemoteArchive = cfg.Remote.Archive
	}
	return a.writeInitOutput(result)
}

func (a *App) runPortable(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("portable requires a subcommand"))
	}
	switch args[0] {
	case "help", "--help", "-h":
		return a.printCommandUsage("portable")
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
	stats, err := rt.Store.PrunePortablePayloads(ctx, store.PortablePruneOptions{
		BodyChars: bodyChars,
		Vacuum:    !*noVacuum,
	})
	closeErr := rt.Store.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := sqliteStoreHealth(ctx, stats.DBPath); err != nil {
		return fmt.Errorf("validate portable db after prune: %w", err)
	}
	manifestPath, sha, err := writePortableDBManifest(stats)
	if err != nil {
		return err
	}
	stats.ManifestPath = manifestPath
	stats.SHA256 = sha
	stats.QuickCheck = "ok"
	return a.writeOutput("portable prune", stats, true)
}

func writePortableDBManifest(stats store.PortablePruneStats) (string, string, error) {
	info, err := os.Stat(stats.DBPath)
	if err != nil {
		return "", "", fmt.Errorf("stat portable db for manifest: %w", err)
	}
	sum, err := fileSHA256(stats.DBPath)
	if err != nil {
		return "", "", fmt.Errorf("hash portable db for manifest: %w", err)
	}
	sumText := fmt.Sprintf("%x", sum)
	manifest := portableDBManifest{
		Schema:      "gitcrawl-portable-sync-v2",
		ExportedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		OutputPath:  filepath.Base(stats.DBPath),
		OutputBytes: info.Size(),
		SHA256:      sumText,
		QuickCheck:  "ok",
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", err
	}
	manifestPath := portableDBManifestPath(stats.DBPath)
	if err := writeAtomicFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		return "", "", fmt.Errorf("write portable db manifest: %w", err)
	}
	return manifestPath, sumText, nil
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
		if err := ensurePortableStoreRemote(ctx, remoteURL, dir); err != nil {
			return "", err
		}
		if err := markPortableStoreCheckout(dir); err != nil {
			return "", err
		}
		if !gitWorktreeClean(ctx, dir) {
			if resetErr := runGit(ctx, "", "-C", dir, "reset", "--hard", "HEAD"); resetErr != nil {
				return "", resetErr
			}
			if retryErr := fastForwardGitCheckout(ctx, dir, false); retryErr != nil {
				return "", retryErr
			}
			if err := removePortableSQLiteSidecars(dir); err != nil {
				return "", err
			}
			return "reset-pulled", nil
		}
		if err := fastForwardGitCheckout(ctx, dir, false); err != nil {
			if !isDirtyPortablePullError(err) {
				return "", err
			}
			if resetErr := runGit(ctx, "", "-C", dir, "reset", "--hard", "HEAD"); resetErr != nil {
				return "", err
			}
			if retryErr := fastForwardGitCheckout(ctx, dir, false); retryErr != nil {
				return "", retryErr
			}
			if err := removePortableSQLiteSidecars(dir); err != nil {
				return "", err
			}
			return "reset-pulled", nil
		}
		if err := removePortableSQLiteSidecars(dir); err != nil {
			return "", err
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
	if err := markPortableStoreCheckout(dir); err != nil {
		return "", err
	}
	if err := removePortableSQLiteSidecars(dir); err != nil {
		return "", err
	}
	return "cloned", nil
}

func markPortableStoreCheckout(dir string) error {
	infoDir := filepath.Join(dir, ".git", "info")
	if err := os.MkdirAll(infoDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(infoDir, portableStoreMarkerFile), []byte("gitcrawl portable store\n"), 0o644)
}

func ensurePortableStoreRemote(ctx context.Context, remoteURL, dir string) error {
	remote := gitBranchRemote(ctx, dir, currentGitBranch(ctx, dir))
	origin, err := gitConfigValue(ctx, dir, "remote."+remote+".url")
	if err != nil {
		return fmt.Errorf("read portable store remote %q: %w", remote, err)
	}
	if !sameGitRemote(origin, remoteURL) {
		return fmt.Errorf("portable store directory %s is a checkout of %q, not %q", dir, gitRemoteForMessage(origin), gitRemoteForMessage(remoteURL))
	}
	return nil
}

func sameGitRemote(left, right string) bool {
	left = canonicalGitRemote(left)
	right = canonicalGitRemote(right)
	return left != "" && left == right
}

func canonicalGitRemote(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" {
		parsed.Scheme = strings.ToLower(parsed.Scheme)
		parsed.Host = strings.ToLower(parsed.Host)
		if parsed.Scheme == "http" || parsed.Scheme == "https" {
			parsed.User = nil
		}
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
		if parsed.Scheme != "file" {
			parsed.Path = strings.TrimSuffix(parsed.Path, ".git")
		}
		return parsed.String()
	}
	if !filepath.IsAbs(value) && strings.Contains(value, ":") && !strings.Contains(value, "://") {
		value = strings.TrimSuffix(strings.TrimSuffix(value, "/"), ".git")
		return canonicalSCPGitRemote(value)
	}
	if abs, err := filepath.Abs(value); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(value)
}

func canonicalSCPGitRemote(value string) string {
	userHost, path, ok := strings.Cut(value, ":")
	if !ok {
		return value
	}
	user := ""
	host := userHost
	if before, after, ok := strings.Cut(userHost, "@"); ok {
		user = before + "@"
		host = after
	}
	return user + strings.ToLower(host) + ":" + path
}

func gitRemoteForMessage(value string) string {
	value = strings.TrimSpace(value)
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" {
		parsed.User = nil
		return parsed.String()
	}
	return value
}

func removePortableSQLiteSidecars(dir string) error {
	_, err := mirror.CleanSQLiteSidecars(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func isDirtyPortablePullError(err error) bool {
	message := err.Error()
	return strings.Contains(message, "Your local changes") || strings.Contains(message, "would be overwritten by merge")
}

func fastForwardGitCheckout(ctx context.Context, dir string, quiet bool) error {
	branch := currentGitBranch(ctx, dir)
	remote := gitBranchRemote(ctx, dir, branch)
	fetchArgs := []string{"-C", dir, "fetch", "--prune"}
	if quiet {
		fetchArgs = append(fetchArgs, "--quiet")
	}
	fetchArgs = append(fetchArgs, remote)
	if err := runGit(ctx, "", fetchArgs...); err != nil {
		return err
	}
	target := gitRemoteBranchRef(ctx, dir, remote, branch)
	if target == "" {
		var err error
		target, err = gitOutput(ctx, "", "-C", dir, "symbolic-ref", "--quiet", "--short", "refs/remotes/"+remote+"/HEAD")
		if err != nil {
			return fmt.Errorf("resolve portable store upstream branch: %w", err)
		}
		if strings.TrimSpace(target) == "" {
			return fmt.Errorf("resolve portable store upstream branch: remote %q has no HEAD", remote)
		}
	}
	mergeArgs := []string{"-C", dir, "merge", "--ff-only"}
	if quiet {
		mergeArgs = append(mergeArgs, "--quiet")
	}
	mergeArgs = append(mergeArgs, target)
	return runGit(ctx, "", mergeArgs...)
}

func gitBranchRemote(ctx context.Context, dir, branch string) string {
	if branch != "" {
		value, err := gitConfigValue(ctx, dir, "branch."+branch+".remote")
		if err == nil && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return "origin"
}

func currentGitBranch(ctx context.Context, dir string) string {
	branch, err := gitOutput(ctx, "", "-C", dir, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(branch)
}

func gitRemoteBranchRef(ctx context.Context, dir, remote, branch string) string {
	if strings.TrimSpace(remote) == "" || strings.TrimSpace(branch) == "" {
		return ""
	}
	ref := "refs/remotes/" + remote + "/" + branch
	if err := runGit(ctx, "", "-C", dir, "show-ref", "--verify", "--quiet", ref); err != nil {
		return ""
	}
	return ref
}

func gitConfigValue(ctx context.Context, dir, key string) (string, error) {
	value, err := gitOutput(ctx, "", "-C", dir, "config", "--get", key)
	return strings.TrimSpace(value), err
}

func runGit(ctx context.Context, workdir string, args ...string) error {
	out, err := runGitCommandOutput(ctx, workdir, args...)
	if err != nil {
		return fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(out))
	}
	return nil
}

func runGitCommandOutput(ctx context.Context, workdir string, args ...string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cmd := exec.Command("git", args...)
	cmd.Dir = workdir
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=ssh -o BatchMode=yes -o ConnectTimeout=10",
	)
	configureCommandGroup(cmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		cleanupCommandGroup(cmd)
		return out.String(), err
	}
	if err := attachCommandGroup(cmd); err != nil {
		killCommandGroup(cmd)
		_ = cmd.Wait()
		cleanupCommandGroup(cmd)
		return out.String(), err
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case err := <-done:
		cleanupCommandGroup(cmd)
		return out.String(), err
	case <-ctx.Done():
		killCommandGroup(cmd)
		err := <-done
		cleanupCommandGroup(cmd)
		if err == nil {
			err = ctx.Err()
		}
		return out.String(), fmt.Errorf("%w: %v", ctx.Err(), err)
	}
}

func gitOutput(ctx context.Context, workdir string, args ...string) (string, error) {
	out, err := runGitCommandOutput(ctx, workdir, args...)
	if err != nil {
		return "", fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(out))
	}
	return strings.TrimSpace(out), nil
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

	cfg, err := config.LoadRuntime(a.configPath)
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
		cfg.ApplyRuntimeEnv()
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		return a.runRemoteDoctor(ctx, cfg, configExists)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}
	storeStatus := store.Status{DBPath: cfg.DBPath}
	sourceHealth := sqliteDBHealth(ctx, cfg.DBPath, cfg.DBPath)
	runtimeHealth := map[string]any{}
	portableStoreStatus := map[string]any{}
	portableRefreshState := map[string]any{}
	repairAction := ""
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		defer rt.Store.Close()
		sourceHealth = sqliteDBHealth(ctx, rt.SourceDBPath, rt.SourceDBPath)
		if rt.RemoteSource {
			runtimeHealth = sqliteDBHealth(ctx, rt.Config.DBPath, rt.SourceDBPath)
			portableStoreStatus = portableStoreGitStatus(ctx, rt.SourceDBPath)
			state := readPortableStoreRefreshState(portableStoreRefreshStatePath(rt.Config.DBPath))
			portableRefreshState = portableRefreshStatePayload(state)
			repairAction = state.LastRepair
			if repairAction == "" {
				repairAction = "none"
			}
		}
		storeStatus, err = rt.Store.Status(ctx)
		if err != nil {
			return err
		}
		storeStatus.DBPath = cfg.DBPath
	}

	githubToken := a.resolveGitHubToken(ctx, cfg)
	openAIKey := config.ResolveOpenAIKey(cfg)
	return a.writeOutput("doctor", map[string]any{
		"version":               version,
		"config_path":           config.ResolvePath(a.configPath),
		"config_exists":         configExists,
		"db_path":               cfg.DBPath,
		"source_db_health":      sourceHealth,
		"runtime_db_health":     runtimeHealth,
		"portable_store_status": portableStoreStatus,
		"portable_refresh":      portableRefreshState,
		"repair_action":         repairAction,
		"github_token_present":  githubToken.Value != "",
		"github_token_source":   githubToken.Source,
		"openai_key_present":    openAIKey.Value != "",
		"openai_key_source":     openAIKey.Source,
		"repository_count":      storeStatus.RepositoryCount,
		"thread_count":          storeStatus.ThreadCount,
		"open_thread_count":     storeStatus.OpenThreadCount,
		"cluster_count":         storeStatus.ClusterCount,
		"last_sync_at":          formatOptionalTime(storeStatus.LastSyncAt),
		"summary_model":         cfg.OpenAI.SummaryModel,
		"embed_model":           cfg.OpenAI.EmbedModel,
		"embedding_basis":       cfg.EmbeddingBasis,
		"api_supported":         false,
	}, true)
}

func portableRefreshStatePayload(state portableStoreRefreshState) map[string]any {
	out := map[string]any{}
	if state.LastAttempt != "" {
		out["last_attempt"] = state.LastAttempt
	}
	if state.LastSuccess != "" {
		out["last_success"] = state.LastSuccess
	}
	if state.LastFailure != "" {
		out["last_failure"] = state.LastFailure
	}
	if state.Error != "" {
		out["error"] = state.Error
	}
	if state.LastRepair != "" {
		out["last_repair"] = state.LastRepair
	}
	if state.LastRepairAt != "" {
		out["last_repair_at"] = state.LastRepairAt
	}
	if state.LastRepairBackup != "" {
		out["last_repair_backup"] = state.LastRepairBackup
	}
	if state.LastRepairError != "" {
		out["last_repair_error"] = state.LastRepairError
	}
	return out
}

func (a *App) runRemoteDoctor(ctx context.Context, cfg config.Config, configExists bool) error {
	remoteToken := config.ResolveRemoteToken(cfg)
	remoteStatus := map[string]any{
		"endpoint": cfg.Remote.Endpoint,
		"archive":  cfg.Remote.Archive,
		"mode":     cfg.Remote.Mode,
		"health":   "unchecked",
	}
	if strings.TrimSpace(cfg.Remote.Endpoint) == "" {
		remoteStatus["health"] = "missing_endpoint"
	} else if strings.TrimSpace(cfg.Remote.Archive) == "" {
		remoteStatus["health"] = "missing_archive"
	} else if remoteToken.Value == "" {
		remoteStatus["health"] = "missing_token"
	} else {
		client, err := a.remoteClient(cfg)
		if err != nil {
			remoteStatus["health"] = "error"
			remoteStatus["error"] = err.Error()
		} else if status, err := client.Status(ctx, "gitcrawl", cfg.Remote.Archive); err != nil {
			remoteStatus["health"] = "error"
			remoteStatus["error"] = err.Error()
		} else {
			remoteStatus["health"] = "ok"
			remoteStatus["last_sync_at"] = status.LastSyncAt
			remoteStatus["last_ingest_at"] = status.LastIngestAt
			remoteStatus["counts"] = status.Counts
		}
	}
	openAIKey := config.ResolveOpenAIKey(cfg)
	return a.writeOutput("doctor", map[string]any{
		"version":              version,
		"config_path":          config.ResolvePath(a.configPath),
		"config_exists":        configExists,
		"remote":               remoteStatus,
		"remote_token_present": remoteToken.Value != "",
		"remote_token_source":  remoteToken.Source,
		"openai_key_present":   openAIKey.Value != "",
		"openai_key_source":    openAIKey.Source,
		"summary_model":        cfg.OpenAI.SummaryModel,
		"embed_model":          cfg.OpenAI.EmbedModel,
		"embedding_basis":      cfg.EmbeddingBasis,
		"api_supported":        false,
	}, true)
}

func sqliteDBHealth(ctx context.Context, dbPath, manifestDBPath string) map[string]any {
	result := map[string]any{
		"path":   dbPath,
		"exists": false,
		"health": "missing",
	}
	if strings.TrimSpace(dbPath) == "" {
		return result
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			result["health"] = "error"
			result["error"] = err.Error()
		}
		return result
	}
	result["exists"] = true
	result["size"] = info.Size()
	if sum, err := fileSHA256(dbPath); err == nil {
		result["sha256"] = fmt.Sprintf("%x", sum)
	}
	manifestPath := portableDBManifestPath(manifestDBPath)
	result["manifest_path"] = manifestPath
	if manifest, ok, err := readPortableDBManifest(manifestPath); err != nil {
		result["manifest"] = "error"
		result["manifest_error"] = err.Error()
	} else if ok {
		result["manifest"] = "present"
		if manifest.OutputBytes > 0 {
			result["manifest_bytes"] = manifest.OutputBytes
		}
		if manifest.SHA256 != "" {
			result["manifest_sha256"] = manifest.SHA256
		}
	} else {
		result["manifest"] = "missing"
	}
	if err := validatePortableSQLiteFile(ctx, dbPath, manifestDBPath); err != nil {
		result["health"] = "error"
		result["error"] = err.Error()
	} else {
		result["health"] = "ok"
	}
	return result
}

func portableStoreGitStatus(ctx context.Context, dbPath string) map[string]any {
	result := map[string]any{}
	root, ok := portableStoreRoot(dbPath)
	if !ok {
		result["state"] = "not_portable"
		return result
	}
	result["root"] = root
	if !portableStoreIsGitWorktree(ctx, root) {
		result["state"] = "not_git"
		return result
	}
	if gitWorktreeClean(ctx, root) {
		result["state"] = "clean"
	} else {
		result["state"] = "dirty"
	}
	if counts, err := gitOutput(ctx, "", "-C", root, "rev-list", "--left-right", "--count", "HEAD...@{upstream}"); err == nil {
		parts := strings.Fields(counts)
		if len(parts) == 2 {
			result["ahead"] = parts[0]
			result["behind"] = parts[1]
		}
	}
	return result
}

func (a *App) runMetadata(args []string) error {
	fs := flag.NewFlagSet("metadata", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("metadata takes flags only"))
	}
	cfg := config.Default()
	manifest := control.NewManifest("gitcrawl", "Git Crawl", "gitcrawl")
	manifest.Description = "Local-first GitHub issue and pull request crawler."
	manifest.Branding = control.Branding{SymbolName: "point.3.connected.trianglepath.dotted", AccentColor: "#2da44e"}
	manifest.Paths = control.Paths{
		DefaultConfig:   config.ResolvePath(""),
		ConfigEnv:       config.DefaultConfigEnv,
		DefaultDatabase: cfg.DBPath,
		DefaultCache:    cfg.CacheDir,
		DefaultLogs:     cfg.LogDir,
	}
	manifest.Capabilities = []string{"metadata", "status", "doctor", "sync", "search", "code-index", "tui", "portable", "remote", "cloud-publish", "clusters", "embeddings"}
	manifest.Privacy = control.Privacy{ContainsPrivateMessages: false, ExportsSecrets: false, LocalOnlyScopes: []string{"github", "git", "sqlite", "portable"}}
	manifest.Commands = map[string]control.Command{
		"status":          {Title: "Status", Argv: []string{"gitcrawl", "status", "--json"}, JSON: true},
		"remote-status":   {Title: "Remote archive status", Argv: []string{"gitcrawl", "remote", "status", "--json"}, JSON: true},
		"remote-archives": {Title: "Remote archive list", Argv: []string{"gitcrawl", "remote", "archives", "--json"}, JSON: true},
		"remote-login":    {Title: "Remote GitHub login", Argv: []string{"gitcrawl", "remote", "login", "--json"}, JSON: true, Mutates: true},
		"cloud-publish":   {Title: "Publish cloud archive", Argv: []string{"gitcrawl", "cloud", "publish", "--json"}, JSON: true, Mutates: true},
		"whoami":          {Title: "Remote identity", Argv: []string{"gitcrawl", "whoami", "--json"}, JSON: true},
		"check-update":    {Title: "Check for updates", Argv: []string{"gitcrawl", "check-update", "--json"}, JSON: true},
		"doctor":          {Title: "Doctor", Argv: []string{"gitcrawl", "doctor", "--json"}, JSON: true},
		"sync":            {Title: "Sync repository", Argv: []string{"gitcrawl", "sync", "--json"}, JSON: true, Mutates: true},
		"search":          {Title: "Search", Argv: []string{"gitcrawl", "search", "--json"}, JSON: true},
		"code-index":      {Title: "Code index", Argv: []string{"gitcrawl", "code", "index", "--json"}, JSON: true, Mutates: true},
		"tui":             {Title: "Terminal cluster browser", Argv: []string{"gitcrawl", "tui"}},
		"tui-json":        {Title: "Terminal cluster data", Argv: []string{"gitcrawl", "tui", "--json"}, JSON: true},
		"portable":        {Title: "Portable store tools", Argv: []string{"gitcrawl", "portable", "prune", "--json"}, JSON: true, Mutates: true},
		"clusters":        {Title: "Clusters", Argv: []string{"gitcrawl", "clusters", "--json"}, JSON: true},
		"legacy-sync-api": {Title: "Legacy sync-status alias", Argv: []string{"gitcrawl", "sync-status"}, Legacy: true, Deprecated: true},
	}
	return a.writeOutput("metadata", manifest, false)
}

func (a *App) runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("status takes flags only"))
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return err
		}
		cfg.ApplyRuntimeEnv()
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		return a.runRemoteStatusWithConfig(ctx, cfg)
	}
	status := store.Status{DBPath: cfg.DBPath}
	if _, err := os.Stat(cfg.DBPath); err == nil {
		st, err := store.OpenReadOnly(ctx, cfg.DBPath)
		if err != nil {
			return err
		}
		defer st.Close()
		status, err = st.Status(ctx)
		if err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	status.DBPath = cfg.DBPath
	return a.writeOutput("status", controlStatus(config.ResolvePath(a.configPath), cfg, status), false)
}

func (a *App) runRemoteStatusWithConfig(ctx context.Context, cfg config.Config) error {
	client, err := a.remoteClient(cfg)
	if err != nil {
		return err
	}
	status, err := client.Status(ctx, "gitcrawl", cfg.Remote.Archive)
	if err != nil {
		return err
	}
	return a.writeOutput("status", remoteControlStatus(config.ResolvePath(a.configPath), cfg, status), false)
}

func controlStatus(configPath string, cfg config.Config, status store.Status) control.Status {
	counts := []control.Count{
		control.NewCount("repositories", "Repositories", int64(status.RepositoryCount)),
		control.NewCount("threads", "Threads", int64(status.ThreadCount)),
		control.NewCount("open_threads", "Open threads", int64(status.OpenThreadCount)),
		control.NewCount("clusters", "Clusters", int64(status.ClusterCount)),
	}
	out := control.NewStatus("gitcrawl", fmt.Sprintf("%d threads across %d repositories", status.ThreadCount, status.RepositoryCount))
	out.State = "current"
	out.ConfigPath = configPath
	out.DatabasePath = status.DBPath
	out.Counts = counts
	if !status.LastSyncAt.IsZero() {
		out.LastSyncAt = status.LastSyncAt.UTC().Format(time.RFC3339)
	}
	db := control.SQLiteDatabase("primary", "GitHub archive", "archive", status.DBPath, true, counts)
	out.DatabaseBytes = db.Bytes
	out.WALBytes = fileSize(status.DBPath + "-wal")
	out.Databases = []control.Database{db}
	return out
}

func remoteControlStatus(configPath string, cfg config.Config, status crawlremote.Status) control.Status {
	counts := append([]control.Count(nil), status.Counts...)
	summary := fmt.Sprintf("remote archive %s", firstNonEmpty(status.Archive, cfg.Remote.Archive))
	threadCount := countValue(counts, "threads")
	repoCount := countValue(counts, "repositories")
	if threadCount > 0 || repoCount > 0 {
		summary = fmt.Sprintf("%d threads across %d repositories", threadCount, repoCount)
	}
	out := control.NewStatus("gitcrawl", summary)
	out.State = "current"
	out.ConfigPath = configPath
	out.Counts = counts
	out.LastSyncAt = firstNonEmpty(status.LastSyncAt, status.LastIngestAt)
	out.Remote = &control.Remote{
		Enabled:      true,
		Mode:         firstNonEmpty(status.Mode, cfg.Remote.Mode),
		Endpoint:     cfg.Remote.Endpoint,
		Archive:      firstNonEmpty(status.Archive, cfg.Remote.Archive),
		LastIngestAt: status.LastIngestAt,
		LastSyncAt:   status.LastSyncAt,
	}
	if len(status.Warnings) > 0 {
		out.Warnings = append(out.Warnings, status.Warnings...)
	}
	out.Databases = []control.Database{
		control.RemoteDatabase("primary", "GitHub archive", "archive", "d1", cfg.Remote.Endpoint, firstNonEmpty(status.Archive, cfg.Remote.Archive), true, counts),
	}
	return out
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
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
	if ref, ok := parseThreadReference(value); ok && ref.Owner != "" && ref.Repo != "" {
		return ref.Owner, ref.Repo, nil
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("expected owner/repo, got %q", value)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

type threadReference struct {
	Owner  string
	Repo   string
	Number int
}

func (ref threadReference) FullName() string {
	if ref.Owner == "" || ref.Repo == "" {
		return ""
	}
	return ref.Owner + "/" + ref.Repo
}

func parseThreadReference(value string) (threadReference, bool) {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "<>()[]{}\"'`")
	value = strings.TrimRight(value, ".,;")
	if value == "" {
		return threadReference{}, false
	}
	if number, ok := parsePositiveIntLiteral(value); ok {
		return threadReference{Number: number}, true
	}
	if strings.HasPrefix(value, "#") {
		if number, ok := parsePositiveIntLiteral(strings.TrimPrefix(value, "#")); ok {
			return threadReference{Number: number}, true
		}
	}
	if match := githubThreadURLPattern.FindStringSubmatch(value); match != nil {
		if number, ok := parsePositiveIntLiteral(match[3]); ok {
			return threadReference{Owner: match[1], Repo: match[2], Number: number}, true
		}
	}
	if match := ownerRepoThreadPattern.FindStringSubmatch(value); match != nil {
		if number, ok := parsePositiveIntLiteral(match[3]); ok {
			return threadReference{Owner: match[1], Repo: match[2], Number: number}, true
		}
	}
	if match := pathThreadPattern.FindStringSubmatch(value); match != nil {
		if number, ok := parsePositiveIntLiteral(match[1]); ok {
			return threadReference{Number: number}, true
		}
	}
	return threadReference{}, false
}

func parsePositiveIntLiteral(value string) (int, bool) {
	if !isDecimalString(value) {
		return 0, false
	}
	number, err := strconv.Atoi(value)
	return number, err == nil && number > 0
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

func parseOptionalThreadNumber(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	ref, ok := parseThreadReference(value)
	if !ok || ref.Number <= 0 {
		return 0, fmt.Errorf("expected positive issue or pull request number, got %q", value)
	}
	return ref.Number, nil
}

func parseRequiredThreadNumber(name, value string) (int, error) {
	parsed, err := parseOptionalThreadNumber(value)
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
	number, err := parseOptionalThreadNumber(numberRaw)
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
	RetireMissing      bool
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
	repoFullName, err := repositoryFullNameByID(ctx, st, repoID)
	if err != nil {
		return nil, 0, err
	}
	addDeterministicReferenceEdges(candidateByPair, nodes, threads, repoFullName)
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

func repositoryFullNameByID(ctx context.Context, st *store.Store, repoID int64) (string, error) {
	repositories, err := st.ListRepositories(ctx)
	if err != nil {
		return "", err
	}
	for _, repo := range repositories {
		if repo.ID == repoID {
			return repo.FullName, nil
		}
	}
	return "", fmt.Errorf("repository id %d not found", repoID)
}

func addDeterministicReferenceEdges(edges map[string]clusterer.Edge, nodes []clusterer.Node, threads map[int64]store.Thread, repoFullName string) {
	threadIDByNumber := make(map[int]int64, len(nodes))
	for _, node := range nodes {
		thread := threads[node.ThreadID]
		threadIDByNumber[thread.Number] = node.ThreadID
	}
	refIDsByThreadID := make(map[int64]map[int64]bool, len(nodes))
	for _, node := range nodes {
		thread := threads[node.ThreadID]
		refNumbers := referencedThreadNumbersByLocation(thread, repoFullName)
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

func referencedThreadNumbersByLocation(thread store.Thread, repoFullName string) map[int]referenceEvidence {
	refs := map[int]referenceEvidence{}
	collectReferencedThreadNumbers(refs, thread.Number, thread.Body, false, repoFullName)
	collectReferencedThreadNumbers(refs, thread.Number, thread.Title, true, repoFullName)
	return refs
}

func collectReferencedThreadNumbers(refs map[int]referenceEvidence, threadNumber int, value string, titleRef bool, repoFullName string) {
	for _, match := range threadReferencePattern.FindAllStringSubmatchIndex(value, -1) {
		numberText := ""
		if match[2] >= 0 {
			refRepo := value[match[2]:match[3]]
			if !strings.EqualFold(refRepo, repoFullName) {
				continue
			}
			numberText = value[match[4]:match[5]]
		} else if match[6] >= 0 {
			numberText = value[match[6]:match[7]]
		} else if match[8] >= 0 {
			numberText = value[match[8]:match[9]]
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

func completeClusterVectorCoverage(ctx context.Context, st *store.Store, query store.ThreadVectorQuery, storedVectors []store.ThreadVector) (bool, error) {
	if strings.TrimSpace(query.Model) == "" || strings.TrimSpace(query.Basis) == "" {
		return false, nil
	}
	if !store.SupportsEmbeddingBasis(query.Basis) {
		return false, nil
	}
	pending, err := st.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID:        query.RepoID,
		Basis:         query.Basis,
		Model:         query.Model,
		IncludeClosed: query.IncludeClosed,
	})
	if err != nil {
		return false, err
	}
	return len(pending) == 0, nil
}

func clusterRepository(ctx context.Context, st *store.Store, repoID int64, storedVectors []store.ThreadVector, options clusterBuildOptions) (clusterRepositoryResult, error) {
	inputs, edgeCount, err := buildDurableClusterInputs(ctx, st, repoID, storedVectors, options)
	if err != nil {
		return clusterRepositoryResult{}, err
	}
	var saveResult store.SaveDurableClustersResult
	if options.RetireMissing {
		saveResult, err = st.SaveCompleteDurableClusters(ctx, repoID, inputs)
	} else {
		saveResult, err = st.SaveDurableClusters(ctx, repoID, inputs)
	}
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
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("expected finite number, got %q", value)
	}
	return parsed, nil
}

func stateIncludesClosed(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "all", "closed":
		return true
	default:
		return false
	}
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

func parseOptionalThreadNumberList(value string) ([]int, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := parseOptionalThreadNumber(strings.TrimSpace(part))
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
		_, err := fmt.Fprintf(a.Stdout, "init config_path=%s runtime_dir=%s db_path=%s portable_store=%s remote=%s archive=%s\n", result.ConfigPath, result.RuntimeDir, result.DBPath, result.PortableStore, result.RemoteEndpoint, result.RemoteArchive)
		return err
	default:
		lines := []string{
			"gitcrawl init",
			"config path: " + result.ConfigPath,
			"db path: " + result.DBPath,
			"cache dir: " + result.CacheDir,
			"vector dir: " + result.VectorDir,
			"log dir: " + result.LogDir,
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
		if result.RemoteEndpoint != "" {
			lines = append(lines,
				"",
				"Remote archive",
				"  endpoint: "+result.RemoteEndpoint,
				"  archive: "+result.RemoteArchive,
				"  mode: "+result.RemoteMode,
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
	if text, ok := commandUsageTexts[command]; ok {
		fmt.Fprint(a.Stdout, text)
		return nil
	}
	switch command {
	case "cluster-explain":
		fmt.Fprint(a.Stdout, commandUsageTexts["cluster-detail"])
		return nil
	case "portable":
		fmt.Fprint(a.Stdout, portableUsageText)
		return nil
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
  metadata             print crawlkit control metadata
  check-update         check for a newer gitcrawl release
  status               print fast read-only archive status
  remote status        print remote archive status
  remote archives      list remote archives visible to the current identity
  remote login         authenticate with GitHub org access for a remote archive
  whoami               print remote archive identity
  cloud publish        publish the local archive to a Worker remote
  init                 create config, optionally from a portable store
  doctor               check config, token, and database readiness
  sync                 sync GitHub issue and pull request metadata
  refresh              run sync, enrichment, embedding, and clustering pipeline
  embed                generate OpenAI embeddings for local thread documents
  threads              list local issue and pull request rows
  code index           index tracked text files from a local Git checkout
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
  clusters-report      write a Markdown report for the top clusters
  durable-clusters     list durable cluster groups
  cluster-detail       dump one latest run cluster, with durable fallback
  cluster-explain      alias for cluster-detail
  neighbors            list vector-nearest local issue and pull request rows
  search               search local thread and source documents; also supports search issues|prs gh syntax
  gh                   moved to Octopool; prints migration note
  portable prune       prune volatile payloads from a portable store
  tui [owner/repo]     browse clusters in the terminal UI; repo is inferred when omitted

No API server is provided. There is intentionally no serve command.
`

var commandUsageTexts = map[string]string{
	"metadata": `gitcrawl metadata prints crawlkit control metadata.

Usage:
  gitcrawl metadata [--json]
`,
	"status": `gitcrawl status prints fast read-only archive status.

Usage:
  gitcrawl status [--json]
`,
	"remote": `gitcrawl remote queries a configured Cloudflare-backed archive.

Usage:
  gitcrawl remote status [--json]
  gitcrawl remote archives [--json]
  gitcrawl remote login --endpoint URL [--json]
  gitcrawl remote login --endpoint URL --github-token-env GITHUB_TOKEN [--json]
  gitcrawl remote whoami [--json]
`,
	"cloud": `gitcrawl cloud manages Worker-backed remote archives.

Usage:
  gitcrawl cloud publish --remote URL --archive id [--json]
`,
	"whoami": `gitcrawl whoami prints the configured remote archive identity.

Usage:
  gitcrawl whoami [--json]
`,
	"check-update": `gitcrawl check-update checks GitHub Releases for a newer gitcrawl build.

Usage:
  gitcrawl check-update [--json] [--force]
`,
	"init": `gitcrawl init creates a local, portable, or cloud archive config.

Usage:
  gitcrawl init [--db path | --runtime-dir path | --portable-store URL | --remote URL --archive id] [--json]
`,
	"configure": `gitcrawl configure updates model fields in the config.

Usage:
  gitcrawl configure [--summary-model name] [--embed-model name] [--embedding-basis title_original] [--json]
`,
	"doctor": `gitcrawl doctor checks config, token, and database readiness.

Usage:
  gitcrawl doctor [--json]
`,
	"sync": `gitcrawl sync mirrors GitHub issue and pull request metadata.

Usage:
  gitcrawl sync owner/repo [--state open|closed|all] [--numbers refs] [--with pr-details] [--include-pr-details] [--json]
`,
	"refresh": `gitcrawl refresh runs sync, enrichment, embedding, and clustering.

Usage:
  gitcrawl refresh owner/repo [--state open|closed|all] [--no-sync] [--no-embed] [--no-cluster] [--json]
`,
	"embed": `gitcrawl embed generates OpenAI embeddings for local thread documents.

Usage:
  gitcrawl embed owner/repo [--number ref] [--limit N] [--force] [--include-closed] [--json]
`,
	"threads": `gitcrawl threads lists local issue and pull request rows.

Usage:
  gitcrawl threads owner/repo [--include-closed] [--numbers refs] [--limit N] [--json]
`,
	"search": `gitcrawl search queries local thread documents, or accepts gh-shaped issue and PR search.

Usage:
  gitcrawl search owner/repo --query text [--scope threads|code|all] [--mode keyword|semantic|hybrid] [--limit N] [--json]
  gitcrawl search issues|prs <query> -R owner/repo [--state open|closed|all] [--json fields] [--limit N] [--sync-if-stale duration]
`,
	"code": `gitcrawl code indexes source from a local Git checkout.

Usage:
  gitcrawl code index owner/repo [--path DIR] [--max-file-bytes N] [--max-total-bytes N] [--max-files N] [--json]
`,
	"cluster": `gitcrawl cluster builds durable clusters from local thread vectors.

Usage:
  gitcrawl cluster owner/repo [--threshold N] [--min-size N] [--max-cluster-size N] [--k N] [--cross-kind-threshold N] [--limit N] [--model name] [--basis semantic|references|hybrid] [--include-closed] [--json]
`,
	"clusters": `gitcrawl clusters lists latest display clusters with durable fallback.

Usage:
  gitcrawl clusters owner/repo [--sort size|recent|oldest] [--min-size N] [--limit N] [--hide-closed] [--json]
`,
	"clusters-report": `gitcrawl clusters-report writes a Markdown report for top display clusters.

Usage:
  gitcrawl clusters-report owner/repo [--sort size|recent|oldest] [--min-size N] [--limit N] [--member-limit N] [--body-chars N] [--hide-closed] [--json]
`,
	"durable-clusters": `gitcrawl durable-clusters lists governed durable cluster groups.

Usage:
  gitcrawl durable-clusters owner/repo [--include-closed] [--sort size|recent|oldest] [--min-size N] [--limit N] [--json]
`,
	"cluster-detail": `gitcrawl cluster-detail dumps one cluster and its member rows.

Usage:
  gitcrawl cluster-detail owner/repo --id N [--source auto|run|durable] [--member-limit N] [--body-chars N] [--hide-closed] [--json]
`,
	"neighbors": `gitcrawl neighbors lists vector-nearest local issue and pull request rows.

Usage:
  gitcrawl neighbors owner/repo --number ref [--limit N] [--include-closed] [--json]
`,
	"runs": `gitcrawl runs lists local pipeline run history.

Usage:
  gitcrawl runs owner/repo [--kind sync|summary|embedding|cluster] [--limit N] [--json]
`,
	"close-thread": `gitcrawl close-thread locally hides one issue or pull request row.

Usage:
  gitcrawl close-thread owner/repo --number ref [--reason text] [--json]
`,
	"reopen-thread": `gitcrawl reopen-thread clears a local thread hide.

Usage:
  gitcrawl reopen-thread owner/repo --number ref [--json]
`,
	"close-cluster": `gitcrawl close-cluster locally hides one durable cluster.

Usage:
  gitcrawl close-cluster owner/repo --id N [--reason text] [--json]
`,
	"reopen-cluster": `gitcrawl reopen-cluster clears a local cluster hide.

Usage:
  gitcrawl reopen-cluster owner/repo --id N [--json]
`,
	"exclude-cluster-member": `gitcrawl exclude-cluster-member locally removes one row from a durable cluster.

Usage:
  gitcrawl exclude-cluster-member owner/repo --id N --number ref [--reason text] [--json]
`,
	"include-cluster-member": `gitcrawl include-cluster-member restores one row to a durable cluster.

Usage:
  gitcrawl include-cluster-member owner/repo --id N --number ref [--json]
`,
	"set-cluster-canonical": `gitcrawl set-cluster-canonical sets the canonical row for a durable cluster.

Usage:
  gitcrawl set-cluster-canonical owner/repo --id N --number ref [--reason text] [--json]
`,
	"gh": `gitcrawl gh moved to octopool.

Usage:
  octopool login
  octopool gh <gh command>
`,
}

const tuiUsageText = `gitcrawl tui opens the local terminal cluster browser.

Usage:
  gitcrawl tui [owner/repo] [--limit N] [--min-size N] [--sort recent|oldest|size] [--layout focus|columns|right-stack] [--hide-closed]

If owner/repo is omitted, gitcrawl uses the most recently updated repository in the local database.
The TUI starts with ghcrawl-style cluster display defaults: --min-size 5, --sort size, --layout columns, and closed historical clusters visible. Pass --min-size 1 for singleton clusters or --hide-closed to focus open-only.
Mouse is supported: click rows, wheel panes, right-click for actions, and use the menu for copy/sort/filter/jump/member triage controls.
Press a to open the same action menu from the keyboard.
Press # to jump directly to an issue or PR number.
Press p to switch between repositories already present in the local store.
Press n to load neighbors for the selected issue or PR.
Enter from the members pane also loads neighbors before opening detail.
The TUI quietly refreshes from the local store every 15 seconds and leaves the current status alone when nothing changed.
`

const portableUsageText = `gitcrawl portable manages local portable-store snapshots.

Usage:
  gitcrawl portable prune [--body-chars N] [--no-vacuum] [--json]

Subcommands:
  prune               prune volatile payloads from the configured portable store
`
