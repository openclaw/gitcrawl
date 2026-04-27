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
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
	"github.com/openclaw/gitcrawl/internal/vector"
)

const (
	defaultTUIMinSize         = 5
	defaultTUIWorkingSetLimit = 500
)

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
	case "runs":
		return a.runRuns(ctx, rest[1:])
	case "search":
		return a.runSearch(ctx, rest[1:])
	case "configure":
		return a.runConfigure(rest[1:])
	case "clusters":
		return a.runClusters(ctx, rest[1:])
	case "cluster-detail":
		return a.runClusterDetail(ctx, rest[1:])
	case "neighbors":
		return a.runNeighbors(ctx, rest[1:])
	case "portable":
		return a.runPortable(ctx, rest[1:])
	case "tui":
		return a.runTUI(ctx, rest[1:])
	case "refresh", "summarize", "key-summaries", "embed", "cluster", "cluster-experiment", "durable-clusters", "cluster-explain", "close-thread", "close-cluster", "exclude-cluster-member", "include-cluster-member", "set-cluster-canonical", "merge-clusters", "split-cluster", "export-sync", "import-sync", "validate-sync", "portable-size", "sync-status", "optimize", "completion":
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

func (a *App) runSearch(ctx context.Context, args []string) error {
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

func (a *App) runClusters(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("clusters", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "", "minimum active member count")
	limitRaw := fs.String("limit", "", "maximum cluster rows")
	sortMode := fs.String("sort", "recent", "sort mode: recent|size")
	hideClosed := fs.Bool("hide-closed", false, "hide non-active or closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "sort": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("clusters requires owner/repo"))
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
	clusters, err := rt.Store.ListClusterSummaries(ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: !*hideClosed,
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("clusters", map[string]any{
		"repository": repo.FullName,
		"clusters":   clusters,
	}, true)
}

func (a *App) runTUI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "", "minimum active member count")
	limitRaw := fs.String("limit", "20", "maximum cluster rows")
	sortMode := fs.String("sort", "", "sort mode: recent|size")
	hideClosed := fs.Bool("hide-closed", false, "hide non-active or closed clusters")
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

	rt, err := a.openLocalRuntimeReadOnly(ctx)
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
		sort = "recent"
	}
	if sort != "recent" && sort != "size" {
		return usageErr(fmt.Errorf("unsupported sort %q", sort))
	}

	interactive := a.format == FormatText && a.canRunInteractiveTUI()
	clusters, err := rt.Store.ListClusterSummaries(ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: !*hideClosed,
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	})
	if err != nil {
		return err
	}
	if interactive {
		workingSet, err := rt.Store.ListClusterSummaries(ctx, store.ClusterSummaryOptions{
			RepoID:        repo.ID,
			IncludeClosed: true,
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
		HideClosed:         *hideClosed,
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

func (a *App) runSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	since := fs.String("since", "", "GitHub since timestamp")
	limitRaw := fs.String("limit", "", "maximum issue/PR rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	includeComments := fs.Bool("include-comments", false, "hydrate issue comments, PR reviews, and PR review comments")
	fs.Bool("include-code", false, "accepted for compatibility; code hydration is not implemented yet")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"since": true, "limit": true})); err != nil {
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

	cfg, err := config.Load(a.configPath)
	if err != nil {
		return err
	}
	token := config.ResolveGitHubToken(cfg)
	if token.Value == "" {
		return fmt.Errorf("missing GitHub token: set %s", cfg.GitHub.TokenEnv)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	client := gh.New(gh.Options{Token: token.Value})
	service := syncer.New(client, st)
	stats, err := service.Sync(ctx, syncer.Options{
		Owner:           owner,
		Repo:            repo,
		Since:           strings.TrimSpace(*since),
		Limit:           limit,
		IncludeComments: *includeComments,
		Reporter: func(message string) {
			fmt.Fprintln(a.Stderr, message)
		},
	})
	if err != nil {
		return err
	}
	return a.writeOutput("sync", stats, true)
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
		if err := runGit(ctx, "", "-C", dir, "pull", "--ff-only"); err != nil {
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
	return "cloned", nil
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
  clusters             list cluster summaries
  cluster-detail       dump one durable cluster
  search               search local thread documents
  portable prune       prune volatile payloads from a portable store
  tui [owner/repo]     browse clusters in the terminal UI; repo is inferred when omitted

No API server is provided. There is intentionally no serve command.
`

const tuiUsageText = `gitcrawl tui opens the local terminal cluster browser.

Usage:
  gitcrawl tui [owner/repo] [--limit N] [--min-size N] [--sort recent|size] [--hide-closed]

If owner/repo is omitted, gitcrawl uses the most recently updated repository in the local database.
The TUI starts at --min-size 5 by default; pass --min-size 1 to show singleton clusters.
Mouse is supported: click rows, wheel panes, right-click for actions, and use the menu for copy/sort/filter controls.
`
