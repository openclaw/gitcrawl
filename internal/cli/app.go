package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/alecthomas/kong"
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

type App struct {
	githubTokenCommand  *string
	githubTokenMu       sync.Mutex
	observedGitHubToken string
	Stdout              io.Writer
	Stderr              io.Writer

	configPath            string
	format                OutputFormat
	getWorkingDirectory   func() (string, error)
	githubAuthTokenLookup func(context.Context) (string, error)
	dbTargetNoticeOnce    sync.Once
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
		Stdout:              os.Stdout,
		Stderr:              os.Stderr,
		format:              FormatText,
		getWorkingDirectory: os.Getwd,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	if len(args) == 0 || rootHelpRequested(args, "config", "format", "github-token-command") {
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
	a.githubTokenCommand = global.GitHubTokenCommand
	a.observedGitHubToken = ""

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
	if a.githubTokenCommand != nil && rest[0] == "capture" {
		return usageErr(fmt.Errorf("capture does not support --github-token-command: offline quota provenance for managed credentials is unavailable"))
	}
	if releaseNotificationAllowed(rest) {
		a.maybeNotifyRelease(ctx, rest)
	}
	// Writable runtimes retain ownership through SQLite Close. Readers release
	// after mirror preparation, so a long-lived TUI does not block subscribers.
	session := &portableCommandSession{}
	ctx = context.WithValue(ctx, portableCommandKey{}, session)
	defer session.close()

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
	case "fill-pr-details":
		return a.runFillPRDetails(ctx, rest[1:])
	case "threads":
		return a.runThreads(ctx, rest[1:])
	case "capture":
		return a.runCapture(ctx, rest[1:])
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
	case "sync-failures":
		return a.runSyncFailures(ctx, rest[1:])
	case "coverage":
		return a.runCoverage(ctx, rest[1:])
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
	case "summarize":
		return a.runSummarize(ctx, rest[1:])
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
	case "key-summaries", "cluster-experiment", "merge-clusters", "split-cluster", "export-sync", "import-sync", "validate-sync", "portable-size", "sync-status", "optimize", "completion":
		return notImplemented(rest[0])
	default:
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
}

type gitcrawlRootArgs struct {
	GitHubTokenCommand *string  `name:"github-token-command" help:"Absolute executable supplying managed GitHub tokens (not supported on Windows)."`
	Config             string   `help:"Config path."`
	Format             string   `default:"text" help:"Output format: text, json, or log."`
	JSON               bool     `name:"json" help:"Write JSON output."`
	Version            bool     `help:"Print version."`
	NoColor            bool     `name:"no-color" help:"Disable color output."`
	Args               []string `arg:"" optional:"" passthrough:"partial" name:"command" help:"Command and arguments."`
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
