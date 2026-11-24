package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/acquireapi"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/github"
)

func (a *App) runAcquireAPI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("acquire-api", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	endpoint := fs.String("endpoint", "", "PostgreSQL archive API origin")
	source := fs.String("source", "", "canonical registered archive ID")
	repo := fs.String("repo", "", "owner/repository")
	publisher := fs.String("publisher-token-env", "", "environment variable with acquisition publisher credential")
	reader := fs.String("reader-token-env", "", "environment variable with archive reader credential")
	appToken := fs.String("github-app-token-env", "", "environment variable with trusted mint-acquisition-token JSON")
	numbers := fs.String("numbers-file", "", "fixed historical number selection (no SQLite)")
	parents := fs.String("parent-selection-file", "", "audited parent-only identity selection (no child fetches)")
	commits := fs.Bool("commits", false, "pinned default-branch commit and attribution scan")
	cycle := fs.Bool("cycle", false, "one resumable reporting turn: incremental, commits, lifecycle, historical")
	commitPages := fs.Int("commit-max-pages", 100, "maximum atomic commit pages per invocation")
	lifecycle := fs.String("lifecycle-numbers-file", "", "fixed lifecycle parent numbers (1..500, no SQLite)")
	maxPages := fs.Int("lifecycle-max-pages", 100, "maximum lifecycle provider pages per invocation")
	since := fs.String("since", "", "initial incremental lower bound, RFC3339")
	timeout := fs.Duration("timeout", 10*time.Minute, "bounded invocation lifetime")
	commitTimeout := fs.Duration("commit-timeout", acquireapi.DefaultCommitTimeout, "per-request bound for run and page commits")
	limit := fs.Int("max-threads", 100, "maximum complete threads per invocation")
	parentPage := fs.Int("parent-page-size", 100, "repository page size (1..100)")
	childPage := fs.Int("child-page-size", 20, "child page size (1..100)")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	var appAuth bool
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "github-app-token-env" {
			appAuth = true
		}
	})
	if fs.NArg() != 0 || *endpoint == "" || *source == "" || *repo == "" || *timeout <= 0 || *timeout > 12*time.Hour || *publisher == "" || *reader == "" || !validAccessEnvName(*publisher) || !validAccessEnvName(*reader) {
		return usageErr(errors.New("acquire-api requires endpoint, source, repo and explicit publisher/reader token environment names"))
	}
	if appAuth && (*appToken == "" || !validAccessEnvName(*appToken) || *appToken == *publisher || *appToken == *reader || os.Getenv("OCTOPOOL_TOKEN") != "" || os.Getenv("OCTOPOOL_URL") != "" || os.Getenv("OCTOPOOL_POOL") != "") {
		return usageErr(errors.New("github-app-token-env requires an explicit environment name and cannot be combined with Octopool configuration"))
	}
	if *cycle && (*commits || *parents != "" || *numbers == "" || *lifecycle == "") {
		return usageErr(errors.New("cycle requires numbers-file and lifecycle-numbers-file, without commits or parent selection"))
	}
	if *commits && (*lifecycle != "" || *parents != "" || *numbers != "") {
		return usageErr(errors.New("commits cannot be combined with thread selections"))
	}
	if !*cycle && *lifecycle != "" && (*parents != "" || *numbers != "") {
		return usageErr(errors.New("lifecycle selection cannot be combined with parent-selection-file or numbers-file"))
	}
	if !*commits && !*cycle {
		var set bool
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "commit-max-pages" {
				set = true
			}
		})
		if set {
			return usageErr(errors.New("commit-max-pages requires commits"))
		}
	}
	if *lifecycle == "" {
		var lifecycleFlag bool
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "lifecycle-max-pages" {
				lifecycleFlag = true
			}
		})
		if lifecycleFlag {
			return usageErr(errors.New("lifecycle-max-pages requires lifecycle-numbers-file"))
		}
	}
	options := acquireapi.Options{Repository: *repo, MaxThreads: *limit, ParentPageSize: *parentPage, ChildPageSize: *childPage}
	var parentSelection acquireapi.ParentSelection
	if *parents != "" {
		if *numbers != "" || *since != "" || *parentPage < 1 || *parentPage > 100 {
			return usageErr(errors.New("parent selection cannot be combined with numbers-file or since; page size must be 1..100"))
		}
		f, err := os.Open(*parents)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(f, (16<<20)+1))
		f.Close()
		if err != nil {
			return err
		}
		parentSelection, err = acquireapi.DecodeParentSelection(raw, *repo)
		if err != nil {
			return usageErr(err)
		}
	}
	if *since != "" {
		t, err := time.Parse(time.RFC3339Nano, *since)
		if err != nil {
			return usageErr(err)
		}
		options.Since = t
	}
	numberFile := *numbers
	if *lifecycle != "" && !*cycle {
		numberFile = *lifecycle
	}
	var lifecycleNumbers []int
	if numberFile != "" {
		var err error
		options.Numbers, err = readAcquisitionNumbers(numberFile)
		if err != nil {
			return err
		}
	}
	if *cycle {
		var err error
		lifecycleNumbers, err = readAcquisitionNumbers(*lifecycle)
		if err != nil {
			return err
		}
	}
	// This path never loads archive config or asks gh for a provider credential.
	// An existing Access binding is used only when explicitly selected via config.
	cfg := config.Default()
	cfg.Remote.Endpoint = *endpoint
	if a.configPath != "" {
		loaded, err := config.Load(a.configPath)
		if err != nil {
			return err
		}
		cfg.CloudflareAccess = loaded.CloudflareAccess
	}
	h, err := remoteAccessHTTPClient(cfg)
	if err != nil {
		return err
	}
	api, err := acquireapi.NewClient(*endpoint, *source, os.Getenv(*publisher), os.Getenv(*reader), h.Transport)
	if err != nil {
		return err
	}
	if err = api.SetCommitTimeout(*commitTimeout); err != nil {
		return usageErr(err)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var rest *github.Client
	var graph *github.OctopoolConversations
	if appAuth {
		deadline, _ := ctx.Deadline()
		token, decodeErr := github.DecodeAcquisitionToken(os.Getenv(*appToken), *repo, deadline)
		if decodeErr != nil {
			return decodeErr
		}
		rest, graph, err = github.NewAppAcquisition(ctx, token)
	} else if *cycle || *parents != "" || *lifecycle != "" || *commits {
		graph, err = github.NewOctopoolConversations(os.Getenv("OCTOPOOL_URL"), os.Getenv("OCTOPOOL_POOL"), os.Getenv("OCTOPOOL_TOKEN"), *repo)
		if err == nil {
			rest = graph.Client
		}
	} else {
		rest, err = github.NewOctopool(os.Getenv("OCTOPOOL_URL"), os.Getenv("OCTOPOOL_POOL"), os.Getenv("OCTOPOOL_TOKEN"), *repo)
	}
	if err != nil {
		return err
	}
	var result acquireapi.Result
	if *cycle {
		turn, cycleErr := acquireapi.RunCycle(ctx, api, rest, graph, acquireapi.CycleOptions{Conversations: options, LifecycleNumbers: lifecycleNumbers, CommitMaxPages: *commitPages, LifecycleMaxPages: *maxPages})
		a.applyCommandJSON(*jsonOut)
		if outputErr := a.writeOutput("acquire-api", turn, *jsonOut); outputErr != nil {
			return outputErr
		}
		if cycleErr != nil {
			return fmt.Errorf("acquire-api cycle: %w", cycleErr)
		}
		return nil
	} else if *parents != "" || *lifecycle != "" || *commits {
		if *commits {
			result, err = acquireapi.RunCommits(ctx, api, graph, acquireapi.CommitOptions{Repository: *repo, Since: options.Since, MaxPages: *commitPages})
		} else if *lifecycle != "" {
			if len(options.Numbers) > *limit || *limit < 1 || *limit > 500 {
				return usageErr(errors.New("lifecycle selection exceeds max-threads (1..500)"))
			}
			result, err = acquireapi.RunLifecycle(ctx, api, graph, acquireapi.LifecycleOptions{Repository: *repo, Numbers: options.Numbers, Since: options.Since, MaxPages: *maxPages})
		} else {
			result, err = acquireapi.RunParents(ctx, api, graph, acquireapi.ParentOptions{Selection: parentSelection, MaxParents: *limit, BatchSize: min(*parentPage, 50)})
		}
	} else {
		result, err = acquireapi.Run(ctx, api, rest, options)
	}
	a.applyCommandJSON(*jsonOut)
	if outputErr := a.writeOutput("acquire-api", result, *jsonOut); outputErr != nil {
		return outputErr
	}
	if err != nil {
		return fmt.Errorf("acquire-api: %w", err)
	}
	return nil
}

func readAcquisitionNumbers(path string) ([]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, 16<<20+1))
	f.Close()
	if err != nil {
		return nil, err
	}
	if len(raw) > 16<<20 {
		return nil, usageErr(errors.New("historical selection exceeds 16 MiB"))
	}
	var numbers []int
	for _, word := range strings.FieldsFunc(string(raw), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\r' || r == '\t' }) {
		n, err := strconv.Atoi(word)
		if err != nil {
			return nil, usageErr(errors.New("invalid historical number"))
		}
		numbers = append(numbers, n)
	}
	if len(numbers) == 0 {
		return nil, usageErr(errors.New("empty historical selection"))
	}
	return numbers, nil
}
