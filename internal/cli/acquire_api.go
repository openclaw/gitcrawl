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
	numbers := fs.String("numbers-file", "", "fixed historical number selection (no SQLite)")
	parents := fs.String("parent-selection-file", "", "audited parent-only identity selection (no child fetches)")
	since := fs.String("since", "", "initial incremental lower bound, RFC3339")
	timeout := fs.Duration("timeout", 10*time.Minute, "bounded invocation lifetime")
	limit := fs.Int("max-threads", 100, "maximum complete threads per invocation")
	parentPage := fs.Int("parent-page-size", 100, "repository page size (1..100)")
	childPage := fs.Int("child-page-size", 20, "child page size (1..100)")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 || *endpoint == "" || *source == "" || *repo == "" || *timeout <= 0 || *timeout > 12*time.Hour || *publisher == "" || *reader == "" || !validAccessEnvName(*publisher) || !validAccessEnvName(*reader) {
		return usageErr(errors.New("acquire-api requires endpoint, source, repo and explicit publisher/reader token environment names"))
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
	if *numbers != "" {
		f, err := os.Open(*numbers)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(f, 16<<20+1))
		f.Close()
		if err != nil {
			return err
		}
		if len(raw) > 16<<20 {
			return usageErr(errors.New("historical selection exceeds 16 MiB"))
		}
		for _, word := range strings.FieldsFunc(string(raw), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\r' || r == '\t' }) {
			n, err := strconv.Atoi(word)
			if err != nil {
				return usageErr(errors.New("invalid historical number"))
			}
			options.Numbers = append(options.Numbers, n)
		}
		if len(options.Numbers) == 0 {
			return usageErr(errors.New("empty historical selection"))
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
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var result acquireapi.Result
	if *parents != "" {
		provider, providerErr := github.NewOctopoolConversations(os.Getenv("OCTOPOOL_URL"), os.Getenv("OCTOPOOL_POOL"), os.Getenv("OCTOPOOL_TOKEN"), *repo)
		if providerErr != nil {
			return providerErr
		}
		result, err = acquireapi.RunParents(ctx, api, provider, acquireapi.ParentOptions{Selection: parentSelection, MaxParents: *limit, BatchSize: min(*parentPage, 50)})
	} else {
		provider, providerErr := github.NewOctopool(os.Getenv("OCTOPOOL_URL"), os.Getenv("OCTOPOOL_POOL"), os.Getenv("OCTOPOOL_TOKEN"), *repo)
		if providerErr != nil {
			return providerErr
		}
		result, err = acquireapi.Run(ctx, api, provider, options)
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
