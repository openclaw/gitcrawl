package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/headlinemetrics"
)

func (a *App) runMetrics(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		return a.printCommandUsage("metrics")
	}
	command := args[0]
	if command != "collect" && command != "import" && command != "status" {
		return usageErr(fmt.Errorf("unknown metrics command %q", command))
	}
	fs := flag.NewFlagSet("metrics "+command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", a.configPath, "metrics JSON config path")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("metrics")
		}
		return usageErr(err)
	}
	if *path == "" || fs.NArg() != 0 {
		return usageErr(errors.New("metrics requires --config and no positional arguments"))
	}
	c, err := headlinemetrics.ReadConfig(*path)
	if err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	var collect headlinemetrics.Collector
	if command == "collect" {
		cfg := config.Default()
		if c.TokenEnv != "" {
			cfg.GitHub.TokenEnv = c.TokenEnv
		}
		token := a.resolveGitHubToken(ctx, cfg)
		var provider func(context.Context) (string, error)
		if a.githubTokenCommand != nil {
			provider, err = githubTokenProvider(*a.githubTokenCommand)
			if err != nil {
				return usageErr(err)
			}
		}
		client := github.New(github.Options{Token: token.Value, TokenProvider: provider, BaseURL: githubBaseURL(), HTTPClient: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}})
		collect = headlinemetrics.GitHubCollector(client, token.Value != "" || provider != nil)
	}
	in := a.Stdin
	if in == nil {
		in = http.NoBody
	}
	result, err := headlinemetrics.Execute(ctx, command, c, collect, in)
	// Partial collection is still a useful structured result; diagnostics stay
	// on stderr and a nonzero exit communicates unavailable required metrics.
	if err == nil || result.RowsWritten > 0 {
		if writeErr := a.writeOutput("metrics "+command, result, false); writeErr != nil {
			return writeErr
		}
	}
	return err
}
