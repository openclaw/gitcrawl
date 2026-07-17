package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/openclaw/crawlkit/releasecheck"
	"github.com/openclaw/gitcrawl/internal/config"
)

const gitcrawlUpgradeHint = "brew upgrade openclaw/tap/gitcrawl"

func gitcrawlReleaseCheckOptions(force bool) releasecheck.Options {
	cfg := config.Default()
	return releasecheck.Options{
		AppName:        "gitcrawl",
		Owner:          "openclaw",
		Repo:           "gitcrawl",
		CurrentVersion: version,
		CacheDir:       cfg.CacheDir,
		Force:          force,
	}
}

func (a *App) maybeNotifyRelease(ctx context.Context, args []string) {
	_, _ = releasecheck.Notify(ctx, releasecheck.NotifyOptions{
		Options:     gitcrawlReleaseCheckOptions(false),
		Stderr:      a.Stderr,
		InstallHint: gitcrawlUpgradeHint,
		Args:        args,
		JSONOutput:  a.format == FormatJSON,
		IsTerminal:  releasecheck.StderrIsTerminal(),
	})
}

func releaseNotificationAllowed(args []string) bool {
	if len(args) == 0 || args[0] != "fill-pr-details" {
		return true
	}
	for _, arg := range args[1:] {
		name, ok := flagName(arg)
		if !ok {
			continue
		}
		name, _, _ = strings.Cut(name, "=")
		if name == "reserve-rate-limit" {
			return false
		}
	}
	return true
}

func (a *App) runCheckUpdate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check-update", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	jsonOut := fs.Bool("json", false, "write JSON output")
	force := fs.Bool("force", false, "force a fresh release check")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("check-update takes flags only"))
	}
	result, err := releasecheck.Check(ctx, gitcrawlReleaseCheckOptions(*force))
	if err != nil && !errors.Is(err, releasecheck.ErrSkipped) {
		return err
	}
	if *jsonOut || a.format == FormatJSON {
		if *jsonOut {
			a.format = FormatJSON
		}
		return a.writeOutput("check-update", result, false)
	}
	_, err = fmt.Fprint(a.Stdout, releasecheck.StatusText("gitcrawl", gitcrawlUpgradeHint, result))
	return err
}
