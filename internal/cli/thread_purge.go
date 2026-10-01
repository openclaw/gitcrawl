package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runPurgeThreads(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-threads", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	raw := fs.String("numbers", "", "explicit issue/PR numbers")
	apply := fs.String("apply", "", "apply the exact previewed plan ID")
	jsonOut := fs.Bool("json", false, "JSON plan/result")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"numbers": true, "apply": true})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("purge-threads requires owner/repo"))
	}
	owner, repo, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	repository := owner + "/" + repo
	numbers, err := parseOptionalThreadNumberList(*raw, repository)
	if err != nil {
		return usageErr(err)
	}
	if len(numbers) == 0 || len(numbers) > 100 {
		return usageErr(fmt.Errorf("purge-threads requires 1..100 explicit --numbers"))
	}
	if flagWasSet(fs, "apply") && len(*apply) != 64 {
		return usageErr(fmt.Errorf("--apply requires the plan_id from a preview"))
	}
	a.applyCommandJSON(*jsonOut)
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	if cfg.Remote.Enabled() {
		return fmt.Errorf("purge-threads requires a native local archive")
	}
	_, portable, err := portableStoreRoot(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	if portable {
		return fmt.Errorf("purge-threads refuses portable stores and mirrors")
	}
	// Lock before opening a writer: an analytics collector owns this same file.
	if *apply != "" {
		lock, err := os.OpenFile(filepath.Join(filepath.Dir(cfg.DBPath), "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		defer lock.Close()
		if err = lockPortableFile(lock); err != nil {
			return fmt.Errorf("collector ownership lock busy: %w", err)
		}
	}
	probe, err := store.OpenReadOnly(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	plan, err := probe.PlanThreadPurge(ctx, repository, numbers)
	closeErr := probe.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if *apply == "" {
		return a.writeOutput("thread_purge_plan", plan, true)
	}
	if *apply != plan.PlanID {
		return fmt.Errorf("purge plan changed; preview again before applying")
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	result, err := st.PurgeThreads(ctx, repository, numbers, *apply)
	if err != nil {
		return err
	}
	return a.writeOutput("thread_purge", result, true)
}
