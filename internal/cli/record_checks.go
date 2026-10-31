package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runRecordChecks(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("record-checks requires status, enable, backfill or disable"))
	}
	action := args[0]
	fs := flag.NewFlagSet("record-checks "+action, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args[1:], map[string]bool{})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("record-checks %s accepts no positional arguments", action))
	}
	a.applyCommandJSON(*jsonOut)
	switch action {
	case "status", "enable", "backfill", "disable":
	default:
		return usageErr(fmt.Errorf("unknown record-checks action %q", action))
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	if cfg.Remote.Enabled() {
		return fmt.Errorf("record-checks requires a native local archive")
	}
	if _, portable, err := portableStoreRoot(ctx, cfg.DBPath); err != nil {
		return err
	} else if portable {
		return fmt.Errorf("record-checks refuses portable stores and mirrors")
	}
	if action == "status" {
		st, err := store.OpenReadOnly(ctx, cfg.DBPath)
		if err != nil {
			return err
		}
		defer st.Close()
		status, err := st.RecordEfficiencyStatus(ctx)
		if err != nil {
			return err
		}
		return a.writeOutput("record-checks", status, true)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	out := map[string]any{"action": action}
	switch action {
	case "enable":
		result, err := st.EnableRecordEfficiency(ctx, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		out["backfill"] = result
	case "backfill":
		result, err := st.BackfillRecordEfficiency(ctx)
		if err != nil {
			return err
		}
		out["backfill"] = result
	case "disable":
		if err := st.DisableRecordEfficiency(ctx); err != nil {
			return err
		}
	}
	status, err := st.RecordEfficiencyStatus(ctx)
	if err != nil {
		return err
	}
	out["status"] = status
	return a.writeOutput("record-checks", out, true)
}
