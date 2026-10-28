package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runChangeContract(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("change-contract requires status, enable, bump or disable"))
	}
	action := args[0]
	fs := flag.NewFlagSet("change-contract "+action, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	retentionRaw := fs.String("retention", "", "retained log span in sequence values")
	reason := fs.String("reason", "", "why every reader must rebuild")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args[1:], map[string]bool{"retention": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("change-contract %s accepts no positional arguments", action))
	}
	a.applyCommandJSON(*jsonOut)
	var retention int64
	if strings.TrimSpace(*retentionRaw) != "" {
		if action != "enable" {
			return usageErr(fmt.Errorf("--retention applies only to change-contract enable"))
		}
		value, err := strconv.ParseInt(strings.TrimSpace(*retentionRaw), 10, 64)
		if err != nil || value < store.MinChangeContractRetention {
			return usageErr(fmt.Errorf("--retention must be an integer of at least %d", store.MinChangeContractRetention))
		}
		retention = value
	}
	if strings.TrimSpace(*reason) != "" && action != "bump" {
		return usageErr(fmt.Errorf("--reason applies only to change-contract bump"))
	}
	switch action {
	case "status":
		rt, err := a.openLocalRuntimeReadOnly(ctx)
		if err != nil {
			return err
		}
		defer rt.Store.Close()
		status, err := rt.Store.ChangeContractStatus(ctx)
		if err != nil {
			return err
		}
		return a.writeOutput("change-contract", status, true)
	case "enable", "bump", "disable":
	default:
		return usageErr(fmt.Errorf("unknown change-contract action %q", action))
	}
	if action == "bump" && strings.TrimSpace(*reason) == "" {
		return usageErr(fmt.Errorf("change-contract bump requires --reason"))
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	if cfg.Remote.Enabled() {
		return fmt.Errorf("change-contract requires a native local archive")
	}
	if _, portable, err := portableStoreRoot(ctx, cfg.DBPath); err != nil {
		return err
	} else if portable {
		return fmt.Errorf("change-contract refuses portable stores and mirrors")
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	result := map[string]any{"action": action}
	switch action {
	case "enable":
		repair, err := st.EnableChangeContract(ctx, store.ChangeContractEnableOptions{Retention: retention})
		if err != nil {
			return err
		}
		result["repair"] = repair
	case "bump":
		generation, err := st.BumpChangeContractGeneration(ctx, *reason, time.Time{})
		if err != nil {
			return err
		}
		result["generation"] = generation
	case "disable":
		removed, err := st.DisableChangeContract(ctx)
		if err != nil {
			return err
		}
		result["removed"] = removed
	}
	status, err := st.ChangeContractStatus(ctx)
	if err != nil {
		return err
	}
	result["status"] = status
	return a.writeOutput("change-contract", result, true)
}
