package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runPurgeThreads(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-threads", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	raw := fs.String("numbers", "", "explicit issue/PR numbers")
	mirror := fs.Bool("runtime-mirror", false, "purge only the existing managed portable runtime mirror and preserve local ownership")
	apply := fs.Bool("apply", false, "apply owner-directed purge and durable exclusion")
	requestID := fs.String("request-id", "", "owner request identifier, required for apply")
	jsonOut := fs.Bool("json", false, "JSON metadata/count audit")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"numbers": true, "request-id": true})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("purge-threads requires owner/repo"))
	}
	owner, repo, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	numbers, err := parseOptionalThreadNumberList(*raw, owner+"/"+repo)
	if err != nil {
		return usageErr(err)
	}
	if len(numbers) == 0 || len(numbers) > 100 {
		return usageErr(fmt.Errorf("purge-threads requires 1..100 explicit --numbers"))
	}
	if *apply && (strings.TrimSpace(*requestID) == "" || len(*requestID) > 128) {
		return usageErr(fmt.Errorf("--apply requires a nonempty --request-id of at most 128 bytes"))
	}
	a.applyCommandJSON(*jsonOut)
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	if cfg.Remote.Enabled() {
		return fmt.Errorf("purge-threads requires a native local archive")
	}
	dbPath := cfg.DBPath
	root, portable, err := portableStoreRoot(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	if *mirror {
		if !portable {
			return fmt.Errorf("--runtime-mirror requires a config pointing to its portable checkout")
		}
		var release func()
		ctx, release, err = acquirePortableOwner(ctx, root)
		if err != nil {
			return err
		}
		defer release()
		dbPath, err = a.portableRuntimeDBPath(ctx, cfg.DBPath)
		if err != nil {
			return err
		}
		dbPath, err = canonicalPortablePath(dbPath)
		if err != nil {
			return err
		}
		canonicalRoot, err := canonicalPortablePath(root)
		if err != nil {
			return err
		}
		if pathWithin(canonicalRoot, dbPath) || pathWithin(filepath.Dir(dbPath), canonicalRoot) {
			return fmt.Errorf("runtime mirror must be outside portable checkout")
		}
		// Inspect an existing mirror only: never materialize, refresh or fetch.
		snapshot, err := inspectPortableMirror(ctx, dbPath, cfg.DBPath)
		if err != nil {
			return err
		}
		if !snapshot.exists {
			return fmt.Errorf("managed runtime mirror does not exist")
		}
		if err = snapshot.recheck(ctx); err != nil {
			return err
		}
	} else if portable {
		return fmt.Errorf("portable checkout requires explicit --runtime-mirror; publisher data is never purged")
	}
	probe, err := store.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return err
	}
	var hasPortable bool
	err = probe.DB().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='portable_metadata' AND type='table')").Scan(&hasPortable)
	if err != nil {
		probe.Close()
		return err
	}
	if hasPortable && !*mirror {
		probe.Close()
		return fmt.Errorf("portable mirror requires its checkout config and --runtime-mirror")
	}
	plan, err := probe.PlanThreadPurge(ctx, owner+"/"+repo, numbers)
	closeErr := probe.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if !*apply {
		return a.writeOutput("thread_purge_plan", plan, true)
	}
	var st *store.Store
	if *mirror {

		st, err = store.OpenThreadPurgeMirror(ctx, dbPath)
	} else {
		// The permanent collector owner lock must precede writer open/migration.
		lock, e := os.OpenFile(filepath.Join(filepath.Dir(dbPath), "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		defer lock.Close()
		if e = lockPortableFile(lock); e != nil {
			return fmt.Errorf("collector ownership lock busy: %w", e)
		}
		st, err = store.Open(ctx, dbPath)
	}
	if err != nil {
		return err
	}
	defer st.Close()
	result, err := st.PurgeThreads(ctx, owner+"/"+repo, numbers, *requestID)
	if err != nil {
		return err
	}
	if *mirror {
		// The portable owner lease fences refresh through commit and this stamp.
		// If interrupted after commit, changed bytes or a live WAL already make
		// inspectPortableMirror preserve the local DB under the existing policy.
		statePath := portableStoreRefreshStatePath(dbPath)
		state := readPortableStoreRefreshState(statePath)
		state.MirrorWritable = true
		if err = writePortableStoreRefreshState(statePath, state); err != nil {
			return fmt.Errorf("owner purge committed; persist writable mirror stamp: %w", err)
		}
	}
	return a.writeOutput("thread_purge", result, true)
}
