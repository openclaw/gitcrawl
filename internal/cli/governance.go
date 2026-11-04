package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runCloseThread(ctx context.Context, args []string) error {
	return a.runLocalClosureCommand(ctx, args, "close-thread")
}

func (a *App) runReopenThread(ctx context.Context, args []string) error {
	return a.runLocalClosureCommand(ctx, args, "reopen-thread")
}

func (a *App) runCloseCluster(ctx context.Context, args []string) error {
	return a.runLocalClosureCommand(ctx, args, "close-cluster")
}

func (a *App) runReopenCluster(ctx context.Context, args []string) error {
	return a.runLocalClosureCommand(ctx, args, "reopen-cluster")
}

func (a *App) runLocalClosureCommand(ctx context.Context, args []string, command string) error {
	cluster := command == "close-cluster" || command == "reopen-cluster"
	closing := command == "close-thread" || command == "close-cluster"
	idFlag, idHelp := "number", "issue or pull request number"
	if cluster {
		idFlag, idHelp = "id", "cluster id"
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String(idFlag, "", idHelp)
	var reason string
	valueFlags := map[string]bool{idFlag: true}
	if closing {
		fs.StringVar(&reason, "reason", "CLI manual close", "local close reason")
		valueFlags["reason"] = true
	}
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, valueFlags)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("%s requires owner/repo", command))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	var id int
	if cluster {
		id, err = parseOptionalPositiveInt(*idRaw)
	} else {
		id, err = parseOptionalThreadNumber(*idRaw, owner+"/"+repoName)
	}
	if err != nil {
		return usageErr(err)
	}
	if id == 0 {
		return usageErr(fmt.Errorf("%s requires --%s", command, idFlag))
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	switch command {
	case "close-thread":
		err = rt.Store.CloseThreadLocally(ctx, repo.ID, id, reason)
	case "reopen-thread":
		err = rt.Store.ReopenThreadLocally(ctx, repo.ID, id)
	case "close-cluster":
		err = rt.Store.CloseClusterLocally(ctx, repo.ID, int64(id), reason)
	case "reopen-cluster":
		err = rt.Store.ReopenClusterLocally(ctx, repo.ID, int64(id))
	default:
		return fmt.Errorf("unsupported local closure command %q", command)
	}
	if err != nil {
		return err
	}
	payload := map[string]any{"repository": repo.FullName, idFlag: id}
	if closing {
		payload["reason"] = strings.TrimSpace(reason)
		payload["closed"] = true
	} else {
		payload["reopened"] = true
	}
	return a.writeOutput(command, payload, true)
}

func (a *App) runExcludeClusterMember(ctx context.Context, args []string) error {
	return a.runClusterMemberCommand(ctx, args, "exclude-cluster-member", "CLI manual exclude", "excluded", (*store.Store).ExcludeClusterMemberLocally)
}

func (a *App) runIncludeClusterMember(ctx context.Context, args []string) error {
	return a.runClusterMemberCommand(ctx, args, "include-cluster-member", "CLI manual include", "included", (*store.Store).IncludeClusterMemberLocally)
}

func (a *App) runSetClusterCanonical(ctx context.Context, args []string) error {
	return a.runClusterMemberCommand(ctx, args, "set-cluster-canonical", "CLI manual canonical", "canonical", (*store.Store).SetClusterCanonicalLocally)
}

func (a *App) runClusterMemberCommand(ctx context.Context, args []string, command, defaultReason, resultKey string,
	mutate func(*store.Store, context.Context, int64, int64, int, string) (store.ClusterMemberOverride, error),
) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	numberRaw := fs.String("number", "", "issue or pull request number")
	reason := fs.String("reason", defaultReason, "local override reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "number": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("%s requires owner/repo", command))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, number, err := parseClusterMemberCommandIDs(command, *idRaw, *numberRaw, owner+"/"+repoName)
	if err != nil {
		return usageErr(err)
	}
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	override, err := mutate(rt.Store, ctx, repo.ID, int64(clusterID), number, *reason)
	if err != nil {
		return err
	}
	return a.writeOutput(command, map[string]any{
		"repository": repo.FullName,
		"override":   override,
		resultKey:    true,
	}, true)
}
