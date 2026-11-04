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
	fs := flag.NewFlagSet("close-thread", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "issue or pull request number")
	reason := fs.String("reason", "CLI manual close", "local close reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("close-thread requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseOptionalThreadNumber(*numberRaw, owner+"/"+repoName)
	if err != nil {
		return usageErr(err)
	}
	if number == 0 {
		return usageErr(fmt.Errorf("close-thread requires --number"))
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
	if err := rt.Store.CloseThreadLocally(ctx, repo.ID, number, *reason); err != nil {
		return err
	}
	return a.writeOutput("close-thread", map[string]any{
		"repository": repo.FullName,
		"number":     number,
		"reason":     strings.TrimSpace(*reason),
		"closed":     true,
	}, true)
}

func (a *App) runReopenThread(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reopen-thread", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "issue or pull request number")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("reopen-thread requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseOptionalThreadNumber(*numberRaw, owner+"/"+repoName)
	if err != nil {
		return usageErr(err)
	}
	if number == 0 {
		return usageErr(fmt.Errorf("reopen-thread requires --number"))
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
	if err := rt.Store.ReopenThreadLocally(ctx, repo.ID, number); err != nil {
		return err
	}
	return a.writeOutput("reopen-thread", map[string]any{
		"repository": repo.FullName,
		"number":     number,
		"reopened":   true,
	}, true)
}

func (a *App) runCloseCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("close-cluster", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	reason := fs.String("reason", "CLI manual close", "local close reason")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "reason": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("close-cluster requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, err := parseOptionalPositiveInt(*idRaw)
	if err != nil {
		return usageErr(err)
	}
	if clusterID == 0 {
		return usageErr(fmt.Errorf("close-cluster requires --id"))
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
	if err := rt.Store.CloseClusterLocally(ctx, repo.ID, int64(clusterID), *reason); err != nil {
		return err
	}
	return a.writeOutput("close-cluster", map[string]any{
		"repository": repo.FullName,
		"id":         clusterID,
		"reason":     strings.TrimSpace(*reason),
		"closed":     true,
	}, true)
}

func (a *App) runReopenCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reopen-cluster", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	idRaw := fs.String("id", "", "cluster id")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("reopen-cluster requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, err := parseOptionalPositiveInt(*idRaw)
	if err != nil {
		return usageErr(err)
	}
	if clusterID == 0 {
		return usageErr(fmt.Errorf("reopen-cluster requires --id"))
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
	if err := rt.Store.ReopenClusterLocally(ctx, repo.ID, int64(clusterID)); err != nil {
		return err
	}
	return a.writeOutput("reopen-cluster", map[string]any{
		"repository": repo.FullName,
		"id":         clusterID,
		"reopened":   true,
	}, true)
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
