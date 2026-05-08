package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runGHShim(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return a.execRealGH(ctx, args)
	}
	switch args[0] {
	case "xcache":
		return a.runGHXCache(args[1:])
	case "search":
		if len(args) >= 2 && isGHSearchKind(args[1]) {
			if err := a.runGHSearch(ctx, args[1:]); err != nil {
				if isLocalGHUnsupported(err) {
					return a.execRealGHMaybeCached(ctx, args)
				}
				return err
			}
			_ = a.incrementGHXCacheCounter("local_hits")
			return nil
		}
	case "issue", "pr":
		if len(args) >= 2 {
			switch args[1] {
			case "view":
				if err := a.runGHThreadView(ctx, args[0], args[2:]); err != nil {
					if isLocalGHUnsupported(err) {
						return a.execRealGHMaybeCached(ctx, args)
					}
					return err
				}
				_ = a.incrementGHXCacheCounter("local_hits")
				return nil
			case "checks":
				if args[0] == "pr" {
					if err := a.runGHPRChecks(ctx, args[2:]); err != nil {
						if isLocalGHUnsupported(err) {
							return a.execRealGHMaybeCached(ctx, args)
						}
						return err
					}
					_ = a.incrementGHXCacheCounter("local_hits")
					return nil
				}
			case "list":
				if err := a.runGHThreadList(ctx, args[0], args[2:]); err != nil {
					if isLocalGHUnsupported(err) {
						return a.execRealGHMaybeCached(ctx, args)
					}
					return err
				}
				_ = a.incrementGHXCacheCounter("local_hits")
				return nil
			}
		}
	case "run":
		if len(args) >= 2 {
			switch args[1] {
			case "list":
				if err := a.runGHRunList(ctx, args[2:]); err != nil {
					if isLocalGHUnsupported(err) {
						return a.execRealGHMaybeCached(ctx, args)
					}
					return err
				}
				_ = a.incrementGHXCacheCounter("local_hits")
				return nil
			case "view":
				if err := a.runGHRunView(ctx, args[2:]); err != nil {
					if isLocalGHUnsupported(err) {
						return a.execRealGHMaybeCached(ctx, args)
					}
					return err
				}
				_ = a.incrementGHXCacheCounter("local_hits")
				return nil
			}
		}
	}
	return a.execRealGHMaybeCached(ctx, args)
}

func (a *App) runGHThreadView(ctx context.Context, resource string, args []string) error {
	fs := flag.NewFlagSet(resource+" view", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	jqRaw := fs.String("jq", "", "jq filter")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"R": true, "repo": true, "json": true, "jq": true})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("gh %s view requires a number or GitHub URL", resource))
	}
	ref, _ := parseThreadReference(fs.Arg(0))
	number, err := parseThreadNumber(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	repoArg := firstNonEmpty(*repoShort, *repoLong)
	if repoArg == "" {
		repoArg = ref.FullName()
	}
	repoValue, err := a.resolveGHRepo(ctx, repoArg)
	if err != nil {
		return localGHUnsupported(err)
	}
	thread, err := a.localGHThread(ctx, repoValue, ghResourceKind(resource), number)
	if err != nil {
		if a.shouldAutoHydrateGHThread(err) {
			owner, repoName, parseErr := parseOwnerRepo(repoValue)
			if parseErr != nil {
				return localGHUnsupported(parseErr)
			}
			if _, syncErr := a.syncRepository(ctx, owner, repoName, syncOptions{
				Numbers:          []int{number},
				IncludePRDetails: resource == "pr",
			}); syncErr != nil {
				return localGHUnsupported(syncErr)
			}
			thread, err = a.localGHThread(ctx, repoValue, ghResourceKind(resource), number)
		}
		if err != nil {
			if errors.Is(err, errLocalGHUnsupported) {
				return err
			}
			return err
		}
	}
	jsonFields := strings.TrimSpace(*jsonFieldsRaw)
	if jsonFields != "" || strings.TrimSpace(*jqRaw) != "" || a.format == FormatJSON {
		if jsonFields == "" {
			jsonFields = "number,title,state,url"
		}
		row, err := a.ghThreadViewJSONRow(ctx, repoValue, thread, jsonFields)
		if err != nil {
			return localGHUnsupported(err)
		}
		return a.writeJSONValue(row, strings.TrimSpace(*jqRaw))
	}
	_, err = fmt.Fprintf(a.Stdout, "title:\t%s\nstate:\t%s\nurl:\t%s\n\n%s\n", thread.Title, thread.State, thread.HTMLURL, strings.TrimSpace(thread.Body))
	return err
}

func (a *App) runGHThreadList(ctx context.Context, resource string, args []string) error {
	fs := flag.NewFlagSet(resource+" list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	stateRaw := fs.String("state", "open", "state")
	limitRaw := fs.String("limit", "", "maximum rows")
	limitShortRaw := fs.String("L", "", "maximum rows")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	jqRaw := fs.String("jq", "", "jq filter")
	searchRaw := fs.String("search", "", "local search query")
	authorRaw := fs.String("author", "", "filter by author")
	assigneeRaw := fs.String("assignee", "", "filter by assignee")
	var labels stringListFlag
	fs.Var(&labels, "label", "filter by label")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{
		"R": true, "repo": true, "state": true, "limit": true, "L": true, "json": true, "jq": true,
		"search": true, "author": true, "assignee": true, "label": true,
	})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("unexpected gh %s list arguments: %s", resource, strings.Join(fs.Args(), " ")))
	}
	if err := validateGHSearchState(strings.TrimSpace(*stateRaw)); err != nil {
		return usageErr(err)
	}
	limit, err := parseGHSearchLimit(*limitRaw, *limitShortRaw)
	if err != nil {
		return usageErr(err)
	}
	repoValue, err := a.resolveGHRepo(ctx, firstNonEmpty(*repoShort, *repoLong))
	if err != nil {
		return localGHUnsupported(err)
	}
	threads, err := a.localGHThreads(ctx, ghThreadListRequest{
		Repo:     repoValue,
		Kind:     ghResourceKind(resource),
		State:    strings.TrimSpace(*stateRaw),
		Query:    strings.TrimSpace(*searchRaw),
		Author:   strings.TrimSpace(*authorRaw),
		Assignee: strings.TrimSpace(*assigneeRaw),
		Labels:   labels.Values(),
		Limit:    limit,
	})
	if err != nil {
		return err
	}
	if len(threads) == 0 && ghThreadListNeedsLiveEmptyCheck(ghThreadListRequest{
		Kind:     ghResourceKind(resource),
		State:    strings.TrimSpace(*stateRaw),
		Query:    strings.TrimSpace(*searchRaw),
		Author:   strings.TrimSpace(*authorRaw),
		Assignee: strings.TrimSpace(*assigneeRaw),
		Labels:   labels.Values(),
	}) {
		fresh, err := a.localGHThreadListHasBroadSync(ctx, repoValue, strings.TrimSpace(*stateRaw))
		if err != nil {
			return err
		}
		if !fresh {
			return localGHUnsupported(fmt.Errorf("empty local %s list has no broad %s sync", resource, ghDefaultListState(*stateRaw)))
		}
	}
	jsonFields := strings.TrimSpace(*jsonFieldsRaw)
	if jsonFields != "" || strings.TrimSpace(*jqRaw) != "" || a.format == FormatJSON {
		if jsonFields == "" {
			jsonFields = "number,title,state,url"
		}
		rows, err := ghSearchJSONRows(threads, jsonFields)
		if err != nil {
			return localGHUnsupported(err)
		}
		return a.writeJSONValue(rows, strings.TrimSpace(*jqRaw))
	}
	for _, thread := range threads {
		if _, err := fmt.Fprintf(a.Stdout, "%d\t%s\t%s\n", thread.Number, thread.Title, thread.HTMLURL); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) localGHThread(ctx context.Context, repoValue, kind string, number int) (store.Thread, error) {
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return store.Thread{}, err
	}
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return store.Thread{}, localGHUnsupported(err)
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return store.Thread{}, localGHUnsupported(err)
	}
	threads, err := rt.Store.ListThreadsFiltered(ctx, store.ThreadListOptions{
		RepoID:        repo.ID,
		IncludeClosed: true,
		Numbers:       []int{number},
	})
	if err != nil {
		return store.Thread{}, err
	}
	for _, thread := range threads {
		if thread.Number == number && thread.Kind == kind {
			return thread, nil
		}
	}
	return store.Thread{}, localGHUnsupported(fmt.Errorf("thread #%d was not found in local cache", number))
}

type ghThreadListRequest struct {
	Repo     string
	Kind     string
	State    string
	Query    string
	Author   string
	Assignee string
	Labels   []string
	Limit    int
}

func (a *App) localGHThreads(ctx context.Context, req ghThreadListRequest) ([]store.Thread, error) {
	owner, repoName, err := parseOwnerRepo(req.Repo)
	if err != nil {
		return nil, err
	}
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return nil, localGHUnsupported(err)
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return nil, localGHUnsupported(err)
	}
	return rt.Store.SearchThreads(ctx, store.ThreadSearchOptions{
		RepoID:               repo.ID,
		Query:                req.Query,
		Kind:                 req.Kind,
		State:                req.State,
		Author:               req.Author,
		Assignee:             req.Assignee,
		Labels:               req.Labels,
		IncludeLocallyClosed: true,
		Limit:                req.Limit,
	})
}

func ghThreadListNeedsLiveEmptyCheck(req ghThreadListRequest) bool {
	if req.Kind != "issue" || strings.TrimSpace(req.Query) != "" || strings.TrimSpace(req.Author) != "" || strings.TrimSpace(req.Assignee) != "" || len(req.Labels) > 0 {
		return false
	}
	return ghDefaultListState(req.State) == "open"
}

func (a *App) localGHThreadListHasBroadSync(ctx context.Context, repoValue, state string) (bool, error) {
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return false, err
	}
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return false, localGHUnsupported(err)
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return false, localGHUnsupported(err)
	}
	lastSync, err := rt.Store.LastSuccessfulListSyncAt(ctx, repo.ID, state)
	if err != nil {
		return false, err
	}
	return !lastSync.IsZero(), nil
}

func (a *App) resolveGHRepo(ctx context.Context, explicit string) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimSpace(explicit), nil
	}
	if envRepo := strings.TrimSpace(os.Getenv("GH_REPO")); envRepo != "" {
		return envRepo, nil
	}
	cmd := exec.CommandContext(ctx, "git", "remote", "get-url", "origin")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("repository is required outside a git checkout; pass -R owner/repo")
	}
	repo, err := ownerRepoFromGitRemote(strings.TrimSpace(string(out)))
	if err != nil {
		return "", err
	}
	return repo, nil
}

func (a *App) execRealGH(ctx context.Context, args []string) error {
	ghPath, err := resolveRealGHPath()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, ghPath, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = a.Stdout
	cmd.Stderr = a.Stderr
	return cmd.Run()
}

func (a *App) writeJSONValue(value any, jqExpr string) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if strings.TrimSpace(jqExpr) == "" {
		_, err = fmt.Fprintf(a.Stdout, "%s\n", data)
		return err
	}
	jqPath, err := exec.LookPath("jq")
	if err != nil {
		return localGHUnsupported(fmt.Errorf("--jq requires jq executable"))
	}
	cmd := exec.Command(jqPath, jqExpr)
	cmd.Stdin = bytes.NewReader(data)
	cmd.Stdout = a.Stdout
	cmd.Stderr = a.Stderr
	return cmd.Run()
}

func ghResourceKind(resource string) string {
	if resource == "pr" {
		return "pull_request"
	}
	return "issue"
}

func parseThreadNumber(value string) (int, error) {
	return parseOptionalThreadNumber(value)
}

func ownerRepoFromGitRemote(value string) (string, error) {
	value = strings.TrimSuffix(strings.TrimSpace(value), ".git")
	value = strings.TrimPrefix(value, "git@github.com:")
	if strings.HasPrefix(value, "https://github.com/") {
		value = strings.TrimPrefix(value, "https://github.com/")
	}
	if strings.HasPrefix(value, "ssh://git@github.com/") {
		value = strings.TrimPrefix(value, "ssh://git@github.com/")
	}
	parts := strings.Split(value, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("could not infer owner/repo from origin remote")
	}
	repo := filepath.Join(parts[len(parts)-2], parts[len(parts)-1])
	return strings.ReplaceAll(repo, string(os.PathSeparator), "/"), nil
}

var errLocalGHUnsupported = errors.New("local gh shim unsupported")

func localGHUnsupported(err error) error {
	if err == nil {
		return errLocalGHUnsupported
	}
	return fmt.Errorf("%w: %v", errLocalGHUnsupported, err)
}

func isLocalGHUnsupported(err error) bool {
	return errors.Is(err, errLocalGHUnsupported) || strings.Contains(err.Error(), "unsupported --json field")
}

type stringListFlag []string

func (f *stringListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, strings.TrimSpace(value))
	return nil
}

func (f *stringListFlag) Values() []string {
	values := make([]string, 0, len(*f))
	for _, value := range *f {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}
