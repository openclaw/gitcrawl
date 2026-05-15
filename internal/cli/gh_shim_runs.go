package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store"
)

var errGHRunListRequiresLive = errors.New("workflow run list requires live gh")

func (a *App) runGHRunList(ctx context.Context, args []string, controls ghShimControls) error {
	if hasAnyGHFlag(args, "--web") {
		return localGHUnsupported(fmt.Errorf("web workflow run flags require live gh"))
	}
	fs := flag.NewFlagSet("run list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	branchRaw := fs.String("branch", "", "branch")
	commitRaw := fs.String("commit", "", "head sha")
	limitRaw := fs.String("limit", "", "maximum rows")
	limitShortRaw := fs.String("L", "", "maximum rows")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	jqRaw := fs.String("jq", "", "jq filter")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{
		"R": true, "repo": true, "branch": true, "commit": true, "limit": true, "L": true, "json": true, "jq": true,
	})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("unexpected gh run list arguments: %s", strings.Join(fs.Args(), " ")))
	}
	limit, err := parseGHSearchLimit(*limitRaw, *limitShortRaw)
	if err != nil {
		return usageErr(err)
	}
	repoValue, err := a.resolveGHRepo(ctx, firstNonEmpty(*repoShort, *repoLong))
	if err != nil {
		return localGHUnsupported(err)
	}
	branch := strings.TrimSpace(*branchRaw)
	commit := strings.TrimSpace(*commitRaw)
	prBranch := false
	if branch != "" && commit == "" {
		if number, findErr := a.findGHPullRequestNumberByBranch(ctx, repoValue, branch); findErr == nil {
			if _, hydrateErr := a.ensureFreshGHPullRequestCache(ctx, repoValue, number); hydrateErr != nil {
				return hydrateErr
			}
			prBranch = true
		}
	}
	if !controls.Cached && commit == "" && !prBranch {
		return fmt.Errorf("%w: %w without exact commit or cached PR branch", errLocalGHUnsupported, errGHRunListRequiresLive)
	}
	runs, err := a.localGHWorkflowRuns(ctx, repoValue, store.WorkflowRunListOptions{
		Branch:  branch,
		HeadSHA: commit,
		Limit:   limit,
	})
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		return localGHUnsupported(fmt.Errorf("no cached workflow runs"))
	}
	if strings.TrimSpace(*jsonFieldsRaw) != "" || strings.TrimSpace(*jqRaw) != "" || a.format == FormatJSON {
		fields := firstNonEmpty(strings.TrimSpace(*jsonFieldsRaw), "databaseId,workflowName,status,conclusion,url,createdAt,updatedAt")
		rows, err := ghWorkflowRunJSONRows(runs, fields)
		if err != nil {
			return localGHUnsupported(err)
		}
		a.writeGHLocalRunNotice()
		return a.writeJSONValue(rows, strings.TrimSpace(*jqRaw))
	}
	a.writeGHLocalRunNotice()
	for _, run := range runs {
		if _, err := fmt.Fprintf(a.Stdout, "%s\t%s\t%s\t%s\n", run.RunID, run.WorkflowName, run.Status, run.HTMLURL); err != nil {
			return err
		}
	}
	return nil
}

func isGHRunListLiveRequired(err error) bool {
	return errors.Is(err, errGHRunListRequiresLive)
}

func (a *App) runGHRunView(ctx context.Context, args []string) error {
	if hasAnyGHFlag(args, "--web", "--log", "--log-failed") {
		return localGHUnsupported(fmt.Errorf("workflow run logs require live gh"))
	}
	fs := flag.NewFlagSet("run view", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	jqRaw := fs.String("jq", "", "jq filter")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"R": true, "repo": true, "json": true, "jq": true})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("gh run view requires a run id"))
	}
	runID := strings.TrimSpace(fs.Arg(0))
	repoValue, err := a.resolveGHRepo(ctx, firstNonEmpty(*repoShort, *repoLong))
	if err != nil {
		return localGHUnsupported(err)
	}
	runs, err := a.localGHWorkflowRuns(ctx, repoValue, store.WorkflowRunListOptions{Limit: 100})
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.RunID != runID {
			continue
		}
		if strings.TrimSpace(*jsonFieldsRaw) != "" || strings.TrimSpace(*jqRaw) != "" || a.format == FormatJSON {
			fields := firstNonEmpty(strings.TrimSpace(*jsonFieldsRaw), "databaseId,workflowName,status,conclusion,url,createdAt,updatedAt")
			rows, err := ghWorkflowRunJSONRows([]store.WorkflowRun{run}, fields)
			if err != nil {
				return localGHUnsupported(err)
			}
			a.writeGHLocalRunNotice()
			return a.writeJSONValue(rows[0], strings.TrimSpace(*jqRaw))
		}
		a.writeGHLocalRunNotice()
		_, err := fmt.Fprintf(a.Stdout, "run: %s\nworkflow: %s\nstatus: %s\nurl: %s\n", run.RunID, run.WorkflowName, run.Status, run.HTMLURL)
		return err
	}
	return localGHUnsupported(fmt.Errorf("cached workflow run %s was not found", runID))
}

func (a *App) writeGHLocalRunNotice() {
	_, _ = fmt.Fprintln(a.Stderr, "gitcrawl: workflow run from local PR cache; use gh run --live or exact check-runs API for release liveness")
}

func (a *App) localGHWorkflowRuns(ctx context.Context, repoValue string, options store.WorkflowRunListOptions) ([]store.WorkflowRun, error) {
	owner, repoName, err := parseOwnerRepo(repoValue)
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
	return rt.Store.ListWorkflowRuns(ctx, repo.ID, options)
}

func ghWorkflowRunJSONRows(runs []store.WorkflowRun, fieldsRaw string) ([]map[string]any, error) {
	fields := parseJSONFields(fieldsRaw)
	rows := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		row := make(map[string]any, len(fields))
		for _, field := range fields {
			switch field {
			case "databaseId", "id":
				if id, err := strconv.ParseInt(run.RunID, 10, 64); err == nil {
					row[field] = id
				} else {
					row[field] = run.RunID
				}
			case "number":
				row[field] = run.RunNumber
			case "workflowName", "name", "displayTitle":
				row[field] = run.WorkflowName
			case "status":
				row[field] = run.Status
			case "conclusion":
				row[field] = run.Conclusion
			case "url":
				row[field] = run.HTMLURL
			case "event":
				row[field] = run.Event
			case "headBranch":
				row[field] = run.HeadBranch
			case "headSha":
				row[field] = run.HeadSHA
			case "createdAt":
				row[field] = run.CreatedAtGH
			case "updatedAt":
				row[field] = run.UpdatedAtGH
			default:
				return nil, fmt.Errorf("unsupported gh run --json field %q", field)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}
