package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/openai"
	"github.com/openclaw/gitcrawl/internal/store"
)

const keySummaryInstructions = `Produce a compact key summary for duplicate detection.
Use plain text and at most three short lines.
State the observed problem or change, affected component, and distinctive conditions.
Preserve concrete errors, file paths, versions, and issue references when present.
Do not speculate or add facts that are not in the evidence.`

type summaryResult struct {
	Repository string               `json:"repository"`
	Model      string               `json:"model"`
	Kind       string               `json:"kind"`
	Selected   int                  `json:"selected"`
	Summarized int                  `json:"summarized"`
	Failed     int                  `json:"failed,omitempty"`
	Status     string               `json:"status"`
	Failures   []summaryFailureStat `json:"failures,omitempty"`
	RunID      int64                `json:"run_id"`
}

type summaryFailureStat struct {
	Number  int    `json:"number"`
	Status  int    `json:"status,omitempty"`
	Type    string `json:"type,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
}

type summarizeOptions struct {
	Number        int
	Limit         int
	Force         bool
	IncludeClosed bool
}

func (a *App) runSummarize(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("summarize", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "summarize one issue or pull request number")
	limitRaw := fs.String("limit", "", "maximum rows to summarize")
	force := fs.Bool("force", false, "regenerate summaries even when evidence is unchanged")
	includeClosed := fs.Bool("include-closed", false, "include closed issue and pull request rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("summarize requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	number, err := parseOptionalThreadNumber(*numberRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	result, err := a.summarizeRepository(ctx, owner, repoName, summarizeOptions{
		Number:        number,
		Limit:         limit,
		Force:         *force,
		IncludeClosed: *includeClosed,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("summarize", result, true)
}

func (a *App) summarizeRepository(ctx context.Context, owner, repoName string, options summarizeOptions) (summaryResult, error) {
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return summaryResult{}, err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return summaryResult{}, err
	}
	token := config.ResolveOpenAIKey(rt.Config)
	if token.Value == "" {
		return summaryResult{}, fmt.Errorf("missing OpenAI API key: set %s", rt.Config.OpenAI.APIKeyEnv)
	}
	tasks, err := rt.Store.ListSummaryTasks(ctx, store.SummaryTaskOptions{
		RepoID:        repo.ID,
		Provider:      "openai",
		Model:         rt.Config.OpenAI.SummaryModel,
		SummaryKind:   store.SummaryKindLLMKey,
		PromptVersion: store.SummaryPromptVersionV1,
		Number:        options.Number,
		Limit:         options.Limit,
		Force:         options.Force,
		IncludeClosed: options.IncludeClosed,
	})
	if err != nil {
		return summaryResult{}, err
	}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	client := openai.New(openai.Options{
		APIKey:  token.Value,
		BaseURL: openAIBaseURL(),
		Retry:   embedRetryOverride(),
	})
	concurrency := rt.Config.OpenAI.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(tasks) {
		concurrency = len(tasks)
	}

	type summaryAPIResult struct {
		task store.SummaryTask
		text string
		err  error
	}
	jobs := make(chan store.SummaryTask)
	results := make(chan summaryAPIResult)
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for task := range jobs {
				fmt.Fprintf(a.Stderr, "[summarize] #%d\n", task.Number)
				text, err := client.Summarize(ctx, rt.Config.OpenAI.SummaryModel, keySummaryInstructions, summaryInput(task))
				results <- summaryAPIResult{task: task, text: text, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, task := range tasks {
			select {
			case jobs <- task:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	summarized := 0
	var failures []summaryFailureStat
	cancelled := false
	for result := range results {
		if result.err != nil {
			if ctx.Err() != nil && (errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded)) {
				cancelled = true
			}
			failures = append(failures, summaryFailure(result.task.Number, result.err))
			continue
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if err := rt.Store.UpsertThreadKeySummary(ctx, store.ThreadKeySummary{
			ThreadRevisionID: result.task.RevisionID,
			SummaryKind:      store.SummaryKindLLMKey,
			PromptVersion:    store.SummaryPromptVersionV1,
			Provider:         "openai",
			Model:            rt.Config.OpenAI.SummaryModel,
			InputHash:        result.task.InputHash,
			OutputHash:       store.StableHash(result.text),
			KeyText:          result.text,
			CreatedAt:        now,
		}); err != nil {
			return summaryResult{}, err
		}
		summarized++
	}

	status := "success"
	switch {
	case cancelled:
		status = "cancelled"
	case len(failures) > 0 && summarized == 0:
		status = "error"
	case len(failures) > 0:
		status = "partial"
	}
	result := summaryResult{
		Repository: repo.FullName,
		Model:      rt.Config.OpenAI.SummaryModel,
		Kind:       store.SummaryKindLLMKey,
		Selected:   len(tasks),
		Summarized: summarized,
		Failed:     len(failures),
		Status:     status,
		Failures:   failures,
	}
	statsJSON, _ := json.Marshal(result)
	run := store.RunRecord{
		RepoID:     repo.ID,
		Kind:       "summary",
		Scope:      "repo",
		Status:     status,
		StartedAt:  started,
		FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
		StatsJSON:  string(statsJSON),
	}
	if len(failures) > 0 {
		run.ErrorText = failures[0].Message
	}
	recordCtx := ctx
	if cancelled {
		var recordCancel context.CancelFunc
		recordCtx, recordCancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer recordCancel()
	}
	runID, recordErr := rt.Store.RecordRun(recordCtx, run)
	if recordErr != nil && !cancelled {
		return summaryResult{}, recordErr
	}
	result.RunID = runID
	if cancelled {
		return result, ctx.Err()
	}
	if status == "error" {
		return result, fmt.Errorf("OpenAI summaries failed: %s", failures[0].Message)
	}
	return result, nil
}

func summaryInput(task store.SummaryTask) string {
	return fmt.Sprintf(
		"kind: %s\nnumber: #%d\ntitle: %s\n\nEvidence:\n%s",
		task.Kind,
		task.Number,
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.Text),
	)
}

func summaryFailure(number int, err error) summaryFailureStat {
	failure := summaryFailureStat{Number: number, Message: err.Error()}
	if apiErr := openai.AsAPIError(err); apiErr != nil {
		failure.Status = apiErr.Status
		failure.Type = apiErr.Type
		failure.Code = apiErr.Code
	}
	return failure
}
