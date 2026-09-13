package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/openai"
	"github.com/openclaw/gitcrawl/internal/store"
)

type embedResult struct {
	Repository string             `json:"repository"`
	Model      string             `json:"model"`
	Basis      string             `json:"basis"`
	Selected   int                `json:"selected"`
	Embedded   int                `json:"embedded"`
	Skipped    int                `json:"skipped"`
	Failed     int                `json:"failed,omitempty"`
	Retries    int                `json:"retries,omitempty"`
	Status     string             `json:"status,omitempty"`
	Failures   []embedFailureStat `json:"failures,omitempty"`
	RunID      int64              `json:"run_id"`
	dbTarget   dbTargetInfo
}

type embedFailureStat struct {
	BatchStart int    `json:"batch_start"`
	BatchEnd   int    `json:"batch_end"`
	Attempts   int    `json:"attempts"`
	Status     int    `json:"status,omitempty"`
	Type       string `json:"type,omitempty"`
	Code       string `json:"code,omitempty"`
	Message    string `json:"message"`
}

func (a *App) runEmbed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("embed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	numberRaw := fs.String("number", "", "embed one issue or pull request number")
	limitRaw := fs.String("limit", "", "maximum rows to embed")
	force := fs.Bool("force", false, "re-embed even when content hash is unchanged")
	includeClosed := fs.Bool("include-closed", false, "include closed issue and pull request rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"number": true, "limit": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("embed requires owner/repo"))
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
	result, err := a.embedRepository(ctx, owner, repoName, embedOptions{
		Number:        number,
		Limit:         limit,
		Force:         *force,
		IncludeClosed: *includeClosed,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("embed", result, true)
}

type embedOptions struct {
	Number        int
	Limit         int
	Force         bool
	IncludeClosed bool
}

func (a *App) embedRepository(ctx context.Context, owner, repoName string, options embedOptions) (embedResult, error) {
	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return embedResult{}, err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return embedResult{}, err
	}
	if rt.Config.EmbeddingBasis == "title_summary" {
		return embedResult{}, fmt.Errorf("embedding basis %q needs summarize support, which is not implemented yet; use `gitcrawl configure --embedding-basis title_original`", rt.Config.EmbeddingBasis)
	}
	token := config.ResolveOpenAIKey(rt.Config)
	if token.Value == "" {
		return embedResult{}, fmt.Errorf("missing OpenAI API key: set %s", rt.Config.OpenAI.APIKeyEnv)
	}
	tasks, err := rt.Store.ListEmbeddingTasks(ctx, store.EmbeddingTaskOptions{
		RepoID:        repo.ID,
		Basis:         rt.Config.EmbeddingBasis,
		Model:         rt.Config.OpenAI.EmbedModel,
		Number:        options.Number,
		Limit:         options.Limit,
		Force:         options.Force,
		IncludeClosed: options.IncludeClosed,
	})
	if err != nil {
		return embedResult{}, err
	}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	batchSize := rt.Config.OpenAI.BatchSize
	if batchSize <= 0 {
		batchSize = 64
	}
	client := openai.New(openai.Options{APIKey: token.Value, BaseURL: embedBaseURL(rt.Config), Dimensions: rt.Config.OpenAI.EmbedDimensions, Retry: embedRetryOverride()})

	type pendingBatch struct {
		start, end int
		attempts   int
	}
	var queue []pendingBatch
	for start := 0; start < len(tasks); start += batchSize {
		end := start + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		queue = append(queue, pendingBatch{start: start, end: end})
	}

	embedded := 0
	totalRetries := 0
	var failures []embedFailureStat
	cancelled := false
	var cancelErr error

	const maxBatchAttempts = 2
	for len(queue) > 0 {
		batch := queue[0]
		queue = queue[1:]
		batch.attempts++
		slice := tasks[batch.start:batch.end]
		texts := make([]string, 0, len(slice))
		for _, task := range slice {
			texts = append(texts, task.Text)
		}
		fmt.Fprintf(a.Stderr, "[embed] embedding %d-%d of %d (attempt %d)\n", batch.start+1, batch.end, len(tasks), batch.attempts)
		if batch.attempts == 1 {
			if truncated := truncatedEmbeddingTaskCount(slice); truncated > 0 {
				fmt.Fprintf(a.Stderr, "[embed] truncated %d input(s) to embedding input budget (%d runes/%d bytes)\n", truncated, store.MaxEmbeddingTextRunes, store.MaxEmbeddingTextBytes)
			}
		}
		vectors, err := client.Embed(ctx, rt.Config.OpenAI.EmbedModel, texts)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				cancelled = true
				cancelErr = err
				break
			}
			retryable := true
			if apiErr := openai.AsAPIError(err); apiErr != nil {
				retryable = apiErr.Retryable()
			}
			if retryable && batch.attempts < maxBatchAttempts {
				totalRetries++
				fmt.Fprintf(a.Stderr, "[embed] batch %d-%d failed (%s), requeueing\n", batch.start+1, batch.end, summarizeEmbedErr(err))
				queue = append(queue, batch)
				continue
			}
			fmt.Fprintf(a.Stderr, "[embed] batch %d-%d failed permanently: %s\n", batch.start+1, batch.end, summarizeEmbedErr(err))
			failures = append(failures, makeEmbedFailureStat(batch.start, batch.end, batch.attempts, err))
			continue
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for index, vector := range vectors {
			task := slice[index]
			if err := rt.Store.UpsertThreadVector(ctx, store.ThreadVector{
				ThreadID:    task.ThreadID,
				Basis:       rt.Config.EmbeddingBasis,
				Model:       rt.Config.OpenAI.EmbedModel,
				Dimensions:  len(vector),
				ContentHash: task.ContentHash,
				Vector:      vector,
				Backend:     "openai",
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				return embedResult{}, err
			}
			embedded++
		}
	}

	failedRows := 0
	for _, f := range failures {
		failedRows += f.BatchEnd - f.BatchStart
	}

	status := "success"
	switch {
	case cancelled:
		status = "cancelled"
	case len(failures) > 0 && embedded == 0:
		status = "error"
	case len(failures) > 0:
		status = "partial"
	}

	result := embedResult{
		Repository: repo.FullName,
		Model:      rt.Config.OpenAI.EmbedModel,
		Basis:      rt.Config.EmbeddingBasis,
		Selected:   len(tasks),
		Embedded:   embedded,
		Failed:     failedRows,
		Retries:    totalRetries,
		Status:     status,
		Failures:   failures,
		dbTarget:   rt.dbTarget(),
	}
	statsJSON, _ := json.Marshal(result)
	runRecord := store.RunRecord{
		RepoID:     repo.ID,
		Kind:       "embedding",
		Scope:      "repo",
		Status:     status,
		StartedAt:  started,
		FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
		StatsJSON:  string(statsJSON),
	}
	if cancelled && cancelErr != nil {
		runRecord.ErrorText = cancelErr.Error()
	} else if status == "error" && len(failures) > 0 {
		runRecord.ErrorText = failures[0].Message
	}
	recordCtx := ctx
	if cancelled {
		var cancelRecord context.CancelFunc
		recordCtx, cancelRecord = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelRecord()
	}
	runID, recordErr := rt.Store.RecordRun(recordCtx, runRecord)
	if recordErr != nil && !cancelled {
		return embedResult{}, recordErr
	}
	result.RunID = runID

	if cancelled {
		return result, cancelErr
	}
	if status == "error" {
		return result, fmt.Errorf("openai embeddings failed: %s", failures[0].Message)
	}
	return result, nil
}

func summarizeEmbedErr(err error) string {
	if apiErr := openai.AsAPIError(err); apiErr != nil {
		parts := []string{fmt.Sprintf("status=%d", apiErr.Status)}
		if apiErr.Type != "" {
			parts = append(parts, "type="+apiErr.Type)
		}
		if apiErr.Code != "" {
			parts = append(parts, "code="+apiErr.Code)
		}
		return strings.Join(parts, " ")
	}
	return err.Error()
}

func makeEmbedFailureStat(start, end, attempts int, err error) embedFailureStat {
	stat := embedFailureStat{
		BatchStart: start,
		BatchEnd:   end,
		Attempts:   attempts,
		Message:    err.Error(),
	}
	if apiErr := openai.AsAPIError(err); apiErr != nil {
		stat.Status = apiErr.Status
		stat.Type = apiErr.Type
		stat.Code = apiErr.Code
		if apiErr.Message != "" {
			stat.Message = apiErr.Message
		}
	}
	return stat
}

func embedRetryOverride() *openai.RetryConfig {
	if strings.TrimSpace(os.Getenv("GITCRAWL_OPENAI_RETRY_DISABLED")) == "1" {
		cfg := openai.NoRetry()
		return &cfg
	}
	return nil
}

func truncatedEmbeddingTaskCount(tasks []store.EmbeddingTask) int {
	count := 0
	for _, task := range tasks {
		if task.TextTruncated {
			count++
		}
	}
	return count
}
