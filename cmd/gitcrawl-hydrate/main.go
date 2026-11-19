// gitcrawl-hydrate is an operator-only, bounded native archive repair command.
// It does not change the ordinary sync command or claim a full-sync checkpoint.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if github.IsExhaustedOctopoolRead(err) {
			os.Exit(75)
		}
		os.Exit(1)
	}
}
func run() error {
	database := flag.String("db", "", "existing staged native SQLite archive")
	repository := flag.String("repo", "", "owner/repository")
	numbersFile := flag.String("numbers-file", "", "file containing issue/PR numbers separated by whitespace or commas")
	batchSize := flag.Int("batch-size", github.MaxConversationBatch, "conversations per request (1..20)")
	checkpointPath := flag.String("inventory-checkpoint", "", "private SQLite page and completion checkpoint for full inventory")
	selectedOutput := flag.String("selected-numbers-output", "", "write the final selected roster to a new private file")
	includeDiscovered := flag.Bool("include-discovered", false, "include every current REST inventory number alongside requested historical numbers")
	fetchWorkers := flag.Int("fetch-workers", 1, "concurrent acquisition batches (1..8); native writes remain serial")
	flag.Parse()
	if *database == "" || *repository == "" || *numbersFile == "" || *batchSize < 1 || *batchSize > github.MaxConversationBatch || *fetchWorkers < 1 || *fetchWorkers > 8 {
		return fmt.Errorf("--db, --repo, --numbers-file and batch-size 1..20 and fetch-workers 1..8 are required")
	}
	if *selectedOutput != "" {
		if _, err := os.Lstat(*selectedOutput); err == nil {
			return fmt.Errorf("selected-numbers-output already exists")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if _, err := os.Stat(*database); err != nil {
		return err
	}
	owner, repo, ok := strings.Cut(*repository, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return fmt.Errorf("expected owner/repository")
	}
	content, err := os.ReadFile(*numbersFile)
	if err != nil {
		return err
	}
	var numbers []int
	seen := map[int]bool{}
	for _, field := range strings.FieldsFunc(string(content), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' || r == '\r' }) {
		n, err := strconv.Atoi(field)
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid number %q", field)
		}
		if seen[n] {
			return fmt.Errorf("duplicate number %d", n)
		}
		seen[n] = true
		numbers = append(numbers, n)
	}
	if len(numbers) == 0 {
		return fmt.Errorf("numbers file is empty")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	st, err := store.OpenWritableWithoutMigration(ctx, *database)
	if err != nil {
		return err
	}
	defer st.Close()
	originalNumbers := slices.Clone(numbers)
	requestedCount := len(numbers)
	numbers, err = st.FilterExcludedNumbers(ctx, *repository, numbers)
	if err != nil {
		return err
	}
	if len(numbers) == 0 && !*includeDiscovered {
		return fmt.Errorf("all requested numbers are excluded")
	}
	client, err := github.NewOctopoolConversations(os.Getenv("OCTOPOOL_URL"), os.Getenv("OCTOPOOL_POOL"), os.Getenv("OCTOPOOL_TOKEN"), *repository)
	if err != nil {
		return err
	}
	reporter := github.Reporter(func(message string) { fmt.Fprintln(os.Stderr, message) })
	if *checkpointPath != "" || *includeDiscovered || len(originalNumbers) > 25 || len(originalNumbers) > *batchSize**fetchWorkers {
		native, err := store.NativeReportFileIdentity(*database)
		if err != nil {
			return err
		}
		identity, err := json.Marshal(native)
		if err != nil {
			return err
		}
		sorted := slices.Clone(originalNumbers)
		slices.Sort(sorted)
		input, _ := json.Marshal(sorted)
		return hydrateStream(ctx, client, st, *repository, originalNumbers, *batchSize, *fetchWorkers, github.InventoryScope{Checkpoint: *checkpointPath, NativeIdentity: string(identity), SelectionSHA256: fmt.Sprintf("%x", sha256.Sum256(input)), IncludeDiscovered: *includeDiscovered, StartedAt: time.Now().UTC()}, *selectedOutput, reporter, json.NewEncoder(os.Stdout))
	}
	inventoryStarted := time.Now().UTC()
	err = client.InventoryNumbers(ctx, numbers, reporter)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	available := client.IssueNumbers()
	selected, missing, discovered := selectNumbers(numbers, available, *includeDiscovered)
	if len(missing) > 0 && !*includeDiscovered {
		return fmt.Errorf("%d requested numbers absent from REST inventory, first #%d", len(missing), missing[0])
	}
	selectedBeforePolicy := len(selected)
	selected, err = st.FilterExcludedNumbers(ctx, *repository, selected)
	if err != nil {
		return err
	}
	selectionDigest := ""
	if *selectedOutput != "" {
		selectionDigest, err = writeSelectedNumbers(*selectedOutput, selected)
		if err != nil {
			return err
		}
	}
	if err = encoder.Encode(map[string]any{"event": "inventory", "started_at": inventoryStarted.Format(time.RFC3339Nano), "finished_at": time.Now().UTC().Format(time.RFC3339Nano), "requested": requestedCount, "excluded_requested": requestedCount - len(numbers), "excluded_selected": selectedBeforePolicy - len(selected), "available": len(available), "selected": len(selected), "selected_numbers_sha256": selectionDigest, "discovered": discovered, "missing_requested": len(missing), "include_discovered": *includeDiscovered}); err != nil {
		return err
	}
	for _, number := range missing {
		if err = encoder.Encode(map[string]any{"event": "unresolved", "number": number, "reason": "absent_from_current_rest_inventory"}); err != nil {
			return err
		}
	}
	windowSize := *batchSize * *fetchWorkers
	for offset := 0; offset < len(selected); offset += windowSize {
		end := min(offset+windowSize, len(selected))
		var batches [][]int
		for start := offset; start < end; start += *batchSize {
			batch, err := st.FilterExcludedNumbers(ctx, *repository, selected[start:min(start+*batchSize, end)])
			if err != nil {
				return err
			}
			if len(batch) > 0 {
				batches = append(batches, batch)
			}
		}
		prepared, err := prepareWindow(ctx, client, batches, reporter)
		if err != nil {
			return err
		}
		for index, batch := range batches {
			stats, err := syncer.New(prepared[index], st).Sync(ctx, syncer.Options{Octopool: true, Owner: owner, Repo: repo, State: "all", Numbers: batch, IncludeComments: true, Force: true, Reporter: reporter})
			if err != nil {
				return err
			}
			if err := writeBatchComplete(encoder, stats); err != nil {
				return err
			}
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("hydrated available records; %d historical numbers remain unresolved", len(missing))
	}
	return nil
}

func selectNumbers(requested, available []int, includeDiscovered bool) (selected, missing []int, discovered int) {
	known := make(map[int]bool, len(available))
	for _, number := range available {
		known[number] = true
	}
	wanted := make(map[int]bool, len(requested))
	for _, number := range requested {
		wanted[number] = true
		if known[number] {
			selected = append(selected, number)
		} else {
			missing = append(missing, number)
		}
	}
	if includeDiscovered {
		for _, number := range available {
			if !wanted[number] {
				selected = append(selected, number)
				discovered++
			}
		}
	}
	slices.Sort(selected)
	slices.Sort(missing)
	return
}

func prepareWindow(parent context.Context, client *github.OctopoolConversations, batches [][]int, reporter github.Reporter) ([]*github.OctopoolConversations, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	prepared := make([]*github.OctopoolConversations, len(batches))
	failures := make([]error, len(batches))
	var workers sync.WaitGroup
	for index, batch := range batches {
		workers.Go(func() {
			prepared[index], failures[index] = client.PrepareBatch(ctx, batch, reporter)
			if failures[index] != nil {
				cancel()
			}
		})
	}
	workers.Wait()
	for index, err := range failures {
		if err != nil {
			failures[index] = fmt.Errorf("acquire batch %v: %w", batches[index], err)
		}
	}
	if err := preparationFailure(parent, failures); err != nil {
		return nil, err
	}
	return prepared, nil
}

// A completion receipt is emitted only after native sync settles the whole
// selected batch. Concurrent policy changes leave it retryable rather than
// claiming that excluded keys were persisted.
func writeBatchComplete(encoder *json.Encoder, stats syncer.Stats) error {
	if stats.OwnerExcluded != 0 {
		return fmt.Errorf("batch policy changed; completion receipt withheld for retry")
	}
	return encoder.Encode(struct {
		Event   string       `json:"event"`
		Numbers []int        `json:"numbers"`
		Stats   syncer.Stats `json:"stats"`
	}{"batch_complete", stats.Numbers, stats})
}

// The inventory receipt binds this frozen roster to its acquisition start.
// Exclusive creation prevents a retry from overwriting the original selection.
func writeSelectedNumbers(path string, numbers []int) (digest string, err error) {
	var content strings.Builder
	for _, number := range numbers {
		fmt.Fprintln(&content, number)
	}
	raw := []byte(content.String())
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			file.Close()
			os.Remove(path)
		}
	}()
	if _, err = file.Write(raw); err != nil {
		return "", err
	}
	if err = file.Sync(); err != nil {
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

// Sibling cancellation is a consequence of the first failure, not another
// failure class. External cancellation and every substantive worker error stay.
func preparationFailure(parent context.Context, failures []error) error {
	var substantive []error
	if err := parent.Err(); err != nil {
		substantive = append(substantive, err)
	}
	for _, err := range failures {
		if err != nil && !onlySiblingCancellation(err) {
			substantive = append(substantive, err)
		}
	}
	if len(substantive) > 0 {
		return errors.Join(substantive...)
	}
	return errors.Join(failures...)
}

func onlySiblingCancellation(err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlySiblingCancellation(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlySiblingCancellation(wrapped.Unwrap())
	}
	return err == context.Canceled
}
