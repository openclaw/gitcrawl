// gitcrawl-hydrate is an operator-only, bounded native archive repair command.
// It does not change the ordinary sync command or claim a full-sync checkpoint.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	database := flag.String("db", "", "existing staged native SQLite archive")
	repository := flag.String("repo", "", "owner/repository")
	numbersFile := flag.String("numbers-file", "", "file containing issue/PR numbers separated by whitespace or commas")
	batchSize := flag.Int("batch-size", 25, "conversations per request (1..25)")
	flag.Parse()
	if *database == "" || *repository == "" || *numbersFile == "" || *batchSize < 1 || *batchSize > 25 {
		return fmt.Errorf("--db, --repo, --numbers-file and batch-size 1..25 are required")
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
	numbers, err = st.FilterExcludedNumbers(ctx, *repository, numbers)
	if err != nil {
		return err
	}
	if len(numbers) == 0 {
		return fmt.Errorf("all requested numbers are excluded")
	}
	client, err := github.NewOctopoolConversations(os.Getenv("OCTOPOOL_URL"), os.Getenv("OCTOPOOL_POOL"), os.Getenv("OCTOPOOL_TOKEN"), *repository)
	if err != nil {
		return err
	}
	reporter := github.Reporter(func(message string) { fmt.Fprintln(os.Stderr, message) })
	if len(numbers) <= *batchSize {
		err = client.InventoryNumbers(ctx, numbers, reporter)
	} else {
		err = client.Inventory(ctx, reporter)
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	for offset := 0; offset < len(numbers); offset += *batchSize {
		batch := numbers[offset:min(offset+*batchSize, len(numbers))]
		// Recheck owner policy immediately before acquisition. Native sync also
		// rechecks it before persistence, including concurrent policy changes.
		batch, err = st.FilterExcludedNumbers(ctx, *repository, batch)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			continue
		}
		if err := client.Prepare(ctx, batch, reporter); err != nil {
			return err
		}
		stats, err := syncer.New(client, st).Sync(ctx, syncer.Options{Octopool: true, Owner: owner, Repo: repo, State: "all", Numbers: batch, IncludeComments: true, Force: true, Reporter: reporter})
		if err != nil {
			return err
		}
		if err := encoder.Encode(stats); err != nil {
			return err
		}
	}
	return nil
}
