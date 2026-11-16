package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

type inventoryRecord struct {
	id, node, kind, hash string
	updated              time.Time
}

func hydrateStream(ctx context.Context, client *github.OctopoolConversations, st *store.Store, repository string, numbers []int, batchSize, workers int, scope github.InventoryScope, selectedOutput string, reporter github.Reporter, encoder *json.Encoder) error {
	numbers, err := st.FilterExcludedNumbers(ctx, repository, numbers)
	if err != nil {
		return err
	}
	stream, err := client.OpenInventory(ctx, scope, reporter)
	if err != nil {
		return err
	}
	defer stream.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pages := make(chan github.InventoryPage, 1)
	result := make(chan error, 1)
	go func() {
		err := stream.Walk(ctx, reporter, func(page github.InventoryPage) error {
			select {
			case pages <- page:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		result <- err
		close(pages)
	}()
	wanted := map[int]bool{}
	for _, n := range numbers {
		wanted[n] = true
	}
	known := map[int]inventoryRecord{}
	canonicalNumbers := map[string]int{}
	nodeNumbers := map[string]int{}
	owner, repo, _ := strings.Cut(repository, "/")
	process := func(page github.InventoryPage) error {
		completed := map[int]bool{}
		for _, n := range page.Completed {
			completed[n] = true
		}
		rows := map[int]map[string]any{}
		var eligible []int
		for _, row := range page.Rows {
			n64, err := jsonNumber(row["number"])
			if err != nil {
				return err
			}
			n := int(n64)
			record, err := inventoryRow(row)
			if err != nil {
				return err
			}
			if previous, ok := canonicalNumbers[record.id]; ok && previous != n {
				return fmt.Errorf("inventory canonical ID belongs to both #%d and #%d", previous, n)
			}
			if previous, ok := nodeNumbers[record.node]; ok && previous != n {
				return fmt.Errorf("inventory node ID belongs to both #%d and #%d", previous, n)
			}
			canonicalNumbers[record.id], nodeNumbers[record.node] = n, n
			if old, ok := known[n]; ok {
				if old.id != record.id || old.node != record.node || old.kind != record.kind {
					return fmt.Errorf("inventory canonical identity changed for #%d", n)
				}
				if record.updated.Before(old.updated) {
					continue
				}
				if record.updated.Equal(old.updated) && record.hash != old.hash {
					fresh, err := client.Client.GetIssue(ctx, owner, repo, n, reporter)
					if err != nil {
						return err
					}
					current, err := inventoryRow(fresh)
					if err != nil {
						return err
					}
					freshNumber, err := jsonNumber(fresh["number"])
					if err != nil {
						return err
					}
					if int(freshNumber) != n || current.id != old.id || current.node != old.node || current.kind != old.kind || current.updated.Before(old.updated) || !strings.EqualFold(fmt.Sprint(fresh["repository_url"]), "https://api.github.com/repos/"+repository) {
						return fmt.Errorf("conflicting inventory identity for #%d", n)
					}
					row, record = fresh, current
				}
			}
			known[n] = record
			if (scope.IncludeDiscovered || wanted[n]) && !completed[n] {
				if rows[n] == nil {
					eligible = append(eligible, n)
				}
				rows[n] = row
			}
		}
		eligible, err = st.FilterExcludedNumbers(ctx, repository, eligible)
		if err != nil {
			return err
		}
		windowSize := batchSize * workers
		for start := 0; start < len(eligible); start += windowSize {
			end := min(start+windowSize, len(eligible))
			var batches [][]map[string]any
			for offset := start; offset < end; offset += batchSize {
				var batch []map[string]any
				for _, n := range eligible[offset:min(offset+batchSize, end)] {
					batch = append(batch, rows[n])
				}
				batches = append(batches, batch)
			}
			prepared, err := prepareRowWindow(ctx, client, batches, reporter)
			if err != nil {
				return err
			}
			for index, batch := range batches {
				var selected []int
				for _, row := range batch {
					n, _ := jsonNumber(row["number"])
					selected = append(selected, int(n))
				}
				stats, err := syncer.New(prepared[index], st).Sync(ctx, syncer.Options{Octopool: true, Owner: owner, Repo: repo, State: "all", Numbers: selected, IncludeComments: true, Force: true, Reporter: reporter})
				if err != nil {
					return err
				}
				if err = writeBatchComplete(encoder, stats); err != nil {
					return err
				}
				if err = stream.Complete(ctx, page.Index, stats.Numbers); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err = encoder.Encode(map[string]any{"event": "inventory_started", "started_at": stream.StartedAt().Format(time.RFC3339Nano), "complete": false, "streaming": true}); err != nil {
		cancel()
		<-result
		return err
	}
	for page := range pages {
		if err = process(page); err != nil {
			cancel()
			<-result
			return err
		}
	}
	if err = <-result; err != nil {
		return err
	}
	available := make([]int, 0, len(known))
	for n := range known {
		available = append(available, n)
	}
	slices.Sort(available)
	selected, missing, discovered := selectNumbers(numbers, available, scope.IncludeDiscovered)
	missing, err = st.FilterExcludedNumbers(ctx, repository, missing)
	if err != nil {
		return err
	}
	beforePolicy := len(selected)
	selected, err = st.FilterExcludedNumbers(ctx, repository, selected)
	if err != nil {
		return err
	}
	digest := ""
	if selectedOutput != "" {
		digest, err = writeSelectedNumbers(selectedOutput, selected)
		if err != nil {
			return err
		}
	}
	if err = encoder.Encode(map[string]any{"event": "inventory", "started_at": stream.StartedAt().Format(time.RFC3339Nano), "finished_at": time.Now().UTC().Format(time.RFC3339Nano), "complete": true, "streaming": true, "requested": len(numbers), "available": len(available), "selected": len(selected), "discovered": discovered, "excluded_selected": beforePolicy - len(selected), "missing_requested": len(missing), "selected_numbers_sha256": digest}); err != nil {
		return err
	}
	for _, n := range missing {
		if err = encoder.Encode(map[string]any{"event": "unresolved", "number": n, "reason": "absent_from_current_rest_inventory"}); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("hydrated available records; %d historical numbers remain unresolved", len(missing))
	}
	return nil
}
func jsonNumber(value any) (int64, error) {
	switch n := value.(type) {
	case json.Number:
		return n.Int64()
	case int:
		return int64(n), nil
	}
	return 0, errors.New("invalid inventory number")
}
func inventoryRow(row map[string]any) (inventoryRecord, error) {
	updated, err := time.Parse(time.RFC3339Nano, fmt.Sprint(row["updated_at"]))
	if err != nil {
		return inventoryRecord{}, errors.New("invalid inventory timestamp")
	}
	raw, err := json.Marshal(row)
	if err != nil {
		return inventoryRecord{}, err
	}
	kind := "issue"
	if _, ok := row["pull_request"]; ok {
		kind = "pull_request"
	}
	return inventoryRecord{fmt.Sprint(row["id"]), fmt.Sprint(row["node_id"]), kind, fmt.Sprintf("%x", sha256.Sum256(raw)), updated}, nil
}
func prepareRowWindow(ctx context.Context, client *github.OctopoolConversations, batches [][]map[string]any, reporter github.Reporter) ([]*github.OctopoolConversations, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	prepared := make([]*github.OctopoolConversations, len(batches))
	failures := make([]error, len(batches))
	var workers sync.WaitGroup
	for index, batch := range batches {
		workers.Go(func() {
			prepared[index], failures[index] = client.PrepareRows(ctx, batch, reporter)
			if failures[index] != nil {
				cancel()
			}
		})
	}
	workers.Wait()
	for _, err := range failures {
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, err
		}
	}
	for _, err := range failures {
		if err != nil {
			return nil, err
		}
	}
	return prepared, nil
}
