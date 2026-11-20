package acquireapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
)

// These DTOs match the PostgreSQL acquisition owner. RawJSON is deliberately a
// string: RawMessage would compact the original event before receipt hashing.
type LifecycleEvent struct {
	RawJSON string `json:"rawJson"`
}
type LifecyclePage struct {
	ThreadID   string           `json:"threadId"`
	Cursor     string           `json:"cursor"`
	NextCursor string           `json:"nextCursor"`
	Terminal   bool             `json:"terminal"`
	Events     []LifecycleEvent `json:"events"`
}
type LifecycleThread struct {
	ID             string `json:"id"`
	ParentSequence string `json:"parentSequence"`
	ParentOrdinal  int    `json:"parentOrdinal"`
	NodeID         string `json:"nodeId"`
}
type LifecycleScope struct {
	Schema           string            `json:"schema"`
	Since            time.Time         `json:"since"`
	Threads          []LifecycleThread `json:"threads"`
	ParentCheckpoint json.RawMessage   `json:"parentCheckpoint"`
}
type lifecycleCheckpoint struct {
	Schema      string `json:"schema"`
	ThreadIndex int    `json:"threadIndex"`
	NextCursor  string `json:"nextCursor"`
}
type LifecycleParent struct {
	LifecycleThread
	Number int
	Kind   string
}
type LifecycleAPI interface {
	State(context.Context) (State, error)
	Begin(context.Context, RunRequest) error
	Commit(context.Context, PageRequest) (Receipt, error)
	LifecycleParents(context.Context, string, []int) ([]LifecycleParent, error)
}
type LifecycleProvider interface {
	ReadLifecycle(context.Context, github.LifecycleQuery, github.Reporter) (github.LifecyclePage, error)
}
type LifecycleOptions struct {
	Repository string
	Numbers    []int
	Since      time.Time // Required for a new run; a resume uses its original lower bound.
	MaxPages   int
}

func lifecycleNumbers(numbers []int) ([]int, error) {
	if len(numbers) < 1 || len(numbers) > 500 {
		return nil, errors.New("lifecycle selection requires 1..500 numbers")
	}
	out := slices.Clone(numbers)
	slices.Sort(out)
	for i, n := range out {
		if n < 1 || i > 0 && n == out[i-1] {
			return nil, errors.New("invalid or duplicate lifecycle number")
		}
	}
	return out, nil
}

// LifecycleParents projects small coordinates from full, validated native rows.
// Every page is pinned to one completed snapshot, never a whole-table scan.
func (c *Client) LifecycleParents(ctx context.Context, repository string, numbers []int) ([]LifecycleParent, error) {
	numbers, err := lifecycleNumbers(numbers)
	if err != nil {
		return nil, err
	}
	meta, err := c.reader.Query(ctx, "gitcrawl", c.source, remote.QueryRequest{Name: "gitcrawl.repositories.current", Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(meta.Rows) != 1 || len(meta.Rows[0]) != len(meta.Columns) || meta.Stats.SnapshotID == "" || meta.Cursor != "" || meta.Stats.NextCursor != "" {
		return nil, errors.New("invalid lifecycle repository snapshot")
	}
	fields := map[string]any{}
	for i, k := range meta.Columns {
		fields[k] = meta.Rows[0][i]
	}
	name, ok := fields["full_name"].(string)
	repoID, idOK := fields["id"].(string)
	if !ok || !idOK || !strings.EqualFold(name, repository) {
		return nil, errors.New("lifecycle source repository mismatch")
	}
	if _, err = positiveID(repoID); err != nil {
		return nil, err
	}
	wanted := map[int]bool{}
	selected := make([]string, len(numbers))
	for i, n := range numbers {
		selected[i] = strconv.Itoa(n)
		wanted[n] = true
	}
	query := remote.QueryRequest{Name: "gitcrawl.threads.list", SnapshotID: meta.Stats.SnapshotID, Limit: len(numbers), Args: map[string]any{"repository": name, "numbers": selected, "includeClosed": true}}
	byNumber := map[int]LifecycleParent{}
	seenIDs := map[string]bool{}
	seenCursors := map[string]bool{}
	for {
		result, err := c.reader.Query(ctx, "gitcrawl", c.source, query)
		if err != nil {
			return nil, err
		}
		if result.Stats.SnapshotID != meta.Stats.SnapshotID || len(result.Rows) > len(numbers) {
			return nil, errors.New("lifecycle parent snapshot changed")
		}
		for _, row := range result.Rows {
			if len(row) != len(result.Columns) {
				return nil, errors.New("invalid lifecycle parent row")
			}
			values := map[string]any{}
			for i, k := range result.Columns {
				values[k] = row[i]
			}
			id, idOK := values["id"].(string)
			number, numOK := values["number"].(string)
			n, err := strconv.Atoi(number)
			native, parseErr := positiveID(id)
			if !idOK || !numOK || err != nil || parseErr != nil || !wanted[n] {
				return nil, errors.New("unexpected lifecycle parent")
			}
			if _, exists := byNumber[n]; exists || seenIDs[id] {
				return nil, errors.New("duplicate lifecycle parent")
			}
			one := result
			one.Rows = [][]any{row}
			one.Cursor = ""
			one.Stats.NextCursor = ""
			if _, err = remotedelivery.AcquisitionParent(one, native); err != nil {
				return nil, err
			}
			kind, _ := values["kind"].(string)
			raw, _ := values["raw_json"].(string)
			sequence, _ := values["sequence"].(string)
			ordinal, _ := values["ordinal"].(string)
			parentRepo, _ := values["repo_id"].(string)
			ord, ordErr := strconv.Atoi(ordinal)
			var parent struct {
				NodeID string `json:"node_id"`
			}
			if parentRepo != repoID || (kind != "issue" && kind != "pull_request") || json.Unmarshal([]byte(raw), &parent) != nil || parent.NodeID == "" || len(parent.NodeID) > 512 || strings.ContainsAny(parent.NodeID, "\x00\r\n") || ordErr != nil || ord < 0 || ord > 499 || strconv.Itoa(ord) != ordinal {
				return nil, errors.New("invalid lifecycle parent coordinates")
			}
			if _, err = positiveID(sequence); err != nil {
				return nil, err
			}
			seenIDs[id] = true
			byNumber[n] = LifecycleParent{LifecycleThread: LifecycleThread{ID: id, ParentSequence: sequence, ParentOrdinal: ord, NodeID: parent.NodeID}, Number: n, Kind: kind}
		}
		if result.Cursor != "" && result.Stats.NextCursor != "" && result.Cursor != result.Stats.NextCursor {
			return nil, errors.New("conflicting lifecycle parent cursors")
		}
		next := result.Stats.NextCursor
		if next == "" {
			next = result.Cursor
		}
		if next == "" {
			break
		}
		if seenCursors[next] || len(result.Rows) == 0 || len(byNumber) >= len(numbers) {
			return nil, errors.New("invalid lifecycle parent pagination")
		}
		seenCursors[next] = true
		query.Cursor = next
	}
	if len(byNumber) != len(numbers) {
		return nil, errors.New("selected lifecycle parent missing; hydrate it before collection")
	}
	out := make([]LifecycleParent, len(numbers))
	for i, n := range numbers {
		out[i] = byNumber[n]
	}
	return out, nil
}

// RunLifecycle only records selected parent timelines. It cannot advance the
// source horizon or claim global closure/merge coverage.
func RunLifecycle(ctx context.Context, api LifecycleAPI, provider LifecycleProvider, o LifecycleOptions) (Result, error) {
	var result Result
	parts := strings.Split(o.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || o.MaxPages < 1 || o.MaxPages > 10000 {
		return result, errors.New("invalid bounded lifecycle options")
	}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("lifecycle acquisition requires a deadline")
	}
	numbers, err := lifecycleNumbers(o.Numbers)
	if err != nil {
		return result, err
	}
	state, err := api.State(ctx)
	if err != nil {
		return result, err
	}
	if state.ActiveRun != nil && state.ActiveRun.Mode != "lifecycle" {
		return result, errors.New("another acquisition mode is active")
	}
	if state.CompletedThrough == nil {
		return result, errors.New("lifecycle requires an established acquisition horizon")
	}
	parents, err := api.LifecycleParents(ctx, o.Repository, numbers)
	if err != nil {
		return result, err
	}
	scope := LifecycleScope{Schema: "github-lifecycle-scope/v1", Since: o.Since.UTC(), Threads: make([]LifecycleThread, len(parents))}
	for i, p := range parents {
		scope.Threads[i] = p.LifecycleThread
	}
	run := state.ActiveRun
	if run == nil {
		scope.ParentCheckpoint, err = parentProgressCheckpoint(state, o.Repository)
		if err != nil {
			return result, err
		}
	}
	if run == nil && state.CompletedRun == nil {
		var prior lifecycleCheckpoint
		if json.Unmarshal(state.Checkpoint, &prior) == nil && prior.Schema == "github-lifecycle-checkpoint/v1" {
			return result, errors.New("completed lifecycle metadata unavailable; reconcile the previous run before collection")
		}
	}
	if run == nil && state.CompletedRun != nil && state.CompletedRun.Mode == "lifecycle" {
		var completed LifecycleScope
		if json.Unmarshal(state.CompletedRun.Scope, &completed) != nil || completed.Schema != scope.Schema {
			return result, errors.New("invalid completed lifecycle scope")
		}
		if slices.Equal(completed.Threads, scope.Threads) && state.CompletedRun.Horizon.Equal(*state.CompletedThrough) && (o.Since.IsZero() || completed.Since.Equal(scope.Since)) {
			var checkpoint lifecycleCheckpoint
			if json.Unmarshal(state.Checkpoint, &checkpoint) != nil || checkpoint.Schema != "github-lifecycle-checkpoint/v1" || checkpoint.ThreadIndex != len(parents) || checkpoint.NextCursor != "" || state.Revision <= state.CompletedRun.ExpectedRevision {
				return result, errors.New("completed lifecycle checkpoint mismatch")
			}
			return Result{RunID: state.CompletedRun.ID, Revision: state.Revision, Complete: true}, nil
		}
	}
	if run == nil {
		if o.Since.IsZero() || !scope.Since.Before(*state.CompletedThrough) || scope.Since.Nanosecond()%1000 != 0 {
			return result, errors.New("new lifecycle run requires a prior UTC --since")
		}
		raw, _ := json.Marshal(scope)
		if len(raw) > 65536 {
			return result, errors.New("lifecycle scope exceeds 64 KiB")
		}
		sum := sha256.Sum256(append(raw, []byte(fmt.Sprintf("/%d/%s", state.Revision, state.CompletedThrough.Format(time.RFC3339Nano)))...))
		run = &RunRequest{ID: "gitcrawl-lifecycle-" + hex.EncodeToString(sum[:16]), Mode: "lifecycle", ExpectedRevision: state.Revision, Horizon: *state.CompletedThrough, Scope: raw}
		if err = api.Begin(ctx, *run); err != nil {
			return result, err
		}
	} else {
		var original LifecycleScope
		if json.Unmarshal(run.Scope, &original) != nil || original.Schema != scope.Schema || !slices.Equal(original.Threads, scope.Threads) || !run.Horizon.Equal(*state.CompletedThrough) || (!o.Since.IsZero() && !original.Since.Equal(scope.Since)) {
			return result, errors.New("resume requires original lifecycle selection, window and parent versions")
		}
		if err := validateParentProgress(original.ParentCheckpoint); err != nil {
			return result, err
		}
		scope = original
	}
	cp := lifecycleCheckpoint{Schema: "github-lifecycle-checkpoint/v1"}
	if state.Revision < run.ExpectedRevision {
		return result, errors.New("invalid lifecycle revision")
	}
	if state.Revision > run.ExpectedRevision {
		if json.Unmarshal(state.Checkpoint, &cp) != nil || cp.Schema != "github-lifecycle-checkpoint/v1" || cp.ThreadIndex < 0 || cp.ThreadIndex >= len(parents) {
			return result, errors.New("invalid lifecycle resume checkpoint")
		}
	}
	result.RunID = run.ID
	result.Revision = state.Revision
	for pageNumber := 0; pageNumber < o.MaxPages; pageNumber++ {
		p := parents[cp.ThreadIndex]
		page, err := provider.ReadLifecycle(ctx, github.LifecycleQuery{NodeID: p.NodeID, Kind: p.Kind, Number: p.Number, Since: scope.Since, Cursor: cp.NextCursor}, nil)
		if err != nil {
			return result, err
		}
		events := make([]LifecycleEvent, 0, len(page.Events))
		for _, raw := range page.Events {
			var e struct {
				CreatedAt string `json:"createdAt"`
			}
			if json.Unmarshal(raw, &e) != nil {
				return result, errors.New("invalid lifecycle event JSON")
			}
			at, err := time.Parse(time.RFC3339Nano, e.CreatedAt)
			if err != nil || at.Before(scope.Since) {
				return result, errors.New("invalid lifecycle event window")
			}
			if at.Before(run.Horizon) {
				events = append(events, LifecycleEvent{RawJSON: string(raw)})
			}
		}
		next := lifecycleCheckpoint{Schema: cp.Schema, ThreadIndex: cp.ThreadIndex, NextCursor: page.NextCursor}
		if page.Terminal {
			next.ThreadIndex++
			next.NextCursor = ""
		}
		checkpoint, _ := json.Marshal(next)
		request := PageRequest{RunID: run.ID, ID: fmt.Sprintf("lifecycle-%d", result.Revision), ExpectedRevision: result.Revision, Checkpoint: checkpoint, Changes: []remote.Change{}, Final: next.ThreadIndex == len(parents), Lifecycle: &LifecyclePage{ThreadID: p.ID, Cursor: cp.NextCursor, NextCursor: next.NextCursor, Terminal: page.Terminal, Events: events}}
		receipt, err := api.Commit(ctx, request)
		if err != nil {
			return result, err
		}
		result.Revision = receipt.Revision
		cp = next
		if page.Terminal {
			result.Threads++
		}
		if request.Final {
			result.Complete = true
			return result, nil
		}
	}
	return result, nil
}
