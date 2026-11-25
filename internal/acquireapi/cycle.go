package acquireapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CycleOptions can include a fixed historical selection. LifecycleNumbers optionally
// restricts scheduled lifecycle turns to an explicitly selected cohort.
// PostgreSQL remains the sole run/checkpoint owner; the scheduler has no cursor.
type CycleOptions struct {
	Conversations     Options
	LifecycleNumbers  []int
	CommitMaxPages    int
	LifecycleMaxPages int
	OneWindow         bool // In a multi-turn batch, yield before another incremental turn.
}

type CycleAPI interface {
	API
	LifecycleAPI
	LifecycleWindowParents(ctx context.Context, repository string, since, until time.Time, sequence int64, after int, pinTag string) ([]LifecycleParent, string, error)
}

type CycleResult struct {
	Result
	Mode             string   `json:"mode"`
	Resumed          bool     `json:"resumed"`
	DeferredModes    []string `json:"deferredModes,omitempty"`
	LifecycleParents int      `json:"lifecycleParents"`
}

type CycleProvider interface {
	CommitProvider
	LifecycleProvider
}

const MaxCycleTurns = 8

// RunCycleTurns reuses one provider session without keeping a scheduler cursor.
// Each turn reads PostgreSQL state again, and an unfinished turn ends the batch.
func RunCycleTurns(ctx context.Context, api CycleAPI, rest Provider, graph CycleProvider, o CycleOptions, limit int) ([]CycleResult, error) {
	if limit < 1 || limit > MaxCycleTurns {
		return nil, fmt.Errorf("cycle turn limit must be 1..%d", MaxCycleTurns)
	}
	turns := make([]CycleResult, 0, limit)
	for range limit {
		turn, err := runCycle(ctx, api, rest, graph, o, o.OneWindow && len(turns) > 0)
		if turn.Mode != "" {
			turns = append(turns, turn)
		}
		if err != nil || !turn.Complete {
			return turns, err
		}
	}
	return turns, nil
}

// RunCycle dispatches one mode. An unfinished run always owns the next turn;
// only durable completion advances the rotation. In particular, another parent
// horizon cannot start ahead of the preceding commit and lifecycle turns.
func RunCycle(ctx context.Context, api CycleAPI, rest Provider, graph CycleProvider, o CycleOptions) (CycleResult, error) {
	return runCycle(ctx, api, rest, graph, o, false)
}

func runCycle(ctx context.Context, api CycleAPI, rest Provider, graph CycleProvider, o CycleOptions, yieldBeforeIncremental bool) (CycleResult, error) {
	var out CycleResult
	if _, ok := ctx.Deadline(); !ok || o.CommitMaxPages < 1 || o.CommitMaxPages > 10000 || o.LifecycleMaxPages < 1 || o.LifecycleMaxPages > 10000 {
		return out, errors.New("cycle requires bounded pages and a deadline")
	}
	if o.Conversations.Since.Nanosecond()%1000 != 0 {
		return out, errors.New("new cycle lower bound requires microsecond precision")
	}
	if len(o.Conversations.Numbers) > 0 {
		if _, err := selectionHash(o.Conversations.Numbers); err != nil {
			return out, err
		}
	}
	if len(o.LifecycleNumbers) > 0 {
		if _, err := lifecycleNumbers(o.LifecycleNumbers); err != nil {
			return out, err
		}
	}
	state, err := api.State(ctx)
	if err != nil {
		return out, err
	}
	mode, since, err := nextCycleMode(state, o.Conversations)
	if err != nil {
		return out, err
	}
	var selection cycleLifecycle
	if mode == "lifecycle" {
		selection, err = selectCycleLifecycle(ctx, api, state, o, since)
		if err != nil {
			return out, err
		}
		if len(selection.parents) == 0 {
			mode, _, err = afterLifecycle(state, o.Conversations)
			if err != nil {
				return out, err
			}
		}
	}
	if mode == "incremental" && yieldBeforeIncremental {
		return out, nil
	}
	out.Mode, out.Resumed, out.LifecycleParents = mode, state.ActiveRun != nil, len(o.LifecycleNumbers)
	switch mode {
	case "incremental", "historical":
		options := o.Conversations
		if mode == "incremental" {
			options.Numbers = nil
		} else if len(options.Numbers) == 0 {
			return out, errors.New("resume the active historical run with its original numbers-file")
		}
		out.Result, err = Run(ctx, api, rest, options)
	case "commits":
		out.Result, err = RunCommits(ctx, api, graph, CommitOptions{Repository: o.Conversations.Repository, Since: since, MaxPages: o.CommitMaxPages})
	case "lifecycle":
		out.LifecycleParents = len(selection.parents)
		options := LifecycleOptions{Repository: o.Conversations.Repository, Since: selection.since, MaxPages: o.LifecycleMaxPages}
		out.Result, err = runLifecycle(ctx, api, graph, options, state, selection.parents, selection.prefix)
	}
	if !out.Complete {
		for _, other := range []string{"incremental", "commits", "lifecycle", "historical"} {
			if other != mode && (other != "historical" || len(o.Conversations.Numbers) > 0) {
				out.DeferredModes = append(out.DeferredModes, other)
			}
		}
	}
	return out, err
}

func nextCycleMode(state State, o Options) (string, time.Time, error) {
	var zero time.Time
	run := state.ActiveRun
	active := run != nil
	if run == nil {
		run = state.CompletedRun
	}
	if run == nil {
		if state.CompletedThrough != nil {
			return "", zero, errors.New("cycle requires the preceding completed run metadata")
		}
		return "incremental", zero, nil
	}
	mode := run.Mode
	var parent scope
	if mode == "" {
		if json.Unmarshal(run.Scope, &parent) != nil || parent.Version != 1 || parent.Repository != o.Repository {
			return "", zero, errors.New("cycle parent scope mismatch")
		}
		mode = parent.Mode
	}
	if active {
		if mode != "incremental" && mode != "historical" && mode != "commits" && mode != "lifecycle" {
			return "", zero, fmt.Errorf("active acquisition mode %q (run %q) is outside the admitted cycle; preserve that owner", mode, run.ID)
		}
		// Native mode owners validate the original selection and resume bounds.
		return mode, zero, nil
	}
	if state.CompletedThrough == nil || !run.Horizon.Equal(*state.CompletedThrough) {
		return "", zero, errors.New("cycle completed horizon mismatch")
	}
	switch mode {
	case "incremental":
		since, err := time.Parse(time.RFC3339Nano, parent.Since)
		if err != nil || since.After(run.Horizon) {
			return "", zero, fmt.Errorf("completed incremental run %q has an invalid lower bound", run.ID)
		}
		// Older parent runs accepted nanosecond bounds. Round down so the
		// supplemental window includes that entire interval without a gap.
		// An empty legacy window gets one microsecond of overlap, not a wedge.
		since = since.Truncate(time.Microsecond)
		if since.Equal(run.Horizon) {
			since = since.Add(-time.Microsecond)
		}
		return "commits", since, nil
	case "commits":
		completed, ok, err := completedCommitScope(state, o.Repository)
		if err != nil || !ok {
			return "", zero, errors.New("cycle completed commit proof unavailable")
		}
		return "lifecycle", completed.Since, nil
	case "lifecycle":
		if strings.HasPrefix(run.ID, lifecycleWindowPrefix) {
			return "lifecycle", zero, nil
		}
		return afterLifecycle(state, o)
	case "historical", "historical-parents":
		return "incremental", zero, nil
	default:
		return "", zero, errors.New("completed mode is outside the admitted cycle")
	}
}

func afterLifecycle(state State, o Options) (string, time.Time, error) {
	var zero time.Time
	raw, err := parentProgressCheckpoint(state, o.Repository)
	if err != nil {
		return "", zero, err
	}
	var cp checkpoint
	if json.Unmarshal(raw, &cp) != nil {
		return "", zero, errors.New("cycle historical progress unavailable")
	}
	if len(o.Numbers) == 0 {
		if cp.History.SHA != "" || cp.History.Offset != 0 {
			return "", zero, errors.New("cycle requires the original numbers-file for retained historical progress")
		}
		return "incremental", zero, nil
	}
	sha, err := selectionHash(o.Numbers)
	if err != nil {
		return "", zero, err
	}
	if cp.History.SHA != "" && cp.History.SHA != sha {
		return "", zero, errors.New("cycle historical selection changed; explicit admission required")
	}
	if cp.History.Offset < 0 || cp.History.Offset > len(o.Numbers) {
		return "", zero, errors.New("cycle historical offset outside selection")
	}
	if cp.History.Offset < len(o.Numbers) {
		return "historical", zero, nil
	}
	return "incremental", zero, nil
}
