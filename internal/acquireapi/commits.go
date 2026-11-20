package acquireapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
)

type CommitScope struct {
	Schema           string          `json:"schema"`
	Repository       string          `json:"repository"`
	RepositoryNodeID string          `json:"repositoryNodeId"`
	Branch           string          `json:"branch"`
	HeadOID          string          `json:"headOid"`
	Since            time.Time       `json:"since"`
	ParentCheckpoint json.RawMessage `json:"parentCheckpoint"`
}
type CommitAttributionPage struct {
	Cursor     string `json:"cursor"`
	NextCursor string `json:"nextCursor"`
	Terminal   bool   `json:"terminal"`
	RawJSON    string `json:"rawJson"`
}
type CommitPage struct {
	Cursor       string                  `json:"cursor"`
	NextCursor   string                  `json:"nextCursor"`
	Terminal     bool                    `json:"terminal"`
	RawJSON      string                  `json:"rawJson"`
	Attributions []CommitAttributionPage `json:"attributions"`
}
type commitCheckpoint struct {
	Schema     string `json:"schema"`
	NextCursor string `json:"nextCursor"`
	Complete   bool   `json:"complete"`
}
type CommitProvider interface {
	ReadCommitHead(context.Context, github.Reporter) (github.CommitHead, error)
	ReadCommit(context.Context, github.CommitQuery, github.Reporter) (github.CommitRead, error)
	ReadCommitAttribution(context.Context, string, string, string, github.Reporter) (github.CommitAttributionRead, error)
}
type CommitAPI interface {
	State(context.Context) (State, error)
	Begin(context.Context, RunRequest) error
	Commit(context.Context, PageRequest) (Receipt, error)
}
type CommitOptions struct {
	Repository string
	Since      time.Time
	MaxPages   int
}

// RunCommits records a bounded pinned-branch scan. It does not certify general
// source coverage, advance the parent horizon, or infer missing merger credit.
func RunCommits(ctx context.Context, api CommitAPI, provider CommitProvider, o CommitOptions) (Result, error) {
	var result Result
	if _, ok := ctx.Deadline(); !ok || o.MaxPages < 1 || o.MaxPages > 10000 || len(strings.Split(o.Repository, "/")) != 2 {
		return result, errors.New("invalid bounded commit options")
	}
	state, err := api.State(ctx)
	if err != nil {
		return result, err
	}
	if state.CompletedThrough == nil {
		return result, errors.New("commits require an established acquisition horizon")
	}
	if state.ActiveRun != nil && state.ActiveRun.Mode != "commits" {
		return result, errors.New("another acquisition mode is active")
	}
	run := state.ActiveRun
	var scope CommitScope
	var head github.CommitHead
	if run != nil {
		if json.Unmarshal(run.Scope, &scope) != nil || scope.Schema != "github-commits-scope/v1" || scope.Repository != o.Repository || !run.Horizon.Equal(*state.CompletedThrough) || !o.Since.IsZero() && !scope.Since.Equal(o.Since) {
			return result, errors.New("commit resume scope changed")
		}
		if err := validateParentProgress(scope.ParentCheckpoint); err != nil {
			return result, err
		}
	} else {
		completed, hasCompleted, e := completedCommitScope(state, o.Repository)
		if e != nil {
			return result, e
		}
		// Omitting --since means recovery of the exact completed run, never a
		// new scan against a branch that may have advanced after the lost ACK.
		if hasCompleted && o.Since.IsZero() {
			return Result{RunID: state.CompletedRun.ID, Revision: state.Revision, Complete: true}, nil
		}
		if o.Since.IsZero() || !o.Since.Before(*state.CompletedThrough) || o.Since.Nanosecond()%1000 != 0 {
			return result, errors.New("commit scan requires prior --since")
		}
		parent, e := parentProgressCheckpoint(state, o.Repository)
		if e != nil {
			return result, e
		}
		head, e = provider.ReadCommitHead(ctx, nil)
		if e != nil {
			return result, e
		}
		if head.Repository != o.Repository {
			return result, errors.New("commit repository changed")
		}
		scope = CommitScope{Schema: "github-commits-scope/v1", Repository: o.Repository, RepositoryNodeID: head.NodeID, Branch: head.Branch, HeadOID: head.OID, Since: o.Since.UTC(), ParentCheckpoint: parent}
		if hasCompleted && completed.RepositoryNodeID == scope.RepositoryNodeID && completed.Branch == scope.Branch && completed.HeadOID == scope.HeadOID && completed.Since.Equal(scope.Since) {
			return Result{RunID: state.CompletedRun.ID, Revision: state.Revision, Complete: true}, nil
		}
		raw, _ := json.Marshal(scope)
		sum := sha256.Sum256(append(raw, []byte(fmt.Sprintf("/%d/%s", state.Revision, state.CompletedThrough.Format(time.RFC3339Nano)))...))
		run = &RunRequest{ID: "gitcrawl-commits-" + hex.EncodeToString(sum[:16]), Mode: "commits", ExpectedRevision: state.Revision, Horizon: *state.CompletedThrough, Scope: raw}
		if err = api.Begin(ctx, *run); err != nil {
			return result, err
		}
	}
	// Recheck only immutable repository identity on resume. The branch may have
	// advanced; traversal remains anchored to the recorded OID.
	if head.NodeID == "" {
		head, err = provider.ReadCommitHead(ctx, nil)
		if err != nil {
			return result, err
		}
	}
	if head.Repository != scope.Repository || head.NodeID != scope.RepositoryNodeID {
		return result, errors.New("commit repository identity changed")
	}
	head.OID = scope.HeadOID
	head.Branch = scope.Branch
	cp := commitCheckpoint{Schema: "github-commits-checkpoint/v1"}
	if state.Revision > run.ExpectedRevision {
		if json.Unmarshal(state.Checkpoint, &cp) != nil || cp.Schema != "github-commits-checkpoint/v1" || cp.Complete {
			return result, errors.New("invalid commit checkpoint")
		}
	}
	result.RunID = run.ID
	result.Revision = state.Revision
	for i := 0; i < o.MaxPages; i++ {
		page, e := provider.ReadCommit(ctx, github.CommitQuery{Head: head, Since: scope.Since, Until: run.Horizon, Cursor: cp.NextCursor}, nil)
		if e != nil {
			return result, e
		}
		next := commitCheckpoint{Schema: cp.Schema, NextCursor: page.NextCursor, Complete: page.Terminal}
		rawCP, _ := json.Marshal(next)
		payload := &CommitPage{Cursor: cp.NextCursor, NextCursor: page.NextCursor, Terminal: page.Terminal, RawJSON: string(page.RawJSON), Attributions: []CommitAttributionPage{}}
		if len(page.RawJSON) > 0 {
			var node struct{ ID, OID string }
			if json.Unmarshal(page.RawJSON, &node) != nil || node.ID == "" || node.OID == "" {
				return result, errors.New("invalid commit")
			}
			cursor := ""
			seen := map[string]bool{}
			size := len(page.RawJSON)
			for n := 0; ; n++ {
				if n >= 100 {
					return result, errors.New("commit attribution exceeds 100 pages")
				}
				a, e := provider.ReadCommitAttribution(ctx, node.ID, node.OID, cursor, nil)
				if e != nil {
					return result, e
				}
				size += len(a.RawJSON)
				if size > MaxPageBytes {
					return result, errors.New("commit attribution exceeds 4 MiB; no progress recorded")
				}
				payload.Attributions = append(payload.Attributions, CommitAttributionPage{Cursor: cursor, NextCursor: a.NextCursor, Terminal: a.Terminal, RawJSON: string(a.RawJSON)})
				if a.Terminal {
					break
				}
				if a.NextCursor == "" || seen[a.NextCursor] || a.NextCursor == cursor {
					return result, errors.New("commit attribution cursor repeated")
				}
				seen[a.NextCursor] = true
				cursor = a.NextCursor
			}
		}
		request := PageRequest{RunID: run.ID, ID: fmt.Sprintf("commits-%d", result.Revision), ExpectedRevision: result.Revision, Checkpoint: rawCP, Changes: []remote.Change{}, Final: page.Terminal, Commits: payload}
		body, _ := json.Marshal(request)
		if len(body) > MaxPageBytes {
			return result, errors.New("encoded commit page exceeds 4 MiB; no progress recorded")
		}
		receipt, e := api.Commit(ctx, request)
		if e != nil {
			return result, e
		}
		result.Revision = receipt.Revision
		cp = next
		if page.Terminal {
			result.Complete = true
			return result, nil
		}
	}
	return result, nil
}

// Completed commit scans retain the preceding parent checkpoint in their
// immutable run scope. A mode switch restores that exact progress, not zero.
func completedCommitScope(state State, repository string) (CommitScope, bool, error) {
	var scope CommitScope
	var cp commitCheckpoint
	isCheckpoint := json.Unmarshal(state.Checkpoint, &cp) == nil && cp.Schema == "github-commits-checkpoint/v1"
	if state.ActiveRun != nil || state.CompletedRun == nil || state.CompletedRun.Mode != "commits" {
		if state.ActiveRun == nil && isCheckpoint {
			return scope, false, errors.New("completed commit metadata unavailable")
		}
		return scope, false, nil
	}
	run := state.CompletedRun
	if !isCheckpoint || !cp.Complete || cp.NextCursor != "" || state.CompletedThrough == nil || !run.Horizon.Equal(*state.CompletedThrough) || state.Revision <= run.ExpectedRevision || json.Unmarshal(run.Scope, &scope) != nil || scope.Schema != "github-commits-scope/v1" || scope.Repository != repository || scope.RepositoryNodeID == "" || scope.HeadOID == "" || scope.Branch == "" || scope.Since.IsZero() || !scope.Since.Before(run.Horizon) {
		return scope, false, errors.New("completed commit checkpoint mismatch")
	}
	if err := validateParentProgress(scope.ParentCheckpoint); err != nil {
		return scope, false, err
	}
	return scope, true, nil
}
func validateParentProgress(raw json.RawMessage) error {
	if len(raw) == 0 {
		return errors.New("parent checkpoint unavailable; reconcile the previous scan")
	}
	var object map[string]json.RawMessage
	if len(raw) > 65536 || json.Unmarshal(raw, &object) != nil || object == nil {
		return errors.New("invalid retained parent checkpoint")
	}
	if len(object) == 0 {
		return nil
	}
	var version int
	if json.Unmarshal(object["version"], &version) != nil || version != 1 {
		return errors.New("unknown retained parent checkpoint; reconcile the previous scan")
	}
	return nil
}
func parentProgressCheckpoint(state State, repository string) (json.RawMessage, error) {
	if state.ActiveRun == nil && state.CompletedRun != nil && state.CompletedRun.Mode == "lifecycle" {
		run := state.CompletedRun
		var scope LifecycleScope
		var cp lifecycleCheckpoint
		if state.CompletedThrough == nil || !run.Horizon.Equal(*state.CompletedThrough) || state.Revision <= run.ExpectedRevision || json.Unmarshal(run.Scope, &scope) != nil || scope.Schema != "github-lifecycle-scope/v1" || len(scope.Threads) == 0 || json.Unmarshal(state.Checkpoint, &cp) != nil || cp.Schema != "github-lifecycle-checkpoint/v1" || cp.ThreadIndex != len(scope.Threads) || cp.NextCursor != "" {
			return nil, errors.New("completed lifecycle checkpoint mismatch")
		}
		if err := validateParentProgress(scope.ParentCheckpoint); err != nil {
			return nil, err
		}
		return scope.ParentCheckpoint, nil
	}
	completed, ok, err := completedCommitScope(state, repository)
	if err != nil {
		return nil, err
	}
	raw := state.Checkpoint
	if ok {
		raw = completed.ParentCheckpoint
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err = validateParentProgress(raw); err != nil {
		return nil, err
	}
	return raw, nil
}
