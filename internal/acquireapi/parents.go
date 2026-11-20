package acquireapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
)

// ParentSelection is an audited identity list, not provider discovery. A PR's
// backing Issue ID cannot be recovered from its GraphQL PullRequest database ID.
type ParentSelection struct {
	Schema     string           `json:"schema"`
	Repository string           `json:"repository"`
	Parents    []SelectedParent `json:"parents"`
}
type SelectedParent struct {
	Number         int    `json:"number"`
	Kind           string `json:"kind"`
	BackingIssueID string `json:"backingIssueId"`
	NodeID         string `json:"nodeId,omitempty"`
}

func DecodeParentSelection(raw []byte, repository string) (ParentSelection, error) {
	var selection ParentSelection
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if len(raw) > 16<<20 {
		return selection, errors.New("parent selection exceeds 16 MiB")
	}
	if err := d.Decode(&selection); err != nil {
		return selection, err
	}
	if d.Decode(new(any)) != io.EOF {
		return selection, errors.New("trailing parent selection data")
	}
	if _, err := parentSelectionHash(selection, repository); err != nil {
		return selection, err
	}
	return selection, nil
}
func parentSelectionHash(s ParentSelection, repo string) (string, error) {
	if s.Schema != "gitcrawl-parent-selection/v1" || s.Repository != repo || len(s.Parents) == 0 || len(s.Parents) > 1000000 {
		return "", errors.New("invalid parent selection scope")
	}
	numbers := map[int]bool{}
	ids := map[string]bool{}
	nodes := map[string]bool{}
	for _, p := range s.Parents {
		id, err := strconv.ParseInt(p.BackingIssueID, 10, 64)
		if p.Number <= 0 || numbers[p.Number] || ids[p.BackingIssueID] || err != nil || id <= 0 || strconv.FormatInt(id, 10) != p.BackingIssueID || (p.Kind != "issue" && p.Kind != "pull_request") || len(p.NodeID) > 256 || strings.ContainsAny(p.NodeID, "\x00\r\n") || (p.NodeID != "" && nodes[p.NodeID]) {
			return "", errors.New("invalid or duplicate parent identity")
		}
		numbers[p.Number] = true
		ids[p.BackingIssueID] = true
		if p.NodeID != "" {
			nodes[p.NodeID] = true
		}
	}
	raw, _ := json.Marshal(s)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type ParentAPI interface {
	State(context.Context) (State, error)
	Begin(context.Context, RunRequest) error
	Resolve(context.Context, ResolveRequest) (ResolvedIDs, error)
	Commit(context.Context, PageRequest) (Receipt, error)
	Parents(context.Context, []int64) (map[int64]remote.Change, error)
	CheckRepository(context.Context, string, string) (int64, error)
}
type ParentProvider interface {
	ReadParentRepository(context.Context, string, string, github.Reporter) (map[string]any, error)
	ReadParents(context.Context, []int, github.Reporter) ([]github.ParentSnapshot, error)
}
type ParentOptions struct {
	Selection             ParentSelection
	MaxParents, BatchSize int
	Now                   func() time.Time
}
type parentScope struct {
	Version      int    `json:"version"`
	Repository   string `json:"repository"`
	SelectionSHA string `json:"selectionSha"`
	ObservedAt   string `json:"observedAt"`
	Start        int    `json:"start"`
	End          int    `json:"end"`
	BatchSize    int    `json:"batchSize"`
}

// RunParents persists only queried parent fields. PostgreSQL owns observation
// allocation and publication; this mode never submits children or a new horizon.
func RunParents(ctx context.Context, api ParentAPI, provider ParentProvider, o ParentOptions) (Result, error) {
	var result Result
	repo := o.Selection.Repository
	owner, name, ok := strings.Cut(repo, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") || o.MaxParents < 1 || o.MaxParents > 10000 || o.BatchSize < 1 || o.BatchSize > 50 {
		return result, errors.New("invalid bounded parent repair options")
	}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("parent repair requires a deadline")
	}
	digest, err := parentSelectionHash(o.Selection, repo)
	if err != nil {
		return result, err
	}
	state, err := api.State(ctx)
	if err != nil {
		return result, err
	}
	metadata, err := provider.ReadParentRepository(ctx, owner, name, nil)
	if err != nil {
		return result, err
	}
	repositoryID, err := api.CheckRepository(ctx, repo, remotedelivery.AcquisitionRawID(metadata["id"]))
	if err != nil {
		return result, err
	}
	var cp checkpoint
	if len(state.Checkpoint) > 0 && string(state.Checkpoint) != "{}" {
		if err = json.Unmarshal(state.Checkpoint, &cp); err != nil || cp.Version != 1 {
			return result, errors.New("unknown acquisition checkpoint")
		}
	}
	run := state.ActiveRun
	var work parentScope
	if run != nil {
		if run.Mode != "historical-parents" {
			return result, errors.New("another acquisition mode is active")
		}
		if err = json.Unmarshal(run.Scope, &work); err != nil {
			return result, err
		}
	} else {
		if state.CompletedThrough == nil {
			return result, errors.New("parent repair requires an established incremental horizon")
		}
		now := time.Now
		if o.Now != nil {
			now = o.Now
		}
		work = parentScope{Version: 1, Repository: repo, SelectionSHA: digest, ObservedAt: now().UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano), BatchSize: o.BatchSize}
		if cp.History.SHA == digest {
			work.Start = cp.History.Offset
		}
		if work.Start < 0 || work.Start > len(o.Selection.Parents) {
			return result, errors.New("invalid parent repair progress")
		}
		if work.Start == len(o.Selection.Parents) {
			return Result{Complete: true, Revision: state.Revision}, nil
		}
		work.End = min(len(o.Selection.Parents), work.Start+o.MaxParents)
		raw, _ := json.Marshal(work)
		sum := sha256.Sum256(append(raw, []byte(fmt.Sprintf("/%d/%s", state.Revision, state.CompletedThrough.Format(time.RFC3339Nano)))...))
		run = &RunRequest{ID: "gitcrawl-parents-" + hex.EncodeToString(sum[:16]), Mode: "historical-parents", ExpectedRevision: state.Revision, Horizon: *state.CompletedThrough, Scope: raw}
		if err = api.Begin(ctx, *run); err != nil {
			return result, err
		}
	}
	if work.Version != 1 || work.Repository != repo || work.SelectionSHA != digest || work.Start < 0 || work.End > len(o.Selection.Parents) || work.End <= work.Start || work.BatchSize < 1 || work.BatchSize > 50 {
		return result, errors.New("resume requires original parent selection")
	}
	if _, err = time.Parse(time.RFC3339Nano, work.ObservedAt); err != nil {
		return result, errors.New("invalid frozen observation time")
	}
	if cp.RunID != run.ID {
		cp = checkpoint{Version: 1, RunID: run.ID, Phase: "parents", History: history{SHA: digest, Offset: work.Start}}
	}
	if cp.Phase != "parents" || cp.History.SHA != digest || cp.History.Offset < work.Start || cp.History.Offset > work.End {
		return result, errors.New("invalid parent repair checkpoint")
	}
	result.RunID = run.ID
	for cp.History.Offset < work.End {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		start := cp.History.Offset
		end := min(work.End, start+min(work.BatchSize, o.BatchSize))
		selected := o.Selection.Parents[start:end]
		numbers := make([]int, len(selected))
		for i, p := range selected {
			numbers[i] = p.Number
		}
		parents, err := provider.ReadParents(ctx, numbers, nil)
		if err != nil {
			return result, fmt.Errorf("parent batch of %d failed before progress; a response-size failure can be retried with a smaller --parent-page-size: %w", len(numbers), err)
		}
		if len(parents) != len(selected) {
			return result, errors.New("incomplete parent batch")
		}
		request := ResolveRequest{Threads: make([]ThreadIdentity, len(selected)), Comments: []CommentIdentity{}}
		for i, p := range parents {
			expected := selected[i]
			if p.Number != expected.Number || p.Kind != expected.Kind || (expected.NodeID != "" && p.NodeID != expected.NodeID) || (p.Kind == "issue" && p.BackingIssueID != expected.BackingIssueID) {
				return result, errors.New("parent provider identity differs from selection")
			}
			request.Threads[i] = ThreadIdentity{GitHubID: expected.BackingIssueID, Kind: expected.Kind, Number: strconv.Itoa(expected.Number)}
		}
		resolved, err := api.Resolve(ctx, request)
		if err != nil {
			return result, err
		}
		if resolved.RepositoryID != strconv.FormatInt(repositoryID, 10) || len(resolved.Threads) != len(selected) || len(resolved.Comments) != 0 {
			return result, errors.New("parent resolution mismatch")
		}
		ids := make([]int64, len(selected))
		seen := map[int64]bool{}
		for i, id := range resolved.Threads {
			ids[i], err = strconv.ParseInt(id, 10, 64)
			if err != nil || ids[i] <= 0 || strconv.FormatInt(ids[i], 10) != id || seen[ids[i]] {
				return result, errors.New("invalid resolved parent identity")
			}
			seen[ids[i]] = true
		}
		priors, err := api.Parents(ctx, ids)
		if err != nil {
			return result, err
		}
		changes := make([]remote.Change, len(selected))
		for i, p := range parents {
			var prior *remote.Change
			if row, ok := priors[ids[i]]; ok {
				prior = &row
			}
			changes[i], err = remotedelivery.ProjectAcquiredParent(repositoryID, ids[i], selected[i].BackingIssueID, p, prior, work.ObservedAt)
			if err != nil {
				return result, err
			}
		}
		// Split only the already-read rows. Persist progress with every accepted
		// prefix; a restart fetches only the still-uncommitted identities.
		for len(changes) > 0 {
			take := len(changes)
			var page PageRequest
			var next checkpoint
			for {
				next = cp
				next.History.Offset += take
				raw, _ := json.Marshal(next)
				page = PageRequest{RunID: run.ID, ID: fmt.Sprintf("step-%d", state.Revision+1), ExpectedRevision: state.Revision, Checkpoint: raw, Changes: changes[:take], Final: next.History.Offset == work.End}
				encoded, marshalErr := json.Marshal(page)
				if marshalErr != nil {
					return result, marshalErr
				}
				if len(encoded) <= MaxPageBytes {
					break
				}
				if take == 1 {
					return result, errors.New("parent row exceeds 4 MiB acquisition page")
				}
				take = (take + 1) / 2
			}
			receipt, err := api.Commit(ctx, page)
			if err != nil {
				return result, err
			}
			if receipt.Revision != state.Revision+1 {
				return result, errors.New("parent repair revision mismatch")
			}
			state.Revision = receipt.Revision
			cp = next
			changes = changes[take:]
			result.Threads += take
			result.Revision = receipt.Revision
		}
	}
	result.Complete = true
	return result, nil
}
