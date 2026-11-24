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

type API interface {
	State(context.Context) (State, error)
	Begin(context.Context, RunRequest) error
	Resolve(context.Context, ResolveRequest) (ResolvedIDs, error)
	Commit(context.Context, PageRequest) (Receipt, error)
	Parent(context.Context, int64) (*remote.Change, error)
	CheckRepository(context.Context, string, string) (int64, error)
}
type Provider interface {
	GetRepo(context.Context, string, string, github.Reporter) (map[string]any, error)
	GetIssue(context.Context, string, string, int, github.Reporter) (map[string]any, error)
	GetPull(context.Context, string, string, int, github.Reporter) (map[string]any, error)
	ReadRESTPage(context.Context, github.RESTPageRequest, github.Reporter) (github.RESTPage, error)
}
type Options struct {
	Repository                    string
	Since                         time.Time // Required on the first incremental run only.
	Numbers                       []int     // Immutable historical selection; empty selects incremental.
	MaxThreads                    int
	ParentPageSize, ChildPageSize int
	Now                           func() time.Time
}
type Result struct {
	RunID    string `json:"runId"`
	Threads  int    `json:"threads"`
	Revision int64  `json:"revision"`
	Complete bool   `json:"complete"`
}
type history struct {
	SHA    string `json:"sha"`
	Offset int    `json:"offset"`
}
type scope struct {
	Version               int    `json:"version"`
	Repository            string `json:"repository"`
	Mode                  string `json:"mode"`
	ObservedAt            string `json:"observedAt"`
	Since                 string `json:"since,omitempty"`
	SelectionSHA          string `json:"selectionSha,omitempty"`
	Start, End            int
	ParentPage, ChildPage int
	History               history `json:"history"`
}
type checkpoint struct {
	Version         int     `json:"version"`
	RunID           string  `json:"runId"`
	Phase           string  `json:"phase"`
	Queue           []int   `json:"queue,omitempty"`
	InventoryCursor string  `json:"inventoryCursor,omitempty"`
	InventoryDone   bool    `json:"inventoryDone,omitempty"`
	Number          int     `json:"number,omitempty"`
	Kind            string  `json:"kind,omitempty"`
	ThreadID        string  `json:"threadId,omitempty"`
	ChildCursor     string  `json:"childCursor,omitempty"`
	Processed       int     `json:"processed"`
	History         history `json:"history"`
}

// Run resumes the server's original run and checkpoint. Only complete provider
// pages advance progress. It never opens SQLite or supplies observation rows.
func Run(ctx context.Context, api API, provider Provider, options Options) (Result, error) {
	var result Result
	owner, repo, ok := strings.Cut(options.Repository, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") || options.MaxThreads < 1 || options.MaxThreads > 10000 || options.ParentPageSize < 1 || options.ParentPageSize > 100 || options.ChildPageSize < 1 || options.ChildPageSize > 100 {
		return result, errors.New("invalid bounded acquisition options")
	}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("acquisition requires a deadline")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	state, err := api.State(ctx)
	if err != nil {
		return result, err
	}
	// Bind the source before Begin can reserve a durable run slot. GetRepo also
	// restores the Octopool numeric-path binding on process restart.
	metadata, err := provider.GetRepo(ctx, owner, repo, nil)
	if err != nil {
		return result, err
	}
	providerID := remotedelivery.AcquisitionRawID(metadata["id"])
	if providerID == "" {
		return result, errors.New("provider repository identity missing")
	}
	repositoryID, err := api.CheckRepository(ctx, options.Repository, providerID)
	if err != nil {
		return result, err
	}
	parentCheckpoint, err := parentProgressCheckpoint(state, options.Repository)
	if err != nil {
		return result, err
	}
	var cp checkpoint
	if len(parentCheckpoint) > 0 && string(parentCheckpoint) != "{}" {
		if err = json.Unmarshal(parentCheckpoint, &cp); err != nil || cp.Version != 1 {
			return result, errors.New("unknown acquisition checkpoint")
		}
	}
	var work scope
	run := state.ActiveRun
	if run != nil {
		if err = json.Unmarshal(run.Scope, &work); err != nil || work.Version != 1 || work.Repository != options.Repository {
			return result, errors.New("active run scope mismatch")
		}
	} else {
		work = scope{Version: 1, Repository: options.Repository, ParentPage: options.ParentPageSize, ChildPage: options.ChildPageSize, History: cp.History}
		observed := now().UTC().Truncate(time.Microsecond)
		work.ObservedAt = observed.Format(time.RFC3339Nano)
		horizon := observed
		if len(options.Numbers) > 0 {
			hash, err := selectionHash(options.Numbers)
			if err != nil {
				return result, err
			}
			if state.CompletedThrough == nil {
				return result, errors.New("historical work requires an established incremental horizon")
			}
			horizon = *state.CompletedThrough
			work.Mode = "historical"
			work.SelectionSHA = hash
			if cp.History.SHA == hash {
				work.Start = cp.History.Offset
			}
			if work.Start >= len(options.Numbers) {
				return Result{Complete: true, Revision: state.Revision}, nil
			}
			work.End = min(len(options.Numbers), work.Start+options.MaxThreads)
		} else {
			work.Mode = "incremental"
			since := options.Since
			if state.CompletedThrough != nil {
				since = state.CompletedThrough.Add(-2 * time.Minute)
			}
			if since.IsZero() || since.After(horizon) {
				return result, errors.New("first incremental run requires a prior --since")
			}
			work.Since = since.UTC().Format(time.RFC3339Nano)
		}
		raw, _ := json.Marshal(work)
		sum := sha256.Sum256(append(raw, []byte(fmt.Sprintf("/%d/%s", state.Revision, horizon.Format(time.RFC3339Nano)))...))
		run = &RunRequest{ID: "gitcrawl-" + hex.EncodeToString(sum[:16]), ExpectedRevision: state.Revision, Horizon: horizon, Scope: raw}
		if err = api.Begin(ctx, *run); err != nil {
			return result, err
		}
	}
	if work.ParentPage < 1 || work.ParentPage > 100 || work.ChildPage < 1 || work.ChildPage > 100 {
		return result, errors.New("invalid recorded page bounds")
	}
	if work.Mode == "historical" {
		hash, err := selectionHash(options.Numbers)
		if err != nil || hash != work.SelectionSHA || work.Start < 0 || work.End > len(options.Numbers) || work.End <= work.Start {
			return result, errors.New("resume requires the original historical selection")
		}
	} else if work.Mode != "incremental" {
		return result, errors.New("unknown recorded acquisition mode")
	}
	if cp.RunID != run.ID {
		cp = checkpoint{Version: 1, RunID: run.ID, Phase: "select", History: work.History}
	}
	result.RunID = run.ID
	cache := map[int]map[string]any{}
	committed := 0
	commit := func(next checkpoint, changes []remote.Change, child *ChildObservation, final bool) error {
		raw, err := json.Marshal(next)
		if err != nil || len(raw) > 65536 {
			return errors.New("checkpoint exceeds 64 KiB")
		}
		if changes == nil {
			changes = []remote.Change{}
		}
		req := PageRequest{RunID: run.ID, ID: fmt.Sprintf("step-%d", state.Revision+1), ExpectedRevision: state.Revision, Checkpoint: raw, Changes: changes, ChildObservation: child, Final: final}
		receipt, err := api.Commit(ctx, req)
		if err != nil {
			return err
		}
		if receipt.Revision != state.Revision+1 {
			return errors.New("acquisition revision mismatch")
		}
		state.Revision = receipt.Revision
		state.Sequence = receipt.Sequence
		cp = next
		result.Revision = state.Revision
		return nil
	}
	for {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		switch cp.Phase {
		case "select":
			if work.Mode == "historical" && work.Start+cp.Processed >= work.End || work.Mode == "incremental" && cp.InventoryDone && len(cp.Queue) == 0 {
				if err = commit(cp, nil, nil, true); err != nil {
					return result, err
				}
				result.Complete = true
				return result, nil
			}
			if committed >= options.MaxThreads {
				return result, nil
			}
			if len(cp.Queue) == 0 {
				if work.Mode == "historical" {
					cp.Queue = []int{options.Numbers[work.Start+cp.Processed]}
				} else {
					page, err := provider.ReadRESTPage(ctx, github.RESTPageRequest{Owner: owner, Repo: repo, Family: "issues", State: "all", Since: work.Since, PerPage: work.ParentPage, Cursor: cp.InventoryCursor}, nil)
					if err != nil {
						return result, err
					}
					for _, row := range page.Rows {
						n, err := strconv.Atoi(remotedelivery.AcquisitionRawID(row["number"]))
						if err != nil || n < 1 || cache[n] != nil {
							return result, errors.New("invalid duplicate inventory identity")
						}
						cache[n] = row
						cp.Queue = append(cp.Queue, n)
					}
					cp.InventoryCursor = page.NextCursor
					cp.InventoryDone = page.NextCursor == ""
					if len(cp.Queue) == 0 {
						if !cp.InventoryDone {
							return result, errors.New("empty inventory with next cursor")
						}
						continue
					}
				}
			}
			n := cp.Queue[0]
			row := cache[n]
			if row == nil {
				row, err = provider.GetIssue(ctx, owner, repo, n, nil)
				if err != nil {
					return result, err
				}
			}
			got, err := strconv.Atoi(remotedelivery.AcquisitionRawID(row["number"]))
			if err != nil || got != n {
				return result, errors.New("provider parent number mismatch")
			}
			kind := "issue"
			if _, ok := row["pull_request"]; ok {
				kind = "pull_request"
			}
			ids, err := api.Resolve(ctx, ResolveRequest{Threads: []ThreadIdentity{{GitHubID: remotedelivery.AcquisitionRawID(row["id"]), Kind: kind, Number: strconv.Itoa(n)}}, Comments: []CommentIdentity{}})
			if err != nil {
				return result, err
			}
			if len(ids.Threads) != 1 {
				return result, errors.New("parent identity count mismatch")
			}
			threadID, err := positiveID(ids.Threads[0])
			if err != nil {
				return result, err
			}
			repoID, err := positiveID(ids.RepositoryID)
			if err != nil {
				return result, err
			}
			if repoID != repositoryID {
				return result, errors.New("resolved repository identity changed")
			}
			prior, err := api.Parent(ctx, threadID)
			if err != nil {
				return result, err
			}
			change, err := remotedelivery.ProjectAcquiredThread(repoID, threadID, options.Repository, row, prior, work.ObservedAt, nil)
			if errors.Is(err, remotedelivery.ErrFullPullRequired) {
				pull, readErr := provider.GetPull(ctx, owner, repo, n, nil)
				if readErr != nil {
					return result, readErr
				}
				merge, verifyErr := github.VerifiedPullMerge(options.Repository, providerID, n, row, pull)
				if verifyErr != nil {
					return result, verifyErr
				}
				change, err = remotedelivery.ProjectAcquiredThread(repoID, threadID, options.Repository, row, prior, work.ObservedAt, merge)
			}
			if err != nil {
				return result, err
			}
			next := cp
			next.Number = n
			next.Kind = kind
			next.ThreadID = ids.Threads[0]
			next.ChildCursor = ""
			next.Phase = "issue_comment"
			if err = commit(next, []remote.Change{change}, &ChildObservation{ThreadID: next.ThreadID, Action: "begin"}, false); err != nil {
				return result, err
			}
			delete(cache, n)
		case "issue_comment", "pull_review", "pull_review_comment":
			page, err := provider.ReadRESTPage(ctx, github.RESTPageRequest{Owner: owner, Repo: repo, Family: cp.Phase, Number: cp.Number, PerPage: work.ChildPage, Cursor: cp.ChildCursor}, nil)
			if err != nil {
				return result, err
			}
			identities := make([]CommentIdentity, 0, len(page.Rows))
			for _, row := range page.Rows {
				identities = append(identities, CommentIdentity{GitHubID: remotedelivery.AcquisitionRawID(row["id"]), Type: cp.Phase, ThreadKind: cp.Kind, ThreadNumber: strconv.Itoa(cp.Number)})
			}
			var ids ResolvedIDs
			if len(identities) > 0 {
				ids, err = api.Resolve(ctx, ResolveRequest{Threads: []ThreadIdentity{}, Comments: identities})
				if err != nil {
					return result, err
				}
			}
			if len(ids.Comments) != len(page.Rows) {
				return result, errors.New("comment identity count mismatch")
			}
			threadID, err := positiveID(cp.ThreadID)
			if err != nil {
				return result, err
			}
			changes := make([]remote.Change, 0, len(page.Rows))
			for i, row := range page.Rows {
				id, err := positiveID(ids.Comments[i])
				if err != nil {
					return result, err
				}
				change, err := remotedelivery.ProjectAcquiredComment(threadID, id, cp.Phase, row)
				if err != nil {
					return result, err
				}
				changes = append(changes, change)
			}
			next := cp
			next.ChildCursor = page.NextCursor
			if page.NextCursor == "" {
				next.ChildCursor = ""
				switch cp.Phase {
				case "issue_comment":
					if cp.Kind == "pull_request" {
						next.Phase = "pull_review"
					} else {
						next.Phase = "complete"
					}
				case "pull_review":
					next.Phase = "pull_review_comment"
				case "pull_review_comment":
					next.Phase = "complete"
				}
			}
			if err = commit(next, changes, &ChildObservation{ThreadID: cp.ThreadID, Action: "page", Family: cp.Phase, Cursor: cp.ChildCursor, NextCursor: page.NextCursor, Terminal: page.NextCursor == ""}, false); err != nil {
				return result, err
			}
		case "complete":
			next := cp
			next.Phase = "select"
			next.Queue = slices.Clone(cp.Queue[1:])
			next.Processed++
			next.ThreadID = ""
			next.Number = 0
			next.Kind = ""
			if work.Mode == "historical" {
				next.History = history{SHA: work.SelectionSHA, Offset: work.Start + next.Processed}
			}
			if err = commit(next, nil, &ChildObservation{ThreadID: cp.ThreadID, Action: "complete"}, false); err != nil {
				return result, err
			}
			committed++
			result.Threads++
		default:
			return result, errors.New("invalid recorded acquisition phase")
		}
	}
}
func positiveID(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 || strconv.FormatInt(n, 10) != s {
		return 0, errors.New("invalid native identity")
	}
	return n, nil
}
func selectionHash(numbers []int) (string, error) {
	if len(numbers) == 0 || len(numbers) > 1000000 {
		return "", errors.New("invalid historical selection size")
	}
	seen := map[int]bool{}
	for _, n := range numbers {
		if n < 1 || seen[n] {
			return "", errors.New("historical numbers must be positive and unique")
		}
		seen[n] = true
	}
	raw, _ := json.Marshal(numbers)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
