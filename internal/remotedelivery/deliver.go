package remotedelivery

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

// DeliveryLimits bound one invocation, including the complete persisted seed
// inspected for parent identities. They do not bound process RSS or disk use.
type DeliveryLimits struct {
	MaxParts          int
	MaxRows, MaxBytes int64
}

type DeliveryResult struct {
	// Receipts counts successful settlements by this invocation, including
	// replayed receipts; it is not a count of unique remote effects.
	Receipts int
	Complete bool
}

type seedDelivery struct {
	state   SeedState
	plan    *remote.GitHubSeedPlan
	parents map[int64]int64
	limits  DeliveryLimits
}

// DeliverSeed resumes exact saved requests. It never retries an uncertain
// request: the pending bytes remain available to the next explicit invocation.
// The caller owns the source pathname and credential provider for this run.
func (o *Outbox) DeliverSeed(ctx context.Context, source string, token remote.TokenProvider, client *http.Client, limits DeliveryLimits) (DeliveryResult, error) {
	var result DeliveryResult
	if _, ok := ctx.Deadline(); !ok || limits.MaxParts <= 0 || limits.MaxRows <= 0 || limits.MaxBytes <= 0 {
		return result, errors.New("seed delivery requires a deadline and positive limits")
	}
	d, err := o.prepareDelivery(ctx, limits)
	if err != nil {
		return result, err
	}
	if d.state.BaselineState == BaselineQualified {
		result.Complete = true
		return result, nil
	}
	if token == nil {
		return result, remote.ErrMissingToken
	}
	for result.Receipts < limits.MaxParts {
		part, err := o.NextPart(ctx)
		if err != nil {
			return result, err
		}
		if part == nil {
			result.Complete = true
			return result, nil
		}
		if part.Ref.SeedID != d.state.SeedID || part.PartCount != d.state.PartCount {
			return result, ErrSeedOutbox
		}
		transportClient := http.Client{Timeout: 30 * time.Second}
		if client != nil {
			transportClient = *client
		}
		transport := transportClient.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		transportClient.Transport = seedPolicyTransport{base: transport, check: func(requestCtx context.Context) error {
			// Each staged row contributes at most one thread reference and one
			// raw JSON value to the source owner's independent evidence budget.
			policyRows := int64(math.MaxInt64)
			if limits.MaxRows <= math.MaxInt64/2 {
				policyRows = limits.MaxRows * 2
			}
			return store.CheckNativeReportDelivery(requestCtx, source, d.state.Anchor,
				store.NativeReportDeliveryLimits{MaxRows: policyRows, MaxBytes: limits.MaxBytes},
				func(yield func(store.NativeReportDeliveryEvidence) error) error {
					if part.Ref.Index == part.PartCount-1 {
						return o.walkSeedChunks(requestCtx, d, func(c remote.GitHubSeedChunk) error { return d.yieldEvidence(c, yield) })
					}
					if part.Ref.Index == 0 {
						return nil
					}
					chunk, err := d.plan.ValidateChunk(part.Body)
					if err != nil {
						return err
					}
					return d.yieldEvidence(chunk, yield)
				})
		}}
		remoteClient, err := remote.NewClient(remote.Options{Endpoint: d.state.Registration.Origin, TokenProvider: token, HTTPClient: &transportClient})
		if err != nil {
			return result, err
		}
		var receipt remote.SeedReceipt
		switch part.Ref.Index {
		case 0:
			receipt, err = remoteClient.BeginGitHubSeed(ctx, d.state.Registration.Archive, d.plan, part.Body)
		case part.PartCount - 1:
			receipt, err = remoteClient.CompleteGitHubSeed(ctx, d.state.Registration.Archive, d.plan, part.Body)
		default:
			receipt, err = remoteClient.ApplyGitHubSeedChunk(ctx, d.state.Registration.Archive, d.plan, part.Body)
		}
		if err != nil {
			return result, err
		}
		if err := o.RecordReceipt(ctx, part.Ref, receipt); err != nil {
			return result, err
		}
		result.Receipts++
		if part.Ref.Index == part.PartCount-1 {
			result.Complete = true
			return result, nil
		}
	}
	return result, nil
}

type seedPolicyTransport struct {
	base  http.RoundTripper
	check func(context.Context) error
}

func (t seedPolicyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Crawlkit has resolved credentials by this point. Redirects traverse the
	// same adapter, so each actual attempt gets a fresh native policy snapshot.
	if err := t.check(r.Context()); err != nil {
		return nil, err
	}
	return t.base.RoundTrip(r)
}

func (o *Outbox) prepareDelivery(ctx context.Context, limits DeliveryLimits) (*seedDelivery, error) {
	state, err := o.State(ctx)
	if err != nil {
		return nil, err
	}
	if state.BaselineState == BaselineQualified {
		return &seedDelivery{state: state}, nil
	}
	var body []byte
	var hash string
	if err := o.base.DB().QueryRowContext(ctx, `select body,payload_sha256 from parts where seed_id=? and part_index=0`, state.SeedID).Scan(&body, &hash); err != nil {
		return nil, err
	}
	if privateSeedHash(body) != hash {
		return nil, ErrSeedOutbox
	}
	plan, err := remote.DecodeGitHubSeedManifest(body)
	if err != nil {
		return nil, err
	}
	d := &seedDelivery{state: state, plan: plan, parents: make(map[int64]int64), limits: limits}
	err = o.walkSeedChunks(ctx, d, func(c remote.GitHubSeedChunk) error {
		for _, change := range c.Changes {
			if change.Table != "threads" {
				continue
			}
			id, err := deliveryInteger(change.Row[deliveryColumn("threads", "id")])
			if err != nil {
				return err
			}
			number, err := deliveryInteger(change.Row[deliveryColumn("threads", "number")])
			if err != nil {
				return err
			}
			key := id
			if _, exists := d.parents[key]; exists {
				return ErrSeedOutbox
			}
			d.parents[key] = number
		}
		return nil
	})
	return d, err
}

func deliveryInteger(v remote.ChangeValue) (int64, error) {
	if v.Kind != "integer" || v.Value == nil {
		return 0, ErrSeedOutbox
	}
	n, err := strconv.ParseInt(*v.Value, 10, 64)
	if err != nil || n <= 0 {
		return 0, ErrSeedOutbox
	}
	return n, nil
}

// walkSeedChunks streams at most one wire body at a time. Even acknowledged
// parts remain subject to the full completion check; receipts are not policy.
func (o *Outbox) walkSeedChunks(ctx context.Context, d *seedDelivery, visit func(remote.GitHubSeedChunk) error) error {
	return o.base.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `select part_index,body,payload_sha256 from parts where seed_id=? order by part_index`, d.state.SeedID)
		if err != nil {
			return err
		}
		defer rows.Close()
		var count int
		var totalRows, totalBytes int64
		for rows.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var index int
			var body []byte
			var hash string
			if err := rows.Scan(&index, &body, &hash); err != nil {
				return err
			}
			if index != count || privateSeedHash(body) != hash || int64(len(body)) > d.limits.MaxBytes-totalBytes {
				return ErrSeedOutbox
			}
			totalBytes += int64(len(body))
			count++
			if index == 0 {
				if hash != d.plan.ManifestSHA256() {
					return ErrSeedOutbox
				}
				continue
			}
			if index == d.state.PartCount-1 {
				if _, err := d.plan.ValidateComplete(body); err != nil {
					return err
				}
				continue
			}
			chunk, err := d.plan.ValidateChunk(body)
			if err != nil {
				return err
			}
			if chunk.Index != index-1 || int64(len(chunk.Changes)) > d.limits.MaxRows-totalRows {
				return ErrSeedOutbox
			}
			totalRows += int64(len(chunk.Changes))
			for _, change := range chunk.Changes {
				var width int
				for _, layout := range privateLayouts {
					if layout.name == change.Table {
						width = len(strings.Fields(layout.columns))
						break
					}
				}
				if width == 0 || len(change.Row) != width {
					return ErrSeedOutbox
				}
			}
			if err := visit(chunk); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if count != d.state.PartCount {
			return ErrSeedOutbox
		}
		return nil
	})
}

func (d *seedDelivery) yieldEvidence(chunk remote.GitHubSeedChunk, yield func(store.NativeReportDeliveryEvidence) error) error {
	evidence := store.NativeReportDeliveryEvidence{}
	for _, c := range chunk.Changes {
		parent, raw := -1, -1
		switch c.Table {
		case "threads":
			parent, raw = deliveryColumn(c.Table, "id"), deliveryColumn(c.Table, "raw_json")
		case "comments":
			parent, raw = deliveryColumn(c.Table, "thread_id"), deliveryColumn(c.Table, "raw_json")
		case "thread_child_observation_reservations", "thread_child_observation_memberships":
			parent = deliveryColumn(c.Table, "thread_id")
		}
		if parent < 0 {
			continue
		}
		id, err := deliveryInteger(c.Row[parent])
		if err != nil {
			return err
		}
		number, ok := d.parents[id]
		if !ok {
			return ErrSeedOutbox
		}
		evidence.ThreadNumbers = append(evidence.ThreadNumbers, number)
		if raw >= 0 {
			v := c.Row[raw]
			if v.Kind == "null" {
				evidence.RawJSON = append(evidence.RawJSON, "")
			} else if v.Kind == "text" && v.Value != nil {
				evidence.RawJSON = append(evidence.RawJSON, *v.Value)
			} else {
				return ErrSeedOutbox
			}
		}
	}
	return yield(evidence)
}

func deliveryColumn(table, column string) int {
	for _, layout := range privateLayouts {
		if layout.name == table {
			return slices.Index(strings.Fields(layout.columns), column)
		}
	}
	return -1
}
