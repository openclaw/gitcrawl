package remotedelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store"
)

var ErrClusterDelivery = errors.New("cluster delivery failed; retain the outbox for reconciliation")

func (s ClusterState) Status() string {
	if s.Receipts == s.PartCount && s.PartCount > 0 {
		return "complete"
	}
	if s.Receipts > 0 {
		return "partial"
	}
	return "staged"
}

func OpenClusterOutbox(ctx context.Context, path string) (*Outbox, error) {
	if _, err := ReadClusterOutboxState(ctx, path); err != nil {
		return nil, err
	}
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	return &Outbox{base: base}, nil
}
func (o *Outbox) ClusterState(ctx context.Context) (ClusterState, error) {
	var state ClusterState
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error { var err error; state, err = readClusterState(ctx, tx); return err })
	return state, err
}

func clusterReceiptMatches(s ClusterState, index int, hash string, r remote.SeedReceipt, begin int64) bool {
	n, err := strconv.ParseInt(r.Sequence, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != r.Sequence || index < 0 || index >= s.PartCount {
		return false
	}
	if begin != 0 && (begin > math.MaxInt64-int64(index) || n != begin+int64(index)) {
		return false
	}
	batch := seedPartBatch(s.SeedID, index, s.PartCount)
	return r.App == "gitcrawl" && r.Archive == s.Registration.Archive && r.BindingID == s.Registration.BindingID && r.BatchID == batch && r.Cursor == batch && r.PayloadSHA256 == hash
}
func sameClusterCapture(a, b ClusterState) bool {
	a.Receipts, b.Receipts = 0, 0
	a.BeginSequence, b.BeginSequence = 0, 0
	return a == b
}

func (o *Outbox) recordClusterReceipt(ctx context.Context, expected ClusterState, index int, receipt remote.SeedReceipt) error {
	return o.write(ctx, func(conn *sql.Conn) error {
		state, err := readClusterState(ctx, conn)
		if err != nil {
			return err
		}
		if !sameClusterCapture(state, expected) {
			return ErrClusterOutbox
		}
		var hash string
		var previous []byte
		if err = conn.QueryRowContext(ctx, `select payload_sha256,case when length(receipt_json)<=4096 then receipt_json end from parts where seed_id=? and part_index=?`, state.SeedID, index).Scan(&hash, &previous); err != nil {
			return err
		}
		if !clusterReceiptMatches(state, index, hash, receipt, state.BeginSequence) {
			return ErrClusterOutbox
		}
		if previous != nil {
			var saved remote.SeedReceipt
			if json.Unmarshal(previous, &saved) != nil {
				return ErrClusterOutbox
			}
			saved.Replayed = receipt.Replayed
			if saved != receipt {
				return ErrClusterOutbox
			}
			return nil
		}
		if index != state.Receipts {
			return ErrClusterOutbox
		}
		if index == 0 {
			begin, _ := strconv.ParseInt(receipt.Sequence, 10, 64)
			if begin > math.MaxInt64-int64(state.PartCount-1) {
				return ErrClusterOutbox
			}
			if _, err = conn.ExecContext(ctx, `update captures set begin_sequence=? where seed_id=? and begin_sequence is null`, begin, state.SeedID); err != nil {
				return err
			}
		}
		body, _ := json.Marshal(receipt)
		if len(body) > 4096 {
			return ErrClusterOutbox
		}
		_, err = conn.ExecContext(ctx, `update parts set receipt_json=? where seed_id=? and part_index=? and receipt_json is null`, body, state.SeedID, index)
		return err
	})
}

type clusterDelivery struct {
	state    ClusterState
	plan     *remote.GitHubSeedPlan
	parts    [][]byte
	evidence []store.NativeClusterDeliveryEvidence
}

func (o *Outbox) prepareClusterDelivery(ctx context.Context, limits DeliveryLimits) (clusterDelivery, error) {
	var d clusterDelivery
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		state, err := readClusterState(ctx, tx)
		if err != nil {
			return err
		}
		if state.Rows > limits.MaxRows || state.Bytes > limits.MaxBytes {
			return ErrClusterOutbox
		}
		d.state = state
		rows, err := tx.QueryContext(ctx, `select part_index,case when length(body)<=4194304 then body end,payload_sha256 from parts where seed_id=? order by part_index`, state.SeedID)
		if err != nil {
			return err
		}
		defer rows.Close()
		var total int64
		for rows.Next() {
			var index int
			var body []byte
			var hash string
			if err = rows.Scan(&index, &body, &hash); err != nil {
				return err
			}
			if index != len(d.parts) || len(body) == 0 || int64(len(body)) > limits.MaxBytes-total || privateSeedHash(body) != hash {
				return ErrClusterOutbox
			}
			total += int64(len(body))
			d.parts = append(d.parts, body)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		if len(d.parts) != state.PartCount {
			return ErrClusterOutbox
		}
		bundle := ClusterBundle{Profile: store.NativeClusterContextProfile, Manifest: d.parts[0], Chunks: d.parts[1 : len(d.parts)-1], Complete: d.parts[len(d.parts)-1]}
		checked, err := inspectClusterBundle(ctx, bundle, ClusterStageLimits{MaxRows: limits.MaxRows, MaxEncodedBytes: limits.MaxBytes})
		if err != nil {
			return err
		}
		checked.Registration = state.Registration
		if !sameClusterCapture(checked, state) {
			return ErrClusterOutbox
		}
		d.plan, err = remote.DecodeGitHubSeedManifest(bundle.Manifest)
		if err != nil {
			return ErrClusterOutbox
		}
		for _, body := range bundle.Chunks {
			chunk, err := d.plan.ValidateChunk(body)
			if err != nil {
				return ErrClusterOutbox
			}
			for _, change := range chunk.Changes {
				if change.Table == "threads" {
					number, _ := strconv.ParseInt(*change.Row[1].Value, 10, 64)
					d.evidence = append(d.evidence, store.NativeClusterDeliveryEvidence{Number: number, NodeID: change.Row[8].Value})
				}
			}
		}
		return nil
	})
	if err != nil {
		return clusterDelivery{}, err
	}
	return d, nil
}

// DeliverClusterSeed makes only explicit, bounded attempts at saved requests.
// No uncertain request is retried in this invocation. Receipts count durable
// settlements by this call, not unique remote effects or provider freshness.
func (o *Outbox) DeliverClusterSeed(ctx context.Context, source string, token remote.TokenProvider, client *http.Client, limits DeliveryLimits) (result DeliveryResult, err error) {
	defer func() { err = clusterDeliveryError(err) }()
	if _, ok := ctx.Deadline(); !ok || limits.MaxParts <= 0 || limits.MaxParts > remote.MaxSeedChunks+2 || !clusterStageLimits(ClusterStageLimits{MaxRows: limits.MaxRows, MaxEncodedBytes: limits.MaxBytes}) {
		return result, ErrClusterOutbox
	}
	d, err := o.prepareClusterDelivery(ctx, limits)
	if err != nil {
		return result, err
	}
	if d.state.Receipts == d.state.PartCount {
		result.Complete = true
		return result, nil
	}
	if token == nil {
		return result, remote.ErrMissingToken
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
		// The full compact reference set is bounded once at preparation. Recheck it
		// per attempt, including redirects and completion, without fetching bodies.
		return store.CheckNativeClusterDelivery(requestCtx, source, d.state.Registration.File, d.state.Registration.Repository, d.state.RepositoryID, d.state.GitHubRepositoryID, store.NativeReportLimits{MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes}, d.evidence)
	}}
	remoteClient, err := remote.NewClient(remote.Options{Endpoint: d.state.Registration.Origin, TokenProvider: token, HTTPClient: &transportClient})
	if err != nil {
		return result, err
	}
	for result.Receipts < limits.MaxParts {
		state, err := o.ClusterState(ctx)
		if err != nil {
			return result, err
		}
		if !sameClusterCapture(state, d.state) {
			return result, ErrClusterOutbox
		}
		if state.Receipts == state.PartCount {
			result.Complete = true
			return result, nil
		}
		index := state.Receipts
		body := d.parts[index]
		var receipt remote.SeedReceipt
		switch index {
		case 0:
			receipt, err = remoteClient.BeginGitHubSeed(ctx, state.Registration.Archive, d.plan, body)
		case state.PartCount - 1:
			receipt, err = remoteClient.CompleteGitHubSeed(ctx, state.Registration.Archive, d.plan, body)
		default:
			receipt, err = remoteClient.ApplyGitHubSeedChunk(ctx, state.Registration.Archive, d.plan, body)
		}
		if err != nil {
			return result, err
		}
		if err = o.recordClusterReceipt(ctx, d.state, index, receipt); err != nil {
			return result, err
		}
		result.Receipts++
		if index == state.PartCount-1 {
			result.Complete = true
			return result, nil
		}
	}
	return result, nil
}
func clusterDeliveryError(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{context.Canceled, context.DeadlineExceeded, ErrClusterOutbox, store.ErrNativeClusterContext, store.ErrNativeReportLimit, store.ErrThreadExcluded, remote.ErrMissingToken} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrClusterDelivery
}
