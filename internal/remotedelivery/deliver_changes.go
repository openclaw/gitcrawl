package remotedelivery

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

var ErrReportDelivery = errors.New("private report delivery failed; retained outbox requires reconciliation")

type ChangeDeliveryResult struct {
	// An empty CaptureID means no pending capture at entry. Complete describes
	// only the pinned capture, never one staged concurrently after entry.
	CaptureID string
	// Receipts counts durable settlements accepted by this invocation, including
	// replay; it is not a count of unique remote effects or HTTP redirect hops.
	Receipts int
	Complete bool
}

type reportChangeDelivery struct {
	id      string
	capture outboxChangeCapture
	limits  DeliveryLimits
	codec   *remote.InventoryCodec
}

// DeliverReportChanges sends only exact saved parts of one capture. Limits
// cover that capture's complete staged rows/bytes, not the qualified baseline.
// MaxParts bounds settlements in this call; ctx bounds all work and HTTP.
// Errors never trigger retries, source capture, migration or credential fallback.
// The caller owns the stable native pathname and the in-memory token provider.
func (o *Outbox) DeliverReportChanges(ctx context.Context, source string, token remote.TokenProvider, client *http.Client, limits DeliveryLimits) (result ChangeDeliveryResult, err error) {
	defer func() { err = reportDeliveryError(err) }()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if _, ok := ctx.Deadline(); !ok || limits.MaxParts <= 0 || limits.MaxRows <= 0 || limits.MaxBytes <= 0 {
		return result, ErrPrivateChangesLimit
	}
	d, err := o.prepareReportDelivery(ctx, limits)
	if err != nil {
		return result, err
	}
	if d == nil {
		return result, nil
	}
	result.CaptureID = d.id
	if token == nil {
		return result, remote.ErrMissingToken
	}
	for result.Receipts < limits.MaxParts {
		part, err := o.NextChangePart(ctx)
		if err != nil {
			return result, err
		}
		if part == nil || part.Ref.CaptureID != d.id {
			// Another handle may have completed this capture and staged its
			// successor. Historical custody can prove completion; never follow it.
			complete, err := o.reportDeliveryCompleted(ctx, d.id)
			if err != nil {
				return result, err
			}
			if !complete {
				return result, ErrSeedOutbox
			}
			result.Complete = true
			return result, nil
		}
		if part.PartCount != d.capture.PartCount {
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
			policyRows := int64(math.MaxInt64)
			if limits.MaxRows <= math.MaxInt64/3 {
				policyRows = 3 * limits.MaxRows
			}
			return store.CheckNativeReportDelivery(requestCtx, source, d.capture.Through,
				store.NativeReportDeliveryLimits{MaxRows: policyRows, MaxBytes: limits.MaxBytes},
				func(yield func(store.NativeReportDeliveryEvidence) error) error {
					first, last := part.Ref.Index, part.Ref.Index+1
					if last == d.capture.PartCount {
						first = 0
					}
					return o.walkReportDelivery(requestCtx, d, first, last, func(tx *sql.Tx, b remote.ChangeBatch) error { return d.yieldEvidence(requestCtx, tx, b, yield) })
				})
		}}
		registration := d.capture.Expected.Registration
		remoteClient, err := remote.NewClient(remote.Options{Endpoint: registration.Origin, TokenProvider: token, HTTPClient: &transportClient})
		if err != nil {
			return result, err
		}
		receipt, err := remoteClient.ApplyChanges(ctx, "gitcrawl", registration.Archive, part.Body)
		if err != nil {
			return result, err
		}
		if err := o.RecordChangeReceipt(ctx, part.Ref, receipt); err != nil {
			return result, err
		}
		result.Receipts++
		if part.Ref.Index == d.capture.PartCount-1 {
			result.Complete = true
			return result, nil
		}
	}
	return result, nil
}

func reportDeliveryError(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{context.Canceled, context.DeadlineExceeded, ErrSeedOutbox, ErrPrivateChanges, ErrPrivateChangesLimit, remote.ErrMissingToken, store.ErrNativeReportSource, store.ErrNativeReportLimit, store.ErrNativeReportBlob, store.ErrThreadExcluded} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrReportDelivery
}

func (o *Outbox) prepareReportDelivery(ctx context.Context, limits DeliveryLimits) (*reportChangeDelivery, error) {
	var d *reportChangeDelivery
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		if err := inventoryVersion(ctx, tx, changeOutboxVersion, 7); err != nil {
			return err
		}
		state, err := readReportState(ctx, tx)
		if err != nil {
			return err
		}
		if state.ActiveChangeID == "" {
			return nil
		}
		capture, err := readChangeCapture(ctx, tx, state.ActiveChangeID)
		if err != nil {
			return err
		}
		if err := checkActiveChange(state, state.ActiveChangeID, capture); err != nil {
			return err
		}
		codec, err := privateInventoryCodec()
		if err != nil {
			return err
		}
		d = &reportChangeDelivery{id: state.ActiveChangeID, capture: capture, limits: limits, codec: codec}
		return nil
	})
	if err != nil || d == nil {
		return d, err
	}
	// Validate all staged bytes/rows before credentials or source access. No seed
	// bodies or full baseline inventory are read, and only one part is decoded
	// at a time. The final source-policy check repeats this bounded walk.
	if err := o.walkReportDelivery(ctx, d, 0, d.capture.PartCount, nil); err != nil {
		return nil, err
	}
	return d, nil
}

func (o *Outbox) reportDeliveryCompleted(ctx context.Context, id string) (bool, error) {
	var complete bool
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		if err := inventoryVersion(ctx, tx, changeOutboxVersion, 7); err != nil {
			return err
		}
		capture, err := readChangeCapture(ctx, tx, id)
		complete = capture.Completed
		return err
	})
	return complete, err
}

func (o *Outbox) walkReportDelivery(ctx context.Context, d *reportChangeDelivery, first, last int, visit func(*sql.Tx, remote.ChangeBatch) error) error {
	return o.base.WithTx(ctx, func(tx *sql.Tx) error {
		// Prebound bytes in SQL before fetching any saved request into Go.
		var size int64
		if err := tx.QueryRowContext(ctx, `select coalesce(sum(length(body)),0) from change_parts where capture_id=? and part_index>=? and part_index<?`, d.id, first, last).Scan(&size); err != nil {
			return err
		}
		if size <= 0 || size > d.limits.MaxBytes {
			return ErrPrivateChangesLimit
		}
		var rows int64
		for i := first; i < last; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			p, _, err := readChangePart(ctx, tx, ChangePartRef{CaptureID: d.id, Index: i}, d.capture)
			if err != nil {
				return err
			}
			batch, err := remote.DecodeChangeBatch(p.Body)
			if err != nil {
				return err
			}
			if int64(len(batch.Changes)) > d.limits.MaxRows-rows {
				return ErrPrivateChangesLimit
			}
			rows += int64(len(batch.Changes))
			if visit != nil {
				if err := visit(tx, batch); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (d *reportChangeDelivery) yieldEvidence(ctx context.Context, tx *sql.Tx, batch remote.ChangeBatch, yield func(store.NativeReportDeliveryEvidence) error) error {
	evidence := store.NativeReportDeliveryEvidence{}
	var evidenceBytes int64
	for _, change := range batch.Changes {
		key, err := d.codec.EncodeKey(change.Table, change.Key)
		if err != nil {
			return ErrSeedOutbox
		}
		var operation string
		var repository int64
		var thread, number sql.NullInt64
		var hash []byte
		// Staging derived these numbers from the complete candidate overlay, not
		// receipt order. They survive source purges and cross-part parent moves.
		if err := tx.QueryRowContext(ctx, `select operation,repository_id,thread_id,thread_number,row_sha256 from change_mutations where capture_id=? and table_name=? and encoded_key=?`, d.id, change.Table, key).Scan(&operation, &repository, &thread, &number, &hash); err != nil {
			return err
		}
		if operation != change.Operation || repository != d.capture.Through.RepositoryID {
			return ErrSeedOutbox
		}
		if operation == "remove" {
			// The stage owner proved qualified old ownership. Applying content
			// exclusion checks here would prevent retracting now-excluded rows.
			continue
		}
		rowHash, err := d.codec.RowDigest(change.Table, change.Key, change.Row)
		if err != nil || !bytes.Equal(rowHash[:], hash) {
			return ErrSeedOutbox
		}
		parent, raw := -1, -1
		switch change.Table {
		case "threads":
			parent, raw = deliveryColumn(change.Table, "id"), deliveryColumn(change.Table, "raw_json")
		case "comments":
			parent, raw = deliveryColumn(change.Table, "thread_id"), deliveryColumn(change.Table, "raw_json")
		case "thread_child_observation_reservations", "thread_child_observation_memberships":
			parent = deliveryColumn(change.Table, "thread_id")
		}
		if parent < 0 {
			continue
		}
		id, err := deliveryInteger(change.Row[parent])
		if err != nil || !thread.Valid || thread.Int64 != id || !number.Valid || number.Int64 <= 0 {
			return ErrSeedOutbox
		}
		if change.Table == "threads" {
			n, err := deliveryInteger(change.Row[deliveryColumn(change.Table, "number")])
			if err != nil || n != number.Int64 {
				return ErrSeedOutbox
			}
		}
		evidence.ThreadNumbers = append(evidence.ThreadNumbers, number.Int64)
		if raw >= 0 {
			v := change.Row[raw]
			if v.Kind == "null" {
				evidence.RawJSON = append(evidence.RawJSON, "")
			} else if v.Kind == "text" && v.Value != nil {
				if int64(len(*v.Value)) > d.limits.MaxBytes-evidenceBytes {
					return store.ErrNativeReportLimit
				}
				evidenceBytes += int64(len(*v.Value))
				evidence.RawJSON = append(evidence.RawJSON, *v.Value)
			} else {
				return ErrSeedOutbox
			}
		}
		if change.Table != "threads" {
			node, err := d.parentNode(ctx, tx, id, number.Int64, d.limits.MaxBytes-evidenceBytes)
			if err != nil {
				return err
			}
			if node.Valid {
				evidenceBytes += int64(len(node.String))
				evidence.NodeIDs = append(evidence.NodeIDs, node.String)
			}
		}
	}
	return yield(evidence)
}

const changeParentNodeSQL = `select operation,repository_id,thread_id,thread_number,length(cast(node_id as blob)),case when length(cast(node_id as blob))<=? then node_id end
from change_mutations where capture_id=? and table_name='threads' and encoded_key=?`
const inventoryParentNodeSQL = `select 'upsert',repository_id,thread_id,thread_number,length(cast(node_id as blob)),case when length(cast(node_id as blob))<=? then node_id end
from inventory_rows where table_name='threads' and encoded_key=?`

// Resolve the exact candidate parent through primary keys. A NULL node in an
// upsert overrides the old node; a removed parent must never fall back to it.
// This reads only compact metadata, with node bytes bounded before transfer.
func (d *reportChangeDelivery) parentNode(ctx context.Context, tx *sql.Tx, id, number, maxBytes int64) (sql.NullString, error) {
	value := strconv.FormatInt(id, 10)
	key, err := d.codec.EncodeKey("threads", []remote.ChangeValue{{Kind: "integer", Value: &value}})
	if err != nil {
		return sql.NullString{}, ErrSeedOutbox
	}
	var operation string
	var repository int64
	var thread, parentNumber, size sql.NullInt64
	var node sql.NullString
	dest := []any{&operation, &repository, &thread, &parentNumber, &size, &node}
	err = tx.QueryRowContext(ctx, changeParentNodeSQL, maxBytes, d.id, key).Scan(dest...)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, inventoryParentNodeSQL, maxBytes, key).Scan(dest...)
	}
	if err != nil {
		return sql.NullString{}, err
	}
	if operation != "upsert" || repository != d.capture.Through.RepositoryID || !thread.Valid || thread.Int64 != id || !parentNumber.Valid || parentNumber.Int64 != number {
		return sql.NullString{}, ErrSeedOutbox
	}
	if size.Valid && (size.Int64 < 0 || size.Int64 > maxBytes) {
		return sql.NullString{}, store.ErrNativeReportLimit
	}
	if size.Valid != node.Valid || (node.Valid && int64(len(node.String)) != size.Int64) {
		return sql.NullString{}, ErrSeedOutbox
	}
	return node, nil
}
