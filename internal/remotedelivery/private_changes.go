package remotedelivery

import (
	"context"
	"errors"
	"strconv"

	"github.com/openclaw/crawlkit/remote"
)

var (
	ErrPrivateChanges      = errors.New("private changes require a supported native range and fixed profile")
	ErrPrivateChangesLimit = errors.New("private changes exceed request byte or part limits")
)

type PrivateChangesRequest struct {
	Archive, BindingID, CaptureID             string
	NativeFrom, NativeThrough, ExpectedCursor string
	MaxBytes                                  int64
	MaxParts                                  int
}

// PrivateChanges contains exact request bodies, not staged or acknowledged
// state. The caller owns current source policy and receipt-qualified custody.
type PrivateChanges struct {
	Parts [][]byte
	Bytes int64
}

// PreparePrivateChanges preserves the supplied deterministic observation order.
// Inputs must remain unchanged during the call. It rejects repeated native keys
// rather than choosing a winner and emits one empty part for an empty interval.
// No source, outbox, network, credentials or acquisition state is accessed.
func PreparePrivateChanges(ctx context.Context, changes []remote.Change, request PrivateChangesRequest) (PrivateChanges, error) {
	if err := ctx.Err(); err != nil {
		return PrivateChanges{}, err
	}
	if request.MaxBytes <= 0 || request.MaxParts <= 0 || request.MaxParts > remote.MaxChangeParts || len(changes) > request.MaxParts*remote.MaxChanges {
		return PrivateChanges{}, ErrPrivateChangesLimit
	}
	if request.ExpectedCursor == "" {
		return PrivateChanges{}, ErrPrivateChanges
	}
	from, err := remote.ParseGitHubNativeAnchor(request.NativeFrom)
	if err != nil {
		return PrivateChanges{}, ErrPrivateChanges
	}
	through, err := remote.ParseGitHubNativeAnchor(request.NativeThrough)
	if err != nil || remote.ValidateGitHubNativeRange(from, through) != nil {
		return PrivateChanges{}, ErrPrivateChanges
	}
	batch := remote.ChangeBatch{Contract: remote.ChangeContract, App: "gitcrawl", Archive: request.Archive, BindingID: request.BindingID,
		CaptureID: request.CaptureID, NativeFrom: request.NativeFrom, NativeThrough: request.NativeThrough,
		ExpectedCursor: request.ExpectedCursor, PartCount: 1, Changes: []remote.Change{}}
	if _, err := remote.EncodeChangeBatch(batch); err != nil {
		return PrivateChanges{}, ErrPrivateChanges
	}
	codec, err := privateInventoryCodec()
	if err != nil {
		return PrivateChanges{}, err
	}
	// Reserve the largest legal envelope while grouping rows. A nearly-full
	// request may be rejected conservatively; we never exceed the wire limit or
	// repeatedly encode growing prefixes to discover where a part ends.
	probe := batch
	probe.PartIndex, probe.PartCount = remote.MaxChangeParts-1, remote.MaxChangeParts
	previous := "chg1:" + request.CaptureID + ":" + strconv.Itoa(remote.MaxChangeParts-2)
	if len(previous) > len(probe.ExpectedCursor) {
		probe.ExpectedCursor = previous
	}
	empty, err := remote.EncodeChangeBatch(probe)
	if err != nil {
		return PrivateChanges{}, ErrPrivateChanges
	}
	header, used := len(empty), len(empty)
	groups := [][]remote.Change{{}}
	seen := make(map[string]struct{}, len(changes))
	var payloadBytes int64
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return PrivateChanges{}, err
		}
		probe.Changes = []remote.Change{change}
		body, err := remote.EncodeChangeBatch(probe)
		if err != nil {
			return PrivateChanges{}, ErrPrivateChanges
		}
		rowBytes := len(body) - header
		// Row bytes alone are a lower bound on the final requests. Stop before
		// retaining or fingerprinting more rows once that budget is exhausted.
		if int64(rowBytes) > request.MaxBytes-payloadBytes {
			return PrivateChanges{}, ErrPrivateChangesLimit
		}
		payloadBytes += int64(rowBytes)
		key, err := codec.EncodeKey(change.Table, change.Key)
		if err != nil {
			return PrivateChanges{}, ErrPrivateChanges
		}
		identity := change.Table + "\x00" + string(key)
		if _, duplicate := seen[identity]; duplicate {
			return PrivateChanges{}, ErrPrivateChanges
		}
		seen[identity] = struct{}{}
		if change.Operation == "upsert" {
			if _, err := codec.RowDigest(change.Table, change.Key, change.Row); err != nil {
				return PrivateChanges{}, ErrPrivateChanges
			}
			if change.Table == "comments" && change.Row[deliveryColumn(change.Table, "raw_json_blob_id")].Kind != "null" {
				return PrivateChanges{}, ErrPrivateChanges
			}
			if change.Table == "thread_child_observation_reservations" || change.Table == "thread_child_observation_memberships" {
				family := change.Row[deliveryColumn(change.Table, "family")]
				if family.Kind != "text" || family.Value == nil || *family.Value != "comments" {
					return PrivateChanges{}, ErrPrivateChanges
				}
			}
		}
		last := len(groups) - 1
		separator := 0
		if len(groups[last]) > 0 {
			separator = 1
		}
		if len(groups[last]) == remote.MaxChanges || rowBytes+separator > remote.MaxChangeBytes-used {
			if len(groups) == request.MaxParts {
				return PrivateChanges{}, ErrPrivateChangesLimit
			}
			groups = append(groups, nil)
			last++
			used, separator = header, 0
		}
		if rowBytes > remote.MaxChangeBytes-header {
			return PrivateChanges{}, ErrPrivateChangesLimit
		}
		groups[last] = append(groups[last], change)
		used += separator + rowBytes
	}
	out := PrivateChanges{Parts: make([][]byte, 0, len(groups))}
	for i, group := range groups {
		if err := ctx.Err(); err != nil {
			return PrivateChanges{}, err
		}
		batch.PartIndex, batch.PartCount, batch.Changes = i, len(groups), group
		body, err := remote.EncodeChangeBatch(batch)
		if err != nil {
			return PrivateChanges{}, ErrPrivateChanges
		}
		if int64(len(body)) > request.MaxBytes-out.Bytes {
			return PrivateChanges{}, ErrPrivateChangesLimit
		}
		out.Parts = append(out.Parts, body)
		out.Bytes += int64(len(body))
		batch.ExpectedCursor = batch.BatchID()
	}
	if err := ctx.Err(); err != nil {
		return PrivateChanges{}, err
	}
	return out, nil
}
