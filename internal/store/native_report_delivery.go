package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
)

// NativeReportDeliveryEvidence contains references from staged content, not
// exclusion-ledger metadata. The caller resolves parent numbers from the staged
// inventory and supplies raw JSON only from its threads/comments.
type NativeReportDeliveryEvidence struct {
	ThreadNumbers []int64
	RawJSON       []string
}

// MaxRows counts every number and raw JSON item, including repeated references.
// MaxBytes counts raw JSON bytes across all yields, not allocations or RSS.
type NativeReportDeliveryLimits struct{ MaxRows, MaxBytes int64 }

// CheckNativeReportDelivery checks all supplied references in one current
// read-only snapshot. walk must call yield synchronously, stop on error, and
// honor ctx; it must not retain yield or perform network effects. The transaction
// is closed before return. Success is a policy observation, not a reusable grant
// or an acquisition/delivery checkpoint. The source pathname must remain bound
// to one physical file during the call, as required by CaptureNativeReport.
func CheckNativeReportDelivery(ctx context.Context, path string, anchor NativeReportAnchor, limits NativeReportDeliveryLimits, walk func(yield func(NativeReportDeliveryEvidence) error) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("native report delivery check requires a deadline")
	}
	if limits.MaxRows <= 0 || limits.MaxBytes <= 0 {
		return ErrNativeReportLimit
	}
	if walk == nil {
		return ErrNativeReportSource
	}
	file, err := NativeReportFileIdentity(path)
	if err != nil || file != anchor.File {
		return ErrNativeReportSource
	}
	source, err := OpenReadOnly(ctx, path)
	if err != nil {
		return err
	}
	defer source.Close()
	tx, err := source.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := checkNativeReportDeliveryAnchor(ctx, tx, anchor); err != nil {
		return err
	}
	var rows, rawBytes int64
	var yieldErr error
	err = walk(func(e NativeReportDeliveryEvidence) error {
		if yieldErr != nil {
			return yieldErr
		}
		yieldErr = func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			for _, count := range []int{len(e.ThreadNumbers), len(e.RawJSON)} {
				if int64(count) > limits.MaxRows-rows {
					return ErrNativeReportLimit
				}
				rows += int64(count)
			}
			for _, raw := range e.RawJSON {
				if int64(len(raw)) > limits.MaxBytes-rawBytes {
					return ErrNativeReportLimit
				}
				rawBytes += int64(len(raw))
			}
			for _, number := range e.ThreadNumbers {
				if number <= 0 {
					return ErrNativeReportSource
				}
			}
			return checkNativeDeliveryEvidence(ctx, tx, anchor.Repository, e)
		}()
		return yieldErr
	})
	if yieldErr != nil {
		return yieldErr
	}
	if err != nil {
		return err
	}
	after, err := NativeReportFileIdentity(path)
	if err != nil || after != file {
		return ErrNativeReportSource
	}
	return ctx.Err()
}

func checkNativeDeliveryEvidence(ctx context.Context, tx *sql.Tx, repository string, e NativeReportDeliveryEvidence) error {
	// Keep parameters below SQLite's baseline limit and avoid one SQL round trip
	// per archived row. Bound strings directly: JSON re-encoding could replace
	// invalid UTF-8 and change the native node interpretation.
	const batchSize = 500
	for start := 0; start < len(e.ThreadNumbers); start += batchSize {
		end := min(start+batchSize, len(e.ThreadNumbers))
		args := make([]any, 1, end-start+1)
		args[0] = repository
		for _, number := range e.ThreadNumbers[start:end] {
			args = append(args, number)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		var excluded bool
		if err := tx.QueryRowContext(ctx, `select exists(select 1 from thread_exclusions where repository=lower(?) and number in (`+placeholders+`))`, args...).Scan(&excluded); err != nil {
			return err
		}
		if excluded {
			return ErrThreadExcluded
		}
	}
	for start := 0; start < len(e.RawJSON); start += batchSize {
		end := min(start+batchSize, len(e.RawJSON))
		args := make([]any, 0, end-start)
		for _, raw := range e.RawJSON[start:end] {
			args = append(args, raw)
		}
		values := strings.TrimSuffix(strings.Repeat("(?),", end-start), ",")
		// Match the native purge/capture owner's top-level node interpretation.
		// Nodes are excluded globally, including policy owned by other repos.
		query := `with evidence(raw) as (values ` + values + `) select exists(select 1 from thread_excluded_nodes where node_id in (select json_extract(case when json_valid(raw) then raw else '{}' end,'$.node_id') from evidence))`
		var excluded bool
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&excluded); err != nil {
			return err
		}
		if excluded {
			return ErrThreadExcluded
		}
	}
	return nil
}

func checkNativeReportDeliveryAnchor(ctx context.Context, tx *sql.Tx, anchor NativeReportAnchor) error {
	parts := strings.Split(anchor.Repository, "/")
	c := anchor.Cursor
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(anchor.Repository, " \t\r\n\\\x00") || strings.ToLower(anchor.Repository) != anchor.Repository || anchor.RepositoryID <= 0 || c.Epoch == "" || c.Generation < 1 || c.SchemaVersion < 1 || c.Sequence < 0 || c.PrunedThrough < 0 || c.PrunedThrough > c.Sequence {
		return ErrNativeReportSource
	}
	scope := sha256.New()
	nativeReportHashText(scope, NativeReportProfile)
	nativeReportHashText(scope, anchor.Repository)
	if hex.EncodeToString(scope.Sum(nil)) != anchor.ScopeSHA256 {
		return ErrNativeReportSource
	}
	var version, cookie int64
	if err := tx.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `pragma schema_version`).Scan(&cookie); err != nil {
		return err
	}
	portable, err := changeContractTablePresent(ctx, tx, "portable_metadata")
	if err != nil {
		return err
	}
	if version != schemaVersion || cookie != c.SchemaVersion || portable {
		return ErrNativeReportSource
	}
	var current NativeReportCursor
	if err := tx.QueryRowContext(ctx, `select s.contract_version,s.epoch,s.generation,q.value,q.pruned_through from change_contract_state s join change_contract_sequence q on q.id=s.id where s.id=1`).Scan(&version, &current.Epoch, &current.Generation, &current.Sequence, &current.PrunedThrough); err != nil {
		return ErrNativeReportSource
	}
	if version != ChangeContractVersion || current.Epoch != c.Epoch || current.Generation != c.Generation || current.Sequence < c.Sequence || current.PrunedThrough < 0 || current.PrunedThrough > current.Sequence {
		return ErrNativeReportSource
	}
	armed, err := changeContractArmed(ctx, tx)
	if err != nil {
		return err
	}
	if !armed {
		return ErrNativeReportSource
	}
	var count, repoID int64
	if err := tx.QueryRowContext(ctx, `select count(*),coalesce(min(id),0) from repositories where lower(full_name)=?`, anchor.Repository).Scan(&count, &repoID); err != nil {
		return err
	}
	if count != 1 || repoID != anchor.RepositoryID {
		return ErrNativeReportSource
	}
	return nil
}
