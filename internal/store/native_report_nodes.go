package store

import (
	"context"
	"errors"
)

// Match TEXT affinity on thread_excluded_nodes.node_id, including SQLite's
// numeric JSON spelling. Reverse policy lookups must use this same expression
// as the indexes, rather than parsing or normalizing provider JSON in Go.
const nativeReportNodeKeySQL = `cast(json_extract(case when json_valid(raw_json) then raw_json else '{}' end,'$.node_id') as text)`

// PrepareNativeReportIndexes explicitly builds the reverse policy lookup
// indexes before a report baseline is captured. It never runs on ordinary open:
// a large archive needs an operator-selected preparation window and deadline.
// Index DDL changes the schema cookie, so older capture anchors are invalidated.
func (s *Store) PrepareNativeReportIndexes(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("native report index preparation requires a deadline")
	}
	return s.WithTx(ctx, func(tx *Store) error {
		var version int
		if err := tx.q().QueryRowContext(ctx, "pragma user_version").Scan(&version); err != nil {
			return err
		}
		portable, err := changeContractTablePresent(ctx, tx.q(), "portable_metadata")
		if err != nil {
			return err
		}
		if version != schemaVersion || portable {
			return ErrNativeReportSource
		}
		_, err = tx.q().ExecContext(ctx, `
create index if not exists idx_threads_report_node on threads(`+nativeReportNodeKeySQL+`, repo_id);
create index if not exists idx_comments_report_node on comments(`+nativeReportNodeKeySQL+`, thread_id);`)
		return err
	})
}
