package remotedelivery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

// WithQualifiedReportIndex pins the current qualified baseline and its compact
// index for one synchronous callback. The view expires when the callback ends.
// Capture may open its own native read transaction inside the callback; network
// effects and staging must wait until both read transactions have closed.
func (o *Outbox) WithQualifiedReportIndex(ctx context.Context, use func(ReportOutboxState, store.NativeReportOldIndex) error) error {
	if _, ok := ctx.Deadline(); !ok || use == nil {
		return ErrSeedOutbox
	}
	return o.base.WithTx(ctx, func(tx *sql.Tx) error {
		s, err := readReportState(ctx, tx)
		if err != nil {
			return err
		}
		codec, err := privateInventoryCodec()
		if err != nil {
			return err
		}
		return use(s, &qualifiedReportIndex{tx: tx, codec: codec, repositoryID: s.BaselineAnchor.RepositoryID})
	})
}

type qualifiedReportIndex struct {
	tx           *sql.Tx
	codec        *remote.InventoryCodec
	repositoryID int64
}

func (v *qualifiedReportIndex) Keys(ctx context.Context, keys []store.NativeReportKey, yield func(store.NativeReportRef) error) error {
	if len(keys) > 500 || yield == nil {
		return ErrSeedOutbox
	}
	groups := make(map[string][]any)
	for _, key := range keys {
		wire, err := inventoryNativeKey(key)
		if err != nil {
			return err
		}
		encoded, err := v.codec.EncodeKey(key.Table, wire)
		if err != nil {
			return ErrSeedOutbox
		}
		groups[key.Table] = append(groups[key.Table], encoded)
	}
	// Bound key lookups to each fixed-profile table. Capture sorts final keys.
	for _, layout := range privateLayouts {
		keys := groups[layout.name]
		if len(keys) == 0 {
			continue
		}
		args := append([]any{layout.name}, keys...)
		if err := v.read(ctx, "", "table_name=? and encoded_key in ("+inventoryPlaceholders(len(keys))+")", args, yield); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (v *qualifiedReportIndex) ThreadSubtrees(ctx context.Context, ids []int64, yield func(store.NativeReportRef) error) error {
	return v.threads(ctx, "thread_id", ids, yield)
}

func (v *qualifiedReportIndex) ThreadNumbers(ctx context.Context, numbers []int64, yield func(store.NativeReportRef) error) error {
	return v.threads(ctx, "thread_number", numbers, yield)
}

func (v *qualifiedReportIndex) threads(ctx context.Context, column string, ids []int64, yield func(store.NativeReportRef) error) error {
	if len(ids) > 500 || yield == nil {
		return ErrSeedOutbox
	}
	if len(ids) == 0 {
		return ctx.Err()
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return v.read(ctx, "inventory_"+column, "table_name in ('threads','comments','thread_child_observation_reservations','thread_child_observation_memberships') and "+column+" in ("+inventoryPlaceholders(len(ids))+")", args, yield)
}

func (v *qualifiedReportIndex) Nodes(ctx context.Context, nodes []string, yield func(store.NativeReportRef) error) error {
	if len(nodes) > 500 || yield == nil {
		return ErrSeedOutbox
	}
	if len(nodes) == 0 {
		return ctx.Err()
	}
	args := make([]any, len(nodes))
	for i, node := range nodes {
		args[i] = node
	}
	return v.read(ctx, "inventory_node_id", "table_name in ('threads','comments') and node_id in ("+inventoryPlaceholders(len(nodes))+")", args, yield)
}

func inventoryPlaceholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

func (v *qualifiedReportIndex) read(ctx context.Context, index, predicate string, args []any, yield func(store.NativeReportRef) error) error {
	// Stream indexed matches so the caller can stop at its lookup budget. A
	// global ordering can sort an entire high-fan-out subtree before yielding.
	from := "inventory_rows"
	if index != "" {
		// These names come only from the fixed lookup methods above.
		from += " indexed by " + index
	}
	rows, err := v.tx.QueryContext(ctx, `select table_name,encoded_key,key_json,repository_id,coalesce(thread_id,0),coalesce(thread_number,0),node_id from `+from+` where `+predicate, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var ref store.NativeReportRef
		var encoded, keyJSON []byte
		var node sql.NullString
		if err := rows.Scan(&ref.Key.Table, &encoded, &keyJSON, &ref.RepositoryID, &ref.ThreadID, &ref.ThreadNumber, &node); err != nil {
			return err
		}
		if ref.RepositoryID != v.repositoryID {
			return ErrSeedOutbox
		}
		var values []remote.ChangeValue
		if json.Unmarshal(keyJSON, &values) != nil {
			return ErrSeedOutbox
		}
		canonical, err := v.codec.EncodeKey(ref.Key.Table, values)
		if err != nil || !bytes.Equal(canonical, encoded) {
			return ErrSeedOutbox
		}
		for _, value := range values {
			native := store.NativeReportValue{}
			switch value.Kind {
			case "text":
				native.Kind, native.Text = store.NativeReportText, *value.Value
			case "integer":
				native.Kind = store.NativeReportInteger
				native.Integer, err = strconv.ParseInt(*value.Value, 10, 64)
				if err != nil {
					return ErrSeedOutbox
				}
			default:
				return ErrSeedOutbox
			}
			ref.Key.Values = append(ref.Key.Values, native)
		}
		if node.Valid {
			ref.NodeID = &node.String
		}
		if err := yield(ref); err != nil {
			return err
		}
	}
	return rows.Err()
}

func inventoryNativeKey(key store.NativeReportKey) ([]remote.ChangeValue, error) {
	var layout *privateLayout
	for i := range privateLayouts {
		if privateLayouts[i].name == key.Table {
			layout = &privateLayouts[i]
			break
		}
	}
	if layout == nil {
		return nil, ErrSeedOutbox
	}
	columns := strings.Fields(layout.keys)
	if len(key.Values) != len(columns) {
		return nil, ErrSeedOutbox
	}
	out := make([]remote.ChangeValue, len(columns))
	for i, value := range key.Values {
		converted, err := privateValue(value, strings.Contains(" "+layout.integers+" ", " "+columns[i]+" "))
		if err != nil {
			return nil, err
		}
		out[i] = remote.ChangeValue{Kind: converted.Kind, Value: converted.Value}
	}
	return out, nil
}
