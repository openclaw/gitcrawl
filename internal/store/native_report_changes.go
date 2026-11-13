package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

type NativeReportKey struct {
	Table  string
	Values []NativeReportValue
}

// NativeReportRef is compact old inventory metadata, not a provider deletion
// assertion. NodeID uses SQLite's nativeReportNodeKeySQL interpretation; nil
// and an empty string differ. Unused parent fields are zero.
type NativeReportRef struct {
	Key                                  NativeReportKey
	RepositoryID, ThreadID, ThreadNumber int64
	NodeID                               *string
}

// NativeReportOldIndex is pinned by its outbox owner to request.Baseline for the
// entire call. Lookups are repository-scoped and yield synchronously, honor ctx
// and callback errors, and never retain callbacks or perform external effects.
// Requests contain at most 500 keys. Results are compact refs, never bodies.
// Thread lookups include the thread and its children.
type NativeReportOldIndex interface {
	Keys(context.Context, []NativeReportKey, func(NativeReportRef) error) error
	ThreadSubtrees(context.Context, []int64, func(NativeReportRef) error) error
	ThreadNumbers(context.Context, []int64, func(NativeReportRef) error) error
	Nodes(context.Context, []string, func(NativeReportRef) error) error
}

type NativeReportDeltaLimits struct {
	MaxJournalRecords, MaxAffectedKeys, MaxLookupRows, MaxRows, MaxNativeBytes int64
}

type NativeReportDeltaRequest struct {
	Baseline NativeReportAnchor
	Limits   NativeReportDeltaLimits
}

type NativeReportDeltaRow struct {
	Key    NativeReportKey
	Values []NativeReportValue
}

type NativeReportDelta struct {
	From, Through                           NativeReportAnchor
	Upserts                                 []NativeReportDeltaRow
	Removals                                []NativeReportRef
	JournalRecords, LookupRows, Rows, Bytes int64
}

// CaptureNativeReportChanges owns one native read transaction through journal,
// dependency expansion and payload reads. The caller pins the separate old
// index first and keeps the pathname stable, as for CaptureNativeReport.
// Rows counts upserts plus removals; Bytes bounds journal keys, lookup metadata
// (including repeated yields), and selected scalar payloads. These are logical
// work bounds, not an RSS guarantee. Nothing is staged or qualified here: the
// outbox owns digest comparison, inventory hashing and predecessor settlement.
// Every error returns a zero result, including a fan-out exceeding any limit.
func CaptureNativeReportChanges(ctx context.Context, path string, request NativeReportDeltaRequest, old NativeReportOldIndex) (NativeReportDelta, error) {
	if err := ctx.Err(); err != nil {
		return NativeReportDelta{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return NativeReportDelta{}, errors.New("native report changes require a deadline")
	}
	l := request.Limits
	if l.MaxJournalRecords <= 0 || l.MaxAffectedKeys <= 0 || l.MaxLookupRows <= 0 || l.MaxRows <= 0 || l.MaxNativeBytes <= 0 {
		return NativeReportDelta{}, ErrNativeReportLimit
	}
	file, err := NativeReportFileIdentity(path)
	if err != nil || file != request.Baseline.File || old == nil {
		return NativeReportDelta{}, ErrNativeReportSource
	}
	source, err := OpenReadOnly(ctx, path)
	if err != nil {
		return NativeReportDelta{}, err
	}
	defer source.Close()
	tx, err := source.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return NativeReportDelta{}, err
	}
	defer tx.Rollback()
	c := nativeReportChanges{ctx: ctx, tx: tx, old: old, limits: l,
		out:  NativeReportDelta{From: request.Baseline, Through: request.Baseline},
		keys: make(map[string]NativeReportKey), prior: make(map[string]NativeReportRef), current: make(map[string]NativeReportRef),
		threads: make(map[int64]bool), forceThreads: make(map[int64]bool), numbers: make(map[int64]bool), nodes: make(map[string]bool)}
	if err := c.capture(); err != nil {
		return NativeReportDelta{}, err
	}
	after, err := NativeReportFileIdentity(path)
	if err != nil || after != file {
		return NativeReportDelta{}, ErrNativeReportSource
	}
	if err := ctx.Err(); err != nil {
		return NativeReportDelta{}, err
	}
	return c.out, nil
}

type nativeReportChanges struct {
	ctx              context.Context
	tx               *sql.Tx
	old              NativeReportOldIndex
	limits           NativeReportDeltaLimits
	out              NativeReportDelta
	keys             map[string]NativeReportKey
	prior            map[string]NativeReportRef
	current          map[string]NativeReportRef
	threads, numbers map[int64]bool
	forceThreads     map[int64]bool
	nodes            map[string]bool
}

func (c *nativeReportChanges) capture() error {
	if err := checkNativeReportDeliveryAnchor(c.ctx, c.tx, c.out.From); err != nil {
		return err
	}
	if err := c.tx.QueryRowContext(c.ctx, `select value,pruned_through from change_contract_sequence where id=1`).Scan(&c.out.Through.Cursor.Sequence, &c.out.Through.Cursor.PrunedThrough); err != nil {
		return err
	}
	if c.out.From.Cursor.Sequence < c.out.Through.Cursor.PrunedThrough || c.out.Through.Cursor.PrunedThrough < c.out.From.Cursor.PrunedThrough {
		return ErrNativeReportSource
	}
	for _, spec := range nativeReportTables {
		if _, err := nativeReportTableDefinition(c.ctx, c.tx, spec); err != nil {
			return err
		}
	}
	// Ordinary open never builds these indexes. Verify the expression, not just
	// the name: an unrelated same-name index is insufficient.
	for _, table := range []string{"threads", "comments"} {
		parent := "repo_id"
		if table == "comments" {
			parent = "thread_id"
		}
		name := "idx_" + table + "_report_node"
		var ddl string
		if err := c.tx.QueryRowContext(c.ctx, `select sql from sqlite_schema where type='index' and tbl_name=? and name=?`, table, name).Scan(&ddl); err != nil {
			return ErrNativeReportSource
		}
		prefix := "CREATE INDEX "
		if len(ddl) < len(prefix) || !strings.EqualFold(ddl[:len(prefix)], prefix) || ddl[len(prefix):] != name+" on "+table+"("+nativeReportNodeKeySQL+", "+parent+")" {
			return ErrNativeReportSource
		}
	}
	if err := c.journal(); err != nil {
		return err
	}
	if err := c.expand(); err != nil {
		return err
	}
	for _, key := range c.sortedKeys() {
		if err := c.readRow(key); err != nil {
			return err
		}
	}
	return nil
}

func (c *nativeReportChanges) bytes(n int64) error {
	if n < 0 || n > c.limits.MaxNativeBytes-c.out.Bytes {
		return ErrNativeReportLimit
	}
	c.out.Bytes += n
	return nil
}

func nativeReportKeyID(key NativeReportKey) (string, error) {
	var kinds []NativeReportValueKind
	switch key.Table {
	case "repositories", "threads", "comments":
		kinds = []NativeReportValueKind{NativeReportInteger}
	case "thread_child_observation_reservations", "thread_child_observation_memberships":
		kinds = []NativeReportValueKind{NativeReportInteger, NativeReportText}
	case "thread_exclusions":
		kinds = []NativeReportValueKind{NativeReportText, NativeReportInteger}
	case "thread_excluded_nodes":
		kinds = []NativeReportValueKind{NativeReportText}
	default:
		return "", ErrNativeReportSource
	}
	if len(key.Values) != len(kinds) {
		return "", ErrNativeReportSource
	}
	var b strings.Builder
	b.WriteString(key.Table)
	b.WriteByte(0)
	var buf [8]byte
	for i, v := range key.Values {
		if v.Kind != kinds[i] || (v.Kind == NativeReportInteger && v.Text != "") || (v.Kind == NativeReportText && v.Integer != 0) {
			return "", ErrNativeReportSource
		}
		b.WriteByte(byte(v.Kind))
		if v.Kind == NativeReportInteger {
			binary.BigEndian.PutUint64(buf[:], uint64(v.Integer))
			b.Write(buf[:])
		} else {
			binary.BigEndian.PutUint64(buf[:], uint64(len(v.Text)))
			b.Write(buf[:])
			b.WriteString(v.Text)
		}
	}
	return b.String(), nil
}

func (c *nativeReportChanges) add(key NativeReportKey) error {
	id, err := nativeReportKeyID(key)
	if err != nil {
		return err
	}
	if _, ok := c.keys[id]; !ok {
		if int64(len(c.keys)) >= c.limits.MaxAffectedKeys {
			return ErrNativeReportLimit
		}
		c.keys[id] = key
	}
	return c.ctx.Err()
}

func (c *nativeReportChanges) sortedKeys() []NativeReportKey {
	ids := make([]string, 0, len(c.keys))
	for id := range c.keys {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	keys := make([]NativeReportKey, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, c.keys[id])
	}
	return keys
}

func (c *nativeReportChanges) journal() error {
	// Preflight bounded keys before materializing even one TEXT node key. Count
	// all row-log records; table-mode global sequence gaps are legitimate.
	limit := c.limits.MaxJournalRecords
	if limit < int64(^uint64(0)>>1) {
		limit++
	}
	args := []any{c.out.From.Cursor.Sequence, c.out.Through.Cursor.Sequence, limit}
	where := ` from change_contract_log where seq>? and seq<=? order by seq limit ?`
	var n, size int64
	if err := c.tx.QueryRowContext(c.ctx, `select count(*),coalesce(sum(case typeof(row_key) when 'null' then 1 when 'integer' then 9 else 9+length(cast(row_key as blob)) end),0) from (select row_key`+where+`)`, args...).Scan(&n, &size); err != nil {
		return err
	}
	if n > c.limits.MaxJournalRecords {
		return ErrNativeReportLimit
	}
	if err := c.bytes(size); err != nil {
		return err
	}
	rows, err := c.tx.QueryContext(c.ctx, `select table_id,op,row_key`+where, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, op int64
		var raw any
		if err := rows.Scan(&id, &op, &raw); err != nil {
			return err
		}
		c.out.JournalRecords++
		if op < ChangeContractOpInsert || op > ChangeContractOpDelete {
			return ErrNativeReportSource
		}
		name := ""
		for _, spec := range changeContractTableSpecs {
			if spec.id == id {
				name = spec.name
				break
			}
		}
		if !slices.ContainsFunc(nativeReportTables, func(s nativeReportTableSpec) bool { return s.name == name }) {
			continue
		}
		key, err := nativeReportJournalKey(name, raw)
		if err != nil {
			return err
		}
		switch name {
		case "repositories":
			if key.Values[0].Integer != c.out.From.RepositoryID {
				continue
			}
		case "thread_child_observation_reservations", "thread_child_observation_memberships":
			if key.Values[1].Text != "comments" {
				continue
			}
		case "thread_exclusions":
			if key.Values[0].Text != c.out.From.Repository {
				continue
			}
			c.numbers[key.Values[1].Integer] = false
		case "thread_excluded_nodes":
			c.nodes[key.Values[0].Text] = false
		}
		if err := c.add(key); err != nil {
			return err
		}
	}
	return rows.Err()
}

func nativeReportJournalKey(table string, raw any) (NativeReportKey, error) {
	key := NativeReportKey{Table: table}
	composite := table == "thread_exclusions" || table == "thread_child_observation_reservations" || table == "thread_child_observation_memberships"
	if composite {
		s, ok := raw.(string)
		if !ok || !utf8.ValidString(s) {
			return key, ErrNativeReportSource
		}
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		var cells []any
		if d.Decode(&cells) != nil || d.Decode(new(any)) != io.EOF {
			return key, ErrNativeReportSource
		}
		for _, cell := range cells {
			v := NativeReportValue{}
			switch x := cell.(type) {
			case string:
				v.Kind, v.Text = NativeReportText, x
			case json.Number:
				n, err := strconv.ParseInt(string(x), 10, 64)
				if err != nil {
					return key, ErrNativeReportSource
				}
				v.Kind, v.Integer = NativeReportInteger, n
			default:
				return key, ErrNativeReportSource
			}
			key.Values = append(key.Values, v)
		}
	} else {
		switch v := raw.(type) {
		case int64:
			key.Values = []NativeReportValue{{Kind: NativeReportInteger, Integer: v}}
		case string:
			key.Values = []NativeReportValue{{Kind: NativeReportText, Text: v}}
		default:
			return key, ErrNativeReportSource
		}
	}
	_, err := nativeReportKeyID(key)
	return key, err
}

func (c *nativeReportChanges) ref(ref NativeReportRef, prior bool) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if c.out.LookupRows >= c.limits.MaxLookupRows {
		return ErrNativeReportLimit
	}
	c.out.LookupRows++
	// Charge variable metadata before encoding/copying it, including duplicates.
	if err := c.bytes(24); err != nil {
		return err
	}
	for _, v := range ref.Key.Values {
		if err := c.bytes(9); err != nil {
			return err
		}
		if err := c.bytes(int64(len(v.Text))); err != nil {
			return err
		}
	}
	if ref.NodeID != nil {
		if err := c.bytes(int64(len(*ref.NodeID))); err != nil {
			return err
		}
	}
	if ref.RepositoryID != c.out.From.RepositoryID {
		return ErrNativeReportSource
	}
	if err := c.add(ref.Key); err != nil {
		return err
	}
	id, _ := nativeReportKeyID(ref.Key)
	if prior {
		if previous, ok := c.prior[id]; ok && !nativeReportRefsEqual(previous, ref) {
			return ErrNativeReportSource
		}
		c.prior[id] = ref
	}
	if ref.Key.Table == "threads" {
		if ref.ThreadID != ref.Key.Values[0].Integer {
			return ErrNativeReportSource
		}
		if _, ok := c.threads[ref.ThreadID]; !ok {
			c.threads[ref.ThreadID] = false
		}
		if !prior {
			c.current[id] = ref
		}
		if _, affected := c.numbers[ref.ThreadNumber]; affected {
			c.forceThreads[ref.ThreadID] = true
		}
	}
	return nil
}

func nativeReportRefsEqual(a, b NativeReportRef) bool {
	return a.RepositoryID == b.RepositoryID && a.ThreadID == b.ThreadID && a.ThreadNumber == b.ThreadNumber && ((a.NodeID == nil && b.NodeID == nil) || (a.NodeID != nil && b.NodeID != nil && *a.NodeID == *b.NodeID))
}

func (c *nativeReportChanges) oldLookup(call func(func(NativeReportRef) error) error) error {
	var stopped error
	err := call(func(ref NativeReportRef) error {
		if stopped == nil {
			stopped = c.ref(ref, true)
		}
		return stopped
	})
	if stopped != nil {
		return stopped
	}
	return err
}

func (c *nativeReportChanges) expand() error {
	seen := make(map[string]bool)
	for {
		var pending []NativeReportKey
		for _, key := range c.sortedKeys() {
			id, _ := nativeReportKeyID(key)
			if !seen[id] {
				seen[id] = true
				pending = append(pending, key)
			}
		}
		for start := 0; start < len(pending); start += 500 {
			batch := pending[start:min(start+500, len(pending))]
			if err := c.oldLookup(func(yield func(NativeReportRef) error) error { return c.old.Keys(c.ctx, batch, yield) }); err != nil {
				return err
			}
			for _, key := range batch {
				if err := c.currentKey(key); err != nil {
					return err
				}
			}
		}
		numbers := nativeReportPendingInts(c.numbers)
		for start := 0; start < len(numbers); start += 500 {
			batch := numbers[start:min(start+500, len(numbers))]
			if err := c.oldLookup(func(yield func(NativeReportRef) error) error { return c.old.ThreadNumbers(c.ctx, batch, yield) }); err != nil {
				return err
			}
			for _, n := range batch {
				if err := c.nativeKeys("threads", `x.repo_id=? and x.number=?`, c.out.From.RepositoryID, n); err != nil {
					return err
				}
			}
		}
		var nodes []string
		for node, done := range c.nodes {
			if !done {
				nodes = append(nodes, node)
				c.nodes[node] = true
			}
		}
		slices.Sort(nodes)
		for start := 0; start < len(nodes); start += 500 {
			batch := nodes[start:min(start+500, len(nodes))]
			if err := c.oldLookup(func(yield func(NativeReportRef) error) error { return c.old.Nodes(c.ctx, batch, yield) }); err != nil {
				return err
			}
			for _, node := range batch {
				for _, table := range []string{"threads", "comments"} {
					if err := c.nativeKeys(table, nativeReportNodeKeySQL+`=?`, node); err != nil {
						return err
					}
				}
			}
		}
		threads := nativeReportPendingInts(c.threads)
		// A title/body edit cannot change descendant eligibility. Only changed
		// parent membership/identity or exclusion policy needs the whole subtree.
		threads = slices.DeleteFunc(threads, func(thread int64) bool {
			key := NativeReportKey{Table: "threads", Values: []NativeReportValue{{Kind: NativeReportInteger, Integer: thread}}}
			id, _ := nativeReportKeyID(key)
			old, hadOld := c.prior[id]
			current, hasCurrent := c.current[id]
			return !c.forceThreads[thread] && hadOld && hasCurrent && old.ThreadNumber == current.ThreadNumber
		})
		for start := 0; start < len(threads); start += 500 {
			batch := threads[start:min(start+500, len(threads))]
			if err := c.oldLookup(func(yield func(NativeReportRef) error) error { return c.old.ThreadSubtrees(c.ctx, batch, yield) }); err != nil {
				return err
			}
			for _, thread := range batch {
				for _, table := range []string{"comments", "thread_child_observation_reservations", "thread_child_observation_memberships"} {
					if err := c.nativeKeys(table, `x.thread_id=?`, thread); err != nil {
						return err
					}
				}
			}
		}
		if len(pending)+len(numbers)+len(nodes)+len(threads) == 0 {
			return nil
		}
	}
}

func nativeReportPendingInts(set map[int64]bool) []int64 {
	var out []int64
	for n, done := range set {
		if !done {
			out = append(out, n)
			set[n] = true
		}
	}
	slices.Sort(out)
	return out
}

func nativeReportSpec(table string) nativeReportTableSpec {
	for _, spec := range nativeReportTables {
		if spec.name == table {
			return spec
		}
	}
	panic("validated native report table missing")
}

func nativeReportKeyPredicate(key NativeReportKey) (string, []any) {
	columns := strings.Fields(nativeReportSpec(key.Table).keys)
	where := make([]string, len(columns))
	args := make([]any, len(columns))
	for i, column := range columns {
		where[i] = "x." + sqliteQuotedIdentifier(column) + "=?"
		v := key.Values[i]
		if v.Kind == NativeReportInteger {
			args[i] = v.Integer
		} else {
			args[i] = v.Text
		}
	}
	return strings.Join(where, " and "), args
}

func (c *nativeReportChanges) currentKey(key NativeReportKey) error {
	predicate, args := nativeReportKeyPredicate(key)
	return c.nativeKeys(key.Table, predicate, args...)
}

// nativeKeys reads fixed keys and integer parent metadata. Variable TEXT keys
// are equality-bound inputs or the literal comments family, never payloads.
func (c *nativeReportChanges) nativeKeys(table, predicate string, args ...any) error {
	keys := strings.Fields(nativeReportSpec(table).keys)
	columns := make([]string, len(keys))
	for i, key := range keys {
		columns[i] = "x." + sqliteQuotedIdentifier(key)
	}
	from, scope, parent := table+" x", "", "0,0"
	switch table {
	case "repositories":
		scope = "x.id=?"
	case "threads":
		scope, parent = "x.repo_id=?", "x.id,x.number"
	case "comments", "thread_child_observation_reservations", "thread_child_observation_memberships":
		from += " join threads t on t.id=x.thread_id"
		scope, parent = "t.repo_id=?", "t.id,t.number"
		if table != "comments" {
			scope += " and x.family='comments'"
		}
	case "thread_exclusions", "thread_excluded_nodes":
		scope, parent = "x.repository=(select lower(full_name) from repositories where id=?)", "0,x.number"
	}
	// Alias qualification retains the exact expression-index semantics.
	if strings.Contains(predicate, nativeReportNodeKeySQL) {
		predicate = strings.ReplaceAll(predicate, "raw_json", "x.raw_json")
		// The admission check above pins this expression index. Force this access
		// path: stale statistics must not choose a parent-first raw-content scan.
		from = strings.Replace(from, table+" x", table+" x indexed by idx_"+table+"_report_node", 1)
	}
	remaining := c.limits.MaxLookupRows - c.out.LookupRows
	if remaining < int64(^uint64(0)>>1) {
		remaining++
	}
	args = append(args, c.out.From.RepositoryID, remaining)
	// Output is sorted after closure. Do not sort this lookup: that could visit
	// an entire high-fan-out subtree before its LIMIT can stop the scan.
	query := "select " + strings.Join(columns, ",") + "," + parent + " from " + from + " where " + predicate + " and " + scope + " limit ?"
	rows, err := c.tx.QueryContext(c.ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		ref := NativeReportRef{RepositoryID: c.out.From.RepositoryID, Key: NativeReportKey{Table: table}}
		raw := make([]any, len(keys))
		dest := make([]any, 0, len(keys)+2)
		for i := range raw {
			dest = append(dest, &raw[i])
		}
		dest = append(dest, &ref.ThreadID, &ref.ThreadNumber)
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		for _, value := range raw {
			switch v := value.(type) {
			case string:
				ref.Key.Values = append(ref.Key.Values, NativeReportValue{Kind: NativeReportText, Text: v})
			case int64:
				ref.Key.Values = append(ref.Key.Values, NativeReportValue{Kind: NativeReportInteger, Integer: v})
			default:
				return ErrNativeReportSource
			}
		}
		if err := c.ref(ref, false); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *nativeReportChanges) readRow(key NativeReportKey) error {
	spec := nativeReportSpec(key.Table)
	names := strings.Fields(spec.columns)
	columns, sizes := make([]string, len(names)), make([]string, len(names))
	for i, name := range names {
		columns[i] = "x." + sqliteQuotedIdentifier(name)
		sizes[i] = "case typeof(" + columns[i] + ") when 'null' then 1 when 'integer' then 9 else 9+length(cast(" + columns[i] + " as blob)) end"
	}
	predicate, keyArgs := nativeReportKeyPredicate(key)
	from := spec.from
	// Correlate eligibility to the selected key. An uncorrelated IN subquery
	// could enumerate every scoped thread again for every changed payload.
	if key.Table == "threads" {
		from = "threads x where exists(" + nativeReportThreads + " and t.id=x.id)"
	} else if key.Table == "comments" || key.Table == "thread_child_observation_reservations" || key.Table == "thread_child_observation_memberships" {
		from = key.Table + " x where exists(" + nativeReportThreads + " and t.id=x.thread_id)"
		if key.Table != "comments" {
			from += " and x.family='comments'"
		}
	}
	from += " and " + predicate
	args := append([]any{c.out.From.RepositoryID}, keyArgs...)
	var count, size int64
	if err := c.tx.QueryRowContext(c.ctx, "select count(*),coalesce(sum("+strings.Join(sizes, "+")+"),0) from "+from, args...).Scan(&count, &size); err != nil {
		return err
	}
	if count == 0 {
		id, _ := nativeReportKeyID(key)
		if prior, ok := c.prior[id]; ok {
			if c.out.Rows >= c.limits.MaxRows {
				return ErrNativeReportLimit
			}
			c.out.Removals = append(c.out.Removals, prior)
			c.out.Rows++
		}
		return nil
	}
	if count != 1 {
		return ErrNativeReportSource
	}
	if c.out.Rows >= c.limits.MaxRows {
		return ErrNativeReportLimit
	}
	if err := c.bytes(size); err != nil {
		return err
	}
	if key.Table == "threads" || key.Table == "comments" {
		var conflict bool
		if err := c.tx.QueryRowContext(c.ctx, "select exists(select 1 from "+from+` and exists(select 1 from thread_excluded_nodes e where e.node_id=json_extract(case when json_valid(x.raw_json) then x.raw_json else '{}' end,'$.node_id')))`, args...).Scan(&conflict); err != nil {
			return err
		}
		if conflict {
			return ErrNativeReportSource
		}
	}
	raw, dest := make([]any, len(columns)), make([]any, len(columns))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := c.tx.QueryRowContext(c.ctx, "select "+strings.Join(columns, ",")+" from "+from, args...).Scan(dest...); err != nil {
		return err
	}
	row := NativeReportDeltaRow{Key: key, Values: make([]NativeReportValue, len(raw))}
	for i, value := range raw {
		switch v := value.(type) {
		case nil:
		case int64:
			row.Values[i] = NativeReportValue{Kind: NativeReportInteger, Integer: v}
		case string:
			row.Values[i] = NativeReportValue{Kind: NativeReportText, Text: v}
		default:
			return ErrNativeReportSource
		}
		if row.Values[i].Kind == NativeReportNull && slices.Contains(strings.Fields(spec.keys), names[i]) {
			return ErrNativeReportSource
		}
		if key.Table == "comments" && names[i] == "raw_json_blob_id" && row.Values[i].Kind != NativeReportNull {
			return ErrNativeReportBlob
		}
	}
	c.out.Upserts = append(c.out.Upserts, row)
	c.out.Rows++
	return nil
}
