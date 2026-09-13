package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

var portableThreadsCreateTablePattern = regexp.MustCompile("(?i)^(\\s*CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?)(?:\"threads\"|`threads`|\\[threads\\]|threads)(\\s*\\()")

type portableSchemaObject struct {
	name string
	sql  string
}

func (s *Store) rebuildPortableCompatibilityThreads(ctx context.Context) ([]string, error) {
	stats := &PortablePruneStats{}
	return s.rebuildPortableCompatibilityThreadsWithOptions(ctx, PortablePruneOptions{}, stats)
}

func (s *Store) rebuildPortableCompatibilityThreadsWithOptions(ctx context.Context, options PortablePruneOptions, stats *PortablePruneStats) (_ []string, retErr error) {
	if stats == nil {
		stats = &PortablePruneStats{}
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open portable threads rebuild connection: %w", err)
	}
	defer conn.Close()
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildPreflight)
	var createSQL string
	if err := conn.QueryRowContext(ctx, `select sql from sqlite_schema where type = 'table' and name = 'threads'`).Scan(&createSQL); err != nil {
		return nil, fmt.Errorf("read portable threads schema: %w", err)
	}
	sibling, err := portableThreadsSiblingName()
	if err != nil {
		return nil, err
	}
	siblingCreateSQL, err := rewritePortableThreadsCreateSQL(createSQL, sibling)
	if err != nil {
		return nil, err
	}
	columns, selectExpressions, verbatimColumns, err := portableThreadsRebuildColumns(ctx, conn)
	if err != nil {
		return nil, err
	}
	ordinaryIndexes, uniqueIndexes, err := portableThreadsRebuildIndexes(ctx, conn)
	if err != nil {
		return nil, err
	}
	triggers, err := portableThreadsRebuildTriggers(ctx, conn)
	if err != nil {
		return nil, err
	}
	hasConvergenceTrigger := false
	for _, trigger := range triggers {
		canonicalConvergence := false
		for _, definition := range observationConvergenceTriggers {
			if definition.table == "threads" && definition.name == trigger.name && trigger.sql == sqliteStoredSQL(observationConvergenceTriggerSQL(definition)) {
				canonicalConvergence = true
				hasConvergenceTrigger = true
				break
			}
		}
		if canonicalConvergence {
			continue
		}
		updatesThreads, err := portableTriggerUpdatesThreads(trigger.sql)
		if err != nil {
			return nil, fmt.Errorf("inspect portable threads trigger %s: %w", trigger.name, err)
		}
		if updatesThreads {
			return nil, fmt.Errorf("portable threads rebuild cannot preserve update-trigger semantics for %s", trigger.name)
		}
	}
	originalSequence, err := portableThreadsSequence(ctx, conn)
	if err != nil {
		return nil, err
	}
	if originalSequence.Valid && !strings.Contains(strings.ToUpper(createSQL), "AUTOINCREMENT") {
		return nil, fmt.Errorf("portable threads sqlite_sequence exists without AUTOINCREMENT schema")
	}
	originalIdentity, err := portableThreadsIdentityForTable(ctx, conn, "threads")
	if err != nil {
		return nil, err
	}
	originalThreadForeignKeys, err := portableForeignKeysForTable(ctx, conn, "threads")
	if err != nil {
		return nil, err
	}
	if err := validatePortableThreadsForeignKeyTransformSafety(originalThreadForeignKeys, verbatimColumns); err != nil {
		return nil, err
	}
	originalChildForeignKeys, err := portableChildForeignKeysReferencingThreads(ctx, conn)
	if err != nil {
		return nil, err
	}
	if err := validatePortableChildForeignKeyIdentityTargets(originalChildForeignKeys); err != nil {
		return nil, err
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildForeignKeys)
	checker := options.foreignKeyCheck
	if checker == nil {
		checker = portableForeignKeyViolationCount
	}
	violations, err := checker(ctx, conn)
	if err != nil {
		return nil, err
	}
	stats.ForeignKeyValidated = true
	stats.ForeignKeyViolations = violations
	if violations != 0 {
		return nil, fmt.Errorf("portable threads rebuild found %d foreign-key violations before rebuild", violations)
	}
	foreignKeysOff := false
	legacyAlterChanged := false
	var originalLegacyAlter int
	defer func() {
		if foreignKeysOff {
			if _, err := conn.ExecContext(context.Background(), `pragma foreign_keys = on`); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("restore portable threads foreign keys: %w", err))
			}
		}
		if legacyAlterChanged {
			if _, err := conn.ExecContext(context.Background(), fmt.Sprintf(`pragma legacy_alter_table = %d`, originalLegacyAlter)); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("restore portable threads legacy alter mode: %w", err))
			}
		}
	}()
	if err := conn.QueryRowContext(ctx, `pragma legacy_alter_table`).Scan(&originalLegacyAlter); err != nil {
		return nil, fmt.Errorf("read portable threads legacy alter mode: %w", err)
	}
	if originalLegacyAlter != 1 {
		if _, err := conn.ExecContext(ctx, `pragma legacy_alter_table = on`); err != nil {
			return nil, fmt.Errorf("enable portable threads legacy alter mode: %w", err)
		}
		legacyAlterChanged = true
	}
	var legacyAlter int
	if err := conn.QueryRowContext(ctx, `pragma legacy_alter_table`).Scan(&legacyAlter); err != nil {
		return nil, fmt.Errorf("verify portable threads legacy alter mode: %w", err)
	}
	if legacyAlter != 1 {
		return nil, fmt.Errorf("enable portable threads legacy alter mode: pragma remained %d", legacyAlter)
	}
	if _, err := conn.ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
		return nil, fmt.Errorf("disable portable threads foreign keys: %w", err)
	}
	foreignKeysOff = true
	var foreignKeys int
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&foreignKeys); err != nil {
		return nil, fmt.Errorf("verify disabled portable threads foreign keys: %w", err)
	}
	if foreignKeys != 0 {
		return nil, fmt.Errorf("disable portable threads foreign keys: pragma remained %d", foreignKeys)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin portable threads rebuild: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildCompactCopy)
	if _, err := tx.ExecContext(ctx, siblingCreateSQL); err != nil {
		return nil, fmt.Errorf("create portable threads sibling: %w", err)
	}
	insertSQL := `insert or abort into ` + sqliteIdentifier(sibling) + ` (` + strings.Join(columns, ", ") + `) select ` + strings.Join(selectExpressions, ", ") + ` from "threads"`
	if _, err := tx.ExecContext(ctx, insertSQL); err != nil {
		return nil, fmt.Errorf("copy compact portable threads: %w", err)
	}
	if options.threadsRebuildHook != nil {
		if err := options.threadsRebuildHook(portableThreadsRebuildHookAfterCopy, tx, sibling); err != nil {
			return nil, fmt.Errorf("portable threads rebuild test hook: %w", err)
		}
	}
	rebuiltIdentity, err := portableThreadsIdentityForTable(ctx, tx, sibling)
	if err != nil {
		return nil, err
	}
	if rebuiltIdentity != originalIdentity {
		return nil, fmt.Errorf("portable threads rebuild identity mismatch: copied %d of %d rows", rebuiltIdentity.count, originalIdentity.count)
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildSchemaSwap)
	if _, err := tx.ExecContext(ctx, `drop table "threads"`); err != nil {
		return nil, fmt.Errorf("drop uncompact portable threads: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `alter table `+sqliteIdentifier(sibling)+` rename to "threads"`); err != nil {
		return nil, fmt.Errorf("rename compact portable threads: %w", err)
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildSchemaRestore)
	if originalSequence.Valid {
		var rebuiltSequence sql.NullInt64
		if err := tx.QueryRowContext(ctx, `select max(seq) from sqlite_sequence where name in ('threads', ?)`, sibling).Scan(&rebuiltSequence); err != nil {
			return nil, fmt.Errorf("read rebuilt portable threads sequence: %w", err)
		}
		highWater := originalSequence.Int64
		if rebuiltSequence.Valid && rebuiltSequence.Int64 > highWater {
			highWater = rebuiltSequence.Int64
		}
		if _, err := tx.ExecContext(ctx, `delete from sqlite_sequence where name in ('threads', ?)`, sibling); err != nil {
			return nil, fmt.Errorf("clear rebuilt portable threads sequence: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `insert into sqlite_sequence(name, seq) values('threads', ?)`, highWater); err != nil {
			return nil, fmt.Errorf("restore portable threads sequence: %w", err)
		}
	}
	for _, index := range uniqueIndexes {
		if _, err := tx.ExecContext(ctx, index.sql); err != nil {
			return nil, fmt.Errorf("recreate portable unique index %s: %w", index.name, err)
		}
	}
	for _, trigger := range triggers {
		if _, err := tx.ExecContext(ctx, trigger.sql); err != nil {
			return nil, fmt.Errorf("recreate portable threads trigger %s: %w", trigger.name, err)
		}
	}
	// Bulk-copy rows intentionally do not fire table triggers. Definitions are
	// restored for future writes; the known convergence triggers' net effect is
	// applied once below instead of once per copied row.
	var convergenceTable int
	if err := tx.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = 'observation_schema_convergence')`).Scan(&convergenceTable); err != nil {
		return nil, fmt.Errorf("inspect portable observation convergence table: %w", err)
	}
	if convergenceTable == 1 && hasConvergenceTrigger {
		if _, err := tx.ExecContext(ctx, `update observation_schema_convergence set checked_observation_sequence = -1 where id = 1`); err != nil {
			return nil, fmt.Errorf("invalidate portable observation convergence after threads rebuild: %w", err)
		}
	}
	rebuiltThreadForeignKeys, err := portableForeignKeysForTable(ctx, tx, "threads")
	if err != nil {
		return nil, err
	}
	if !slices.Equal(originalThreadForeignKeys, rebuiltThreadForeignKeys) {
		return nil, fmt.Errorf("portable threads rebuild changed threads foreign-key schema")
	}
	rebuiltChildForeignKeys, err := portableChildForeignKeysReferencingThreads(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(originalChildForeignKeys, rebuiltChildForeignKeys) {
		return nil, fmt.Errorf("portable threads rebuild changed child foreign-key schema")
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit portable threads rebuild: %w", err)
	}
	if _, err := conn.ExecContext(context.Background(), `pragma foreign_keys = on`); err != nil {
		return nil, fmt.Errorf("restore portable threads foreign keys: %w", err)
	}
	foreignKeysOff = false
	if legacyAlterChanged {
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf(`pragma legacy_alter_table = %d`, originalLegacyAlter)); err != nil {
			return nil, fmt.Errorf("restore portable threads legacy alter mode: %w", err)
		}
		legacyAlterChanged = false
	}
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&foreignKeys); err != nil {
		return nil, fmt.Errorf("verify restored portable threads foreign keys: %w", err)
	}
	if foreignKeys != 1 {
		return nil, fmt.Errorf("restore portable threads foreign keys: pragma remained %d", foreignKeys)
	}
	// One indexed full FK proof ran before the rebuild. Child rows and outgoing
	// FK source values are untouched, parent (id, repo_id) identity is exact,
	// and both parent/child FK definitions are equivalent, so zero is preserved.
	return ordinaryIndexes, nil
}

func portableThreadsSequence(ctx context.Context, q dbQueries) (sql.NullInt64, error) {
	var sequenceTable int
	if err := q.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = 'sqlite_sequence')`).Scan(&sequenceTable); err != nil {
		return sql.NullInt64{}, fmt.Errorf("inspect portable threads sequence table: %w", err)
	}
	if sequenceTable == 0 {
		return sql.NullInt64{}, nil
	}
	var sequence sql.NullInt64
	if err := q.QueryRowContext(ctx, `select seq from sqlite_sequence where name = 'threads'`).Scan(&sequence); err != nil {
		if err == sql.ErrNoRows {
			return sql.NullInt64{}, nil
		}
		return sql.NullInt64{}, fmt.Errorf("read portable threads sequence: %w", err)
	}
	return sequence, nil
}

type portableThreadsIdentity struct {
	count  int64
	digest [sha256.Size]byte
}

func portableThreadsIdentityForTable(ctx context.Context, q dbQueries, table string) (portableThreadsIdentity, error) {
	rows, err := q.QueryContext(ctx, `select id, repo_id from `+sqliteIdentifier(table)+` order by id`)
	if err != nil {
		return portableThreadsIdentity{}, fmt.Errorf("read portable threads identity from %s: %w", table, err)
	}
	defer rows.Close()
	hash := sha256.New()
	var identity portableThreadsIdentity
	var encoded [16]byte
	for rows.Next() {
		var id, repoID int64
		if err := rows.Scan(&id, &repoID); err != nil {
			return portableThreadsIdentity{}, fmt.Errorf("scan portable threads identity from %s: %w", table, err)
		}
		binary.BigEndian.PutUint64(encoded[:8], uint64(id))
		binary.BigEndian.PutUint64(encoded[8:], uint64(repoID))
		_, _ = hash.Write(encoded[:])
		identity.count++
	}
	if err := rows.Err(); err != nil {
		return portableThreadsIdentity{}, fmt.Errorf("read portable threads identity rows from %s: %w", table, err)
	}
	copy(identity.digest[:], hash.Sum(nil))
	return identity, nil
}

type portableForeignKeyDefinition struct {
	ownerTable      string
	id              int
	sequence        int
	referencedTable string
	fromColumn      string
	toColumn        string
	onUpdate        string
	onDelete        string
	match           string
}

func portableForeignKeysForTable(ctx context.Context, q dbQueries, table string) ([]portableForeignKeyDefinition, error) {
	rows, err := q.QueryContext(ctx, `pragma foreign_key_list(`+sqliteIdentifier(table)+`)`)
	if err != nil {
		return nil, fmt.Errorf("inspect portable foreign keys for %s: %w", table, err)
	}
	defer rows.Close()
	var definitions []portableForeignKeyDefinition
	for rows.Next() {
		var definition portableForeignKeyDefinition
		var toColumn sql.NullString
		definition.ownerTable = table
		if err := rows.Scan(
			&definition.id,
			&definition.sequence,
			&definition.referencedTable,
			&definition.fromColumn,
			&toColumn,
			&definition.onUpdate,
			&definition.onDelete,
			&definition.match,
		); err != nil {
			return nil, fmt.Errorf("scan portable foreign keys for %s: %w", table, err)
		}
		definition.toColumn = toColumn.String
		definitions = append(definitions, definition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read portable foreign keys for %s: %w", table, err)
	}
	sortPortableForeignKeys(definitions)
	return definitions, nil
}

func portableChildForeignKeysReferencingThreads(ctx context.Context, q dbQueries) ([]portableForeignKeyDefinition, error) {
	tables, err := portableTableNames(ctx, q)
	if err != nil {
		return nil, err
	}
	var definitions []portableForeignKeyDefinition
	for _, table := range tables {
		if table == "threads" {
			continue
		}
		tableDefinitions, err := portableForeignKeysForTable(ctx, q, table)
		if err != nil {
			return nil, err
		}
		for _, definition := range tableDefinitions {
			if strings.EqualFold(definition.referencedTable, "threads") {
				definitions = append(definitions, definition)
			}
		}
	}
	sortPortableForeignKeys(definitions)
	return definitions, nil
}

func sortPortableForeignKeys(definitions []portableForeignKeyDefinition) {
	sort.Slice(definitions, func(left, right int) bool {
		leftKey := fmt.Sprintf("%s\x00%08d\x00%08d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", definitions[left].ownerTable, definitions[left].id, definitions[left].sequence, definitions[left].referencedTable, definitions[left].fromColumn, definitions[left].toColumn, definitions[left].onUpdate, definitions[left].onDelete, definitions[left].match)
		rightKey := fmt.Sprintf("%s\x00%08d\x00%08d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", definitions[right].ownerTable, definitions[right].id, definitions[right].sequence, definitions[right].referencedTable, definitions[right].fromColumn, definitions[right].toColumn, definitions[right].onUpdate, definitions[right].onDelete, definitions[right].match)
		return leftKey < rightKey
	})
}

func validatePortableChildForeignKeyIdentityTargets(definitions []portableForeignKeyDefinition) error {
	for _, definition := range definitions {
		toColumn := definition.toColumn
		if toColumn == "" {
			toColumn = "id"
		}
		if !strings.EqualFold(toColumn, "id") && !strings.EqualFold(toColumn, "repo_id") {
			return fmt.Errorf("portable threads rebuild cannot preserve child foreign key %s.%s referencing mutable threads column %s", definition.ownerTable, definition.fromColumn, toColumn)
		}
	}
	return nil
}

func validatePortableThreadsForeignKeyTransformSafety(definitions []portableForeignKeyDefinition, verbatimColumns map[string]bool) error {
	for _, definition := range definitions {
		if !verbatimColumns[strings.ToLower(definition.fromColumn)] {
			return fmt.Errorf("portable threads rebuild cannot preserve foreign key from transformed threads column %s", definition.fromColumn)
		}
		if !strings.EqualFold(definition.referencedTable, "threads") {
			continue
		}
		toColumn := definition.toColumn
		if toColumn == "" {
			toColumn = "id"
		}
		if !strings.EqualFold(toColumn, "id") && !strings.EqualFold(toColumn, "repo_id") {
			return fmt.Errorf("portable threads rebuild cannot preserve self-referencing foreign key targeting transformed threads column %s", toColumn)
		}
	}
	return nil
}

func portableThreadsSiblingName() (string, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate portable threads sibling name: %w", err)
	}
	return "threads_portable_" + hex.EncodeToString(value[:]), nil
}

func rewritePortableThreadsCreateSQL(createSQL, sibling string) (string, error) {
	match := portableThreadsCreateTablePattern.FindStringSubmatchIndex(createSQL)
	if match == nil {
		return "", fmt.Errorf("unrecognized portable threads CREATE TABLE SQL")
	}
	return createSQL[:match[3]] + sqliteIdentifier(sibling) + createSQL[match[4]:], nil
}

func portableThreadsRebuildColumns(ctx context.Context, q dbQueries) ([]string, []string, map[string]bool, error) {
	rows, err := q.QueryContext(ctx, `pragma table_xinfo("threads")`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("inspect portable threads columns: %w", err)
	}
	defer rows.Close()
	var columns, expressions []string
	seen := make(map[string]bool)
	verbatimColumns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey, hidden int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey, &hidden); err != nil {
			return nil, nil, nil, fmt.Errorf("scan portable threads columns: %w", err)
		}
		seen[name] = true
		if hidden != 0 {
			continue
		}
		columns = append(columns, sqliteIdentifier(name))
		switch name {
		case "body":
			expressions = append(expressions, `"body_excerpt"`)
		case "raw_json":
			expressions = append(expressions, `''`)
		default:
			expressions = append(expressions, sqliteIdentifier(name))
			verbatimColumns[strings.ToLower(name)] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("read portable threads columns: %w", err)
	}
	for _, required := range []string{"body", "body_excerpt", "raw_json"} {
		if !seen[required] {
			return nil, nil, nil, fmt.Errorf("portable threads rebuild requires column %s", required)
		}
	}
	return columns, expressions, verbatimColumns, nil
}

func portableThreadsRebuildIndexes(ctx context.Context, q dbQueries) ([]string, []portableSchemaObject, error) {
	rows, err := q.QueryContext(ctx, `pragma index_list("threads")`)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect portable threads indexes: %w", err)
	}
	defer rows.Close()
	var ordinary []string
	var uniqueNames []string
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			return nil, nil, fmt.Errorf("scan portable threads indexes: %w", err)
		}
		if origin != "c" {
			continue
		}
		if unique == 0 {
			ordinary = append(ordinary, name)
			continue
		}
		uniqueNames = append(uniqueNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read portable threads indexes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close portable threads indexes: %w", err)
	}
	var uniqueIndexes []portableSchemaObject
	for _, name := range uniqueNames {
		var indexSQL sql.NullString
		if err := q.QueryRowContext(ctx, `select sql from sqlite_schema where type = 'index' and tbl_name = 'threads' and name = ?`, name).Scan(&indexSQL); err != nil {
			return nil, nil, fmt.Errorf("read portable unique index %s: %w", name, err)
		}
		if !indexSQL.Valid || !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(indexSQL.String)), "CREATE UNIQUE INDEX") {
			return nil, nil, fmt.Errorf("portable unique index %s has unrecognized SQL", name)
		}
		uniqueIndexes = append(uniqueIndexes, portableSchemaObject{name: name, sql: indexSQL.String})
	}
	sort.Strings(ordinary)
	sort.Slice(uniqueIndexes, func(left, right int) bool { return uniqueIndexes[left].name < uniqueIndexes[right].name })
	return ordinary, uniqueIndexes, nil
}

func portableThreadsRebuildTriggers(ctx context.Context, q dbQueries) ([]portableSchemaObject, error) {
	rows, err := q.QueryContext(ctx, `select name, sql from sqlite_schema where type = 'trigger' and tbl_name = 'threads' order by name`)
	if err != nil {
		return nil, fmt.Errorf("inspect portable threads triggers: %w", err)
	}
	defer rows.Close()
	var triggers []portableSchemaObject
	for rows.Next() {
		var trigger portableSchemaObject
		var triggerSQL sql.NullString
		if err := rows.Scan(&trigger.name, &triggerSQL); err != nil {
			return nil, fmt.Errorf("scan portable threads trigger: %w", err)
		}
		if !triggerSQL.Valid || !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(triggerSQL.String)), "CREATE TRIGGER") {
			return nil, fmt.Errorf("portable threads trigger %s has unrecognized SQL", trigger.name)
		}
		trigger.sql = triggerSQL.String
		triggers = append(triggers, trigger)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read portable threads triggers: %w", err)
	}
	return triggers, nil
}
