package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
)

// ValidateArchiveReferences checks the canonical relationships even when an
// imported archive omitted the corresponding foreign-key declarations.
func ValidateArchiveReferences(ctx context.Context, db *sql.DB) error {
	reference, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return err
	}
	defer reference.Close()
	reference.SetMaxOpenConns(1)
	if err := (&Store{db: reference}).migrate(ctx); err != nil {
		return fmt.Errorf("prepare canonical archive relationships: %w", err)
	}
	tables, err := portableTableNames(ctx, reference)
	if err != nil {
		return err
	}
	columns := make(map[string]map[string]bool, len(tables))
	for _, table := range tables {
		columns[table], err = portableTableColumnSet(ctx, db, table)
		if err != nil {
			return err
		}
	}
	for _, table := range tables {
		if len(columns[table]) == 0 {
			continue
		}
		definitions, err := portableForeignKeysForTable(ctx, reference, table)
		if err != nil {
			return err
		}
		groups := make(map[int][]portableForeignKeyDefinition)
		var ids []int
		for _, definition := range definitions {
			if _, exists := groups[definition.id]; !exists {
				ids = append(ids, definition.id)
			}
			groups[definition.id] = append(groups[definition.id], definition)
		}
		slices.Sort(ids)
		for _, id := range ids {
			relation := groups[id]
			var nonNull, matches []string
			childSupported, parentSupported := true, true
			for _, column := range relation {
				if column.toColumn == "" {
					return fmt.Errorf("canonical archive relationship requires a named target column")
				}
				childSupported = childSupported && columns[table][column.fromColumn]
				parentSupported = parentSupported && columns[column.referencedTable][column.toColumn]
				child := "child." + sqliteIdentifier(column.fromColumn)
				nonNull = append(nonNull, child+" is not null")
				matches = append(matches, "parent."+sqliteIdentifier(column.toColumn)+" = "+child)
			}
			if !childSupported {
				continue
			}
			query := "select exists(select 1 from " + sqliteIdentifier(table) + " child where " + strings.Join(nonNull, " and ")
			if parentSupported {
				query += " and not exists(select 1 from " + sqliteIdentifier(relation[0].referencedTable) + " parent where " + strings.Join(matches, " and ") + ")"
			}
			query += ")"
			var invalid bool
			if err := db.QueryRowContext(ctx, query).Scan(&invalid); err != nil {
				return fmt.Errorf("check archive relationship for %s: %w", table, err)
			}
			if invalid {
				return fmt.Errorf("archive referential closure check failed for %s", table)
			}
		}
	}
	return nil
}
