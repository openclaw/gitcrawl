package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
)

type PortablePruneOptions struct {
	BodyChars int
	Vacuum    bool
}

type PortablePruneStats struct {
	DBPath              string `json:"db_path"`
	BodyChars           int    `json:"body_chars"`
	BytesBefore         int64  `json:"bytes_before"`
	BytesAfter          int64  `json:"bytes_after"`
	ThreadsPruned       int64  `json:"threads_pruned"`
	RepositoriesPruned  int64  `json:"repositories_pruned"`
	FingerprintsPruned  int64  `json:"fingerprints_pruned"`
	DocumentsDeleted    int64  `json:"documents_deleted"`
	DocumentsFTSRebuilt bool   `json:"documents_fts_rebuilt"`
	Vacuumed            bool   `json:"vacuumed"`
}

func (s *Store) PrunePortablePayloads(ctx context.Context, options PortablePruneOptions) (PortablePruneStats, error) {
	if options.BodyChars <= 0 {
		options.BodyChars = 256
	}
	stats := PortablePruneStats{
		DBPath:    s.path,
		BodyChars: options.BodyChars,
		Vacuumed:  options.Vacuum,
	}
	if info, err := os.Stat(s.path); err == nil {
		stats.BytesBefore = info.Size()
	}

	if s.hasColumn(ctx, "threads", "body") {
		if s.hasColumn(ctx, "threads", "body_excerpt") && s.hasColumn(ctx, "threads", "body_length") {
			if result, err := s.db.ExecContext(ctx, `
				update threads
				   set body_length = case when body is not null then length(body) else body_length end,
				       body_excerpt = case
				         when body is not null and length(body) > ? then substr(body, 1, ?)
				         when body is not null then body
				         else body_excerpt
				       end
				 where body is not null
			`, options.BodyChars, options.BodyChars); err != nil {
				return stats, fmt.Errorf("prune thread body excerpts: %w", err)
			} else {
				stats.ThreadsPruned += rowsAffected(result)
			}
			if _, err := s.db.ExecContext(ctx, `update threads set body = body_excerpt`); err != nil {
				return stats, fmt.Errorf("replace thread bodies with excerpts: %w", err)
			}
		} else {
			if result, err := s.db.ExecContext(ctx, `
				update threads
				   set body = case when length(body) > ? then substr(body, 1, ?) else body end
				 where body is not null
			`, options.BodyChars, options.BodyChars); err != nil {
				return stats, fmt.Errorf("trim thread bodies: %w", err)
			} else {
				stats.ThreadsPruned += rowsAffected(result)
			}
		}
	}
	if s.hasColumn(ctx, "threads", "raw_json") {
		if _, err := s.db.ExecContext(ctx, `update threads set raw_json = '' where raw_json is not null and raw_json != ''`); err != nil {
			return stats, fmt.Errorf("clear thread raw json: %w", err)
		}
	}
	if s.hasColumn(ctx, "repositories", "raw_json") {
		result, err := s.db.ExecContext(ctx, `update repositories set raw_json = '' where raw_json is not null and raw_json != ''`)
		if err != nil {
			return stats, fmt.Errorf("clear repository raw json: %w", err)
		}
		stats.RepositoriesPruned = rowsAffected(result)
	}
	if s.tableExists(ctx, "thread_fingerprints") {
		result, err := s.db.ExecContext(ctx, `
			update thread_fingerprints
			   set title_tokens_json = '[]',
			       linked_refs_json = '[]',
			       module_buckets_json = '[]',
			       feature_json = '{}'
		`)
		if err != nil {
			return stats, fmt.Errorf("slim fingerprint details: %w", err)
		}
		stats.FingerprintsPruned = rowsAffected(result)
	}
	if s.tableExists(ctx, "documents") {
		result, err := s.db.ExecContext(ctx, `delete from documents`)
		if err != nil {
			return stats, fmt.Errorf("delete generated documents: %w", err)
		}
		stats.DocumentsDeleted = rowsAffected(result)
	}
	if s.tableExists(ctx, "documents_fts") {
		if _, err := s.db.ExecContext(ctx, `insert into documents_fts(documents_fts) values('rebuild')`); err != nil {
			return stats, fmt.Errorf("rebuild document fts: %w", err)
		}
		stats.DocumentsFTSRebuilt = true
	}
	if options.Vacuum {
		if _, err := s.db.ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
			return stats, fmt.Errorf("checkpoint wal: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `vacuum`); err != nil {
			return stats, fmt.Errorf("vacuum database: %w", err)
		}
	}
	if info, err := os.Stat(s.path); err == nil {
		stats.BytesAfter = info.Size()
	}
	return stats, nil
}

func (s *Store) tableExists(ctx context.Context, table string) bool {
	var name string
	err := s.db.QueryRowContext(ctx, `select name from sqlite_master where type in ('table', 'virtual table') and name = ?`, table).Scan(&name)
	return err == nil && name == table
}

func rowsAffected(result sql.Result) int64 {
	rows, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return rows
}
