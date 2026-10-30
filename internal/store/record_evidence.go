package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

const recordEvidenceSchemaSQL = `
create table if not exists record_evidence_blobs (
 sha256 text primary key,
 raw_bytes blob,
 base_sha256 text references record_evidence_blobs(sha256),
 prefix_bytes integer not null default 0,
 suffix_bytes integer not null default 0,
 delta_gzip blob,
 depth integer not null default 0
) without rowid;
create table if not exists record_check_observations (
 id integer primary key autoincrement,
 source_table text not null,
 source_key text not null,
 observed_at text not null,
 evidence_sha256 text references record_evidence_blobs(sha256),
 check_json text not null
);
create index if not exists idx_record_check_observations_source
 on record_check_observations(source_table,source_key,id);
`

// retainRecordCheck preserves exact native envelope bytes, including whitespace
// and unknown fields. The content-addressed blob is shared by identical checks;
// observations are never deduplicated, including A-B-A and equal-clock checks.
// These are native inputs, not an assertion of HTTP wire-response capture.
func (s *Store) retainRecordCheck(ctx context.Context, table, key, at, raw string, check any) error {
	if !s.recordChecks {
		return nil
	}
	var hash any
	if raw != "" {
		sum := sha256.Sum256([]byte(raw))
		hash = hex.EncodeToString(sum[:])
		if err := s.retainExactBlob(ctx, table, key, hash.(string), []byte(raw)); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(check)
	if err != nil {
		return err
	}
	_, err = s.q().ExecContext(ctx, `insert into record_check_observations(source_table,source_key,observed_at,evidence_sha256,check_json) values(?,?,?,?,?)`, table, key, at, hash, string(encoded))
	return err
}

// retainCheckRow journals each change to the narrow latest-check overlay. The
// trigger includes all columns, so later evidence reservation is retained too.
func (s *Store) ensureRecordCheckHistory(ctx context.Context) error {
	if _, err := s.q().ExecContext(ctx, recordEvidenceSchemaSQL); err != nil {
		return err
	}
	for _, table := range []string{"thread_checks", "pull_request_detail_fetches", "repository_checks", "analytics_coverage_checks", "pull_request_review_thread_syncs", "analytics_coverage_count_samples"} {
		columns, err := s.tableColumns(ctx, table)
		if err != nil {
			return err
		}
		if len(columns) == 0 {
			continue
		}
		pairs := ""
		for _, column := range columns {
			if pairs != "" {
				pairs += ","
			}
			pairs += "'" + column + "',NEW." + sqliteQuotedIdentifier(column)
		}
		key := "cast(NEW.thread_id as text)"
		at := "NEW.fetched_at"
		switch table {
		case "thread_checks":
			at = "NEW.checked_at"
		case "repository_checks":
			key = "cast(NEW.repo_id as text)"
			at = "NEW.checked_at"
		case "analytics_coverage_count_samples":
			key = "NEW.repository"
			at = "NEW.observed_at"
		case "analytics_coverage_checks":
			key = "json_array(NEW.repository,NEW.coverage)"
			at = "NEW.observed_at"
		}
		for _, event := range []string{"insert", "update"} {
			name := "record_history_" + table + "_" + event
			statement := "create trigger if not exists " + name + " after " + event + " on " + table + " begin insert into record_check_observations(source_table,source_key,observed_at,check_json) values('" + table + "'," + key + ",coalesce(" + at + ",''),json_object(" + pairs + ")); end"
			if _, err := s.q().ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("create check history %s: %w", name, err)
			}
		}
	}
	return nil
}

func (s *Store) dropRecordCheckHistory(ctx context.Context) error {
	for _, table := range []string{"thread_checks", "pull_request_detail_fetches", "repository_checks", "analytics_coverage_checks", "pull_request_review_thread_syncs", "analytics_coverage_count_samples"} {
		for _, event := range []string{"insert", "update"} {
			if _, err := s.q().ExecContext(ctx, "drop trigger if exists record_history_"+table+"_"+event); err != nil {
				return err
			}
		}
	}
	return nil
}

// EvidenceRaw reconstructs exact native bytes. Deltas share the prior envelope,
// not normalized content; bounded chain depth limits read amplification.
func (s *Store) EvidenceRaw(ctx context.Context, hash string) ([]byte, error) {
	return s.evidenceRaw(ctx, hash, 0)
}

func (s *Store) evidenceRaw(ctx context.Context, hash string, traversed int) ([]byte, error) {
	if traversed > 32 {
		return nil, fmt.Errorf("evidence chain exceeds bound")
	}
	var raw, delta []byte
	var base sql.NullString
	var prefix, suffix, depth int
	err := s.q().QueryRowContext(ctx, `select raw_bytes,base_sha256,prefix_bytes,suffix_bytes,delta_gzip,depth from record_evidence_blobs where sha256=?`, hash).Scan(&raw, &base, &prefix, &suffix, &delta, &depth)
	if err != nil {
		return nil, err
	}
	if base.Valid {
		if depth < 1 || depth > 32 {
			return nil, fmt.Errorf("invalid evidence depth")
		}
		previous, err := s.evidenceRaw(ctx, base.String, traversed+1)
		if err != nil {
			return nil, err
		}
		if prefix < 0 || suffix < 0 || prefix+suffix > len(previous) {
			return nil, fmt.Errorf("invalid evidence bounds")
		}
		reader, err := gzip.NewReader(bytes.NewReader(delta))
		if err != nil {
			return nil, err
		}
		middle, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return nil, err
		}
		raw = append(append(append([]byte{}, previous[:prefix]...), middle...), previous[len(previous)-suffix:]...)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != hash {
		return nil, fmt.Errorf("evidence checksum mismatch")
	}
	return raw, nil
}

func (s *Store) retainExactBlob(ctx context.Context, table, key, hash string, raw []byte) error {
	var exists int
	if err := s.q().QueryRowContext(ctx, `select exists(select 1 from record_evidence_blobs where sha256=?)`, hash).Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		return nil
	}
	var base string
	var depth int
	err := s.q().QueryRowContext(ctx, `select b.sha256,b.depth from record_check_observations o join record_evidence_blobs b on b.sha256=o.evidence_sha256 where o.source_table=? and o.source_key=? order by o.id desc limit 1`, table, key).Scan(&base, &depth)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil && depth < 32 {
		previous, err := s.EvidenceRaw(ctx, base)
		if err != nil {
			return err
		}
		prefix := 0
		for prefix < len(raw) && prefix < len(previous) && raw[prefix] == previous[prefix] {
			prefix++
		}
		suffix := 0
		for suffix < len(raw)-prefix && suffix < len(previous)-prefix && raw[len(raw)-1-suffix] == previous[len(previous)-1-suffix] {
			suffix++
		}
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		if _, err = writer.Write(raw[prefix : len(raw)-suffix]); err != nil {
			return err
		}
		if err = writer.Close(); err != nil {
			return err
		}
		if compressed.Len()+96 < len(raw) {
			_, err = s.q().ExecContext(ctx, `insert into record_evidence_blobs(sha256,base_sha256,prefix_bytes,suffix_bytes,delta_gzip,depth) values(?,?,?,?,?,?)`, hash, base, prefix, suffix, compressed.Bytes(), depth+1)
			return err
		}
	}
	_, err = s.q().ExecContext(ctx, `insert into record_evidence_blobs(sha256,raw_bytes) values(?,?)`, hash, raw)
	return err
}

// RetainHydrationObservation journals fetched native child inputs independently
// of admission to current state. Call inside the owning transaction after parent
// identity validation; exclusions are rechecked here. The batch blob preserves
// empty membership and unknown fields as well as rejected stale inputs.
func (s *Store) RetainHydrationObservation(ctx context.Context, threadID int64, at string, sequence int64, payload any) error {
	if !s.recordChecks {
		return nil
	}
	var excluded bool
	if err := s.q().QueryRowContext(ctx, `select exists(select 1 from threads t join repositories r on r.id=t.repo_id join thread_exclusions e on e.repository=lower(r.full_name) and e.number=t.number where t.id=?)`, threadID).Scan(&excluded); err != nil {
		return err
	}
	if excluded {
		return ErrThreadExcluded
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.retainRecordCheck(ctx, "hydration_inputs", fmt.Sprint(threadID), at, string(raw), map[string]any{"observation_sequence": sequence})
}
