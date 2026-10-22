package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type captureDirty struct {
	table, key string
	initial    bool
}
type captureWritten struct {
	seq   int64
	table captureTable
	row   captureRow
}

func (s *Store) materializeCaptureRow(ctx context.Context, t captureTable, r captureRow, epoch string) (int64, error) {
	if err := validateCaptureRow(t, r); err != nil {
		return 0, err
	}
	var image int64
	var value string
	err := s.q().QueryRowContext(ctx, `SELECT i.image_id,i.value_json FROM capture_heads h JOIN capture_images i ON i.image_id=h.image_id WHERE h.table_name=? AND h.key_json=? AND h.present=1`, t.Name, r.Key).Scan(&image, &value)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if err == sql.ErrNoRows || value != r.Value {
		result, err := s.q().ExecContext(ctx, `INSERT INTO capture_images(image_epoch,table_name,key_json,value_json) VALUES(?,?,?,?)`, epoch, t.Name, r.Key, r.Value)
		if err != nil {
			return 0, err
		}
		image, err = result.LastInsertId()
		if err != nil {
			return 0, err
		}
	}
	_, err = s.q().ExecContext(ctx, `INSERT INTO capture_heads(table_name,key_json,present,image_id,clocks_json) VALUES(?,?,1,?,?) ON CONFLICT(table_name,key_json) DO UPDATE SET present=1,image_id=excluded.image_id,clocks_json=excluded.clocks_json`, t.Name, r.Key, image, r.Clocks)
	return image, err
}
func (s *Store) finishObservationCapture(ctx context.Context) error {
	c := s.captureTx
	if c == nil {
		return nil
	}
	// A native callback may invoke a compatibility helper. A schema change inside
	// the transaction must fail before images can silently omit a new column.
	if err := s.validateCaptureCached(ctx); err != nil {
		return err
	}
	rows, err := s.q().QueryContext(ctx, `SELECT table_name,key_json,initial_present FROM _capture_dirty WHERE tx_id=? ORDER BY first_touch,table_name,key_json`, c.ID)
	if err != nil {
		return err
	}
	var dirty []captureDirty
	for rows.Next() {
		var d captureDirty
		if err = rows.Scan(&d.table, &d.key, &d.initial); err != nil {
			rows.Close()
			return err
		}
		dirty = append(dirty, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var first, last int64
	count := 0
	var written []captureWritten
	for _, d := range dirty {
		t, err := captureTableNamed(d.table)
		if err != nil {
			return err
		}
		r, err := readCaptureRow(ctx, s.q(), t, d.key)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		operation, clocks := "upsert", r.Clocks
		var image any
		if err == sql.ErrNoRows {
			if !d.initial {
				continue
			}
			operation, clocks = "withdraw", "{}"
			_, err = s.q().ExecContext(ctx, `INSERT INTO capture_heads(table_name,key_json,present,clocks_json) VALUES(?,?,0,'{}') ON CONFLICT(table_name,key_json) DO UPDATE SET present=0,image_id=NULL,clocks_json='{}'`, d.table, d.key)
			if err != nil {
				return err
			}
		} else {
			image, err = s.materializeCaptureRow(ctx, t, r, c.Epoch)
			if err != nil {
				return err
			}
		}
		cause := "native_write"
		if c.Kind == "owner_purge" {
			cause = "owner_requested"
		}
		result, err := s.q().ExecContext(ctx, `INSERT INTO capture_observations(tx_id,epoch,table_name,key_json,operation,image_id,clocks_json,cause) VALUES(?,?,?,?,?,?,?,?)`, c.ID, c.Epoch, d.table, d.key, operation, image, clocks, cause)
		if err != nil {
			return err
		}
		seq, err := result.LastInsertId()
		if err != nil {
			return err
		}
		if first == 0 {
			first = seq
		}
		last = seq
		count++
		if _, err = s.q().ExecContext(ctx, `UPDATE capture_heads SET last_seq=? WHERE table_name=? AND key_json=?`, seq, d.table, d.key); err != nil {
			return err
		}
		if operation == "upsert" {
			written = append(written, captureWritten{seq: seq, table: t, row: r})
		}
	}
	// Resolve dependencies only after every final row image exists. SQL statement
	// order must not leak an earlier blob/member state into a committed frame.
	for _, w := range written {
		supports, err := captureRowSupports(ctx, s.q(), w.table, w.row)
		if err != nil {
			return err
		}
		for _, support := range supports {
			t, _ := captureTableNamed(support.Table)
			image, err := s.materializeCaptureRow(ctx, t, support.Row, c.Epoch)
			if err != nil {
				return err
			}
			if _, err = s.q().ExecContext(ctx, `INSERT INTO capture_supports(seq,role,ordinal,image_id,clocks_json) VALUES(?,?,?,?,?)`, w.seq, support.Role, support.Ordinal, image, support.Row.Clocks); err != nil {
				return err
			}
		}
	}
	var mode string
	if err = s.q().QueryRowContext(ctx, `SELECT mode FROM _capture_context WHERE singleton=1`).Scan(&mode); err != nil {
		return err
	}
	if mode == "identity_change" {
		c.Reason = "identity_change"
	}
	if count > 0 || c.Kind != "observation" || c.Reason != "" {
		var f, l, previous, reason any
		if count > 0 {
			f, l = first, last
		}
		if c.Previous != "" {
			previous = c.Previous
		}
		if c.Reason != "" {
			reason = c.Reason
		}
		if _, err = s.q().ExecContext(ctx, `INSERT INTO capture_commits(tx_id,epoch,kind,previous_epoch,reset_reason,first_seq,last_seq,observation_count,sealed_at) VALUES(?,?,?,?,?,?,?,?,?)`, c.ID, c.Epoch, c.Kind, previous, reason, f, l, count, time.Now().UTC().Format(timeLayout)); err != nil {
			return err
		}
	}
	if _, err = s.q().ExecContext(ctx, `DELETE FROM _capture_dirty WHERE tx_id=?`, c.ID); err != nil {
		return err
	}
	_, err = s.q().ExecContext(ctx, `UPDATE _capture_context SET open=0,tx_id=NULL,mode='ordinary' WHERE singleton=1`)
	return err
}

type captureSupport struct {
	Role    string     `json:"role"`
	Ordinal int        `json:"ordinal"`
	Table   string     `json:"table_name"`
	Row     captureRow `json:"row"`
}

func captureField(r captureRow, name string) (string, bool, error) {
	var fields map[string][]json.RawMessage
	if err := json.Unmarshal([]byte(r.Value), &fields); err != nil {
		return "", false, err
	}
	c := fields[name]
	if len(c) != 2 {
		return "", false, fmt.Errorf("missing captured field %s", name)
	}
	if string(c[0]) == `"null"` {
		return "", false, nil
	}
	var value string
	err := json.Unmarshal(c[1], &value)
	return value, true, err
}
func captureKey(cells ...[2]string) string { b, _ := json.Marshal(cells); return string(b) }
func captureRowSupports(ctx context.Context, q dbQueries, t captureTable, r captureRow) ([]captureSupport, error) {
	var out []captureSupport
	add := func(role, table, key string, ordinal int, optional bool) error {
		other, err := captureTableNamed(table)
		if err != nil {
			return err
		}
		row, err := readCaptureRow(ctx, q, other, key)
		if err == sql.ErrNoRows && optional {
			return nil
		}
		if err != nil {
			return fmt.Errorf("capture support %s: %w", role, err)
		}
		out = append(out, captureSupport{Role: role, Ordinal: ordinal, Table: table, Row: row})
		return nil
	}
	switch t.Name {
	case "thread_revisions":
		id, present, err := captureField(r, "raw_json_blob_id")
		if err != nil {
			return nil, err
		}
		if present {
			err = add("github_blob", "blobs", captureKey([2]string{"integer", id}), 0, false)
		}
		return out, err
	case "pull_request_review_threads":
		id, _, err := captureField(r, "thread_id")
		if err != nil {
			return nil, err
		}
		err = add("review_membership", "pull_request_review_thread_syncs", captureKey([2]string{"integer", id}), 0, true)
		return out, err
	case "pull_request_review_thread_syncs":
		parent, _, err := captureField(r, "thread_id")
		if err != nil {
			return nil, err
		}
		memberJSON, known, err := captureField(r, "review_thread_ids_json")
		if err != nil || !known {
			return out, err
		}
		var ids []string
		if err = json.Unmarshal([]byte(memberJSON), &ids); err != nil || ids == nil {
			return nil, errors.New("capture membership must be a complete array or SQL NULL")
		}
		seen := map[string]bool{}
		for i, id := range ids {
			if id == "" || strings.TrimSpace(id) != id || seen[id] {
				return nil, errors.New("invalid capture membership ID")
			}
			seen[id] = true
			if err = add("review_thread", "pull_request_review_threads", captureKey([2]string{"integer", parent}, [2]string{"text", id}), i, false); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// ResetObservationCapture is a source-owned lineage boundary after an offline
// restore/rebuild. It does not fabricate lost observations or run a migration.
func (s *Store) ResetObservationCapture(ctx context.Context, fork bool) error {
	return s.WithTx(ctx, func(tx *Store) error {
		if tx.captureTx == nil {
			return errors.New("capture not installed")
		}
		return tx.rotateCaptureEpoch(ctx, "epoch_reset", "source_restore", fork)
	})
}
func (s *Store) rotateCaptureEpoch(ctx context.Context, kind, reason string, fork bool) error {
	c := s.captureTx
	if c == nil {
		return errors.New("capture transaction required")
	}
	epoch, err := captureID()
	if err != nil {
		return err
	}
	c.Previous, c.Epoch, c.Kind, c.Reason = c.Epoch, epoch, kind, reason
	var seq int64
	if err = s.q().QueryRowContext(ctx, `SELECT coalesce(max(seq),0) FROM capture_observations`).Scan(&seq); err != nil {
		return err
	}
	_, err = s.q().ExecContext(ctx, `UPDATE capture_meta SET epoch=?,epoch_start_tx_id=?,epoch_start_seq=? WHERE singleton=1`, epoch, c.ID, seq)
	if err != nil {
		return err
	}
	if fork {
		archive, err := captureID()
		if err != nil {
			return err
		}
		if _, err = s.q().ExecContext(ctx, `UPDATE capture_meta SET archive_id=?`, archive); err != nil {
			return err
		}
	}
	_, err = s.q().ExecContext(ctx, `UPDATE _capture_context SET mode=? WHERE singleton=1`, kind)
	return err
}

func captureInt(n int64) string { return strconv.FormatInt(n, 10) }
