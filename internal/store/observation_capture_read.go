package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ObservationSnapshot pins ONE SQLite snapshot for the entire baseline and H.
// Callers own the private package/staging lifecycle and owner-removal barrier.
// Slow publication must consume the sealed output after Close, not pin the WAL.
type ObservationSnapshot struct {
	tx         *sql.Tx
	reader     *Store
	Capability ObservationCapability
}

func (s *Store) OpenObservationSnapshot(ctx context.Context) (*ObservationSnapshot, error) {
	// Crawlkit's writer pool is deliberately one connection. Pin a dedicated
	// read-only handle so a baseline cannot consume that writer connection.
	reader, err := OpenReadOnly(ctx, s.path)
	if err != nil {
		return nil, err
	}
	tx, err := reader.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		reader.Close()
		return nil, err
	}
	fail := func(err error) (*ObservationSnapshot, error) { tx.Rollback(); reader.Close(); return nil, err }
	if err = validateCaptureSchema(ctx, tx); err != nil {
		return fail(err)
	}
	c, err := captureCapability(ctx, tx)
	if err != nil {
		return fail(err)
	}
	return &ObservationSnapshot{tx: tx, reader: reader, Capability: c}, nil
}
func (s *ObservationSnapshot) Close() error {
	err := s.tx.Rollback()
	closeErr := s.reader.Close()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		return err
	}
	return closeErr
}
func (s *Store) CheckObservationCapability(ctx context.Context, expected ObservationCapability) error {
	snap, err := s.OpenObservationSnapshot(ctx)
	if err != nil {
		return err
	}
	defer snap.Close()
	c := snap.Capability
	if c.ArchiveID != expected.ArchiveID || c.Epoch != expected.Epoch || c.RegistryDigest != expected.RegistryDigest {
		return errors.New("capture capability revoked; new baseline required")
	}
	return nil
}

type captureStreamRecord struct {
	Stream string `json:"stream"`
	Row    any    `json:"row"`
}
type ObservationStreamCount struct {
	Rows   int64  `json:"rows,string"`
	Bytes  int64  `json:"bytes,string"`
	SHA256 string `json:"sha256"`
}
type ObservationBaselineSeal struct {
	Capability ObservationCapability             `json:"capability"`
	Streams    map[string]ObservationStreamCount `json:"streams"`
	SHA256     string                            `json:"sha256"`
}

// WriteBaseline emits JSONL with a terminal seal over all preceding bytes.
// An interrupted stream has no valid seal. The receiving package must remain
// private/unpublished until the seal is checked and capability revalidated.
// This API creates no extra full-body source backup or filesystem package.
func (s *ObservationSnapshot) WriteBaseline(ctx context.Context, w io.Writer) (ObservationBaselineSeal, error) {
	seal := ObservationBaselineSeal{Capability: s.Capability, Streams: map[string]ObservationStreamCount{}}
	total := sha256.New()
	digests := map[string]io.Writer{}
	sums := map[string]interface{ Sum([]byte) []byte }{}
	// Explicit zero-row streams distinguish a completely scanned empty dataset
	// from an omitted/unknown one. The seal covers every declared stream.
	streamNames := []string{"header", "capture_commits", "capture_observations", "capture_images", "capture_supports"}
	for _, table := range captureTables() {
		streamNames = append(streamNames, table.Name)
	}
	for _, name := range streamNames {
		h := sha256.New()
		digests[name] = h
		sums[name] = h
		seal.Streams[name] = ObservationStreamCount{}
	}
	emit := func(stream string, row any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := json.Marshal(captureStreamRecord{Stream: stream, Row: row})
		if err != nil {
			return err
		}
		b = append(b, '\n')
		if digests[stream] == nil {
			h := sha256.New()
			digests[stream] = h
			sums[stream] = h
		}
		if n, err := w.Write(b); err != nil {
			return err
		} else if n != len(b) {
			return io.ErrShortWrite
		}
		total.Write(b)
		digests[stream].Write(b)
		c := seal.Streams[stream]
		c.Rows++
		c.Bytes += int64(len(b))
		seal.Streams[stream] = c
		return nil
	}
	if err := emit("header", map[string]any{"format_version": 1, "capability": s.Capability, "registry_json": observationRegistry}); err != nil {
		return seal, err
	}
	for _, t := range captureTables() {
		var keys []string
		for _, k := range t.Key {
			keys = append(keys, captureQuote(k))
		}
		rows, err := s.tx.QueryContext(ctx, "SELECT "+t.keyExpr("")+","+t.rowExpr(false)+","+t.rowExpr(true)+" FROM "+captureQuote(t.Name)+" ORDER BY "+strings.Join(keys, ","))
		if err != nil {
			return seal, err
		}
		for rows.Next() {
			r := captureRow{}
			if err = rows.Scan(&r.Key, &r.Value, &r.Clocks); err != nil {
				rows.Close()
				return seal, err
			}
			if err = validateCaptureRow(t, r); err != nil {
				rows.Close()
				return seal, err
			}
			supports, err := captureRowSupports(ctx, s.tx, t, r)
			if err != nil {
				rows.Close()
				return seal, err
			}
			if err = emit(t.Name, map[string]any{"key_json": r.Key, "value_json": r.Value, "clocks_json": r.Clocks, "supports": supports}); err != nil {
				rows.Close()
				return seal, err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return seal, err
		}
	}
	// All retained epochs are exported with their original origin identity and
	// explicit NULL redactions. No mutable head is used to reconstruct old frames.
	for _, table := range []string{"capture_commits", "capture_observations", "capture_images", "capture_supports"} {
		info, err := s.tx.QueryContext(ctx, "PRAGMA table_info("+table+")")
		if err != nil {
			return seal, err
		}
		var cols []string
		for info.Next() {
			var cid, nn, pk int
			var name, typ string
			var d any
			if err = info.Scan(&cid, &name, &typ, &nn, &d, &pk); err != nil {
				info.Close()
				return seal, err
			}
			cols = append(cols, captureLiteral(name), `json(`+captureCell(captureQuote(name))+`)`)
		}
		err = info.Err()
		info.Close()
		if err != nil {
			return seal, err
		}
		order := "1"
		switch table {
		case "capture_commits":
			order = "tx_id"
		case "capture_observations":
			order = "seq"
		case "capture_images":
			order = "image_id"
		case "capture_supports":
			order = "seq,role,ordinal"
		}
		rows, err := s.tx.QueryContext(ctx, "SELECT json_object("+strings.Join(cols, ",")+") FROM "+table+" ORDER BY "+order)
		if err != nil {
			return seal, err
		}
		for rows.Next() {
			var raw string
			if err = rows.Scan(&raw); err != nil {
				rows.Close()
				return seal, err
			}
			if err = emit(table, json.RawMessage(raw)); err != nil {
				rows.Close()
				return seal, err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return seal, err
		}
	}
	for stream, h := range sums {
		c := seal.Streams[stream]
		c.SHA256 = hex.EncodeToString(h.Sum(nil))
		seal.Streams[stream] = c
	}
	seal.SHA256 = hex.EncodeToString(total.Sum(nil))
	b, err := json.Marshal(captureStreamRecord{Stream: "seal", Row: seal})
	if err != nil {
		return seal, err
	}
	b = append(b, '\n')
	if n, err := w.Write(b); err != nil {
		return seal, err
	} else if n != len(b) {
		return seal, io.ErrShortWrite
	}
	return seal, nil
}

// RefuseObservationTransformation protects the live capability against old
// prune/sanitize callers. Derived copies must explicitly detach first.
func RefuseObservationTransformation(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	var version int
	if err := q.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == observationSchemaVersion {
		return fmt.Errorf("capture-owned archive requires explicit derived-copy detachment before transformation")
	}
	var n int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name='capture_meta'`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("capture-owned archive requires explicit derived-copy detachment before transformation")
	}
	return nil
}
