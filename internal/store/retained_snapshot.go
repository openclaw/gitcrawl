package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

var ErrRetainedSnapshot = errors.New("retained snapshot requires a matching quiescent native file and SHA-256")

// RetainedSnapshotRequest binds a stopped native file. Its owner must exclude
// writers for the whole operation; immutable SQLite deliberately takes no locks.
type RetainedSnapshotRequest struct {
	NativeReportRequest
	SHA256 string
}

type RetainedSnapshot struct {
	FileSHA256 string
	Capture    NativeReportCapture
}

// StreamRetainedSnapshot visits full typed rows without retaining the inventory
// in memory. Table runs after its rows, including empty tables. Callback effects
// are provisional until this function succeeds and verifies the source again.
func StreamRetainedSnapshot(ctx context.Context, path string, req RetainedSnapshotRequest, row func(NativeReportTable, []NativeReportValue) error, table func(NativeReportTable) error) (RetainedSnapshot, error) {
	var out RetainedSnapshot
	if row == nil || table == nil {
		return out, ErrRetainedSnapshot
	}
	err := withRetainedSnapshot(ctx, path, req, func(tx *sql.Tx) error {
		anchor, err := retainedSnapshotAnchor(ctx, tx, req.NativeReportRequest)
		if err != nil {
			return err
		}
		out.Capture.Anchor = anchor
		h := sha256.New()
		nativeReportHashText(h, anchor.ScopeSHA256)
		for _, spec := range nativeReportTables {
			t, err := walkNativeReportTable(ctx, tx, spec, anchor.RepositoryID, req.Limits, &out.Capture.Rows, &out.Capture.Bytes, false, row)
			if err != nil {
				return err
			}
			nativeReportHashText(h, t.SHA256)
			if err := table(t); err != nil {
				return err
			}
			out.Capture.Tables = append(out.Capture.Tables, t)
		}
		out.Capture.SHA256 = hex.EncodeToString(h.Sum(nil))
		return nil
	})
	if err != nil {
		return RetainedSnapshot{}, err
	}
	out.FileSHA256 = req.SHA256
	return out, nil
}

// MeasureRetainedSnapshot shares the capture selection and scalar accounting.
// This is not an encoded-size or RSS estimate and does not qualify completeness.
func MeasureRetainedSnapshot(ctx context.Context, path string, req RetainedSnapshotRequest) (NativeReportMeasurement, error) {
	var out NativeReportMeasurement
	err := withRetainedSnapshot(ctx, path, req, func(tx *sql.Tx) error {
		anchor, err := retainedSnapshotAnchor(ctx, tx, req.NativeReportRequest)
		if err != nil {
			return err
		}
		for _, spec := range nativeReportTables {
			t, err := nativeReportTableRead(ctx, tx, spec, anchor.RepositoryID, false)
			if err != nil {
				return err
			}
			m, err := measureNativeReportTable(ctx, tx, spec, t, anchor.RepositoryID)
			if err != nil {
				return err
			}
			if m.Rows > req.Limits.MaxRows-out.Rows || m.ColumnNameBytes > req.Limits.MaxBytes-out.Bytes {
				return ErrNativeReportLimit
			}
			out.Bytes += m.ColumnNameBytes
			if m.ScalarBytes > req.Limits.MaxBytes-out.Bytes {
				return ErrNativeReportLimit
			}
			out.Rows += m.Rows
			out.Bytes += m.ScalarBytes
			out.Tables = append(out.Tables, m)
		}
		return nil
	})
	if err != nil {
		return NativeReportMeasurement{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return NativeReportMeasurement{}, err
	}
	out.File, out.SourceBytes, out.SourceModifiedAt = req.File, info.Size(), info.ModTime().UTC().Format(time.RFC3339Nano)
	return out, nil
}

func withRetainedSnapshot(ctx context.Context, path string, req RetainedSnapshotRequest, read func(*sql.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrRetainedSnapshot
	}
	if req.Limits.MaxRows <= 0 || req.Limits.MaxBytes <= 0 {
		return ErrNativeReportLimit
	}
	parts := strings.Split(req.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(req.Repository, " \t\r\n\\\x00") || strings.ToLower(req.Repository) != req.Repository {
		return ErrRetainedSnapshot
	}
	before, err := retainedSnapshotFile(ctx, path, req)
	if err != nil {
		return err
	}
	if err := withNativeReportTransaction(ctx, path, req.NativeReportRequest, true, read); err != nil {
		return err
	}
	after, err := retainedSnapshotFile(ctx, path, req)
	if err != nil {
		return err
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return ErrRetainedSnapshot
	}
	return nil
}

func retainedSnapshotFile(ctx context.Context, path string, req RetainedSnapshotRequest) (os.FileInfo, error) {
	expected, err := hex.DecodeString(req.SHA256)
	if err != nil || len(expected) != sha256.Size || strings.ToLower(req.SHA256) != req.SHA256 {
		return nil, ErrRetainedSnapshot
	}
	id, err := NativeReportFileIdentity(path)
	if err != nil || id != req.File {
		return nil, ErrRetainedSnapshot
	}
	// WAL content is not part of the pinned main-file digest. Empty WAL/SHM may
	// remain after an earlier reader; no sidecar is created or removed here.
	for _, suffix := range []string{"-wal", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if err == nil && (!info.Mode().IsRegular() || info.Size() != 0) {
			return nil, ErrRetainedSnapshot
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, ErrRetainedSnapshot
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	buf := make([]byte, 128<<10)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || hex.EncodeToString(h.Sum(nil)) != req.SHA256 {
		return nil, ErrRetainedSnapshot
	}
	id, err = NativeReportFileIdentity(path)
	if err != nil || id != req.File {
		return nil, ErrRetainedSnapshot
	}
	return after, nil
}

func retainedSnapshotAnchor(ctx context.Context, tx *sql.Tx, req NativeReportRequest) (NativeReportAnchor, error) {
	out := NativeReportAnchor{File: req.File, Repository: req.Repository}
	var version, count int
	if err := tx.QueryRowContext(ctx, "pragma user_version").Scan(&version); err != nil {
		return out, err
	}
	portable, err := changeContractTablePresent(ctx, tx, "portable_metadata")
	if err != nil {
		return out, err
	}
	if version != schemaVersion || portable {
		return out, ErrRetainedSnapshot
	}
	if err := tx.QueryRowContext(ctx, "select count(*),coalesce(min(id),0) from repositories where lower(full_name)=?", req.Repository).Scan(&count, &out.RepositoryID); err != nil {
		return out, err
	}
	if count != 1 || out.RepositoryID <= 0 {
		return out, ErrRetainedSnapshot
	}
	h := sha256.New()
	nativeReportHashText(h, NativeReportProfile)
	nativeReportHashText(h, req.Repository)
	out.ScopeSHA256 = hex.EncodeToString(h.Sum(nil))
	return out, nil
}
