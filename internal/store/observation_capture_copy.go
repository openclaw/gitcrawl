package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
)

// DetachObservationCaptureCopy is restricted to a distinct frozen file with
// matching current source lineage. Publication callers must perform their final
// secure rewrite/VACUUM before exposing the transformed copy.
func DetachObservationCaptureCopy(ctx context.Context, sourcePath, copyPath string, db *sql.DB) error {
	present, err := capturePresent(ctx, db)
	if err != nil || !present {
		return err
	}
	original, err := os.Stat(sourcePath)
	if err != nil {
		return err
	}
	copyInfo, err := os.Stat(copyPath)
	if err != nil {
		return err
	}
	// Bind the SQL handle as well as the supplied pathname. A mismatched handle
	// must never allow a real source to be detached using a different file's name.
	var attachedPath string
	if err = db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&attachedPath); err != nil {
		return err
	}
	attachedInfo, err := os.Stat(attachedPath)
	if err != nil {
		return err
	}
	if !os.SameFile(copyInfo, attachedInfo) {
		return errors.New("capture copy handle/path mismatch")
	}
	if os.SameFile(original, copyInfo) {
		return errors.New("capture detachment requires a distinct derived copy")
	}
	source, err := OpenReadOnly(ctx, sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	a, err := captureCapability(ctx, source.DB())
	if err != nil {
		return err
	}
	b, err := captureCapability(ctx, db)
	if err != nil {
		return err
	}
	if a.ArchiveID != b.ArchiveID || a.Epoch != b.Epoch {
		return errors.New("capture copy lineage changed; refusing publication")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = validateCaptureSchema(ctx, tx); err != nil {
		return err
	}
	for _, ddl := range captureDDL() {
		parts := strings.Fields(ddl)
		if parts[1] == "TRIGGER" {
			if _, err = tx.ExecContext(ctx, "DROP TRIGGER "+captureQuote(parts[2])); err != nil {
				return err
			}
		}
	}
	for _, table := range []string{"capture_supports", "capture_observations", "_capture_dirty", "_capture_context", "capture_heads", "capture_images", "capture_commits", "capture_meta"} {
		if _, err = tx.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "PRAGMA user_version=16"); err != nil {
		return err
	}
	return tx.Commit()
}
