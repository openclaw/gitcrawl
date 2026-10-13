//go:build native_code_overlay

// This file is overlaid into internal/store for the isolated schema16 producer.
package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
)

func NativeSHA256(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (s *Store) NativeCodeWriterReady(ctx context.Context) error {
	var version int
	if e := s.q().QueryRowContext(ctx, "pragma user_version").Scan(&version); e != nil {
		return e
	}
	if version != 16 {
		return fmt.Errorf("schema16 required")
	}
	if s.hasTable(ctx, "capture_commits") {
		return fmt.Errorf("capture-owned source is outside this isolated producer contract")
	}
	changes, e := inspectStructuralCompatibilityMigrations(ctx, s, version, inspectPRDetailSchema(ctx, s))
	if e != nil {
		return e
	}
	if len(changes) != 0 {
		return fmt.Errorf("native schema needs migration; refusing code-only writer")
	}
	return nil
}
func (s *Store) NativeCodeSnapshotMatches(ctx context.Context, snap CodeSnapshot, docs []CodeDocument) (int64, bool, error) {
	old, e := s.LatestCodeSnapshot(ctx, snap.RepoID)
	if e == sql.ErrNoRows {
		return 0, false, nil
	}
	if e != nil {
		return 0, false, e
	}
	if old.RepoID != snap.RepoID || old.SourceRoot != snap.SourceRoot || old.GitSHA != snap.GitSHA || old.WorktreeDirty != snap.WorktreeDirty || old.FileCount != snap.FileCount || old.ByteCount != snap.ByteCount {
		return old.ID, false, nil
	}
	wanted := map[string]CodeDocument{}
	for _, d := range docs {
		if _, ok := wanted[d.Path]; ok {
			return 0, false, fmt.Errorf("duplicate code path")
		}
		wanted[d.Path] = d
	}
	rows, e := s.q().QueryContext(ctx, "select repo_id,path,language,content_hash,byte_size from code_documents where snapshot_id=?", old.ID)
	if e != nil {
		return 0, false, e
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var repo, size int64
		var path, lang, hash string
		if e = rows.Scan(&repo, &path, &lang, &hash, &size); e != nil {
			return 0, false, e
		}
		d, ok := wanted[path]
		if !ok || repo != snap.RepoID || lang != d.Language || hash != d.ContentHash || size != d.ByteSize {
			return old.ID, false, nil
		}
		seen++
	}
	if e = rows.Err(); e != nil {
		return 0, false, e
	}
	return old.ID, seen == len(docs), nil
}

func OpenNativeProducerWriter(ctx context.Context, path string) (*Store, error) {
	info, e := os.Stat(path)
	if e != nil {
		return nil, e
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("existing regular native archive required")
	}
	absolute, e := filepath.Abs(path)
	if e != nil {
		return nil, e
	}
	u := url.URL{Scheme: "file", Path: absolute}
	q := u.Query()
	q.Set("mode", "rw")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "synchronous(FULL)")
	if runtime.GOOS == "darwin" {
		q.Add("_pragma", "fullfsync(1)")
		q.Add("_pragma", "checkpoint_fullfsync(1)")
	}
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, path: path}
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, e
	}
	if e = s.NativeCodeWriterReady(ctx); e != nil {
		db.Close()
		return nil, e
	}
	var journal string
	if e = db.QueryRowContext(ctx, "pragma journal_mode").Scan(&journal); e != nil || journal != "wal" {
		db.Close()
		return nil, fmt.Errorf("existing WAL archive required")
	}
	return s, nil
}
