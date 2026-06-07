package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type CodeSnapshot struct {
	ID            int64  `json:"id"`
	RepoID        int64  `json:"repo_id"`
	SourceRoot    string `json:"source_root"`
	GitSHA        string `json:"git_sha"`
	WorktreeDirty bool   `json:"worktree_dirty"`
	FileCount     int    `json:"file_count"`
	ByteCount     int64  `json:"byte_count"`
	IndexedAt     string `json:"indexed_at"`
}

type CodeDocument struct {
	ID          int64  `json:"id"`
	SnapshotID  int64  `json:"snapshot_id"`
	RepoID      int64  `json:"repo_id"`
	Path        string `json:"path"`
	Language    string `json:"language"`
	ContentHash string `json:"content_hash"`
	Text        string `json:"text"`
	ByteSize    int64  `json:"byte_size"`
	UpdatedAt   string `json:"updated_at"`
}

type CodeSearchHit struct {
	DocumentID    int64  `json:"document_id"`
	Path          string `json:"path"`
	Language      string `json:"language"`
	Snippet       string `json:"snippet"`
	GitSHA        string `json:"git_sha"`
	SourceRoot    string `json:"source_root"`
	WorktreeDirty bool   `json:"worktree_dirty"`
}

func (s *Store) ReplaceCodeSnapshot(ctx context.Context, snapshot CodeSnapshot, documents []CodeDocument) (int64, error) {
	var snapshotID int64
	err := s.WithTx(ctx, func(tx *Store) error {
		dirty := 0
		if snapshot.WorktreeDirty {
			dirty = 1
		}
		err := tx.q().QueryRowContext(ctx, `
			insert into code_snapshots(repo_id, source_root, git_sha, worktree_dirty, file_count, byte_count, indexed_at)
			values(?, ?, ?, ?, ?, ?, ?)
			returning id
		`, snapshot.RepoID, snapshot.SourceRoot, snapshot.GitSHA, dirty, snapshot.FileCount, snapshot.ByteCount, snapshot.IndexedAt).Scan(&snapshotID)
		if err != nil {
			return fmt.Errorf("insert code snapshot: %w", err)
		}
		stmt, err := tx.q().PrepareContext(ctx, `
			insert into code_documents(snapshot_id, repo_id, path, language, content_hash, text_content, byte_size, updated_at)
			values(?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return fmt.Errorf("prepare code document insert: %w", err)
		}
		defer stmt.Close()
		for _, document := range documents {
			if _, err := stmt.ExecContext(ctx, snapshotID, snapshot.RepoID, document.Path, document.Language, document.ContentHash, document.Text, document.ByteSize, document.UpdatedAt); err != nil {
				return fmt.Errorf("insert code document %s: %w", document.Path, err)
			}
		}
		if _, err := tx.q().ExecContext(ctx, `delete from code_snapshots where repo_id = ? and id != ?`, snapshot.RepoID, snapshotID); err != nil {
			return fmt.Errorf("prune old code snapshots: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return snapshotID, nil
}

func (s *Store) LatestCodeSnapshot(ctx context.Context, repoID int64) (CodeSnapshot, error) {
	if !s.hasTable(ctx, "code_snapshots") {
		return CodeSnapshot{}, sql.ErrNoRows
	}
	var snapshot CodeSnapshot
	var dirty int
	err := s.q().QueryRowContext(ctx, `
		select id, repo_id, source_root, git_sha, worktree_dirty, file_count, byte_count, indexed_at
		from code_snapshots
		where repo_id = ?
		order by indexed_at desc, id desc
		limit 1
	`, repoID).Scan(&snapshot.ID, &snapshot.RepoID, &snapshot.SourceRoot, &snapshot.GitSHA, &dirty, &snapshot.FileCount, &snapshot.ByteCount, &snapshot.IndexedAt)
	if err != nil {
		return CodeSnapshot{}, err
	}
	snapshot.WorktreeDirty = dirty != 0
	return snapshot, nil
}

func (s *Store) SearchCodeDocuments(ctx context.Context, repoID int64, query string, limit int) ([]CodeSearchHit, error) {
	if !s.hasTable(ctx, "code_documents") || !s.hasTable(ctx, "code_documents_fts") {
		return []CodeSearchHit{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	snapshot, err := s.LatestCodeSnapshot(ctx, repoID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []CodeSearchHit{}, nil
		}
		return nil, fmt.Errorf("latest code snapshot: %w", err)
	}
	matchQuery := ftsQuery(query)
	if matchQuery == "" {
		return s.searchCodeDocumentsLike(ctx, snapshot, query, limit)
	}
	rows, err := s.q().QueryContext(ctx, `
		select cd.id, cd.path, cd.language,
			snippet(code_documents_fts, 2, '[', ']', '...', 24)
		from code_documents_fts
		join code_documents cd on cd.id = code_documents_fts.rowid
		where cd.snapshot_id = ? and code_documents_fts match ?
		order by bm25(code_documents_fts)
		limit ?
	`, snapshot.ID, matchQuery, limit)
	if err != nil {
		return s.searchCodeDocumentsLike(ctx, snapshot, query, limit)
	}
	defer rows.Close()
	hits, err := scanCodeSearchHits(rows, snapshot)
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return s.searchCodeDocumentsLike(ctx, snapshot, query, limit)
	}
	return hits, nil
}

func (s *Store) searchCodeDocumentsLike(ctx context.Context, snapshot CodeSnapshot, query string, limit int) ([]CodeSearchHit, error) {
	needle := strings.TrimSpace(strings.ToLower(query))
	if needle == "" {
		return []CodeSearchHit{}, nil
	}
	pattern := "%" + escapeLike(needle) + "%"
	rows, err := s.q().QueryContext(ctx, `
		select id, path, language, substr(text_content, 1, 320)
		from code_documents
		where snapshot_id = ?
		  and (lower(path) like ? escape '\' or lower(text_content) like ? escape '\')
		order by path
		limit ?
	`, snapshot.ID, pattern, pattern, limit)
	if err != nil {
		return nil, fmt.Errorf("search code documents: %w", err)
	}
	defer rows.Close()
	return scanCodeSearchHits(rows, snapshot)
}

func scanCodeSearchHits(rows *sql.Rows, snapshot CodeSnapshot) ([]CodeSearchHit, error) {
	out := make([]CodeSearchHit, 0)
	for rows.Next() {
		var hit CodeSearchHit
		if err := rows.Scan(&hit.DocumentID, &hit.Path, &hit.Language, &hit.Snippet); err != nil {
			return nil, fmt.Errorf("scan code search hit: %w", err)
		}
		hit.GitSHA = snapshot.GitSHA
		hit.SourceRoot = snapshot.SourceRoot
		hit.WorktreeDirty = snapshot.WorktreeDirty
		out = append(out, hit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate code search hits: %w", err)
	}
	return out, nil
}
