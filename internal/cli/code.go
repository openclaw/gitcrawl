package cli

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/codeindex"
	"github.com/openclaw/gitcrawl/internal/store"
)

type codeIndexResult struct {
	Repository         string `json:"repository"`
	SourceRoot         string `json:"source_root"`
	GitSHA             string `json:"git_sha"`
	WorktreeDirty      bool   `json:"worktree_dirty"`
	SnapshotID         int64  `json:"snapshot_id"`
	FilesSeen          int    `json:"files_seen"`
	FilesIndexed       int    `json:"files_indexed"`
	BytesIndexed       int64  `json:"bytes_indexed"`
	SkippedBinary      int    `json:"skipped_binary"`
	SkippedLarge       int    `json:"skipped_large"`
	SkippedMissing     int    `json:"skipped_missing"`
	SkippedNonRegular  int    `json:"skipped_non_regular"`
	SkippedOutsideRoot int    `json:"skipped_outside_root"`
	IndexedAt          string `json:"indexed_at"`
}

func (a *App) runCode(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("code requires a subcommand"))
	}
	switch args[0] {
	case "index":
		return a.runCodeIndex(ctx, args[1:])
	default:
		return usageErr(fmt.Errorf("unknown code subcommand %q", args[0]))
	}
}

func (a *App) runCodeIndex(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("code index", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("path", ".", "local Git checkout")
	maxFileBytesRaw := fs.String("max-file-bytes", strconv.FormatInt(codeindex.DefaultMaxFileBytes, 10), "maximum bytes per indexed file")
	maxTotalBytesRaw := fs.String("max-total-bytes", strconv.FormatInt(codeindex.DefaultMaxTotalBytes, 10), "maximum bytes across indexed files")
	maxFilesRaw := fs.String("max-files", strconv.Itoa(codeindex.DefaultMaxFiles), "maximum tracked text files")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"path": true, "max-file-bytes": true, "max-total-bytes": true, "max-files": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("code index requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	maxFileBytes, err := strconv.ParseInt(strings.TrimSpace(*maxFileBytesRaw), 10, 64)
	if err != nil || maxFileBytes <= 0 {
		return usageErr(fmt.Errorf("code index requires positive --max-file-bytes"))
	}
	maxTotalBytes, err := strconv.ParseInt(strings.TrimSpace(*maxTotalBytesRaw), 10, 64)
	if err != nil || maxTotalBytes <= 0 {
		return usageErr(fmt.Errorf("code index requires positive --max-total-bytes"))
	}
	maxFiles, err := parseOptionalPositiveInt(*maxFilesRaw)
	if err != nil || maxFiles <= 0 {
		return usageErr(fmt.Errorf("code index requires positive --max-files"))
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	if rt.RemoteSource {
		return fmt.Errorf("code index requires a local database; portable-store runtime mirrors are refreshed from their source")
	}
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("repository %s/%s is not mirrored; run gitcrawl sync first", owner, repoName)
		}
		return err
	}
	scanned, err := codeindex.Scan(ctx, codeindex.Options{
		Path:          *path,
		MaxFileBytes:  maxFileBytes,
		MaxTotalBytes: maxTotalBytes,
		MaxFiles:      maxFiles,
	})
	if err != nil {
		return err
	}
	indexedAt := time.Now().UTC().Format(time.RFC3339Nano)
	documents := make([]store.CodeDocument, 0, len(scanned.Documents))
	for _, document := range scanned.Documents {
		documents = append(documents, store.CodeDocument{
			RepoID:      repo.ID,
			Path:        document.Path,
			Language:    document.Language,
			ContentHash: document.ContentHash,
			Text:        document.Text,
			ByteSize:    document.ByteSize,
			UpdatedAt:   indexedAt,
		})
	}
	snapshotID, err := rt.Store.ReplaceCodeSnapshot(ctx, store.CodeSnapshot{
		RepoID:        repo.ID,
		SourceRoot:    scanned.SourceRoot,
		GitSHA:        scanned.GitSHA,
		WorktreeDirty: scanned.WorktreeDirty,
		FileCount:     scanned.FilesIndexed,
		ByteCount:     scanned.BytesIndexed,
		IndexedAt:     indexedAt,
	}, documents)
	if err != nil {
		return err
	}
	return a.writeOutput("code index", codeIndexResult{
		Repository:         repo.FullName,
		SourceRoot:         scanned.SourceRoot,
		GitSHA:             scanned.GitSHA,
		WorktreeDirty:      scanned.WorktreeDirty,
		SnapshotID:         snapshotID,
		FilesSeen:          scanned.FilesSeen,
		FilesIndexed:       scanned.FilesIndexed,
		BytesIndexed:       scanned.BytesIndexed,
		SkippedBinary:      scanned.SkippedBinary,
		SkippedLarge:       scanned.SkippedLarge,
		SkippedMissing:     scanned.SkippedMissing,
		SkippedNonRegular:  scanned.SkippedNonRegular,
		SkippedOutsideRoot: scanned.SkippedOutsideRoot,
		IndexedAt:          indexedAt,
	}, true)
}
