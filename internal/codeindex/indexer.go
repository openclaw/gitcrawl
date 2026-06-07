package codeindex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const (
	DefaultMaxFileBytes  int64 = 512 * 1024
	DefaultMaxTotalBytes int64 = 256 * 1024 * 1024
	DefaultMaxFiles            = 100000
)

type Options struct {
	Path          string
	MaxFileBytes  int64
	MaxTotalBytes int64
	MaxFiles      int
}

type Document struct {
	Path        string
	Language    string
	ContentHash string
	Text        string
	ByteSize    int64
}

type Result struct {
	SourceRoot         string
	GitSHA             string
	WorktreeDirty      bool
	FilesSeen          int
	FilesIndexed       int
	BytesIndexed       int64
	SkippedBinary      int
	SkippedLarge       int
	SkippedMissing     int
	SkippedNonRegular  int
	SkippedOutsideRoot int
	Documents          []Document
}

func Scan(ctx context.Context, options Options) (Result, error) {
	path := strings.TrimSpace(options.Path)
	if path == "" {
		path = "."
	}
	if options.MaxFileBytes <= 0 {
		options.MaxFileBytes = DefaultMaxFileBytes
	}
	if options.MaxTotalBytes <= 0 {
		options.MaxTotalBytes = DefaultMaxTotalBytes
	}
	if options.MaxFiles <= 0 {
		options.MaxFiles = DefaultMaxFiles
	}

	root, err := gitText(ctx, path, "rev-parse", "--show-toplevel")
	if err != nil {
		return Result{}, err
	}
	root, err = filepath.Abs(strings.TrimSpace(root))
	if err != nil {
		return Result{}, fmt.Errorf("resolve source root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return Result{}, fmt.Errorf("resolve source root symlinks: %w", err)
	}
	sha, err := gitText(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return Result{}, err
	}
	status, err := gitBytes(ctx, root, "status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return Result{}, err
	}
	rawEntries, err := gitBytes(ctx, root, "ls-files", "-z", "--cached", "--stage")
	if err != nil {
		return Result{}, err
	}
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return Result{}, fmt.Errorf("open source root: %w", err)
	}
	defer rootFS.Close()

	result := Result{
		SourceRoot:    root,
		GitSHA:        strings.TrimSpace(string(sha)),
		WorktreeDirty: len(bytes.TrimSpace(status)) > 0,
	}
	for _, rawEntry := range bytes.Split(rawEntries, []byte{0}) {
		if len(rawEntry) == 0 {
			continue
		}
		result.FilesSeen++
		metadata, rawPath, ok := bytes.Cut(rawEntry, []byte{'\t'})
		fields := bytes.Fields(metadata)
		if !ok || len(fields) != 3 {
			return Result{}, fmt.Errorf("parse tracked entry %q", rawEntry)
		}
		mode := string(fields[0])
		stage := string(fields[2])
		if stage != "0" || (mode != "100644" && mode != "100755") {
			result.SkippedNonRegular++
			continue
		}
		path := filepath.ToSlash(string(rawPath))
		if !safeRelativePath(path) {
			return Result{}, fmt.Errorf("git returned unsafe tracked path %q", path)
		}
		rootPath := filepath.FromSlash(path)
		info, err := rootFS.Lstat(rootPath)
		if err != nil {
			if os.IsNotExist(err) {
				result.SkippedMissing++
				continue
			}
			if pathEscapesRoot(err) {
				result.SkippedOutsideRoot++
				continue
			}
			return Result{}, fmt.Errorf("stat tracked file %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			result.SkippedNonRegular++
			continue
		}
		if info.Size() > options.MaxFileBytes {
			result.SkippedLarge++
			continue
		}
		data, tooLarge, err := readFileAtMost(rootFS, rootPath, options.MaxFileBytes)
		if err != nil {
			if os.IsNotExist(err) {
				result.SkippedMissing++
				continue
			}
			if pathEscapesRoot(err) {
				result.SkippedOutsideRoot++
				continue
			}
			return Result{}, fmt.Errorf("read tracked file %s: %w", path, err)
		}
		if tooLarge {
			result.SkippedLarge++
			continue
		}
		if bytes.IndexByte(data, 0) >= 0 || !utf8.Valid(data) {
			result.SkippedBinary++
			continue
		}
		if result.FilesIndexed >= options.MaxFiles {
			return Result{}, fmt.Errorf("tracked text file count exceeds --max-files %d", options.MaxFiles)
		}
		if result.BytesIndexed+int64(len(data)) > options.MaxTotalBytes {
			return Result{}, fmt.Errorf("tracked text corpus exceeds --max-total-bytes %d", options.MaxTotalBytes)
		}
		sum := sha256.Sum256(data)
		result.Documents = append(result.Documents, Document{
			Path:        path,
			Language:    languageForPath(path),
			ContentHash: hex.EncodeToString(sum[:]),
			Text:        string(data),
			ByteSize:    int64(len(data)),
		})
		result.FilesIndexed++
		result.BytesIndexed += int64(len(data))
	}
	finalSHA, err := gitText(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(finalSHA) != result.GitSHA {
		return Result{}, fmt.Errorf("repository HEAD changed during code scan; retry indexing")
	}
	finalStatus, err := gitBytes(ctx, root, "status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return Result{}, err
	}
	result.WorktreeDirty = result.WorktreeDirty || len(bytes.TrimSpace(finalStatus)) > 0
	return result, nil
}

func readFileAtMost(root *os.Root, path string, maxBytes int64) ([]byte, bool, error) {
	file, err := root.Open(path)
	if err != nil {
		return nil, false, err
	}
	limit := maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	data, readErr := io.ReadAll(io.LimitReader(file, limit))
	closeErr := file.Close()
	if readErr != nil {
		return nil, false, readErr
	}
	if closeErr != nil {
		return nil, false, closeErr
	}
	if int64(len(data)) > maxBytes {
		return nil, true, nil
	}
	return data, false, nil
}

func pathEscapesRoot(err error) bool {
	return err != nil && strings.Contains(err.Error(), "path escapes from parent")
}

func safeRelativePath(path string) bool {
	if path == "" || filepath.IsAbs(filepath.FromSlash(path)) {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	return clean != ".." && !strings.HasPrefix(clean, "../")
}

func languageForPath(path string) string {
	base := strings.ToLower(filepath.Base(path))
	switch base {
	case "dockerfile":
		return "dockerfile"
	case "makefile":
		return "makefile"
	}
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
	if ext == "" {
		return "text"
	}
	return ext
}

func gitText(ctx context.Context, dir string, args ...string) (string, error) {
	out, err := gitBytes(ctx, dir, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func gitBytes(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s failed: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
