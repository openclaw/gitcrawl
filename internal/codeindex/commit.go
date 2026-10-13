package codeindex

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ScanCommit reads immutable Git blobs, never worktree files or checkout filters.
// Callers own fetching and retention. A full object ID pins every document even
// when HEAD or the upstream branch advances while this scan is running.
func ScanCommit(ctx context.Context, options Options, commit string) (Result, error) {
	if !fullObjectID(commit) {
		return Result{}, fmt.Errorf("code scan requires a full lowercase commit object ID")
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
	if options.Path == "" {
		return Result{}, fmt.Errorf("code repository path required")
	}
	git := func(args ...string) ([]byte, error) {
		cmd := commitGit(ctx, options.Path, args...)
		var out boundedGitOutput
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("read commit metadata: %w", err)
		}
		return out.Bytes(), nil
	}
	resolved, err := git("rev-parse", "--verify", commit+"^{commit}")
	if err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(string(resolved)) != commit {
		return Result{}, fmt.Errorf("commit identity differs")
	}
	dir, err := git("rev-parse", "--absolute-git-dir")
	if err != nil {
		return Result{}, err
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(dir)))
	if err != nil {
		return Result{}, err
	}
	entries, err := git("ls-tree", "-r", "-l", "-z", "--full-tree", commit)
	if err != nil {
		return Result{}, err
	}
	result := Result{SourceRoot: root, GitSHA: commit, WorktreeDirty: false}
	cmd := commitGit(ctx, root, "cat-file", "--batch")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return Result{}, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return Result{}, err
	}
	if err = cmd.Start(); err != nil {
		stdin.Close()
		return Result{}, err
	}
	finished := false
	defer func() {
		stdin.Close()
		if !finished {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	reader := bufio.NewReaderSize(stdout, 4096)
	for _, entry := range bytes.Split(entries, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		result.FilesSeen++
		metadata, rawPath, ok := bytes.Cut(entry, []byte{'\t'})
		fields := strings.Fields(string(metadata))
		if !ok || len(fields) != 4 {
			return Result{}, fmt.Errorf("invalid commit tree entry")
		}
		if fields[1] != "blob" || (fields[0] != "100644" && fields[0] != "100755") {
			result.SkippedNonRegular++
			continue
		}
		path := string(rawPath)
		if !utf8.Valid(rawPath) || !safeRelativePath(path) {
			return Result{}, fmt.Errorf("unsafe commit path")
		}
		size, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil || size < 0 || !fullObjectID(fields[2]) {
			return Result{}, fmt.Errorf("invalid commit blob metadata")
		}
		if size > options.MaxFileBytes {
			result.SkippedLarge++
			continue
		}
		if _, err := fmt.Fprintln(stdin, fields[2]); err != nil {
			return Result{}, err
		}
		header, err := reader.ReadString('\n')
		if err != nil || len(header) > 256 {
			return Result{}, fmt.Errorf("invalid commit blob header")
		}
		if strings.TrimSuffix(header, "\n") != fields[2]+" blob "+fields[3] {
			return Result{}, fmt.Errorf("commit blob identity/size differs")
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return Result{}, err
		}
		if end, err := reader.ReadByte(); err != nil || end != '\n' {
			return Result{}, fmt.Errorf("incomplete commit blob")
		}
		if blobObjectID(data, len(fields[2])) != fields[2] {
			return Result{}, fmt.Errorf("commit blob checksum differs")
		}
		if bytes.IndexByte(data, 0) >= 0 || !utf8.Valid(data) {
			result.SkippedBinary++
			continue
		}
		if result.FilesIndexed >= options.MaxFiles {
			return Result{}, fmt.Errorf("tracked text file count exceeds --max-files %d", options.MaxFiles)
		}
		if size > options.MaxTotalBytes-result.BytesIndexed {
			return Result{}, fmt.Errorf("tracked text corpus exceeds --max-total-bytes %d", options.MaxTotalBytes)
		}
		sum := sha256.Sum256(data)
		result.Documents = append(result.Documents, Document{Path: path, Language: languageForPath(path), ContentHash: hex.EncodeToString(sum[:]), Text: string(data), ByteSize: size})
		result.FilesIndexed++
		result.BytesIndexed += size
	}
	if err := stdin.Close(); err != nil {
		return Result{}, err
	}
	err = cmd.Wait()
	finished = true
	if err != nil {
		return Result{}, fmt.Errorf("read commit blobs: %w", err)
	}
	return result, nil
}

func fullObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

func blobObjectID(data []byte, width int) string {
	material := append([]byte(fmt.Sprintf("blob %d\x00", len(data))), data...)
	if width == 40 {
		sum := sha1.Sum(material)
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(material)
	return hex.EncodeToString(sum[:])
}

type boundedGitOutput struct{ bytes.Buffer }

func (w *boundedGitOutput) Write(p []byte) (int, error) {
	if w.Len()+len(p) > 32<<20 {
		return 0, fmt.Errorf("commit tree metadata exceeds32MiB")
	}
	return w.Buffer.Write(p)
}

func commitGit(ctx context.Context, root string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "core.fsmonitor=false", "-C", root}, args...)...)
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if !strings.HasPrefix(strings.ToUpper(key), "GIT_") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0", "GIT_NO_REPLACE_OBJECTS=1", "GIT_NO_LAZY_FETCH=1")
	return cmd
}
