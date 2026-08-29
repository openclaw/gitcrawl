package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const portableOperationTimeout = 2 * time.Minute

type portableOwnerKey struct{}
type portableGitKey struct{}
type portableCommandKey struct{}

type portableCommandSession struct {
	mu     sync.Mutex
	owner  *portableOwner
	retain bool
}

func (session *portableCommandSession) close() {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.owner != nil {
		_ = session.owner.file.Close()
	}
}

type portableGitExecutable struct {
	path string
	info os.FileInfo
}

type portableOwner struct {
	root string
	file *os.File
	git  portableGitExecutable
}

// Resolve existing ancestors too: a first clone and later operations must use
// the same lock, including when the store is reached through a directory link.
func canonicalPortablePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if info, statErr := os.Lstat(abs); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(abs)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(abs), target)
		}
		return canonicalPortablePath(target)
	}
	parent := filepath.Dir(abs)
	if parent == abs {
		return "", err
	}
	resolved, err = canonicalPortablePath(parent)
	return filepath.Join(resolved, filepath.Base(abs)), err
}

func portableGitContext(ctx context.Context, executable string) (context.Context, error) {
	if executable == "" {
		if _, ok := ctx.Value(portableGitKey{}).(portableGitExecutable); ok {
			return ctx, nil
		}
		executable = os.Getenv("GITCRAWL_PORTABLE_GIT")
	}
	if executable == "" {
		var err error
		executable, err = exec.LookPath("git")
		if err != nil {
			return ctx, fmt.Errorf("resolve portable Git executable: %w", err)
		}
	} else if !filepath.IsAbs(executable) {
		return ctx, fmt.Errorf("portable Git executable must be an absolute path")
	}
	executable, err := filepath.Abs(executable)
	if err != nil {
		return ctx, err
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return ctx, fmt.Errorf("resolve portable Git executable: %w", err)
	}
	info, err := os.Stat(executable)
	if err != nil || !info.Mode().IsRegular() {
		return ctx, fmt.Errorf("portable Git executable is not a regular file")
	}
	return context.WithValue(ctx, portableGitKey{}, portableGitExecutable{path: executable, info: info}), nil
}

// The permanent sibling lock survives recovery renames. Never unlink it: doing
// so would allow two owners to lock different inodes at the same pathname.
func acquirePortableOwner(ctx context.Context, root string) (context.Context, func(), error) {
	root, err := canonicalPortablePath(root)
	if err != nil {
		return ctx, nil, err
	}
	session, _ := ctx.Value(portableCommandKey{}).(*portableCommandSession)
	if session != nil {
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.owner != nil {
			ctx = context.WithValue(ctx, portableOwnerKey{}, session.owner)
			ctx = context.WithValue(ctx, portableGitKey{}, session.owner.git)
		}
	}
	if owner, ok := ctx.Value(portableOwnerKey{}).(*portableOwner); ok {
		if owner.root != root {
			return ctx, nil, fmt.Errorf("portable operation already owns a different store")
		}
		return ctx, func() {}, owner.check()
	}
	ctx, err = portableGitContext(ctx, "")
	if err != nil {
		return ctx, nil, err
	}
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(root), 0o755); err != nil {
		return ctx, nil, err
	}
	lockPath := filepath.Join(filepath.Dir(root), "."+filepath.Base(root)+".gitcrawl.lock")
	if info, err := os.Lstat(lockPath); err == nil && !info.Mode().IsRegular() {
		return ctx, nil, fmt.Errorf("portable ownership lock is not a regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ctx, nil, err
	}
	file, err := openPortableLockFile(lockPath)
	if err != nil {
		return ctx, nil, err
	}
	owner := &portableOwner{root: root, file: file, git: ctx.Value(portableGitKey{}).(portableGitExecutable)}
	if err := owner.check(); err != nil {
		_ = file.Close()
		return ctx, nil, err
	}
	if err := lockPortableFile(file); err != nil {
		_ = file.Close()
		return ctx, nil, fmt.Errorf("portable store is busy (ownership lock): %w", err)
	}
	if session != nil && session.retain {
		session.owner = owner
		return context.WithValue(ctx, portableOwnerKey{}, owner), func() {}, nil
	}
	return context.WithValue(ctx, portableOwnerKey{}, owner), func() {
		_ = file.Close()
	}, nil
}

func (owner *portableOwner) check() error {
	opened, err := owner.file.Stat()
	if err != nil {
		return err
	}
	current, err := os.Lstat(owner.file.Name())
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(opened, current) {
		return fmt.Errorf("portable ownership lock changed")
	}
	return nil
}

func validatePortableRelativePath(value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\\\x00\r\n\t:<>\"|?*") || filepath.IsAbs(value) {
		return fmt.Errorf("portable database must be a clean relative slash path")
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." || strings.EqualFold(part, ".git") || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return fmt.Errorf("portable database must be a clean relative slash path outside Git metadata")
		}
		name, _, _ := strings.Cut(strings.ToUpper(part), ".")
		if name == "CON" || name == "PRN" || name == "AUX" || name == "NUL" || len(name) == 4 && (strings.HasPrefix(name, "COM") || strings.HasPrefix(name, "LPT")) && name[3] >= '1' && name[3] <= '9' {
			return fmt.Errorf("portable path contains a reserved device name")
		}
	}
	return nil
}

func validatePortableRemote(value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.HasPrefix(value, "-") || strings.ContainsAny(value, "\x00\r\n") || strings.Contains(value, "::") {
		return fmt.Errorf("invalid portable remote")
	}
	if strings.Contains(value, "://") {
		parsed, err := url.Parse(value)
		if err != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("invalid portable remote URL")
		}
		switch parsed.Scheme {
		case "https", "http", "ssh", "git", "file":
		default:
			return fmt.Errorf("unsupported portable remote transport")
		}
	}
	return nil
}
