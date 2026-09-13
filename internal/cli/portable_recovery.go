package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func refreshPortableStoreForDB(ctx context.Context, dbPath string) error {
	root, ok, err := portableStoreRoot(ctx, dbPath)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	ctx, release, err := acquirePortableOwner(ctx, root)
	if err != nil {
		return err
	}
	defer release()
	root = ctx.Value(portableOwnerKey{}).(*portableOwner).root
	clean := gitWorktreeClean(ctx, root)
	if !clean {
		removed, _ := removeStaleGitIndexLock(ctx, root, staleGitIndexLockAge)
		if removed {
			clean = gitWorktreeClean(ctx, root)
		}
	}
	if !clean {
		return errPortableStoreDirty
	}
	pullCtx, cancel := context.WithTimeout(ctx, portableStoreRefreshTimeout)
	defer cancel()
	if _, err := fastForwardGitCheckoutWithStaleIndexLockRetry(pullCtx, root, true); err != nil {
		return err
	}
	return removePortableSQLiteSidecars(root)
}

type portableRepairResult struct {
	Action           string
	DBBackupPath     string
	StoreBackupPath  string
	RemovedIndexLock bool
}

func repairMalformedPortableStoreForDB(ctx context.Context, dbPath, configPath string) (portableRepairResult, error) {
	result := portableRepairResult{Action: "reset-pulled"}
	root, ok, err := portableStoreRoot(ctx, dbPath)
	if err != nil {
		return result, err
	}
	if !ok {
		return result, nil
	}
	ctx, release, err := acquirePortableOwner(ctx, root)
	if err != nil {
		return result, err
	}
	defer release()
	root = ctx.Value(portableOwnerKey{}).(*portableOwner).root
	if !portableStoreRepairAllowed(root, configPath) {
		return result, fmt.Errorf("refuse destructive repair for unmarked portable store checkout %s", root)
	}
	backupPath, err := preserveMalformedPortableDB(root, dbPath)
	if err != nil {
		return result, err
	}
	result.DBBackupPath = backupPath
	pullCtx, cancel := context.WithTimeout(ctx, portableStoreRepairTimeout)
	defer cancel()
	if !gitWorktreeClean(pullCtx, root) {
		removed, err := runGitWithStaleIndexLockRetry(pullCtx, root, "-C", root, "reset", "--hard", "HEAD")
		result.RemovedIndexLock = result.RemovedIndexLock || removed
		if err != nil {
			return result, err
		}
	}
	removed, err := fastForwardGitCheckoutWithStaleIndexLockRetry(pullCtx, root, true)
	result.RemovedIndexLock = result.RemovedIndexLock || removed
	if err != nil {
		return result, err
	}
	return result, removePortableSQLiteSidecars(root)
}

func recloneMalformedPortableStoreForDB(ctx context.Context, dbPath, configPath string) (portableRepairResult, error) {
	result := portableRepairResult{Action: "recloned"}
	root, ok, err := portableStoreRoot(ctx, dbPath)
	if err != nil {
		return result, err
	}
	if !ok {
		return result, nil
	}
	ctx, release, err := acquirePortableOwner(ctx, root)
	if err != nil {
		return result, err
	}
	defer release()
	root = ctx.Value(portableOwnerKey{}).(*portableOwner).root
	if !portableStoreRepairAllowed(root, configPath) {
		return result, fmt.Errorf("refuse reclone for unmarked portable store checkout %s", root)
	}
	remote := portableStoreRemoteURL(ctx, root)
	if strings.TrimSpace(remote) == "" {
		return result, fmt.Errorf("portable store remote not found for %s", root)
	}
	branch := currentGitBranch(ctx, root)
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	backupPath := filepath.Join(filepath.Dir(root), "backups", "checkout-malformed-"+timestamp)
	if err := os.MkdirAll(filepath.Dir(backupPath), 0o755); err != nil {
		return result, fmt.Errorf("create portable checkout backup parent: %w", err)
	}
	if err := os.Rename(root, backupPath); err != nil {
		return result, fmt.Errorf("preserve malformed portable checkout: %w", err)
	}
	result.StoreBackupPath = backupPath
	cloneCtx, cancel := context.WithTimeout(ctx, portableStoreRepairTimeout)
	defer cancel()
	cloneArgs := []string{"clone", "--depth", "1"}
	if strings.TrimSpace(branch) != "" {
		cloneArgs = append(cloneArgs, "--branch", branch)
	}
	cloneArgs = append(cloneArgs, "--", remote, root)
	if err := runGit(cloneCtx, "", cloneArgs...); err != nil {
		_ = os.RemoveAll(root)
		_ = os.Rename(backupPath, root)
		return result, err
	}
	if err := markPortableStoreCheckout(root); err != nil {
		return result, err
	}
	return result, removePortableSQLiteSidecars(root)
}

func isPortableSourceRepairableHealthError(err error) bool {
	return isSQLiteCorruption(err) || isPortableManifestMismatch(err)
}

func isSQLiteCorruption(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database disk image is malformed") ||
		strings.Contains(message, "file is not a database") ||
		strings.Contains(message, "sqlite quick_check failed") ||
		strings.Contains(message, "sqlite_corrupt") ||
		strings.Contains(message, "error code 11") ||
		strings.Contains(message, "(11)")
}

func isPortableManifestMismatch(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "portable manifest mismatch")
}

func preserveMalformedPortableDB(root, dbPath string) (string, error) {
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	backupDir := filepath.Join(filepath.Dir(root), "backups", "malformed-"+timestamp)
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return "", fmt.Errorf("create malformed db backup: %w", err)
	}
	paths := []string{
		dbPath,
		dbPath + "-wal",
		dbPath + "-shm",
		dbPath + ".manifest.json",
	}
	if archivePath, _, compressed, err := portableSourceArtifact(dbPath); err == nil && compressed {
		paths = append(paths, archivePath)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", err
		}
		target := filepath.Join(backupDir, filepath.Base(path)+".malformed")
		if strings.HasSuffix(path, ".manifest.json") {
			target = filepath.Join(backupDir, filepath.Base(path))
		}
		if err := copyFileAtomic(path, target); err != nil {
			return "", fmt.Errorf("preserve malformed db evidence: %w", err)
		}
	}
	return backupDir, nil
}

func fastForwardGitCheckoutWithStaleIndexLockRetry(ctx context.Context, root string, quiet bool) (bool, error) {
	ctx, release, err := acquirePortableOwner(ctx, root)
	if err != nil {
		return false, err
	}
	defer release()
	err = fastForwardGitCheckout(ctx, root, quiet)
	if err == nil {
		return false, nil
	}
	if !isGitIndexLockError(err) {
		return false, err
	}
	removed, cleanupErr := removeStaleGitIndexLock(ctx, root, staleGitIndexLockAge)
	if cleanupErr != nil || !removed {
		if cleanupErr != nil {
			return false, fmt.Errorf("%w; cleanup stale index lock: %v", err, cleanupErr)
		}
		return false, err
	}
	return true, fastForwardGitCheckout(ctx, root, quiet)
}

func runGitWithStaleIndexLockRetry(ctx context.Context, root string, args ...string) (bool, error) {
	ctx, release, err := acquirePortableOwner(ctx, root)
	if err != nil {
		return false, err
	}
	defer release()
	err = runGit(ctx, "", args...)
	if err == nil {
		return false, nil
	}
	if !isGitIndexLockError(err) {
		return false, err
	}
	removed, cleanupErr := removeStaleGitIndexLock(ctx, root, staleGitIndexLockAge)
	if cleanupErr != nil || !removed {
		if cleanupErr != nil {
			return false, fmt.Errorf("%w; cleanup stale index lock: %v", err, cleanupErr)
		}
		return false, err
	}
	return true, runGit(ctx, "", args...)
}

func isGitIndexLockError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "index.lock") && strings.Contains(message, "file exists")
}

func removeStaleGitIndexLock(ctx context.Context, root string, minAge time.Duration) (bool, error) {
	lockPath := filepath.Join(root, ".git", "index.lock")
	info, err := os.Stat(lockPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if minAge > 0 && time.Since(info.ModTime()) < minAge {
		return false, nil
	}
	lsofPath, err := exec.LookPath("lsof")
	if err != nil {
		return false, nil
	}
	cmd := exec.CommandContext(ctx, lsofPath, lockPath)
	out, err := cmd.CombinedOutput()
	if strings.TrimSpace(string(out)) != "" {
		return false, nil
	}
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			return false, nil
		}
	}
	if err := os.Remove(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, nil
}
