package cli

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func copyFileAtomic(sourcePath, targetPath string) error {
	tempPath, err := stageFileCopyTemp(sourcePath, targetPath, 0o600)
	if err != nil {
		return err
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		_ = os.Remove(tempPath)
		removeSQLiteTempSidecars(tempPath)
		return fmt.Errorf("replace portable runtime db: %w", err)
	}
	removeSQLiteTempSidecars(tempPath)
	removeSQLiteTempSidecars(targetPath)
	return nil
}

func stageFileCopyTemp(sourcePath, targetPath string, mode os.FileMode) (string, error) {
	return stageFileCopyTempContext(context.Background(), sourcePath, targetPath, mode)
}

func stageFileCopyTempContext(ctx context.Context, sourcePath, targetPath string, mode os.FileMode) (string, error) {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return "", fmt.Errorf("create portable runtime dir: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return "", fmt.Errorf("open portable source db: %w", err)
	}
	defer source.Close()
	temp, err := os.CreateTemp(filepath.Dir(targetPath), "."+filepath.Base(targetPath)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create portable runtime temp db: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
			removeSQLiteTempSidecars(tempPath)
		}
	}()
	if _, err := io.Copy(temp, portableContextReader{ctx: ctx, reader: source}); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("copy portable runtime db: %w", err)
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("chmod portable runtime db: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("close portable runtime db: %w", err)
	}
	cleanup = false
	return tempPath, nil
}

func copySQLiteFileAtomicVerified(ctx context.Context, sourcePath, targetPath string) ([32]byte, error) {
	var digest [32]byte
	tempPath, err := stagePortableSQLiteSourceTempContext(ctx, sourcePath, targetPath, 0o600)
	if err != nil {
		return digest, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
			removeSQLiteTempSidecars(tempPath)
		}
	}()
	if err := validatePortableSQLiteFile(ctx, tempPath, sourcePath); err != nil {
		return digest, fmt.Errorf("validate portable runtime temp db: %w", err)
	}
	digest, err = portableFileSHA256(ctx, tempPath)
	if err != nil {
		return digest, err
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		return digest, fmt.Errorf("replace portable runtime db: %w", err)
	}
	cleanup = false
	removeSQLiteTempSidecars(tempPath)
	removeSQLiteTempSidecars(targetPath)
	return digest, nil
}

func stagePortableSQLiteSourceTemp(sourceDBPath, targetPath string, mode os.FileMode) (string, error) {
	return stagePortableSQLiteSourceTempContext(context.Background(), sourceDBPath, targetPath, mode)
}

func stagePortableSQLiteSourceTempContext(ctx context.Context, sourceDBPath, targetPath string, mode os.FileMode) (string, error) {
	sourcePath, manifest, compressed, err := portableSourceArtifact(sourceDBPath)
	if err != nil {
		return "", err
	}
	if !compressed {
		return stageFileCopyTempContext(ctx, sourcePath, targetPath, mode)
	}
	if err := validatePortableArchiveContext(ctx, sourcePath, manifest); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return "", fmt.Errorf("create portable runtime dir: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return "", fmt.Errorf("open portable source archive: %w", err)
	}
	defer source.Close()
	reader, err := gzip.NewReader(source)
	if err != nil {
		return "", fmt.Errorf("open portable gzip archive: %w", err)
	}
	defer reader.Close()
	temp, err := os.CreateTemp(filepath.Dir(targetPath), "."+filepath.Base(targetPath)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create portable runtime temp db: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
			removeSQLiteTempSidecars(tempPath)
		}
	}()
	if manifest.OutputBytes <= 0 || manifest.OutputBytes == int64(^uint64(0)>>1) {
		_ = temp.Close()
		return "", fmt.Errorf("portable manifest mismatch: invalid outputBytes")
	}
	written, copyErr := io.Copy(temp, portableContextReader{ctx: ctx, reader: io.LimitReader(reader, manifest.OutputBytes+1)})
	if copyErr != nil {
		_ = temp.Close()
		return "", fmt.Errorf("inflate portable runtime db: %w", copyErr)
	}
	if written != manifest.OutputBytes {
		_ = temp.Close()
		return "", fmt.Errorf(
			"portable manifest mismatch: inflated size %d != %d",
			written,
			manifest.OutputBytes,
		)
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("chmod portable runtime db: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("close portable runtime db: %w", err)
	}
	cleanup = false
	return tempPath, nil
}

// publishPortableCheckoutPair replaces the checkout database and manifest with
// the pruned mirror pair. Both files are staged and validated inside the
// checkout before the first rename so a staging or validation failure leaves
// the previously published pair untouched.
func publishPortableCheckoutPair(ctx context.Context, mirrorDBPath, mirrorManifestPath, checkoutDBPath, checkoutManifestPath string) error {
	root, ok, err := portableStoreRoot(ctx, checkoutDBPath)
	if err != nil {
		return err
	}
	if ok {
		var release func()
		ctx, release, err = acquirePortableOwner(ctx, root)
		if err != nil {
			return err
		}
		defer release()
	}
	mode := os.FileMode(0o644)
	if info, err := os.Stat(checkoutDBPath); err == nil {
		mode = info.Mode().Perm()
	}
	manifestMode := mode
	if info, err := os.Stat(checkoutManifestPath); err == nil {
		manifestMode = info.Mode().Perm()
	}
	tempDB, err := stageFileCopyTemp(mirrorDBPath, checkoutDBPath, mode)
	if err != nil {
		return fmt.Errorf("stage published portable db: %w", err)
	}
	stagedDB := tempDB
	defer func() {
		if tempDB != "" {
			_ = os.Remove(tempDB)
		}
		removeSQLiteTempSidecars(stagedDB)
	}()
	tempManifest, err := stageFileCopyTemp(mirrorManifestPath, checkoutManifestPath, manifestMode)
	if err != nil {
		return fmt.Errorf("stage published portable manifest: %w", err)
	}
	defer func() {
		if tempManifest != "" {
			_ = os.Remove(tempManifest)
		}
	}()
	if err := sqliteStoreImmutableHealth(ctx, tempDB); err != nil {
		return fmt.Errorf("validate staged portable db: %w", err)
	}
	if err := validatePortableDBManifest(ctx, tempDB, tempManifest); err != nil {
		return fmt.Errorf("validate staged portable manifest: %w", err)
	}
	// The pair is replaced with two adjacent renames; a crash between them is
	// the same manifest-mismatch state a consumer's interrupted `git pull` can
	// produce, and manifest validation plus the git-based repair path recover
	// it. Concurrent publishers are out of contract (see the portable-store
	// caveats) the same way concurrent `git push` publishers are. A failed
	// manifest rename rolls the database back from the backup so an error
	// return leaves a consistent previous pair.
	rollbackDB := ""
	if _, err := os.Stat(checkoutDBPath); err == nil {
		backup, err := stageRollbackBackup(checkoutDBPath)
		if err != nil {
			return fmt.Errorf("back up published portable db: %w", err)
		}
		rollbackDB = backup
	}
	defer func() {
		if rollbackDB != "" {
			_ = os.Remove(rollbackDB)
		}
	}()
	if err := os.Rename(tempDB, checkoutDBPath); err != nil {
		return fmt.Errorf("replace published portable db: %w", err)
	}
	tempDB = ""
	removeSQLiteTempSidecars(checkoutDBPath)
	if err := os.Rename(tempManifest, checkoutManifestPath); err != nil {
		if rollbackDB != "" {
			backupPath := rollbackDB
			rollbackDB = ""
			if restoreErr := os.Rename(backupPath, checkoutDBPath); restoreErr != nil {
				// Keep the backup on disk for manual recovery.
				return fmt.Errorf("replace published portable manifest: %w; restoring the previous database from %s also failed: %v", err, backupPath, restoreErr)
			}
		}
		return fmt.Errorf("replace published portable manifest: %w", err)
	}
	tempManifest = ""
	return nil
}

// stageRollbackBackup snapshots path under a unique sibling name, preferring a
// hard link and falling back to a byte copy on filesystems without link
// support. The caller owns the returned file.
func stageRollbackBackup(path string) (string, error) {
	placeholder, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".publish-rollback-*")
	if err != nil {
		return "", err
	}
	backupPath := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		_ = os.Remove(backupPath)
		return "", err
	}
	if err := os.Remove(backupPath); err != nil {
		return "", err
	}
	if err := os.Link(path, backupPath); err == nil {
		return backupPath, nil
	}
	mode := os.FileMode(0o600)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	temp, err := stageFileCopyTemp(path, backupPath, mode)
	if err != nil {
		return "", err
	}
	if err := os.Rename(temp, backupPath); err != nil {
		_ = os.Remove(temp)
		removeSQLiteTempSidecars(temp)
		return "", err
	}
	return backupPath, nil
}

func removeSQLiteTempSidecars(path string) {
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func sweepOrphanPortableRuntimeTempFiles(mirrorPath string, maxAge time.Duration) {
	dir := filepath.Dir(mirrorPath)
	prefix := "." + filepath.Base(mirrorPath) + ".tmp-"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-maxAge)
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() && strings.HasPrefix(name, prefix) {
			info, err := entry.Info()
			if err == nil && info.ModTime().Before(cutoff) {
				_ = os.Remove(filepath.Join(dir, name))
			}
		}
	}
}
