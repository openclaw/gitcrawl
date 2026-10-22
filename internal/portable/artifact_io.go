package portable

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func fileSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func writeGzipArchive(sourcePath, archivePath string) (retErr error) {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	archive, err := os.OpenFile(archivePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if closeErr := archive.Close(); closeErr != nil {
			retErr = errors.Join(retErr, closeErr)
		}
		if !complete {
			_ = os.Remove(archivePath)
		}
	}()
	writer, err := gzip.NewWriterLevel(archive, gzip.BestCompression)
	if err != nil {
		return err
	}
	if _, err := io.Copy(writer, source); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := archive.Sync(); err != nil {
		return err
	}
	complete = true
	return nil
}

func validateGzipArchive(archivePath string, expectedArchiveBytes int64, expectedArchiveSHA string, expectedOutputBytes int64, expectedOutputSHA string) error {
	info, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("re-stat portable archive: %w", err)
	}
	if info.Size() != expectedArchiveBytes {
		return fmt.Errorf("portable manifest archiveBytes %d does not match archive size %d", expectedArchiveBytes, info.Size())
	}
	archiveSHA, err := fileSHA256(archivePath)
	if err != nil {
		return fmt.Errorf("re-hash portable archive: %w", err)
	}
	if archiveSHA != expectedArchiveSHA {
		return fmt.Errorf("portable manifest archiveSha256 does not match archive")
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open portable archive: %w", err)
	}
	defer archive.Close()
	reader, err := gzip.NewReader(archive)
	if err != nil {
		return fmt.Errorf("open portable gzip stream: %w", err)
	}
	hash := sha256.New()
	written, copyErr := io.Copy(hash, io.LimitReader(reader, expectedOutputBytes+1))
	closeErr := reader.Close()
	if copyErr != nil {
		return fmt.Errorf("read portable gzip stream: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close portable gzip stream: %w", closeErr)
	}
	if written != expectedOutputBytes {
		return fmt.Errorf("portable archive expands to %d bytes, expected %d", written, expectedOutputBytes)
	}
	if hex.EncodeToString(hash.Sum(nil)) != expectedOutputSHA {
		return fmt.Errorf("portable archive contents do not match database sha256")
	}
	return nil
}

func writeSyncedFile(filePath string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(filePath)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func syncRegularFile(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func bestEffortSyncDir(dir string) {
	file, err := os.Open(dir)
	if err == nil {
		_ = file.Sync()
		_ = file.Close()
	}
}

func removeSQLiteSidecars(dbPath string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		sidecar := dbPath + suffix
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove portable SQLite sidecar %s: %w", sidecar, err)
		}
		if _, err := os.Lstat(sidecar); err == nil {
			return fmt.Errorf("portable SQLite sidecar remains after removal: %s", sidecar)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("verify portable SQLite sidecar removal %s: %w", sidecar, err)
		}
	}
	return nil
}

// splitPartName names part index i of a split archive beside its database.
func splitPartName(dbName string, index int) string {
	return fmt.Sprintf("%s.part-%03d", dbName, index)
}

// writeSplitArchive copies the database into ordered parts of at most
// partBytes. Each part is written exclusively and synced; a failure removes
// every part it created.
func writeSplitArchive(dbPath string, partBytes int64) (parts []ArchivePart, retErr error) {
	if partBytes <= 0 {
		return nil, fmt.Errorf("split part size must be positive")
	}
	source, err := os.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() <= 0 {
		return nil, fmt.Errorf("split archive requires a non-empty database")
	}
	count := (info.Size() + partBytes - 1) / partBytes
	if count > maxArchiveParts {
		return nil, fmt.Errorf("split archive needs %d parts; maximum is %d", count, maxArchiveParts)
	}
	dir := filepath.Dir(dbPath)
	defer func() {
		if retErr != nil {
			for _, part := range parts {
				_ = os.Remove(filepath.Join(dir, part.Path))
			}
			parts = nil
		}
	}()
	for index := 0; int64(index) < count; index++ {
		name := splitPartName(filepath.Base(dbPath), index)
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return parts, err
		}
		parts = append(parts, ArchivePart{Path: name})
		hash := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(source, partBytes))
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
			return parts, err
		}
		if written <= 0 {
			return parts, fmt.Errorf("split archive part %d is empty", index)
		}
		parts[index].Bytes = written
		parts[index].SHA256 = hex.EncodeToString(hash.Sum(nil))
	}
	return parts, nil
}

// validateSplitArchive verifies part names, sizes and digests, and that the
// ordered concatenation is exactly the manifest database.
func validateSplitArchive(dir string, parts []ArchivePart, archiveBytes int64, archiveSHA string, outputBytes int64, outputSHA string, maxPartBytes *int64) error {
	if len(parts) == 0 || len(parts) > maxArchiveParts {
		return fmt.Errorf("portable manifest split archive must have 1..%d parts", maxArchiveParts)
	}
	if archiveBytes != outputBytes || archiveSHA != outputSHA {
		return fmt.Errorf("portable manifest split archive metadata must equal the database size and sha256")
	}
	whole := sha256.New()
	var total int64
	for index, part := range parts {
		if part.Path == "" || filepath.Base(part.Path) != part.Path || part.Path == "." || part.Path == ".." {
			return fmt.Errorf("portable manifest archive part path must be a basename")
		}
		if part.Bytes <= 0 || part.SHA256 == "" {
			return fmt.Errorf("portable manifest archive part %d metadata is incomplete", index)
		}
		if maxPartBytes != nil && part.Bytes > *maxPartBytes {
			return fmt.Errorf("portable manifest archive part %d exceeds maxArchiveBytes", index)
		}
		file, err := os.Open(filepath.Join(dir, part.Path))
		if err != nil {
			return fmt.Errorf("open portable archive part: %w", err)
		}
		hash := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(hash, whole), io.LimitReader(file, part.Bytes+1))
		closeErr := file.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return fmt.Errorf("read portable archive part: %w", err)
		}
		if written != part.Bytes {
			return fmt.Errorf("portable archive part %d has %d bytes, expected %d", index, written, part.Bytes)
		}
		if hex.EncodeToString(hash.Sum(nil)) != part.SHA256 {
			return fmt.Errorf("portable archive part %d does not match its sha256", index)
		}
		total += written
	}
	if total != outputBytes {
		return fmt.Errorf("portable archive parts total %d bytes, expected %d", total, outputBytes)
	}
	if hex.EncodeToString(whole.Sum(nil)) != outputSHA {
		return fmt.Errorf("portable archive parts do not match database sha256")
	}
	return nil
}
