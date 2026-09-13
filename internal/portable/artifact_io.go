package portable

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
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
