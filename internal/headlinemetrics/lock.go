package headlinemetrics

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var errWriterBusy = errors.New("another metrics writer is running")

// The persistent sidecar must never be unlinked: all writers lock the same inode.
// Closing the file (including process exit) releases ownership without stale PIDs.
func acquireWriter(database string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(database), 0700); err != nil {
		return nil, err
	}
	path := filepath.Clean(database) + ".writer.lock"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return nil, statErr
		}
		if !info.Mode().IsRegular() {
			return nil, errors.New("metrics writer lock must be a regular file")
		}
		f, err = os.OpenFile(path, os.O_RDWR, 0600)
	}
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*os.File, error) {
		_ = f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	entry, err := os.Lstat(path)
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() || !entry.Mode().IsRegular() || !os.SameFile(info, entry) {
		return fail(errors.New("metrics writer lock path changed or is not regular"))
	}
	if err := lockWriterFile(f); err != nil {
		return fail(fmt.Errorf("lock metrics database: %w", err))
	}
	if err := f.Chmod(0600); err != nil {
		return fail(err)
	}
	return f, nil
}
