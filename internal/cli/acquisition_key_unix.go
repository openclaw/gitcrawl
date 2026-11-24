//go:build !windows

package cli

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

func readAcquisitionRootKey(path string) ([]byte, error) {
	refused := errors.New("mint requires root and an absolute root-owned regular key file with mode 0400 or 0600")
	if os.Geteuid() != 0 || !filepath.IsAbs(path) {
		return nil, refused
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || (before.Mode().Perm() != 0400 && before.Mode().Perm() != 0600) {
		return nil, refused
	}
	owner, ok := before.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || owner.Nlink != 1 {
		return nil, refused
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, refused
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) || after.Mode() != before.Mode() {
		return nil, refused
	}
	raw, err := io.ReadAll(io.LimitReader(f, (32<<10)+1))
	if err != nil || len(raw) > 32<<10 {
		return nil, errors.New("private key file could not be read within its size bound")
	}
	return raw, nil
}
