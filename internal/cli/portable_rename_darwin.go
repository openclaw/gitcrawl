package cli

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func renamePortableExclusive(from, to string) error {
	err := unix.RenameatxNp(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_EXCL)
	if err == unix.ENOSYS || err == unix.EINVAL || err == unix.ENOTSUP {
		return fmt.Errorf("%w: %w", errPortableExclusiveUnavailable, err)
	}
	return err
}
