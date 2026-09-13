package cli

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func renamePortableExclusive(from, to string) error {
	err := unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
	if err == unix.ENOSYS || err == unix.EINVAL || err == unix.EOPNOTSUPP {
		return fmt.Errorf("%w: %w", errPortableExclusiveUnavailable, err)
	}
	return err
}
