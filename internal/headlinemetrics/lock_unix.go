//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package headlinemetrics

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func lockWriterFile(f *os.File) error {
	var info unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &info); err != nil {
		return err
	}
	if info.Nlink != 1 {
		return errors.New("metrics writer lock must not have hardlink aliases")
	}
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return errWriterBusy
	}
	return err
}
