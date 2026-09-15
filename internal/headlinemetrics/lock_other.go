//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package headlinemetrics

import (
	"errors"
	"os"
)

func lockWriterFile(*os.File) error {
	return errors.New("metrics writer locking is unsupported on this platform")
}
