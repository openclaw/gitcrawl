//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package headlinemetrics

import (
	"errors"
	"os"
)

func lockWriterFile(*os.File) error {
	return errors.New("metrics writer locking is unsupported on this platform")
}

func checkSingleLink(*os.File) error {
	return errors.New("metrics file identity checks are unsupported on this platform")
}
