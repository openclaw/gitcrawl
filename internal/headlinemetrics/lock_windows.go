package headlinemetrics

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func checkSingleLink(f *os.File) error {
	handle := windows.Handle(f.Fd())
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if info.NumberOfLinks != 1 {
		return errors.New("metrics files must not have hardlink aliases")
	}
	return nil
}

func lockWriterFile(f *os.File) error {
	if err := checkSingleLink(f); err != nil {
		return err
	}
	handle := windows.Handle(f.Fd())

	err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return errWriterBusy
	}
	return err
}
