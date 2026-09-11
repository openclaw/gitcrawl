package cli

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func renamePortableExclusive(from, to string) error {
	from, err := portableWindowsExtendedPath(from)
	if err != nil {
		return err
	}
	to, err = portableWindowsExtendedPath(to)
	if err != nil {
		return err
	}
	source, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	destination, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	return windows.MoveFile(source, destination)
}

func portableWindowsExtendedPath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	// Match Go's threshold without changing short-path Win32 normalization.
	if len(path) < 248 {
		return path, nil
	}
	if strings.HasPrefix(path, `\\?\`) || strings.HasPrefix(path, `\\.\`) {
		return path, nil
	}
	if strings.HasPrefix(path, `\\`) {
		return `\\?\UNC\` + path[2:], nil
	}
	return `\\?\` + path, nil
}
