//go:build !darwin && !linux

package store

import "io/fs"

func nativeReportPhysicalIdentity(fs.FileInfo) (NativeReportFile, bool) {
	return NativeReportFile{}, false
}
