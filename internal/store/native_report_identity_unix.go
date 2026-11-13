//go:build darwin || linux

package store

import (
	"io/fs"
	"runtime"
	"strconv"
	"syscall"
)

func nativeReportPhysicalIdentity(info fs.FileInfo) (NativeReportFile, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return NativeReportFile{}, false
	}
	return NativeReportFile{Platform: runtime.GOOS, Device: strconv.FormatUint(uint64(stat.Dev), 10), Inode: strconv.FormatUint(uint64(stat.Ino), 10)}, true
}
