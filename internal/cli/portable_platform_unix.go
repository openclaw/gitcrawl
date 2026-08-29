//go:build !windows

package cli

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

func lockPortableFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}

func openPortableLockFile(path string) (*os.File, error) {
	descriptor, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(descriptor), path), nil
}

func portableFreeBytes(path string) (uint64, error) {
	var stats unix.Statfs_t
	err := unix.Statfs(path, &stats)
	return uint64(stats.Bavail) * uint64(stats.Bsize), err
}

func configurePortableProcess(cmd *exec.Cmd) error {
	configureCommandGroup(cmd)
	return nil
}

func attachPortableProcess(cmd *exec.Cmd) error { return nil }

func killPortableProcess(cmd *exec.Cmd) { killCommandGroup(cmd) }

func terminatePortableProcess(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
}
