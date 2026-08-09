//go:build !windows

package main

import (
	"os"
	"syscall"
)

func commandSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP}
}
