//go:build windows

package main

import "os"

func commandSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
