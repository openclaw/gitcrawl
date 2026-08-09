//go:build !windows

package main

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSignalContextCancelsOnInterrupt(t *testing.T) {
	testSignalContextCancellation(t, os.Interrupt)
}

func TestSignalContextCancelsOnSIGTERM(t *testing.T) {
	testSignalContextCancellation(t, syscall.SIGTERM)
}

func testSignalContextCancellation(t *testing.T, processSignal os.Signal) {
	t.Helper()
	ctx, stop := signalContext(context.Background())
	defer stop()
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("find process: %v", err)
	}
	if err := process.Signal(processSignal); err != nil {
		t.Fatalf("send %v: %v", processSignal, err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("signal context did not cancel")
	}
}
