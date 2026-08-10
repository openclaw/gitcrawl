//go:build !windows

package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunGitKillsChildProcessGroupOnContextCancel(t *testing.T) {
	dir := t.TempDir()
	gitPath := filepath.Join(dir, "git")
	readyPath := filepath.Join(dir, "child-ready")
	script := "#!/bin/sh\n(sleep 30) &\necho ready > \"$GITCRAWL_TEST_CHILD_READY\"\nwait\n"
	if err := os.WriteFile(gitPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GITCRAWL_TEST_CHILD_READY", readyPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceledAt := make(chan time.Time, 1)
	go func() {
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(readyPath); err == nil {
				canceledAt <- time.Now()
				cancel()
				return
			}
			select {
			case <-ticker.C:
			case <-deadline.C:
				canceledAt <- time.Now()
				cancel()
				return
			}
		}
	}()
	err := runGit(ctx, "", "fetch")
	elapsed := time.Since(<-canceledAt)
	if _, statErr := os.Stat(readyPath); statErr != nil {
		t.Fatalf("fake git child never became ready: %v", statErr)
	}
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("runGit took %s after context cancellation", elapsed)
	}
}
