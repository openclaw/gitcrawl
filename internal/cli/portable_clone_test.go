//go:build darwin || linux || windows

package cli

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPortableExclusiveRenamePreservesExistingDestination(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "directory"}[directory], func(t *testing.T) {
			root := t.TempDir()
			from, to := filepath.Join(root, "source"), filepath.Join(root, "destination")
			if directory {
				if err := os.Mkdir(from, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(to, 0o755); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(from, []byte("new"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(to, []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(to)
			if err != nil {
				t.Fatal(err)
			}
			if err := renamePortableExclusive(from, to); err == nil {
				t.Fatal("exclusive rename replaced an existing destination")
			}
			after, err := os.Stat(to)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("destination changed: %v", err)
			}
			if err := os.Remove(to); err != nil {
				t.Fatal(err)
			}
			if err := renamePortableExclusive(from, to); errors.Is(err, errPortableExclusiveUnavailable) {
				if _, err := os.Stat(from); err != nil {
					t.Fatalf("unsupported rename changed its source: %v", err)
				}
				if _, err := os.Lstat(to); !os.IsNotExist(err) {
					t.Fatalf("unsupported rename changed its destination: %v", err)
				}
			} else if err != nil {
				t.Fatalf("rename to absent destination: %v", err)
			}
		})
	}
}
