package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPortableExclusiveRenameNormalizesWindowsPaths(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{`C:\archive\file`, `C:\archive\file`},
		{`C:\archive\trailing.`, `C:\archive\trailing.`},
		{`\\server\share\file`, `\\server\share\file`},
		{`\\?\C:\archive\file`, `\\?\C:\archive\file`},
		{`\\?\UNC\server\share\file`, `\\?\UNC\server\share\file`},
		{`C:\archive\` + strings.Repeat("a", 250), `\\?\C:\archive\` + strings.Repeat("a", 250)},
		{`\\server\share\` + strings.Repeat("a", 250), `\\?\UNC\server\share\` + strings.Repeat("a", 250)},
	} {
		got, err := portableWindowsExtendedPath(tc.input)
		if err != nil || got != tc.want {
			t.Errorf("extended path %q = %q, %v; want %q", tc.input, got, err, tc.want)
		}
	}
}

func TestPortableExclusiveRenameLongPaths(t *testing.T) {
	root := t.TempDir()
	for len(root) < 320 {
		root = filepath.Join(root, strings.Repeat("segment", 7))
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	from, to := filepath.Join(root, "source"), filepath.Join(root, "destination")
	if err := os.Mkdir(from, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(to, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := renamePortableExclusive(from, to); err == nil {
		t.Fatal("long-path rename replaced an existing directory")
	}
	if err := os.Remove(to); err != nil {
		t.Fatal(err)
	}
	if err := renamePortableExclusive(from, to); err != nil {
		t.Fatalf("long-path publication failed: %v", err)
	}
	if _, err := os.Stat(to); err != nil {
		t.Fatal(err)
	}
}
