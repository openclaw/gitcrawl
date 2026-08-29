//go:build !windows

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestPortableConfigProtocolFailsClosed(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "config-output")
	wrapper := filepath.Join(root, "git")
	writePortableSafetyFile(t, wrapper, []byte("#!/bin/sh\ncat \"$GITCRAWL_TEST_CONFIG_OUTPUT\"\n"))
	if err := os.Chmod(wrapper, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_TEST_CONFIG_OUTPUT", output)
	ctx, err := portableGitContext(context.Background(), wrapper)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, output, want string }{
		{"unpaired", "local\x00", "unsupported Git configuration output"},
		{"unknown-scope", "future\x00core.bare\nfalse\x00", "unsupported Git configuration scope"},
		{"bare", "local\x00core.bare\ntrue\x00", "portable checkout cannot be bare"},
		{"filter", "worktree\x00filter.synthetic.clean\nprivate-value\x00", "unsupported portable Git configuration (worktree scope: filters)"},
		{"url-redirection", "global\x00url.https://example.invalid/private-subsection.insteadof\nprivate-value\x00", "unsupported portable Git configuration (global scope: redirection, submodules or extensions)"},
		{"remote-command", "local\x00remote.origin.uploadpack\nprivate-value\x00", "unsupported portable Git configuration (local scope: origin options)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			writePortableSafetyFile(t, output, []byte(test.output))
			digest, err := portableConfigIdentity(ctx, root)
			if err == nil || err.Error() != test.want || digest != ([32]byte{}) {
				t.Fatalf("unsafe config received identity or leaked data: digest=%x, err=%v", digest, err)
			}
		})
	}
	// The trusted command scope may neutralize an unsafe single-valued setting;
	// its exact bytes still participate in the identity used at recheck.
	valid := "local\x00core.bare\nfalse\x00command\x00core.hookspath\n/dev/null\x00"
	writePortableSafetyFile(t, output, []byte(valid))
	first, err := portableConfigIdentity(ctx, root)
	if err != nil || first != sha256.Sum256([]byte(valid)) {
		t.Fatalf("valid config identity: %x, %v", first, err)
	}
	changed := valid + "local\x00user.name\nchanged\x00"
	writePortableSafetyFile(t, output, []byte(changed))
	second, err := portableConfigIdentity(ctx, root)
	if err != nil || second == first || second != sha256.Sum256([]byte(changed)) {
		t.Fatalf("changed safe config did not change identity: %x, %v", second, err)
	}
}

func TestPortableMetadataRejectsLinksAndOrphanPairs(t *testing.T) {
	for _, name := range []string{"paired", "orphan-pack", "orphan-index", "linked-metadata"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			pack := filepath.Join(root, ".git", "objects", "pack", "pack-fixture")
			writePortableSafetyFile(t, pack+".pack", []byte("historical pack"))
			writePortableSafetyFile(t, pack+".idx", []byte("historical index"))
			want := ""
			switch name {
			case "orphan-pack":
				if err := os.Remove(pack + ".idx"); err != nil {
					t.Fatal(err)
				}
				want = "portable store contains an unresolved orphan pack"
			case "orphan-index":
				if err := os.Remove(pack + ".pack"); err != nil {
					t.Fatal(err)
				}
				want = "portable store contains an unresolved orphan pack"
			case "linked-metadata":
				target := filepath.Join(t.TempDir(), "unrelated")
				writePortableSafetyFile(t, target, []byte("do not follow"))
				if err := os.Symlink(target, filepath.Join(root, ".git", "config")); err != nil {
					t.Fatal(err)
				}
				want = "portable Git metadata contains a link or special file"
			}
			before := portableTestSnapshot(t, root)
			err := checkPortableMetadata(context.Background(), root)
			if want == "" && err != nil || want != "" && (err == nil || err.Error() != want) {
				t.Fatalf("metadata admission: want %q, got %v", want, err)
			}
			if !bytes.Equal(before, portableTestSnapshot(t, root)) {
				t.Fatal("metadata inspection removed historical objects")
			}
		})
	}
}
