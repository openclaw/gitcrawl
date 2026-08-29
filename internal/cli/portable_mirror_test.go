package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writePortableSafetyFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writePortableSafetyState(t *testing.T, path string, state portableStoreRefreshState) {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	writePortableSafetyFile(t, portableStoreRefreshStatePath(path), data)
}

func TestPortableMirrorPreservationIdentity(t *testing.T) {
	for _, name := range []string{"missing", "replica", "legacy-manifest", "unknown-source", "local-bytes", "writable", "wal", "shm", "journal"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			source, mirror := filepath.Join(root, "source.db"), filepath.Join(root, "runtime.db")
			writePortableSafetyFile(t, source, []byte("published generation"))
			portableTestManifest(t, source, "source.db", false)
			manifest, _, err := readPortableDBManifest(portableDBManifestPath(source))
			if err != nil {
				t.Fatal(err)
			}
			if name != "missing" {
				writePortableSafetyFile(t, mirror, []byte("published generation"))
			}
			state := portableStoreRefreshState{MirrorHealthSourceSHA256: strings.ToUpper(manifest.SHA256)}
			preserve := false
			switch name {
			case "legacy-manifest":
				state.MirrorHealthSourceSHA256 = ""
			case "unknown-source":
				state.MirrorHealthSourceSHA256 = ""
				if err := os.Remove(portableDBManifestPath(source)); err != nil {
					t.Fatal(err)
				}
				preserve = true
			case "local-bytes":
				writePortableSafetyFile(t, mirror, []byte("local maintainer work"))
				preserve = true
			case "writable":
				state.MirrorWritable, preserve = true, true
			case "wal", "shm", "journal":
				writePortableSafetyFile(t, mirror+"-"+name, []byte("pending local write"))
				preserve = true
			}
			writePortableSafetyState(t, mirror, state)
			before := portableTestSnapshot(t, root)
			inspected, err := inspectPortableMirror(context.Background(), mirror, source)
			if err != nil || inspected.exists != (name != "missing") || inspected.preserve != preserve {
				t.Fatalf("mirror ownership: exists=%t preserve=%t err=%v", inspected.exists, inspected.preserve, err)
			}
			if err := inspected.recheck(context.Background()); err != nil {
				t.Fatalf("unchanged mirror rejected: %v", err)
			}
			if !bytes.Equal(before, portableTestSnapshot(t, root)) {
				t.Fatal("inspection changed runtime, source, sidecars or ownership metadata")
			}
		})
	}
}

func TestPortableMirrorRejectsInvalidMetadata(t *testing.T) {
	for _, name := range []string{"malformed", "oversized", "directory", "runtime-directory", "bad-source-manifest"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path, source := filepath.Join(root, "runtime.db"), filepath.Join(root, "source.db")
			statePath := portableStoreRefreshStatePath(path)
			want := "invalid runtime refresh metadata"
			switch name {
			case "malformed":
				writePortableSafetyFile(t, statePath, []byte("{broken"))
			case "oversized":
				writePortableSafetyFile(t, statePath, bytes.Repeat([]byte(" "), (1<<20)+1))
			case "directory":
				if err := os.Mkdir(statePath, 0o700); err != nil {
					t.Fatal(err)
				}
			case "runtime-directory":
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				want = "runtime mirror is not a regular file"
			case "bad-source-manifest":
				writePortableSafetyFile(t, path, []byte("local bytes"))
				writePortableSafetyFile(t, portableDBManifestPath(source), []byte("{broken"))
				want = "manifest"
			}
			before := portableTestSnapshot(t, root)
			if _, err := inspectPortableMirror(context.Background(), path, source); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected %q refusal, got %v", want, err)
			}
			if !bytes.Equal(before, portableTestSnapshot(t, root)) {
				t.Fatal("invalid runtime state was repaired or deleted during inspection")
			}
		})
	}
}

func TestPortableMirrorRecheckDetectsConcurrentChanges(t *testing.T) {
	for _, name := range []string{"parent", "metadata-created", "metadata-replaced", "metadata-content", "runtime-created", "runtime-removed", "runtime-replaced", "runtime-content", "sidecar-created", "sidecar-removed", "sidecar-grown"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "runtime", "archive.db")
			writePortableSafetyFile(t, filepath.Join(filepath.Dir(path), "sentinel"), []byte("keep"))
			if name != "runtime-created" {
				writePortableSafetyFile(t, path, []byte("old"))
			}
			statePath := portableStoreRefreshStatePath(path)
			if name != "metadata-created" {
				writePortableSafetyState(t, path, portableStoreRefreshState{MirrorWritable: true, LastSuccess: "old"})
			}
			if name == "sidecar-removed" || name == "sidecar-grown" {
				writePortableSafetyFile(t, path+"-wal", []byte("old"))
			}
			mirror, err := inspectPortableMirror(context.Background(), path, filepath.Join(root, "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			if err := mirror.recheck(context.Background()); err != nil {
				t.Fatalf("baseline rejected: %v", err)
			}
			want := "runtime mirror changed"
			switch name {
			case "parent":
				if err := os.Rename(filepath.Dir(path), filepath.Join(root, "previous-runtime")); err != nil {
					t.Fatal(err)
				}
				writePortableSafetyFile(t, path, []byte("new"))
				want = "runtime mirror directory changed"
			case "metadata-created", "metadata-replaced":
				if name == "metadata-replaced" {
					if err := os.Rename(statePath, statePath+".previous"); err != nil {
						t.Fatal(err)
					}
				}
				writePortableSafetyState(t, path, portableStoreRefreshState{MirrorWritable: true})
				want = "runtime refresh metadata"
			case "metadata-content":
				writePortableSafetyState(t, path, portableStoreRefreshState{MirrorWritable: true, LastSuccess: "new"})
				if err := os.Chtimes(statePath, mirror.state.ModTime(), mirror.state.ModTime()); err != nil {
					t.Fatal(err)
				}
				want = "runtime refresh metadata changed"
			case "runtime-created":
				writePortableSafetyFile(t, path, []byte("new"))
				want = "runtime mirror appeared"
			case "runtime-removed":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "runtime-replaced":
				if err := os.Rename(path, path+".previous"); err != nil {
					t.Fatal(err)
				}
				writePortableSafetyFile(t, path, []byte("old"))
			case "runtime-content":
				// Same inode, size and mtime: the digest must catch this write.
				writePortableSafetyFile(t, path, []byte("new"))
				if err := os.Chtimes(path, mirror.info.ModTime(), mirror.info.ModTime()); err != nil {
					t.Fatal(err)
				}
			case "sidecar-created":
				writePortableSafetyFile(t, path+"-journal", []byte("pending transaction"))
				want = "runtime SQLite sidecar appeared"
			case "sidecar-removed":
				if err := os.Remove(path + "-wal"); err != nil {
					t.Fatal(err)
				}
				want = "runtime SQLite sidecar changed"
			case "sidecar-grown":
				writePortableSafetyFile(t, path+"-wal", []byte("new transaction"))
				want = "runtime SQLite sidecar changed"
			}
			before := portableTestSnapshot(t, root)
			if err := mirror.recheck(context.Background()); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected %q refusal, got %v", want, err)
			}
			if !bytes.Equal(before, portableTestSnapshot(t, root)) {
				t.Fatal("recheck modified concurrent writer's state")
			}
		})
	}
}
