package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/control"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (fixture portableRefreshFixture) command(t *testing.T, args ...string) []byte {
	t.Helper()
	app := New()
	var stdout, stderr bytes.Buffer
	app.Stdout, app.Stderr = &stdout, &stderr
	if err := app.Run(context.Background(), append([]string{"--config", fixture.configPath}, args...)); err != nil {
		t.Fatalf("%v: %v; %s", args, err, stderr.String())
	}
	return stdout.Bytes()
}

func portableTestDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	digest, err := fileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func TestPortableLocalWorkSurvivesRefreshAndReads(t *testing.T) {
	for _, mode := range []string{"control", "refresh", "legacy-refresh"} {
		t.Run(mode, func(t *testing.T) {
			refresh := mode != "control"
			fixture := newPortableRefreshFixture(t, true)
			fixture.command(t, "threads", "openclaw/openclaw", "--json")
			fixture.command(t, "close-thread", "openclaw/openclaw", "--number", "1", "--reason", "keep local decision", "--json")
			// A second kind of local write must survive as well, not just the
			// close fields. Use the existing writable store contract.
			seedPortableThread(t, fixture.mirror, 3, "local thread")
			before := portableTestDigest(t, fixture.mirror)
			baseline := readPortableStoreRefreshState(portableStoreRefreshStatePath(fixture.mirror))
			if mode == "legacy-refresh" {
				baseline.MirrorWritable = false
				info, err := os.Stat(fixture.mirror)
				if err != nil {
					t.Fatal(err)
				}
				baseline.MirrorHealthSize = info.Size()
				baseline.MirrorHealthModTime = info.ModTime().UTC().Format(time.RFC3339Nano)
				if err := writePortableStoreRefreshState(portableStoreRefreshStatePath(fixture.mirror), baseline); err != nil {
					t.Fatal(err)
				}
			}
			readClosure := func() (string, string) {
				t.Helper()
				st, err := store.OpenReadOnly(context.Background(), fixture.mirror)
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				var closedAt, reason sql.NullString
				if err := st.DB().QueryRow(`select closed_at_local, close_reason_local from threads where number=1`).Scan(&closedAt, &reason); err != nil {
					t.Fatal(err)
				}
				if !closedAt.Valid || !reason.Valid || closedAt.String == "" || reason.String != "keep local decision" {
					t.Fatalf("lost closure: %v %v", closedAt, reason)
				}
				return closedAt.String, reason.String
			}
			closedAt, reason := readClosure()
			if refresh {
				fixture.advance(t, true)
				result, err := fixture.refresh(t)
				if err != nil || result.Result != "updated" || result.MirrorResult != "preserved-local" {
					t.Fatalf("refresh: %+v %v", result, err)
				}
				if fmt.Sprintf("%x", before) == result.SHA256 {
					t.Fatal("fixture did not publish a conflicting generation")
				}
			}
			for range 3 {
				if portableTestDigest(t, fixture.mirror) != before {
					t.Fatal("runtime bytes changed")
				}
				var output struct {
					Threads []store.Thread `json:"threads"`
				}
				if err := json.Unmarshal(fixture.command(t, "threads", "openclaw/openclaw", "--json"), &output); err != nil {
					t.Fatal(err)
				}
				threads := output.Threads
				if len(threads) != 1 || threads[0].Number != 3 || threads[0].Title != "local thread" {
					t.Fatalf("local read changed: %+v", threads)
				}
				if at, why := readClosure(); at != closedAt || why != reason {
					t.Fatal("closure fields changed")
				}
				if portableTestDigest(t, fixture.mirror) != before {
					t.Fatal("ordinary read replaced local runtime")
				}
			}
			state := readPortableStoreRefreshState(portableStoreRefreshStatePath(fixture.mirror))
			if state.MirrorHealthSourceSHA256 != baseline.MirrorHealthSourceSHA256 {
				t.Fatal("local mirror was mislabeled as the latest source")
			}
			var status control.Status
			if err := json.Unmarshal(fixture.command(t, "status", "--json"), &status); err != nil {
				t.Fatal(err)
			}
			if refresh && status.State != "stale" {
				t.Fatalf("preserved runtime reported fresh: %+v", status)
			}
		})
	}
}

func TestPortableRuntimeCorruptionOwnership(t *testing.T) {
	for _, writable := range []bool{false, true} {
		t.Run(fmt.Sprintf("writable=%t", writable), func(t *testing.T) {
			fixture := newPortableRefreshFixture(t, false)
			fixture.command(t, "threads", "openclaw/openclaw", "--json")
			if writable {
				fixture.command(t, "close-thread", "openclaw/openclaw", "--number", "1", "--json")
			}
			if err := os.WriteFile(fixture.mirror, []byte("broken sqlite"), 0o600); err != nil {
				t.Fatal(err)
			}
			before := portableTestDigest(t, fixture.mirror)
			app := New()
			app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
			err := app.Run(context.Background(), []string{"--config", fixture.configPath, "threads", "openclaw/openclaw", "--json"})
			if writable {
				if err == nil || portableTestDigest(t, fixture.mirror) != before {
					t.Fatalf("corrupt local work was overwritten: %v", err)
				}
			} else if err != nil || portableTestDigest(t, fixture.mirror) != portableTestDigest(t, filepath.Join(fixture.checkout, fixture.relative)) {
				t.Fatalf("disposable replica did not recover: %v", err)
			}
		})
	}
}

func TestPortableStatusGzipReadOnly(t *testing.T) {
	fixture := newPortableRefreshFixture(t, true)
	logical := filepath.Join(fixture.checkout, fixture.relative)
	for _, materialized := range []bool{false, true} {
		if materialized {
			fixture.command(t, "threads", "openclaw/openclaw", "--json")
		}
		before := portableTestSnapshot(t, filepath.Dir(fixture.configPath))
		var status control.Status
		if err := json.Unmarshal(fixture.command(t, "status", "--json"), &status); err != nil {
			t.Fatal(err)
		}
		if countValue(status.Counts, "repositories") != 1 || countValue(status.Counts, "threads") != 1 || status.DatabaseBytes <= 0 {
			t.Fatalf("empty populated subscriber status: %+v", status)
		}
		path := logical + ".gz"
		if materialized {
			path = fixture.mirror
		}
		if !sameExistingPath(status.DatabasePath, path) || status.DatabaseBytes != fileSize(path) {
			t.Fatalf("wrong inventory: %+v", status)
		}
		if _, err := os.Stat(logical); !os.IsNotExist(err) {
			t.Fatalf("status materialized logical database: %v", err)
		}
		after := portableTestSnapshot(t, filepath.Dir(fixture.configPath))
		if !bytes.Equal(before, after) {
			t.Fatal("status changed checkout, runtime, metadata, or config")
		}
	}
	// Status must report a broken runtime, not silently repair it from source.
	if err := os.WriteFile(fixture.mirror, []byte("broken sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := portableTestSnapshot(t, filepath.Dir(fixture.configPath))
	app := New()
	app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
	if err := app.Run(context.Background(), []string{"--config", fixture.configPath, "status", "--json"}); err == nil {
		t.Fatal("corrupt runtime reported successful status")
	}
	if !bytes.Equal(before, portableTestSnapshot(t, filepath.Dir(fixture.configPath))) {
		t.Fatal("status repaired the corrupt runtime")
	}
}

func portableTestSnapshot(t *testing.T, root string) []byte {
	t.Helper()
	files := map[string][32]byte{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			files[path] = portableTestDigest(t, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(files)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
