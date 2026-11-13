//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	crawlstore "github.com/openclaw/crawlkit/store"
)

func TestReadSeedOutboxStateDoesNotOpenWriter(t *testing.T) {
	ctx, native := nativeFixture(t)
	c, _ := projected(t, ctx, native)
	o, path := newSeedOutbox(t, ctx, c)
	expected := outboxState(t, ctx, o)
	if _, err := o.base.DB().ExecContext(ctx, "pragma journal_mode=DELETE"); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err := ReadSeedOutboxState(ctx, path)
	if err != nil || state != expected {
		t.Fatalf("read-only state: %+v %v", state, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 || !bytes.Equal(before, after) {
		t.Fatal("status changed bytes or permissions", err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("status opened a writer", suffix, err)
		}
	}
}

func TestSeedOutboxRefusalBeforeFileEffects(t *testing.T) {
	ctx, native := nativeFixture(t)
	c, _ := projected(t, ctx, native)
	r := seedRegistration(c)
	for _, origin := range []string{"http://example.test", "http://localhost:8123", "https://example.test/path", "https://example.test?", "https://example.test#"} {
		path := filepath.Join(t.TempDir(), "absent", "outbox.sqlite")
		bad := r
		bad.Origin = origin
		if _, err := CreateSeedOutbox(ctx, path, bad, c, StageLimits{MaxEncodedBytes: 32 << 20}); !errors.Is(err, ErrSeedOutbox) {
			t.Fatal("invalid destination accepted", err)
		}
		if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid registration created parent", err)
		}
	}
	for _, origin := range []string{"https://example.test", "http://[::1]:8123"} {
		valid := r
		valid.Origin = origin
		o, err := CreateSeedOutbox(ctx, filepath.Join(t.TempDir(), "outbox.sqlite"), valid, c, StageLimits{MaxEncodedBytes: 32 << 20})
		if err != nil {
			t.Fatal(err)
		}
		o.Close()
	}
	for _, canceled := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "absent", "outbox.sqlite")
		callCtx, cancel := context.WithCancel(ctx)
		if canceled {
			cancel()
		}
		_, err := CreateSeedOutbox(callCtx, path, r, c, StageLimits{MaxEncodedBytes: 1})
		cancel()
		if canceled && !errors.Is(err, context.Canceled) || !canceled && !errors.Is(err, ErrPrivateSeedLimit) {
			t.Fatal("wrong pre-effect refusal", err)
		}
		if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("refusal created parent", err)
		}
	}
	for _, path := range []string{":memory:", "relative.sqlite", "file:/tmp/fixture.sqlite", filepath.Join(t.TempDir(), "x") + "/../outbox.sqlite"} {
		if _, err := CreateSeedOutbox(ctx, path, r, c, StageLimits{MaxEncodedBytes: 32 << 20}); !errors.Is(err, ErrSeedOutbox) {
			t.Fatal("nonliteral path admitted", err)
		}
	}
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o750); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "private", "outbox.sqlite")
	o, err := CreateSeedOutbox(ctx, path, r, c, StageLimits{MaxEncodedBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	o.Close()
	for name, mode := range map[string]os.FileMode{parent: 0o750, filepath.Dir(path): 0o700, path: 0o600} {
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatal("private creation changed existing directory or file mode", err)
		}
	}
}

func TestSeedOutboxExistingNativeUnknownAndVersionRefusedUntouched(t *testing.T) {
	ctx, native := nativeFixture(t)
	c, _ := projected(t, ctx, native)
	if _, err := native.DB().ExecContext(ctx, `pragma journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	if err := native.Close(); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(t.TempDir(), "unknown.sqlite")
	db, err := crawlstore.Open(ctx, crawlstore.Options{Path: unknown})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `pragma journal_mode=DELETE; create table sentinel(value text); insert into sentinel values('retained')`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	o, version := newSeedOutbox(t, ctx, c)
	if _, err := o.base.DB().ExecContext(ctx, `pragma user_version=99; pragma journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	o.Close()
	sentinel := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(sentinel, []byte("retained"), 0o640); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{native.Path(), unknown, version, sentinel} {
		if err := os.Chmod(path, 0o640); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := CreateSeedOutbox(ctx, path, seedRegistration(c), c, StageLimits{MaxEncodedBytes: 32 << 20}); !errors.Is(err, os.ErrExist) {
			t.Fatal("existing target not refused", err)
		}
		if got, err := OpenSeedOutbox(ctx, path); err == nil {
			got.Close()
			t.Fatal("unknown/native/version target opened writable")
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) || info.Mode().Perm() != 0o640 {
			t.Fatal("refusal changed bytes or original mode")
		}
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Stat(path + suffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refusal created a sidecar", err)
			}
		}
	}
	link := filepath.Join(t.TempDir(), "link.sqlite")
	if err := os.Symlink(native.Path(), link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSeedOutbox(ctx, link); !errors.Is(err, ErrSeedOutbox) {
		t.Fatal("symlink opened", err)
	}
}

// Cancel at the actual exclusive-file boundary, without a timer or a
// production injection point. The embedded context owns Done/Err semantics.
type cancelOnClaim struct {
	context.Context
	path   string
	cancel context.CancelFunc
}

func (c cancelOnClaim) Err() error {
	if _, err := os.Lstat(c.path); err == nil {
		c.cancel()
	}
	return c.Context.Err()
}

func TestSeedOutboxClaimedFailureRetainsDiagnosticFile(t *testing.T) {
	ctx, native := nativeFixture(t)
	c, _ := projected(t, ctx, native)
	path := filepath.Join(t.TempDir(), "private", "outbox.sqlite")
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err := CreateSeedOutbox(cancelOnClaim{Context: callCtx, path: path, cancel: cancel}, path, seedRegistration(c), c, StageLimits{MaxEncodedBytes: 32 << 20})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("claim-boundary cancellation not returned", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 0 || info.Mode().Perm() != 0o600 {
		t.Fatal("claimed diagnostic file not retained", err)
	}
	if _, err := CreateSeedOutbox(ctx, path, seedRegistration(c), c, StageLimits{MaxEncodedBytes: 32 << 20}); !errors.Is(err, os.ErrExist) {
		t.Fatal("partial file silently retried", err)
	}
	if _, err := OpenSeedOutbox(ctx, path); err == nil {
		t.Fatal("partial file recognized as complete staging")
	}
	info, err = os.Stat(path)
	if err != nil || info.Size() != 0 {
		t.Fatal("partial recognition mutated file", err)
	}
}
