package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/crawlkit/control"
	"github.com/openclaw/gitcrawl/internal/store"
)

func newPortableTransitionFixture(t *testing.T, compressed bool) (portableRefreshFixture, string) {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	dir := t.TempDir()
	publisher := filepath.Join(dir, "publisher")
	if err := os.Mkdir(publisher, 0o755); err != nil {
		t.Fatal(err)
	}
	portableTestGit(t, publisher, "init", "-b", "main")
	seedPortableThread(t, filepath.Join(publisher, "archive.db"), 1, "original raw generation")
	if compressed {
		portableTestManifest(t, filepath.Join(publisher, "archive.db"), "archive.db", true)
	}
	portableTestCommit(t, publisher)
	remotePath := filepath.ToSlash(publisher)
	if filepath.VolumeName(publisher) != "" {
		remotePath = "/" + remotePath
	}
	fixture := portableRefreshFixture{
		remote:   (&url.URL{Scheme: "file", Path: remotePath}).String(),
		checkout: filepath.Join(dir, "subscriber"), configPath: filepath.Join(dir, "config.toml"), relative: "archive.db",
		mirror: filepath.Join(dir, "runtime", "subscriber", "archive.db"),
	}
	fixture.init(t)
	return fixture, publisher
}

func portableTransitionThreads(t *testing.T, fixture portableRefreshFixture, want string) {
	t.Helper()
	var output struct {
		Threads []store.Thread `json:"threads"`
	}
	if err := json.Unmarshal(fixture.command(t, "threads", "openclaw/openclaw", "--json"), &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Threads) != 1 || output.Threads[0].Title != want {
		t.Fatalf("runtime threads = %+v, want %q", output.Threads, want)
	}
}

func publishPortableTransition(t *testing.T, publisher, format string) [32]byte {
	t.Helper()
	path := filepath.Join(publisher, "archive.db")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		temp, err := stagePortableSQLiteSourceTemp(path, path, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(temp, path); err != nil {
			t.Fatal(err)
		}
	}
	seedPortableThread(t, path, 1, "replacement generation")
	digest := portableTestDigest(t, path)
	if format != "raw" {
		portableTestManifest(t, path, "archive.db", format == "gzip")
	}
	portableTestCommit(t, publisher)
	return digest
}

func assertPortableReplica(t *testing.T, fixture portableRefreshFixture, digest [32]byte) {
	t.Helper()
	if portableTestDigest(t, fixture.mirror) != digest {
		t.Fatal("runtime bytes differ from the published generation")
	}
	state := readPortableStoreRefreshState(portableStoreRefreshStatePath(fixture.mirror))
	if state.MirrorWritable || state.MirrorHealthSourceSHA256 != fmt.Sprintf("%x", digest) {
		t.Fatalf("replica lost its source identity: %+v", state)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(fixture.mirror + suffix); !os.IsNotExist(err) {
			t.Fatalf("replica read created %s: %v", suffix, err)
		}
	}
}

func TestPortableReplicaTransitions(t *testing.T) {
	cases := []struct {
		name, format, update string
		compressed           bool
	}{
		{"raw-to-gzip-init", "gzip", "init", false},
		{"raw-to-raw-init", "raw", "init", false},
		{"raw-to-manifest-init", "manifest", "init", false},
		{"gzip-to-gzip-init", "gzip", "init", true},
		{"gzip-to-manifest-init", "manifest", "init", true},
		{"raw-to-gzip-refresh", "gzip", "refresh", false},
		{"raw-to-manifest-refresh", "manifest", "refresh", false},
		{"raw-to-gzip-read", "gzip", "read", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, prepared := range []bool{true, false} {
				t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
					fixture, publisher := newPortableTransitionFixture(t, tc.compressed)
					if prepared {
						for range 2 {
							portableTransitionThreads(t, fixture, "original raw generation")
						}
						if !tc.compressed {
							assertPortableReplica(t, fixture, portableTestDigest(t, filepath.Join(publisher, fixture.relative)))
						}
					} else if _, err := os.Stat(fixture.mirror); !os.IsNotExist(err) {
						t.Fatalf("init unexpectedly prepared runtime: %v", err)
					}
					before := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD")
					digest := publishPortableTransition(t, publisher, tc.format)
					switch tc.update {
					case "init":
						var result struct {
							Action string `json:"portable_store"`
						}
						data := fixture.command(t, "init", "--portable-store", fixture.remote, "--store-dir", fixture.checkout, "--portable-db", fixture.relative, "--json")
						if err := json.Unmarshal(data, &result); err != nil || result.Action != "pulled" {
							t.Fatalf("re-init: %s %v", data, err)
						}
					case "refresh":
						result, err := fixture.refresh(t)
						if err != nil || result.Result != "updated" || result.MirrorResult != "promoted" {
							t.Fatalf("refresh: %+v %v", result, err)
						}
					case "read":
						t.Setenv("GITCRAWL_PORTABLE_REFRESH_TTL", "0")
					}
					for range 2 {
						portableTransitionThreads(t, fixture, "replacement generation")
						assertPortableReplica(t, fixture, digest)
					}
					if got := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD"); got == before || got != portableTestGit(t, publisher, "rev-parse", "HEAD") {
						t.Fatal("subscriber did not advance to publisher HEAD")
					}
					if tc.format == "gzip" {
						if _, err := os.Stat(filepath.Join(fixture.checkout, fixture.relative)); !os.IsNotExist(err) {
							t.Fatalf("gzip transition retained raw artifact: %v", err)
						}
					}
				})
			}
		})
	}
}

func TestPortableRawLocalWorkTransitions(t *testing.T) {
	for _, format := range []string{"raw", "gzip"} {
		t.Run(format, func(t *testing.T) {
			for _, ownership := range []string{"close-thread", "external-write"} {
				t.Run(ownership, func(t *testing.T) {
					fixture, publisher := newPortableTransitionFixture(t, false)
					portableTransitionThreads(t, fixture, "original raw generation")
					baseline := readPortableStoreRefreshState(portableStoreRefreshStatePath(fixture.mirror))
					if ownership == "close-thread" {
						fixture.command(t, "close-thread", "openclaw/openclaw", "--number", "1", "--reason", "preserve raw closure", "--json")
						seedPortableThread(t, fixture.mirror, 3, "local extra thread")
					} else {
						info, err := os.Stat(fixture.mirror)
						if err != nil {
							t.Fatal(err)
						}
						seedPortableThread(t, fixture.mirror, 1, "external raw generation")
						if fileSize(fixture.mirror) != info.Size() {
							t.Fatal("external rewrite must preserve file size")
						}
						if err := os.Chtimes(fixture.mirror, info.ModTime(), info.ModTime()); err != nil {
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
						var at, reason sql.NullString
						if err := st.DB().QueryRow(`select closed_at_local, close_reason_local from threads where number=1`).Scan(&at, &reason); err != nil {
							t.Fatal(err)
						}
						return at.String, reason.String
					}
					var closedAt, reason string
					if ownership == "close-thread" {
						closedAt, reason = readClosure()
						if closedAt == "" || reason != "preserve raw closure" {
							t.Fatal("close-thread did not create a closure")
						}
					}
					local := portableTestDigest(t, fixture.mirror)
					publishPortableTransition(t, publisher, format)
					fixture.init(t)
					for range 2 {
						want := "external raw generation"
						if ownership == "close-thread" {
							want = "local extra thread"
						}
						portableTransitionThreads(t, fixture, want)
						if portableTestDigest(t, fixture.mirror) != local {
							t.Fatal("source transition or ordinary read changed local runtime bytes")
						}
						if ownership == "close-thread" {
							if at, why := readClosure(); at != closedAt || why != reason {
								t.Fatal("source transition changed raw local closure")
							}
						}
					}
					state := readPortableStoreRefreshState(portableStoreRefreshStatePath(fixture.mirror))
					if !state.MirrorWritable || state.MirrorHealthSourceSHA256 != baseline.MirrorHealthSourceSHA256 {
						t.Fatalf("local work lost ownership or original source identity: %+v", state)
					}
				})
			}
		})
	}
}

func TestPortableRawReplicaStatusDoesNotClaimOwnership(t *testing.T) {
	fixture, publisher := newPortableTransitionFixture(t, false)
	portableTransitionThreads(t, fixture, "original raw generation")
	digest := portableTestDigest(t, filepath.Join(publisher, fixture.relative))
	before := portableTestSnapshot(t, filepath.Dir(fixture.configPath))
	var result control.Status
	if err := json.Unmarshal(fixture.command(t, "status", "--json"), &result); err != nil {
		t.Fatal(err)
	}
	if result.State == "stale" || countValue(result.Counts, "threads") != 1 || !sameExistingPath(result.DatabasePath, fixture.mirror) || result.DatabaseBytes != fileSize(fixture.mirror) {
		t.Fatalf("incorrect raw runtime status: %+v", result)
	}
	if string(before) != string(portableTestSnapshot(t, filepath.Dir(fixture.configPath))) {
		t.Fatal("status changed raw subscriber state")
	}
	assertPortableReplica(t, fixture, digest)
}

func TestPortableRawReplicaHealthDoesNotInventIdentity(t *testing.T) {
	fixture, publisher := newPortableTransitionFixture(t, false)
	portableTransitionThreads(t, fixture, "original raw generation")
	statePath := portableStoreRefreshStatePath(fixture.mirror)
	state := readPortableStoreRefreshState(statePath)
	state.MirrorHealthSourceSHA256 = ""
	if err := writePortableStoreRefreshState(statePath, state); err != nil {
		t.Fatal(err)
	}
	if err := portableMirrorCachedHealth(context.Background(), fixture.mirror, filepath.Join(fixture.checkout, fixture.relative), statePath); err != nil {
		t.Fatal(err)
	}
	if got := readPortableStoreRefreshState(statePath).MirrorHealthSourceSHA256; got != "" {
		t.Fatalf("health-only check invented raw replica identity: %s", got)
	}
	publishPortableTransition(t, publisher, "gzip")
	result, err := fixture.refresh(t)
	if err != nil || result.MirrorResult != "preserved-local" {
		t.Fatalf("unproven legacy runtime was not preserved: %+v %v", result, err)
	}
	for range 2 {
		portableTransitionThreads(t, fixture, "original raw generation")
	}
	if got := readPortableStoreRefreshState(statePath).MirrorHealthSourceSHA256; got != "" {
		t.Fatalf("preservation invented a legacy source identity: %s", got)
	}
}

func TestPortableRawWALSurvivesTransition(t *testing.T) {
	fixture, publisher := newPortableTransitionFixture(t, false)
	portableTransitionThreads(t, fixture, "original raw generation")
	st, err := store.Open(context.Background(), fixture.mirror)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.DB().Exec(`update threads set title='uncheckpointed local work' where number=1`); err != nil {
		t.Fatal(err)
	}
	wal := fixture.mirror + "-wal"
	if fileSize(wal) == 0 {
		t.Fatal("fixture did not create a live WAL")
	}
	beforeDB, beforeWAL := portableTestDigest(t, fixture.mirror), portableTestDigest(t, wal)
	publishPortableTransition(t, publisher, "gzip")
	fixture.init(t)
	result, err := fixture.refresh(t)
	if err != nil || result.MirrorResult != "preserved-local" {
		t.Fatalf("live WAL was not preserved: %+v %v", result, err)
	}
	for range 2 {
		portableTransitionThreads(t, fixture, "uncheckpointed local work")
		if portableTestDigest(t, fixture.mirror) != beforeDB || portableTestDigest(t, wal) != beforeWAL {
			t.Fatal("source transition changed the local database or live WAL")
		}
	}
}
