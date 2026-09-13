package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/control"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestPortableStatusSeparatesExportFromSync(t *testing.T) {
	for _, compression := range []string{"", "gzip"} {
		t.Run(compression, func(t *testing.T) {
			t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
			t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
			t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
			ctx := context.Background()
			dir := t.TempDir()
			sourceConfig := filepath.Join(dir, "source.toml")
			source := filepath.Join(dir, "source.db")
			app := New()
			app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
			if err := app.Run(ctx, []string{"--config", sourceConfig, "init", "--db", source}); err != nil {
				t.Fatal(err)
			}
			seedPortableThread(t, source, 1, "recent thread")
			st, err := store.Open(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.DB().ExecContext(ctx, `insert into repo_sync_state(repo_id, last_open_close_reconciled_at, updated_at)
				values(1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			fixture := portableRefreshFixture{
				remote: filepath.Join(dir, "publisher"), checkout: filepath.Join(dir, "subscriber"),
				configPath: filepath.Join(dir, "reader.toml"), relative: "data/archive.db",
			}
			if err := app.Run(ctx, []string{"--config", sourceConfig, "portable", "export", "--profile", "current-state-v1",
				"--output-dir", filepath.Join(fixture.remote, "data"), "--database-name", "archive.db",
				"--public-path", fixture.relative, "--compression", compression, "--json"}); err != nil {
				t.Fatal(err)
			}
			portableTestGit(t, fixture.remote, "init", "-b", "main")
			portableTestCommit(t, fixture.remote)
			fixture.init(t)
			manifest, exists, err := readPortableDBManifest(portableDBManifestPath(filepath.Join(fixture.checkout, fixture.relative)))
			if err != nil || !exists {
				t.Fatalf("manifest: exists=%t err=%v", exists, err)
			}
			exportedAt, err := time.Parse(time.RFC3339Nano, manifest.ExportedAt)
			if err != nil {
				t.Fatal(err)
			}
			for _, materialized := range []bool{false, true} {
				if materialized {
					fixture.command(t, "threads", "openclaw/openclaw", "--json")
				}
				before := portableTestSnapshot(t, dir)
				var status control.Status
				if err := json.Unmarshal(fixture.command(t, "status", "--json"), &status); err != nil {
					t.Fatal(err)
				}
				if status.LastSyncAt != "" || status.LastExportAt != exportedAt.UTC().Format(time.RFC3339) {
					t.Fatalf("materialized=%t: sync=%q export=%q, want no sync and export=%s", materialized, status.LastSyncAt, status.LastExportAt, exportedAt)
				}
				if !bytes.Equal(before, portableTestSnapshot(t, dir)) {
					t.Fatal("status changed archive, config, or runtime files")
				}
			}
			var doctor struct {
				LastSyncAt   string `json:"last_sync_at"`
				LastExportAt string `json:"last_export_at"`
			}
			if err := json.Unmarshal(fixture.command(t, "doctor", "--json"), &doctor); err != nil {
				t.Fatal(err)
			}
			if doctor.LastSyncAt != "" || doctor.LastExportAt != exportedAt.UTC().Format(time.RFC3339Nano) {
				t.Fatalf("doctor timestamps: %+v", doctor)
			}
			fixture.command(t, "close-thread", "openclaw/openclaw", "--number", "1", "--json")
			fixture.advance(t, compression == "gzip")
			nextPath := portableDBManifestPath(filepath.Join(fixture.remote, fixture.relative))
			next, _, err := readPortableDBManifest(nextPath)
			if err != nil {
				t.Fatal(err)
			}
			next.ExportedAt = exportedAt.Add(time.Hour).Format(time.RFC3339Nano)
			encoded, err := json.Marshal(next)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(nextPath, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			portableTestCommit(t, fixture.remote)
			result, err := fixture.refresh(t)
			if err != nil || result.MirrorResult != "preserved-local" {
				t.Fatalf("refresh local runtime: %+v, %v", result, err)
			}
			var stale control.Status
			if err := json.Unmarshal(fixture.command(t, "status", "--json"), &stale); err != nil {
				t.Fatal(err)
			}
			if stale.State != "stale" || stale.LastExportAt != "" {
				t.Fatalf("stale runtime borrowed checkout freshness: %+v", stale)
			}
		})
	}
}

func TestApplyPortableExportTime(t *testing.T) {
	const exportedAt = "2026-06-01T02:03:04.123456789Z"
	for _, tc := range []struct {
		name       string
		manifest   string
		runtime    bool
		runtimeSHA string
		wantTime   bool
		wantError  bool
	}{
		{name: "missing manifest"},
		{name: "legacy manifest", manifest: `{"sha256":"abc"}`},
		{name: "source", manifest: `{"sha256":"abc","exportedAt":"` + exportedAt + `"}`, wantTime: true},
		{name: "matching runtime", manifest: `{"sha256":"ABC","exportedAt":"` + exportedAt + `"}`, runtime: true, runtimeSHA: "abc", wantTime: true},
		{name: "stale runtime", manifest: `{"sha256":"abc","exportedAt":"` + exportedAt + `"}`, runtime: true, runtimeSHA: "def"},
		{name: "unknown runtime source", manifest: `{"exportedAt":"` + exportedAt + `"}`, runtime: true},
		{name: "zero timestamp", manifest: `{"exportedAt":"0001-01-01T00:00:00Z"}`},
		{name: "invalid timestamp", manifest: `{"exportedAt":"invalid"}`, wantError: true},
		{name: "invalid manifest", manifest: `{`, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "source.db")
			if tc.manifest != "" {
				if err := os.WriteFile(portableDBManifestPath(source), []byte(tc.manifest), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			runtime := ""
			if tc.runtime {
				runtime = filepath.Join(t.TempDir(), "runtime.db")
				if err := writePortableStoreRefreshState(portableStoreRefreshStatePath(runtime), portableStoreRefreshState{MirrorHealthSourceSHA256: tc.runtimeSHA}); err != nil {
					t.Fatal(err)
				}
			}
			for _, storedTime := range []time.Time{{}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} {
				status := store.Status{LastExportAt: storedTime}
				err := applyPortableExportTime(&status, source, runtime)
				if (err != nil) != tc.wantError {
					t.Fatalf("error = %v, want error=%t", err, tc.wantError)
				}
				if tc.wantTime {
					if status.LastExportAt.Format(time.RFC3339Nano) != exportedAt {
						t.Fatalf("export time = %s, want %s", status.LastExportAt, exportedAt)
					}
				} else if !status.LastExportAt.Equal(storedTime) {
					t.Fatalf("stored export time changed: got %s, want %s", status.LastExportAt, storedTime)
				}
			}
		})
	}
}
