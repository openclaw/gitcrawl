package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	portableexport "github.com/openclaw/gitcrawl/internal/portable"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestPortableExportCommandRecentSubset(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	config, source := filepath.Join(dir, "config.toml"), filepath.Join(dir, "source.db")
	app := New()
	app.Stdout = io.Discard
	app.Stderr = io.Discard
	if err := app.Run(ctx, []string{"--config", config, "init", "--db", source}); err != nil {
		t.Fatal(err)
	}
	seedPortableThread(t, source, 1, "old open")
	seedPortableThread(t, source, 2, "old closed")
	writer, err := store.Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.DB().Exec(`update threads set created_at_gh='2026-01-01T00:00:00Z',updated_at_gh='2026-01-01T00:00:00Z'; update threads set state='closed' where number=2`); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	app = New()
	app.Stdout = &output
	app.Stderr = io.Discard
	if err := app.Run(ctx, []string{"--config", config, "portable", "export", "--profile", "current-state-v1", "--output-dir", filepath.Join(dir, "out"), "--since", "2026-11-01T00:00:00Z", "--json"}); err != nil {
		t.Fatal(err)
	}
	var result portableexport.ExportResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.ArtifactCommitted || result.Subset == nil || result.Subset.Since != "2026-11-01T00:00:00Z" {
		t.Fatalf("result: %+v", result)
	}
	reader, err := store.OpenReadOnly(ctx, result.DatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count, number int
	if err := reader.DB().QueryRow(`select count(*),min(number) from threads`).Scan(&count, &number); err != nil || count != 1 || number != 1 {
		t.Fatalf("recent rows %d/%d: %v", count, number, err)
	}
	if err := writer.DB().QueryRow(`select count(*) from threads`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("source rows %d: %v", count, err)
	}
}

func TestPortableRecentRefreshScopeAdmission(t *testing.T) {
	fixture := newPortableRefreshFixture(t, false)
	if _, err := fixture.refresh(t); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(fixture.remote, fixture.relative)
	output := filepath.Join(t.TempDir(), "recent")
	result, err := portableexport.Export(t.Context(), portableexport.ExportOptions{
		SourceDBPath: source, OutputDir: output, DatabaseName: filepath.Base(source), PublicPath: fixture.relative,
		Profile: portableexport.CurrentStateV1, Since: "2026-11-01T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	dbBytes := portableTestReadFile(t, result.DatabasePath)
	manifestBytes := portableTestReadFile(t, result.ManifestPath)
	for path, data := range map[string][]byte{source: dbBytes, source + ".manifest.json": manifestBytes} {
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	portableTestCommit(t, fixture.remote)
	refreshed, err := fixture.refresh(t)
	if err != nil || refreshed.Subset == nil || refreshed.Subset.Since != "2026-11-01T00:00:00Z" {
		t.Fatalf("recent refresh: %+v %v", refreshed, err)
	}
	head := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD")
	mirror := portableTestSHA256(t, fixture.mirror)
	status := fixture.command(t, "status", "--json")
	if !strings.Contains(string(status), "Recent portable subset since 2026-11-01T00:00:00Z") {
		t.Fatal("status hid subset coverage")
	}
	for _, mode := range []string{"omitted scope", "forged since", "missing manifest"} {
		t.Run(mode, func(t *testing.T) {
			var manifest map[string]any
			if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "omitted scope":
				delete(manifest, "subset")
			case "forged since":
				manifest["subset"].(map[string]any)["since"] = "2020-01-01T00:00:00Z"
			}
			if mode == "missing manifest" {
				if err := os.Remove(source + ".manifest.json"); err != nil {
					t.Fatal(err)
				}
			} else {
				data, err := json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(source+".manifest.json", data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			// The ordinary source/install validator must also reject absent manifests.
			if err := validatePortableSQLiteSourceFile(t.Context(), source, source); err == nil {
				t.Fatal("source admission accepted missing/forged scope")
			}
			portableTestCommit(t, fixture.remote)
			if _, err := fixture.refresh(t); err == nil {
				t.Fatal("strict refresh accepted missing/forged scope")
			}
			if got := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD"); got != head {
				t.Fatal("refusal advanced checkout")
			}
			if portableTestSHA256(t, fixture.mirror) != mirror {
				t.Fatal("refusal replaced last good mirror")
			}
		})
	}
}
