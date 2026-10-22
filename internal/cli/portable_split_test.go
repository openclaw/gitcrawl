package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// portableTestSplitManifest replaces any raw database, gzip archive or older
// parts beside path with a split archive of partBytes-sized parts.
func portableTestSplitManifest(t *testing.T, path, relative string, partBytes int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := fileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := filepath.Glob(path + ".part-*")
	for _, stale := range append(old, path+".gz") {
		if err := os.Remove(stale); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	manifest := portableDBManifest{Schema: "gitcrawl-portable-sync-v2", OutputPath: relative, OutputBytes: int64(len(data)), SHA256: fmt.Sprintf("%x", digest), QuickCheck: "ok",
		Compression: "split", ArchiveBytes: int64(len(data)), ArchiveSHA256: fmt.Sprintf("%x", digest), MaxArchiveBytes: int64(partBytes)}
	for index := 0; index*partBytes < len(data); index++ {
		end := min((index+1)*partBytes, len(data))
		name := fmt.Sprintf("%s.part-%03d", filepath.Base(path), index)
		if err := os.WriteFile(filepath.Join(filepath.Dir(path), name), data[index*partBytes:end], 0o644); err != nil {
			t.Fatal(err)
		}
		partDigest, err := fileSHA256(filepath.Join(filepath.Dir(path), name))
		if err != nil {
			t.Fatal(err)
		}
		manifest.ArchiveParts = append(manifest.ArchiveParts, portableArchivePart{Path: name, Bytes: int64(end - index*partBytes), SHA256: fmt.Sprintf("%x", partDigest)})
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(portableDBManifestPath(path), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
}

func (fixture portableRefreshFixture) advanceSplit(t *testing.T, number int, body string) {
	t.Helper()
	path := filepath.Join(fixture.remote, fixture.relative)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		temp, err := stagePortableSQLiteSourceTemp(path, path, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(temp, path); err != nil {
			t.Fatal(err)
		}
	}
	seedPortableThread(t, path, number, body)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	portableTestSplitManifest(t, path, fixture.relative, int(info.Size()/4+1))
	portableTestCommit(t, fixture.remote)
}

func TestPortableRefreshTransitionsGzipToSplitArchive(t *testing.T) {
	fixture := newPortableRefreshFixture(t, true)
	if result, err := fixture.refresh(t); err != nil || result.MirrorResult != "promoted" {
		t.Fatalf("initial gzip: %+v %v", result, err)
	}
	for generation, body := range []string{"split transition", "split successor"} {
		fixture.advanceSplit(t, 10+generation, body)
		result, err := fixture.refresh(t)
		if err != nil || result.Result != "updated" || result.MirrorResult != "promoted" {
			t.Fatalf("split generation %d: %+v %v", generation, result, err)
		}
		manifest, ok, err := readPortableDBManifest(portableDBManifestPath(filepath.Join(fixture.checkout, fixture.relative)))
		if err != nil || !ok || manifest.Compression != "split" || len(manifest.ArchiveParts) < 2 {
			t.Fatalf("checkout manifest: %+v ok=%v err=%v", manifest, ok, err)
		}
		if sum, _ := fileSHA256(fixture.mirror); fmt.Sprintf("%x", sum) != manifest.SHA256 || result.SHA256 != manifest.SHA256 {
			t.Fatalf("mirror does not match reassembled split archive")
		}
		if result.Repack != "geometric" {
			t.Fatalf("split generation %d repack = %q", generation, result.Repack)
		}
		if loose := portableTestGit(t, fixture.checkout, "count-objects", "-v"); !strings.Contains(loose, "count: 0\n") {
			t.Fatalf("refresh left loose objects: %s", loose)
		}
		if _, err := os.Stat(filepath.Join(fixture.checkout, fixture.relative) + ".gz"); !os.IsNotExist(err) {
			t.Fatalf("retired gzip archive remains in checkout: %v", err)
		}
	}
	// A reader-style fast-forward leaves fetched split objects loose; the next
	// strict refresh is a no-op for refs but still consolidates them.
	fixture.advanceSplit(t, 15, "reader fast-forward generation")
	portableTestGit(t, fixture.checkout, "fetch", "-q", "origin", "main")
	portableTestGit(t, fixture.checkout, "merge", "-q", "--ff-only", "FETCH_HEAD")
	portableTestGit(t, fixture.checkout, "update-ref", "refs/remotes/origin/main", "FETCH_HEAD")
	if loose := portableTestGit(t, fixture.checkout, "count-objects", "-v"); strings.Contains(loose, "count: 0\n") {
		t.Fatalf("fixture fast-forward did not leave loose objects: %s", loose)
	}
	if result, err := fixture.refresh(t); err != nil || result.Result != "no-op" || result.Repack != "geometric" {
		t.Fatalf("no-op consolidation: %+v %v", result, err)
	}
	if loose := portableTestGit(t, fixture.checkout, "count-objects", "-v"); !strings.Contains(loose, "count: 0\n") {
		t.Fatalf("no-op refresh left loose objects: %s", loose)
	}
	if result, err := fixture.refresh(t); err != nil || result.Result != "no-op" || result.Repack == "failed" || result.Repack == "" {
		t.Fatalf("settled split refresh: %+v %v", result, err)
	}
	// Generations kept as fetched packs (fetch.unpackLimit=1) leave nothing
	// loose; repeated packed fetches must still be folded together.
	portableTestGit(t, fixture.checkout, "config", "fetch.unpackLimit", "1")
	for generation := 0; generation < 3; generation++ {
		fixture.advanceSplit(t, 40+generation, fmt.Sprintf("packed fetch generation %d", generation))
		result, err := fixture.refresh(t)
		if err != nil || result.Result != "updated" || result.Repack != "geometric" {
			t.Fatalf("packed fetch generation %d: %+v %v", generation, result, err)
		}
	}
	objects := portableTestGit(t, fixture.checkout, "count-objects", "-v")
	if !strings.Contains(objects, "count: 0\n") || strings.Contains(objects, "packs: 4") || strings.Contains(objects, "packs: 5") {
		t.Fatalf("packed fetches were not consolidated: %s", objects)
	}
	portableTestGit(t, fixture.checkout, "repack", "-a", "-d", "-q")
	if result, err := fixture.refresh(t); err != nil || result.Result != "no-op" || result.Repack != "current" {
		t.Fatalf("single-pack split refresh: %+v %v", result, err)
	}
	portableTestGit(t, fixture.checkout, "config", "--unset", "fetch.unpackLimit")
	head := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD")
	// A tampered part is refused before the checkout or mirror advance.
	path := filepath.Join(fixture.remote, fixture.relative)
	fixture.advanceSplit(t, 20, "tampered generation")
	if err := os.WriteFile(path+".part-001", []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	portableTestCommit(t, fixture.remote)
	before, _ := fileSHA256(fixture.mirror)
	if result, err := fixture.refresh(t); err == nil || result.Result == "updated" {
		t.Fatalf("tampered split accepted: %+v", result)
	}
	if after, _ := fileSHA256(fixture.mirror); after != before {
		t.Fatal("refused split refresh replaced the mirror")
	}
	if got := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD"); got != head {
		t.Fatal("refused split refresh advanced the checkout")
	}
}

func TestSplitPortableSourceInflatesAndRejectsInvalidParts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourceDB := filepath.Join(dir, "checkout", "data", "openclaw__openclaw.sync.db")
	mirrorDB := filepath.Join(dir, "runtime", "openclaw__openclaw.sync.db")
	seedPortableThread(t, sourceDB, 7, "split portable source")
	info, err := os.Stat(sourceDB)
	if err != nil {
		t.Fatal(err)
	}
	portableTestSplitManifest(t, sourceDB, "data/openclaw__openclaw.sync.db", int(info.Size()/3+1))
	changed, err := refreshPortableRuntimeDB(ctx, sourceDB, mirrorDB, false, filepath.Join(dir, "config.toml"))
	if err != nil || !changed {
		t.Fatalf("refresh split portable source: changed=%v err=%v", changed, err)
	}
	manifest, _, err := readPortableDBManifest(portableDBManifestPath(sourceDB))
	if err != nil {
		t.Fatal(err)
	}
	if sum, _ := fileSHA256(mirrorDB); fmt.Sprintf("%x", sum) != manifest.SHA256 {
		t.Fatal("inflated split mirror does not match manifest")
	}
	if err := validatePortableSQLiteSourceFile(ctx, sourceDB, sourceDB); err != nil {
		t.Fatalf("validate split source: %v", err)
	}
	for _, tc := range []struct {
		name    string
		mutate  func(*portableDBManifest)
		message string
	}{
		{"escaping part", func(m *portableDBManifest) { m.ArchiveParts[0].Path = "../escape" }, "unique basename"},
		{"duplicate part", func(m *portableDBManifest) { m.ArchiveParts[1].Path = m.ArchiveParts[0].Path }, "unique basename"},
		{"missing parts", func(m *portableDBManifest) { m.ArchiveParts = nil }, "parts"},
		{"total mismatch", func(m *portableDBManifest) { m.ArchiveParts = m.ArchiveParts[:1] }, "total"},
		{"archive digest", func(m *portableDBManifest) { m.ArchiveSHA256 = strings.Repeat("0", 64) }, "equal the database"},
		{"part budget", func(m *portableDBManifest) { m.MaxArchiveBytes = 1 }, "budget"},
		{"single archive path", func(m *portableDBManifest) { m.ArchivePath = "x.gz" }, "archivePath"},
		{"parts without split", func(m *portableDBManifest) { m.Compression = "" }, "require split"},
		{"parts with gzip", func(m *portableDBManifest) { m.Compression = "gzip" }, "require split"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := manifest
			changed.ArchiveParts = append([]portableArchivePart(nil), manifest.ArchiveParts...)
			tc.mutate(&changed)
			encoded, _ := json.Marshal(changed)
			if err := os.WriteFile(portableDBManifestPath(sourceDB), encoded, 0o644); err != nil {
				t.Fatal(err)
			}
			_, _, _, err := portableSourceArtifact(sourceDB)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error = %v, want %q", err, tc.message)
			}
		})
	}
	encoded, _ := json.Marshal(manifest)
	if err := os.WriteFile(portableDBManifestPath(sourceDB), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(sourceDB), manifest.ArchiveParts[1].Path), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := refreshPortableRuntimeDB(ctx, sourceDB, mirrorDB, true, filepath.Join(dir, "config.toml")); err == nil || !strings.Contains(err.Error(), "portable manifest mismatch") {
		t.Fatalf("tampered part error = %v", err)
	}
	if backup, err := preserveMalformedPortableDB(filepath.Join(dir, "checkout"), sourceDB); err == nil {
		for _, part := range manifest.ArchiveParts {
			if _, err := os.Stat(filepath.Join(backup, part.Path+".malformed")); err != nil {
				t.Fatalf("malformed evidence missing part %s: %v", part.Path, err)
			}
		}
	} else {
		t.Fatalf("preserve malformed split source: %v", err)
	}
}

func TestConsolidatePortableObjectsSkipsLargeLooseBacklog(t *testing.T) {
	fixture := newPortableRefreshFixture(t, false)
	objects := filepath.Join(fixture.checkout, ".git", "objects")
	for index := 0; index <= portableRepackLooseLimit; index++ {
		path := filepath.Join(fixture.checkout, fmt.Sprintf("loose-%d", index))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("loose object %d", index)), 0o600); err != nil {
			t.Fatal(err)
		}
		portableTestGit(t, fixture.checkout, "hash-object", "-w", path)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := filepath.Glob(filepath.Join(objects, "pack", "*.pack"))
	ctx, err := portableGitContext(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	result, err := consolidatePortableObjects(ctx, fixture.checkout)
	if err != nil || result != "skipped-loose-backlog" {
		t.Fatalf("backlog result=%q err=%v", result, err)
	}
	after, _ := filepath.Glob(filepath.Join(objects, "pack", "*.pack"))
	if len(after) != len(before) {
		t.Fatal("skipped consolidation wrote a pack")
	}
}

func TestPortableStatusReadsSplitArchiveWithoutRuntime(t *testing.T) {
	fixture := newPortableRefreshFixture(t, true)
	fixture.advanceSplit(t, 30, "split status generation")
	if result, err := fixture.refresh(t); err != nil || result.Result != "updated" {
		t.Fatalf("split refresh: %+v %v", result, err)
	}
	if err := os.Remove(fixture.mirror); err != nil {
		t.Fatal(err)
	}
	logical := filepath.Join(fixture.checkout, fixture.relative)
	output := fixture.command(t, "status", "--json")
	var status struct {
		DatabasePath string `json:"database_path"`
		Databases    []struct {
			Kind string `json:"kind"`
		} `json:"databases"`
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal(output, &status); err != nil {
		t.Fatal(err)
	}
	if !sameExistingPath(status.DatabasePath, logical+".part-000") || len(status.Databases) == 0 || status.Databases[0].Kind != "sqlite-split" {
		t.Fatalf("split status inventory: %s", output)
	}
	if !strings.Contains(strings.Join(status.Warnings, " "), "split artifact") {
		t.Fatalf("split status warning missing: %s", output)
	}
	if _, err := os.Stat(logical); !os.IsNotExist(err) {
		t.Fatalf("status materialized logical database: %v", err)
	}
}

func TestSplitPortableArchiveValidationRejectsMissingAndResizedParts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourceDB := filepath.Join(dir, "data", "archive.db")
	seedPortableThread(t, sourceDB, 4, "split validation")
	info, err := os.Stat(sourceDB)
	if err != nil {
		t.Fatal(err)
	}
	portableTestSplitManifest(t, sourceDB, "data/archive.db", int(info.Size()/2+1))
	first, manifest, compressed, err := portableSourceArtifact(sourceDB)
	if err != nil || !compressed {
		t.Fatalf("split source: compressed=%v err=%v", compressed, err)
	}
	if err := validatePortableArchive(first, manifest); err != nil {
		t.Fatalf("valid split archive: %v", err)
	}
	second := portableArchivePartPaths(first, manifest)[1]
	data, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, append(data, 0), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validatePortableArchiveContext(ctx, first, manifest); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("resized part error = %v", err)
	}
	if err := os.Remove(second); err != nil {
		t.Fatal(err)
	}
	if err := validatePortableArchiveContext(ctx, first, manifest); err == nil {
		t.Fatal("missing part accepted")
	}
	if _, err := stagePortableSQLiteSourceTempContext(ctx, sourceDB, filepath.Join(dir, "runtime", "archive.db"), 0o600); err == nil {
		t.Fatal("staged a split archive with a missing part")
	}
	tampered := manifest
	tampered.ArchiveParts = append([]portableArchivePart(nil), manifest.ArchiveParts...)
	tampered.ArchiveParts[0].SHA256 = "short"
	if err := validatePortableSplitMetadata(tampered); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("short part digest error = %v", err)
	}
	tampered.ArchiveParts[0].SHA256 = manifest.ArchiveParts[0].SHA256
	tampered.ArchiveParts[0].Bytes = manifest.OutputBytes + 1
	if err := validatePortableSplitMetadata(tampered); err == nil {
		t.Fatal("oversized part accepted")
	}
}

func TestPortableExportRejectsUnknownPackaging(t *testing.T) {
	app := New()
	app.Stdout, app.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
	err := app.Run(context.Background(), []string{"--config", filepath.Join(t.TempDir(), "missing.toml"),
		"portable", "export", "--profile", "current-state-v1", "--output-dir", filepath.Join(t.TempDir(), "out"), "--compression", "zstd"})
	if err == nil || !strings.Contains(err.Error(), `"gzip" or "split"`) {
		t.Fatalf("unknown packaging error = %v", err)
	}
}

func TestPortableRefreshReportsRepackFailureWithoutRefusing(t *testing.T) {
	fixture := newPortableRefreshFixture(t, true)
	fixture.advanceSplit(t, 50, "repack failure generation")
	pack := filepath.Join(fixture.checkout, ".git", "objects", "pack")
	if err := os.Chmod(pack, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(pack, 0o755) })
	result, err := fixture.refresh(t)
	if err != nil || result.Result != "updated" || result.Repack != "failed" || !strings.Contains(result.RepackReason, "writable") {
		t.Fatalf("repack failure: %+v %v", result, err)
	}
	if sum, _ := fileSHA256(fixture.mirror); fmt.Sprintf("%x", sum) != result.SHA256 {
		t.Fatal("repack failure changed the validated generation")
	}
}
