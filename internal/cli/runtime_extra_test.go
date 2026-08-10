package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	portableexport "github.com/openclaw/gitcrawl/internal/portable"
)

func writeTestCompressedPortableSource(t *testing.T, dbPath string) string {
	t.Helper()
	manifestPath := writeTestPortableManifest(t, dbPath)
	manifest, ok, err := readPortableDBManifest(manifestPath)
	if err != nil || !ok {
		t.Fatalf("read direct portable manifest: ok=%v err=%v", ok, err)
	}
	archivePath := dbPath + ".gz"
	source, err := os.Open(dbPath)
	if err != nil {
		t.Fatalf("open portable db: %v", err)
	}
	archive, err := os.Create(archivePath)
	if err != nil {
		_ = source.Close()
		t.Fatalf("create portable archive: %v", err)
	}
	writer := gzip.NewWriter(archive)
	if _, err := io.Copy(writer, source); err != nil {
		_ = writer.Close()
		_ = archive.Close()
		_ = source.Close()
		t.Fatalf("compress portable db: %v", err)
	}
	if err := writer.Close(); err != nil {
		_ = archive.Close()
		_ = source.Close()
		t.Fatalf("close portable gzip writer: %v", err)
	}
	if err := archive.Close(); err != nil {
		_ = source.Close()
		t.Fatalf("close portable archive: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("close portable db: %v", err)
	}
	archiveInfo, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("stat portable archive: %v", err)
	}
	archiveSum, err := fileSHA256(archivePath)
	if err != nil {
		t.Fatalf("hash portable archive: %v", err)
	}
	manifest.Compression = "gzip"
	manifest.ArchivePath = filepath.Base(archivePath)
	manifest.ArchiveBytes = archiveInfo.Size()
	manifest.ArchiveSHA256 = fmt.Sprintf("%x", archiveSum)
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal compressed portable manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		t.Fatalf("write compressed portable manifest: %v", err)
	}
	if err := os.Remove(dbPath); err != nil {
		t.Fatalf("remove direct portable db: %v", err)
	}
	return archivePath
}

func TestLocalRuntimeDBTarget(t *testing.T) {
	direct := localRuntime{Config: config.Config{DBPath: "/data/gitcrawl.db"}}
	if got, want := direct.dbTarget(), (dbTargetInfo{DBTarget: "direct", DBTargetPath: "/data/gitcrawl.db"}); got != want {
		t.Fatalf("direct db target = %+v, want %+v", got, want)
	}

	redirected := localRuntime{
		Config:       config.Config{DBPath: "/runtime/store/data/gitcrawl.db"},
		SourceDBPath: "/checkout/data/gitcrawl.db",
		RemoteSource: true,
	}
	want := dbTargetInfo{
		DBTarget:         "runtime-mirror",
		DBTargetPath:     "/runtime/store/data/gitcrawl.db",
		PortableSourceDB: "/checkout/data/gitcrawl.db",
	}
	if got := redirected.dbTarget(); got != want {
		t.Fatalf("redirected db target = %+v, want %+v", got, want)
	}
}

func TestPortableManifestValidatesSemanticArtifactIdentity(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "artifact.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`create table repositories(id integer primary key, full_name text, updated_at text); insert into repositories values(1, 'openclaw/gitcrawl', 'first')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	artifactID, err := portableexport.ComputeArtifactID(ctx, dbPath, portableexport.CurrentStateSemanticV1)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := portableDBManifestPath(dbPath)
	manifest := portableDBManifest{
		Schema:            "gitcrawl-portable-sync-v2",
		Profile:           portableexport.CurrentStateV1,
		ArtifactID:        artifactID,
		ArtifactIDProfile: portableexport.CurrentStateSemanticV1,
		QuickCheck:        "ok",
	}
	refresh := func() {
		t.Helper()
		info, err := os.Stat(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		sum, err := fileSHA256(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		manifest.OutputBytes = info.Size()
		manifest.SHA256 = fmt.Sprintf("%x", sum)
		data, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	refresh()
	if err := validatePortableDBManifest(ctx, dbPath, manifestPath); err != nil {
		t.Fatalf("validate semantic manifest: %v", err)
	}
	manifest.ArtifactIDProfile = ""
	manifest.ArtifactID = manifest.SHA256
	refresh()
	if err := validatePortableDBManifest(ctx, dbPath, manifestPath); err != nil {
		t.Fatalf("validate legacy exact-SHA artifact identity: %v", err)
	}
	manifest.ArtifactID = strings.Repeat("0", 64)
	refresh()
	if err := validatePortableDBManifest(ctx, dbPath, manifestPath); err == nil || !strings.Contains(err.Error(), "legacy artifactId") {
		t.Fatalf("mismatched legacy artifact identity error = %v", err)
	}
	manifest.ArtifactIDProfile = portableexport.CurrentStateSemanticV1
	manifest.ArtifactID = artifactID
	refresh()

	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`update repositories set updated_at = 'second'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	refresh()
	if err := validatePortableDBManifest(ctx, dbPath, manifestPath); err != nil {
		t.Fatalf("local-only change invalidated semantic manifest: %v", err)
	}

	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`update repositories set full_name = 'openclaw/changed'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	refresh()
	if err := validatePortableDBManifest(ctx, dbPath, manifestPath); err == nil || !strings.Contains(err.Error(), "artifactId") {
		t.Fatalf("meaningful change semantic manifest error = %v", err)
	}
}

func TestSweepOrphanPortableRuntimeTempFiles(t *testing.T) {
	dir := t.TempDir()
	mirrorPath := filepath.Join(dir, "x.db")
	old := time.Now().Add(-2 * portableRuntimeTempMaxAge)
	oldMatching := []string{".x.db.tmp-123", ".x.db.tmp-123-wal", ".x.db.tmp-123-shm"}
	oldPreserved := []string{"old.db", ".other.db.tmp-9", ".notes.tmp-backup"}
	for _, name := range append(append([]string(nil), oldMatching...), oldPreserved...) {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("backdate %s: %v", name, err)
		}
	}
	fresh := filepath.Join(dir, ".x.db.tmp-fresh")
	if err := os.WriteFile(fresh, []byte("fresh"), 0o600); err != nil {
		t.Fatalf("write fresh temp: %v", err)
	}

	sweepOrphanPortableRuntimeTempFiles(mirrorPath, portableRuntimeTempMaxAge)

	for _, name := range oldMatching {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("old matching temp %s still exists: %v", name, err)
		}
	}
	preserved := []string{fresh}
	for _, name := range oldPreserved {
		preserved = append(preserved, filepath.Join(dir, name))
	}
	for _, path := range preserved {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("preserved file %s: %v", path, err)
		}
	}
}

func TestCopySQLiteFileAtomicVerifiedRemovesTempFiles(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	target := filepath.Join(dir, "target.db")
	seedPortableThread(t, source, 1, "copy temp cleanup")
	if err := copySQLiteFileAtomicVerified(context.Background(), source, target); err != nil {
		t.Fatalf("copy verified sqlite file: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".target.db.tmp-*"))
	if err != nil {
		t.Fatalf("glob temp files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("verified copy left temp files: %v", matches)
	}
	invalidSource := filepath.Join(dir, "invalid.db")
	failedTarget := filepath.Join(dir, "failed.db")
	if err := os.WriteFile(invalidSource, []byte("not sqlite"), 0o600); err != nil {
		t.Fatalf("write invalid source: %v", err)
	}
	if err := copySQLiteFileAtomicVerified(context.Background(), invalidSource, failedTarget); err == nil {
		t.Fatal("invalid sqlite source should fail validation")
	}
	matches, err = filepath.Glob(filepath.Join(dir, ".failed.db.tmp-*"))
	if err != nil {
		t.Fatalf("glob failed-copy temp files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("failed verified copy left temp files: %v", matches)
	}

	tempPath := filepath.Join(dir, ".failed.db.tmp-123")
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.WriteFile(tempPath+suffix, []byte("sidecar"), 0o600); err != nil {
			t.Fatalf("write temp sidecar: %v", err)
		}
	}
	removeSQLiteTempSidecars(tempPath)
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(tempPath + suffix); !os.IsNotExist(err) {
			t.Fatalf("temp sidecar %s still exists: %v", suffix, err)
		}
	}
}

func TestRefreshPortableRuntimeDBInflatesCompressedSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	checkoutDir := filepath.Join(dir, "checkout")
	sourceDB := filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db")
	mirrorDB := filepath.Join(dir, "runtime", "openclaw__openclaw.sync.db")
	seedPortableThread(t, sourceDB, 7, "compressed portable source")
	writeTestCompressedPortableSource(t, sourceDB)

	changed, err := refreshPortableRuntimeDB(
		ctx,
		sourceDB,
		mirrorDB,
		false,
		filepath.Join(dir, "config.toml"),
	)
	if err != nil || !changed {
		t.Fatalf("refresh compressed portable source: changed=%v err=%v", changed, err)
	}
	if err := sqliteStoreHealth(ctx, mirrorDB); err != nil {
		t.Fatalf("inflated mirror health: %v", err)
	}
	manifest, ok, err := readPortableDBManifest(portableDBManifestPath(sourceDB))
	if err != nil || !ok {
		t.Fatalf("read compressed manifest: ok=%v err=%v", ok, err)
	}
	sum, err := fileSHA256(mirrorDB)
	if err != nil {
		t.Fatalf("hash inflated mirror: %v", err)
	}
	if got := fmt.Sprintf("%x", sum); got != manifest.SHA256 {
		t.Fatalf("inflated mirror sha256 = %s, want %s", got, manifest.SHA256)
	}
}

func TestCompressedPortableSourceFailureKeepsLastGoodMirror(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	checkoutDir := filepath.Join(dir, "checkout")
	sourceDB := filepath.Join(checkoutDir, "data", "openclaw__openclaw.sync.db")
	mirrorDB := filepath.Join(dir, "runtime", "openclaw__openclaw.sync.db")
	seedPortableThread(t, sourceDB, 8, "new compressed content")
	archivePath := writeTestCompressedPortableSource(t, sourceDB)
	seedPortableThread(t, mirrorDB, 9, "last good mirror")
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(mirrorDB, past, past); err != nil {
		t.Fatalf("age mirror: %v", err)
	}
	before, err := fileSHA256(mirrorDB)
	if err != nil {
		t.Fatalf("hash mirror before: %v", err)
	}
	if err := os.WriteFile(archivePath, []byte("truncated"), 0o644); err != nil {
		t.Fatalf("corrupt portable archive: %v", err)
	}

	changed, err := refreshPortableRuntimeDB(
		ctx,
		sourceDB,
		mirrorDB,
		false,
		filepath.Join(dir, "config.toml"),
	)
	if err == nil || changed || !strings.Contains(err.Error(), "portable manifest mismatch") {
		t.Fatalf("corrupt compressed source: changed=%v err=%v", changed, err)
	}
	after, err := fileSHA256(mirrorDB)
	if err != nil {
		t.Fatalf("hash mirror after: %v", err)
	}
	if after != before {
		t.Fatal("failed compressed refresh replaced the last good mirror")
	}
}

func TestCompressedPortableManifestRejectsEscapingArchivePath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	manifest := portableDBManifest{
		Schema:        "gitcrawl-portable-sync-v2",
		OutputBytes:   1,
		SHA256:        strings.Repeat("a", 64),
		Compression:   "gzip",
		ArchivePath:   "../escape.db.gz",
		ArchiveBytes:  1,
		ArchiveSHA256: strings.Repeat("b", 64),
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(portableDBManifestPath(dbPath), data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, _, _, err := portableSourceArtifact(dbPath); err == nil ||
		!strings.Contains(err.Error(), "archivePath escapes") {
		t.Fatalf("escaping archivePath error = %v", err)
	}
}

func TestCompressedPortableManifestRejectsInvalidMetadata(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	manifestPath := portableDBManifestPath(dbPath)
	base := portableDBManifest{
		Schema:        "gitcrawl-portable-sync-v2",
		OutputBytes:   1,
		SHA256:        strings.Repeat("a", 64),
		Compression:   "gzip",
		ArchivePath:   filepath.Base(dbPath) + ".gz",
		ArchiveBytes:  1,
		ArchiveSHA256: strings.Repeat("b", 64),
	}
	cases := []struct {
		name    string
		mutate  func(*portableDBManifest)
		message string
	}{
		{
			name:    "unsupported compression",
			mutate:  func(manifest *portableDBManifest) { manifest.Compression = "zstd" },
			message: "unsupported compression",
		},
		{
			name:    "missing archive path",
			mutate:  func(manifest *portableDBManifest) { manifest.ArchivePath = "" },
			message: "archivePath must be relative",
		},
		{
			name:    "absolute archive path",
			mutate:  func(manifest *portableDBManifest) { manifest.ArchivePath = "/tmp/store.db.gz" },
			message: "archivePath must be relative",
		},
		{
			name:    "drive letter archive path",
			mutate:  func(manifest *portableDBManifest) { manifest.ArchivePath = `C:\store.db.gz` },
			message: "archivePath must be relative",
		},
		{
			name:    "current directory archive path",
			mutate:  func(manifest *portableDBManifest) { manifest.ArchivePath = "." },
			message: "archivePath escapes the store",
		},
		{
			name:    "missing archive bytes",
			mutate:  func(manifest *portableDBManifest) { manifest.ArchiveBytes = 0 },
			message: "archiveBytes missing",
		},
		{
			name:    "missing archive sha",
			mutate:  func(manifest *portableDBManifest) { manifest.ArchiveSHA256 = "" },
			message: "archiveSha256 missing",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manifest := base
			tc.mutate(&manifest)
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatalf("marshal manifest: %v", err)
			}
			if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
				t.Fatalf("write manifest: %v", err)
			}
			if _, _, _, err := portableSourceArtifact(dbPath); err == nil ||
				!strings.Contains(err.Error(), tc.message) {
				t.Fatalf("portable source error = %v, want %q", err, tc.message)
			}
		})
	}
}

func TestCompressedPortableArchiveRejectsTampering(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data", "openclaw__openclaw.sync.db")
	seedPortableThread(t, dbPath, 10, "compressed portable source")
	archivePath := writeTestCompressedPortableSource(t, dbPath)
	manifestPath := portableDBManifestPath(dbPath)
	manifest, ok, err := readPortableDBManifest(manifestPath)
	if err != nil || !ok {
		t.Fatalf("read compressed manifest: ok=%v err=%v", ok, err)
	}

	manifest.ArchiveBytes++
	if err := validatePortableArchive(archivePath, manifest); err == nil ||
		!strings.Contains(err.Error(), "archive size") {
		t.Fatalf("archive size error = %v", err)
	}

	manifest.ArchiveBytes--
	manifest.ArchiveSHA256 = strings.Repeat("0", 64)
	if err := validatePortableArchive(archivePath, manifest); err == nil ||
		!strings.Contains(err.Error(), "archive sha256") {
		t.Fatalf("archive hash error = %v", err)
	}
}

func TestCompressedPortableArchiveRejectsInvalidPayload(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data", "openclaw__openclaw.sync.db")
	seedPortableThread(t, dbPath, 11, "compressed portable source")
	archivePath := writeTestCompressedPortableSource(t, dbPath)
	manifestPath := portableDBManifestPath(dbPath)
	targetPath := filepath.Join(dir, "runtime", filepath.Base(dbPath))

	writeArchive := func(data []byte, outputBytes int64) {
		t.Helper()
		if err := os.WriteFile(archivePath, data, 0o644); err != nil {
			t.Fatalf("write archive: %v", err)
		}
		manifest, ok, err := readPortableDBManifest(manifestPath)
		if err != nil || !ok {
			t.Fatalf("read compressed manifest: ok=%v err=%v", ok, err)
		}
		info, err := os.Stat(archivePath)
		if err != nil {
			t.Fatalf("stat archive: %v", err)
		}
		sum, err := fileSHA256(archivePath)
		if err != nil {
			t.Fatalf("hash archive: %v", err)
		}
		manifest.OutputBytes = outputBytes
		manifest.ArchiveBytes = info.Size()
		manifest.ArchiveSHA256 = fmt.Sprintf("%x", sum)
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatalf("marshal compressed manifest: %v", err)
		}
		if err := os.WriteFile(manifestPath, encoded, 0o644); err != nil {
			t.Fatalf("write compressed manifest: %v", err)
		}
	}

	writeArchive([]byte("not gzip"), 1)
	if _, err := stagePortableSQLiteSourceTemp(dbPath, targetPath, 0o600); err == nil ||
		!strings.Contains(err.Error(), "open portable gzip archive") {
		t.Fatalf("invalid gzip error = %v", err)
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte("short")); err != nil {
		t.Fatalf("write gzip payload: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip payload: %v", err)
	}
	writeArchive(compressed.Bytes(), 10)
	if _, err := stagePortableSQLiteSourceTemp(dbPath, targetPath, 0o600); err == nil ||
		!strings.Contains(err.Error(), "inflated size") {
		t.Fatalf("inflated size error = %v", err)
	}
}

func TestValidateCompressedPortableSQLiteSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data", "openclaw__openclaw.sync.db")
	seedPortableThread(t, dbPath, 12, "compressed validation source")
	writeTestCompressedPortableSource(t, dbPath)

	if err := validatePortableSQLiteSourceFile(ctx, dbPath, dbPath); err != nil {
		t.Fatalf("validate compressed portable source: %v", err)
	}
}

func TestPreserveMalformedCompressedPortableSource(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "checkout")
	dbPath := filepath.Join(root, "data", "openclaw__openclaw.sync.db")
	seedPortableThread(t, dbPath, 13, "compressed evidence source")
	archivePath := writeTestCompressedPortableSource(t, dbPath)

	backupDir, err := preserveMalformedPortableDB(root, dbPath)
	if err != nil {
		t.Fatalf("preserve compressed portable source: %v", err)
	}
	for _, name := range []string{
		filepath.Base(archivePath) + ".malformed",
		filepath.Base(portableDBManifestPath(dbPath)),
	} {
		if _, err := os.Stat(filepath.Join(backupDir, name)); err != nil {
			t.Fatalf("stat preserved evidence %s: %v", name, err)
		}
	}
}

func writeTestPortableManifest(t *testing.T, dbPath string) string {
	t.Helper()
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat db for manifest: %v", err)
	}
	sum, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatalf("hash db for manifest: %v", err)
	}
	manifest := portableDBManifest{
		Schema:      "gitcrawl-portable-sync-v2",
		ExportedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		OutputPath:  filepath.Base(dbPath),
		OutputBytes: info.Size(),
		SHA256:      fmt.Sprintf("%x", sum),
		QuickCheck:  "ok",
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	manifestPath := portableDBManifestPath(dbPath)
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return manifestPath
}

func TestPublishPortableCheckoutPairPreservesCheckoutMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mirrorDB := filepath.Join(dir, "mirror", "x.db")
	checkoutDB := filepath.Join(dir, "checkout", "x.db")
	seedPortableThread(t, mirrorDB, 1, "publish pair source")
	mirrorManifest := writeTestPortableManifest(t, mirrorDB)
	seedPortableThread(t, checkoutDB, 2, "old checkout content")
	if err := os.Chmod(checkoutDB, 0o600); err != nil {
		t.Fatalf("chmod checkout db: %v", err)
	}
	checkoutManifest := portableDBManifestPath(checkoutDB)
	if err := os.WriteFile(checkoutManifest, []byte("old manifest"), 0o640); err != nil {
		t.Fatalf("write old checkout manifest: %v", err)
	}

	if err := publishPortableCheckoutPair(ctx, mirrorDB, mirrorManifest, checkoutDB, checkoutManifest); err != nil {
		t.Fatalf("publish portable checkout pair: %v", err)
	}
	want, err := os.ReadFile(mirrorDB)
	if err != nil {
		t.Fatalf("read mirror db: %v", err)
	}
	got, err := os.ReadFile(checkoutDB)
	if err != nil {
		t.Fatalf("read published checkout db: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("published checkout db does not match mirror db")
	}
	for path, want := range map[string]os.FileMode{checkoutDB: 0o600, checkoutManifest: 0o640} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat published file %s: %v", path, err)
		}
		if info.Mode().Perm() != want {
			t.Fatalf("published file %s mode = %o, want the preserved %o", path, info.Mode().Perm(), want)
		}
	}
	for _, pattern := range []string{".*.tmp-*", ".*.publish-rollback-*"} {
		leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(checkoutDB), pattern))
		if err != nil {
			t.Fatalf("glob %s leftovers: %v", pattern, err)
		}
		if len(leftovers) != 0 {
			t.Fatalf("publish left %s files: %v", pattern, leftovers)
		}
	}
}

func TestPublishPortableCheckoutPairKeepsOldPairOnValidationFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mirrorDB := filepath.Join(dir, "mirror", "x.db")
	if err := os.MkdirAll(filepath.Dir(mirrorDB), 0o755); err != nil {
		t.Fatalf("mkdir mirror: %v", err)
	}
	if err := os.WriteFile(mirrorDB, []byte("not sqlite"), 0o600); err != nil {
		t.Fatalf("write corrupt mirror db: %v", err)
	}
	mirrorManifest := mirrorDB + ".manifest.json"
	if err := os.WriteFile(mirrorManifest, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write mirror manifest: %v", err)
	}
	checkoutDB := filepath.Join(dir, "checkout", "x.db")
	seedPortableThread(t, checkoutDB, 1, "existing published content")
	checkoutManifest := writeTestPortableManifest(t, checkoutDB)
	beforeDB, err := os.ReadFile(checkoutDB)
	if err != nil {
		t.Fatalf("read checkout db: %v", err)
	}
	beforeManifest, err := os.ReadFile(checkoutManifest)
	if err != nil {
		t.Fatalf("read checkout manifest: %v", err)
	}

	if err := publishPortableCheckoutPair(ctx, mirrorDB, mirrorManifest, checkoutDB, checkoutManifest); err == nil {
		t.Fatal("corrupt mirror db should fail staged validation")
	}
	afterDB, err := os.ReadFile(checkoutDB)
	if err != nil {
		t.Fatalf("read checkout db after failure: %v", err)
	}
	afterManifest, err := os.ReadFile(checkoutManifest)
	if err != nil {
		t.Fatalf("read checkout manifest after failure: %v", err)
	}
	if !bytes.Equal(beforeDB, afterDB) || !bytes.Equal(beforeManifest, afterManifest) {
		t.Fatal("failed publish must leave the previous checkout pair untouched")
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(checkoutDB), ".*.tmp-*"))
	if err != nil {
		t.Fatalf("glob staged temps: %v", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("failed publish left staged temp files: %v", leftovers)
	}
}

func TestPublishPortableCheckoutPairRollsBackDBWhenManifestRenameFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mirrorDB := filepath.Join(dir, "mirror", "x.db")
	seedPortableThread(t, mirrorDB, 1, "new publish content")
	mirrorManifest := writeTestPortableManifest(t, mirrorDB)
	checkoutDB := filepath.Join(dir, "checkout", "x.db")
	seedPortableThread(t, checkoutDB, 2, "old publish content")
	checkoutManifest := portableDBManifestPath(checkoutDB)
	// A directory at the manifest destination makes the final rename fail
	// after the database rename already succeeded.
	if err := os.MkdirAll(checkoutManifest, 0o755); err != nil {
		t.Fatalf("mkdir manifest blocker: %v", err)
	}
	before, err := os.ReadFile(checkoutDB)
	if err != nil {
		t.Fatalf("read checkout db: %v", err)
	}

	if err := publishPortableCheckoutPair(ctx, mirrorDB, mirrorManifest, checkoutDB, checkoutManifest); err == nil {
		t.Fatal("manifest rename onto a directory should fail")
	}
	after, err := os.ReadFile(checkoutDB)
	if err != nil {
		t.Fatalf("read checkout db after failed publish: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("failed manifest rename must roll the checkout db back")
	}
	backups, err := filepath.Glob(filepath.Join(filepath.Dir(checkoutDB), ".*.publish-rollback-*"))
	if err != nil {
		t.Fatalf("glob rollback backups: %v", err)
	}
	if len(backups) != 0 {
		t.Fatalf("rollback backup should be consumed after a successful restore: %v", backups)
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(checkoutDB), ".*.tmp-*"))
	if err != nil {
		t.Fatalf("glob staged temps: %v", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("failed publish left staged temp files: %v", leftovers)
	}
}

func TestPortableStoreRootPropagatesGitProbeFailure(t *testing.T) {
	dir := t.TempDir()
	if err := runGit(context.Background(), "", "init", dir); err != nil {
		t.Fatalf("git init: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := portableStoreRoot(ctx, filepath.Join(dir, "gitcrawl.db"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("portable store root error = %v, want context canceled", err)
	}
}

func TestPortableStoreRootBindsCandidateToOwningWorktree(t *testing.T) {
	ctx := context.Background()
	outer := t.TempDir()
	if err := runGit(ctx, "", "init", outer); err != nil {
		t.Fatalf("git init: %v", err)
	}
	nested := filepath.Join(outer, "data")
	if err := os.MkdirAll(filepath.Join(nested, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir stray Git metadata: %v", err)
	}

	root, ok, err := portableStoreRoot(ctx, filepath.Join(nested, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, outer) {
		t.Fatalf("portable store root = %q, ok=%v, want outer worktree %q", root, ok, outer)
	}
}

func TestPortableStoreRootIgnoresInheritedGitSelection(t *testing.T) {
	ctx := context.Background()
	portable := t.TempDir()
	if err := runGit(ctx, "", "init", portable); err != nil {
		t.Fatalf("git init portable: %v", err)
	}
	other := t.TempDir()
	if err := runGit(ctx, "", "init", other); err != nil {
		t.Fatalf("git init other: %v", err)
	}
	t.Setenv("GIT_DIR", filepath.Join(other, ".git"))
	t.Setenv("GIT_WORK_TREE", other)

	root, ok, err := portableStoreRoot(ctx, filepath.Join(portable, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, portable) {
		t.Fatalf("portable store root = %q, ok=%v, want %q", root, ok, portable)
	}
}

func TestPortableStoreRootIgnoresGitTraceOutput(t *testing.T) {
	ctx := context.Background()
	portable := t.TempDir()
	if err := runGit(ctx, "", "init", portable); err != nil {
		t.Fatalf("git init: %v", err)
	}
	t.Setenv("GIT_TRACE", "1")

	root, ok, err := portableStoreRoot(ctx, filepath.Join(portable, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, portable) {
		t.Fatalf("portable store root = %q, ok=%v, want %q", root, ok, portable)
	}
}

func TestPortableStoreRootIgnoresCommandScopeGitConfig(t *testing.T) {
	ctx := context.Background()
	portable := t.TempDir()
	if err := runGit(ctx, "", "init", portable); err != nil {
		t.Fatalf("git init: %v", err)
	}
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.bare")
	t.Setenv("GIT_CONFIG_VALUE_0", "true")

	root, ok, err := portableStoreRoot(ctx, filepath.Join(portable, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if !ok || !sameExistingPath(root, portable) {
		t.Fatalf("portable store root = %q, ok=%v, want %q", root, ok, portable)
	}
}

func TestPortableManifestGenerationUnchanged(t *testing.T) {
	tests := []struct {
		name            string
		state           portableStoreRefreshState
		manifestModTime string
		manifestSize    int64
		sourceSHA256    string
		want            bool
	}{
		{
			name: "manifest hash matches",
			state: portableStoreRefreshState{
				MirrorHealthSourceSHA256: "ABC123",
			},
			sourceSHA256: "abc123",
			want:         true,
		},
		{
			name: "legacy manifest stamp migrates to hash",
			state: portableStoreRefreshState{
				MirrorHealthManifestModTime: "2026-07-20T00:00:00Z",
				MirrorHealthManifestSize:    512,
			},
			manifestModTime: "2026-07-20T00:00:00Z",
			manifestSize:    512,
			sourceSHA256:    "abc123",
			want:            true,
		},
		{
			name:  "manifest-less legacy store reuses cached health",
			state: portableStoreRefreshState{},
			want:  true,
		},
		{
			name: "removed manifest invalidates prior manifest stamp",
			state: portableStoreRefreshState{
				MirrorHealthManifestModTime: "2026-07-20T00:00:00Z",
				MirrorHealthManifestSize:    512,
				MirrorHealthSourceSHA256:    "abc123",
			},
			want: false,
		},
		{
			name: "hash mismatch overrides unchanged manifest metadata",
			state: portableStoreRefreshState{
				MirrorHealthManifestModTime: "2026-07-20T00:00:00Z",
				MirrorHealthManifestSize:    512,
				MirrorHealthSourceSHA256:    "old",
			},
			manifestModTime: "2026-07-20T00:00:00Z",
			manifestSize:    512,
			sourceSHA256:    "new",
			want:            false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := portableManifestGenerationUnchanged(
				test.state,
				test.manifestModTime,
				test.manifestSize,
				test.sourceSHA256,
			)
			if got != test.want {
				t.Fatalf("portableManifestGenerationUnchanged() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestManifestlessPortableMirrorReusesCachedOpenHealth(t *testing.T) {
	dir := t.TempDir()
	mirrorPath := filepath.Join(dir, "gitcrawl.db")
	statePath := portableStoreRefreshStatePath(mirrorPath)
	if err := os.WriteFile(mirrorPath, []byte("legacy portable mirror"), 0o600); err != nil {
		t.Fatalf("write mirror: %v", err)
	}
	info, err := os.Stat(mirrorPath)
	if err != nil {
		t.Fatalf("stat mirror: %v", err)
	}
	if err := writePortableStoreRefreshState(statePath, portableStoreRefreshState{
		MirrorHealthSize:    info.Size(),
		MirrorHealthModTime: info.ModTime().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("write cached health state: %v", err)
	}

	openHealthCalls := 0
	fullHealthCalls := 0
	err = sqliteStoreCachedHealthWithManifestChecks(
		context.Background(),
		mirrorPath,
		filepath.Join(dir, "source.db"),
		statePath,
		"",
		0,
		"",
		func(context.Context, string) error {
			openHealthCalls++
			return nil
		},
		func(context.Context, string) error {
			fullHealthCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("cached health check: %v", err)
	}
	if openHealthCalls != 1 || fullHealthCalls != 0 {
		t.Fatalf("health calls: open=%d full=%d, want open=1 full=0", openHealthCalls, fullHealthCalls)
	}
}

func TestLegacyPortableMirrorHealthMigratesSourceHash(t *testing.T) {
	dir := t.TempDir()
	mirrorPath := filepath.Join(dir, "gitcrawl.db")
	statePath := portableStoreRefreshStatePath(mirrorPath)
	if err := os.WriteFile(mirrorPath, []byte("legacy portable mirror"), 0o600); err != nil {
		t.Fatalf("write mirror: %v", err)
	}
	info, err := os.Stat(mirrorPath)
	if err != nil {
		t.Fatalf("stat mirror: %v", err)
	}
	const manifestModTime = "2026-07-20T00:00:00Z"
	const manifestSize = int64(512)
	const sourceSHA256 = "abc123"
	if err := writePortableStoreRefreshState(statePath, portableStoreRefreshState{
		MirrorHealthSize:            info.Size(),
		MirrorHealthModTime:         info.ModTime().UTC().Format(time.RFC3339Nano),
		MirrorHealthManifestModTime: manifestModTime,
		MirrorHealthManifestSize:    manifestSize,
	}); err != nil {
		t.Fatalf("write legacy cached health state: %v", err)
	}

	openHealthCalls := 0
	fullHealthCalls := 0
	err = sqliteStoreCachedHealthWithManifestChecks(
		context.Background(),
		mirrorPath,
		filepath.Join(dir, "source.db"),
		statePath,
		manifestModTime,
		manifestSize,
		sourceSHA256,
		func(context.Context, string) error {
			openHealthCalls++
			return nil
		},
		func(context.Context, string) error {
			fullHealthCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("cached health migration: %v", err)
	}
	if openHealthCalls != 0 || fullHealthCalls != 1 {
		t.Fatalf("health calls: open=%d full=%d, want open=0 full=1", openHealthCalls, fullHealthCalls)
	}
	state := readPortableStoreRefreshState(statePath)
	if state.MirrorHealthSourceSHA256 != sourceSHA256 {
		t.Fatalf("cached source hash = %q, want %q", state.MirrorHealthSourceSHA256, sourceSHA256)
	}
}

func TestPortableRuntimeUtilityBranches(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	mirror := filepath.Join(dir, "runtime", "source.db")
	if _, err := portableRuntimeNeedsCopy(source, mirror); err == nil {
		t.Fatal("missing source should fail")
	}
	if err := os.WriteFile(source, []byte("v1"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	needs, err := portableRuntimeNeedsCopy(source, mirror)
	if err != nil || !needs {
		t.Fatalf("missing mirror needs copy=%v err=%v", needs, err)
	}
	if err := copyFileAtomic(source, mirror); err != nil {
		t.Fatalf("copy mirror: %v", err)
	}
	if err := os.WriteFile(mirror+"-wal", []byte("wal"), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	if err := os.WriteFile(mirror+"-shm", []byte("shm"), 0o644); err != nil {
		t.Fatalf("write shm: %v", err)
	}
	if err := os.Chtimes(mirror, time.Now().Add(time.Hour), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("age mirror: %v", err)
	}
	needs, err = portableRuntimeNeedsCopy(source, mirror)
	if err != nil || needs {
		t.Fatalf("fresh mirror needs copy=%v err=%v", needs, err)
	}
	if err := copyFileAtomic(source, mirror); err != nil {
		t.Fatalf("recopy mirror: %v", err)
	}
	if _, err := os.Stat(mirror + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("wal sidecar should be removed, err=%v", err)
	}
	if _, err := os.Stat(mirror + "-shm"); !os.IsNotExist(err) {
		t.Fatalf("shm sidecar should be removed, err=%v", err)
	}

	statePath := portableStoreRefreshStatePath(mirror)
	state := portableStoreRefreshState{LastAttempt: "attempt", LastSuccess: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := writePortableStoreRefreshState(statePath, state); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if got := readPortableStoreRefreshState(statePath); got.LastAttempt != "attempt" || got.LastSuccess == "" {
		t.Fatalf("state = %+v", got)
	}
	if err := os.WriteFile(statePath, []byte("{"), 0o600); err != nil {
		t.Fatalf("write invalid state: %v", err)
	}
	if got := readPortableStoreRefreshState(statePath); got.LastAttempt != "" {
		t.Fatalf("invalid state should decode empty, got %+v", got)
	}
	now := time.Now().UTC()
	if recentPortableRefresh("", now, time.Minute) || recentPortableRefresh("bad", now, time.Minute) || !recentPortableRefresh(now.Format(time.RFC3339Nano), now, time.Minute) {
		t.Fatal("recent refresh classification mismatch")
	}
	lockPath := filepath.Join(dir, "refresh.lock")
	if err := os.WriteFile(lockPath, []byte("123\n"), 0o600); err != nil {
		t.Fatalf("write lock: %v", err)
	}
	removeStalePortableRefreshLock(lockPath, now)
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("fresh lock should remain: %v", err)
	}
	old := now.Add(-3 * portableStoreRefreshTimeout)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatalf("age lock: %v", err)
	}
	removeStalePortableRefreshLock(lockPath, now)
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("stale lock should be removed, err=%v", err)
	}
	t.Setenv("GITCRAWL_PORTABLE_REFRESH_TTL", "0")
	if got := portableStoreRefreshInterval(); got != 0 {
		t.Fatalf("zero ttl = %s", got)
	}
	t.Setenv("GITCRAWL_PORTABLE_REFRESH_TTL", "bad")
	if got := portableStoreRefreshInterval(); got != portableStoreRefreshTTL {
		t.Fatalf("bad ttl fallback = %s", got)
	}
	if err := refreshPortableStoreForDB(context.Background(), source); err != nil {
		t.Fatalf("non-portable refresh should be no-op: %v", err)
	}
	configDir := filepath.Join(dir, "config-root")
	t.Setenv("GITCRAWL_CONFIG", filepath.Join(configDir, "config.toml"))
	defaultStore := filepath.Join(configDir, "stores", "gitcrawl-store")
	if err := os.MkdirAll(defaultStore, 0o755); err != nil {
		t.Fatalf("mkdir default store: %v", err)
	}
	if !portableStoreRepairAllowed(defaultStore, "") {
		t.Fatal("default portable store should be repairable")
	}
	if portableStoreRepairAllowed(configDir, "") {
		t.Fatal("config root should not be repairable without marker")
	}
	markedStore := filepath.Join(dir, "custom-store")
	markedInfo := filepath.Join(markedStore, ".git", "info")
	if err := os.MkdirAll(markedInfo, 0o755); err != nil {
		t.Fatalf("mkdir marked store info: %v", err)
	}
	if err := os.WriteFile(filepath.Join(markedInfo, portableStoreMarkerFile), []byte("gitcrawl portable store\n"), 0o644); err != nil {
		t.Fatalf("write marked store marker: %v", err)
	}
	if !portableStoreRepairAllowed(markedStore, "") {
		t.Fatal("marked portable store should be repairable")
	}
	explicitConfigDir := filepath.Join(dir, "explicit-config-root")
	explicitConfigPath := filepath.Join(explicitConfigDir, "nested", "config.toml")
	explicitStore := filepath.Join(explicitConfigDir, "nested", "stores", "gitcrawl-store")
	if err := os.MkdirAll(explicitStore, 0o755); err != nil {
		t.Fatalf("mkdir explicit default store: %v", err)
	}
	if !portableStoreRepairAllowed(explicitStore, explicitConfigPath) {
		t.Fatal("explicit-config default portable store should be repairable")
	}
	lockRoot := filepath.Join(dir, "locked-store")
	lockPath = filepath.Join(lockRoot, ".git", "index.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatalf("write index lock: %v", err)
	}
	oldLock := now.Add(-time.Minute)
	if err := os.Chtimes(lockPath, oldLock, oldLock); err != nil {
		t.Fatalf("age index lock: %v", err)
	}
	removed, err := removeStaleGitIndexLock(context.Background(), lockRoot, staleGitIndexLockAge)
	if err != nil || !removed {
		t.Fatalf("remove stale index lock removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("stale index lock should be removed, err=%v", err)
	}

	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatalf("rewrite index lock: %v", err)
	}
	if err := os.Chtimes(lockPath, oldLock, oldLock); err != nil {
		t.Fatalf("age index lock with failing lsof: %v", err)
	}
	fakeBin := filepath.Join(dir, "fake-bin")
	if err := os.MkdirAll(fakeBin, 0o755); err != nil {
		t.Fatalf("mkdir fake bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "lsof"), []byte("#!/bin/sh\nexit 2\n"), 0o755); err != nil {
		t.Fatalf("write fake lsof: %v", err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	removed, err = removeStaleGitIndexLock(context.Background(), lockRoot, staleGitIndexLockAge)
	if err != nil || removed {
		t.Fatalf("failing lsof should not remove lock, removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("index lock should remain when lsof fails: %v", err)
	}
}
