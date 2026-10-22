package portable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExportSplitCommitsVerifiedOrderedParts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()

	baseline, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "baseline")))
	if err != nil {
		t.Fatalf("baseline export: %v", err)
	}
	partBudget := baseline.BytesAfter/3 + 1
	options := testExportOptions(sourcePath, filepath.Join(dir, "split"))
	options.Compression = CompressionSplit
	options.MaxArchiveBytes = &partBudget
	split, err := Export(ctx, options)
	if err != nil {
		t.Fatalf("split export: %v", err)
	}
	if split.Compression != CompressionSplit || split.ArchivePath != "" || len(split.ArchiveParts) != 3 ||
		split.ArchiveBytes != split.BytesAfter || split.ArchiveSHA256 != split.SHA256 {
		t.Fatalf("split result = %+v", split)
	}
	if _, err := os.Stat(split.DatabasePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("split export retained raw database: %v", err)
	}
	manifest := readManifest(t, split.ManifestPath)
	if manifest.Compression != CompressionSplit || manifest.ArchivePath != "" || len(manifest.ArchiveParts) != 3 ||
		manifest.ArchiveBytes != split.BytesAfter || manifest.ArchiveSHA256 != split.SHA256 ||
		manifest.MaxArchiveBytes == nil || *manifest.MaxArchiveBytes != partBudget {
		t.Fatalf("split manifest = %+v", manifest)
	}
	var whole bytes.Buffer
	for index, part := range manifest.ArchiveParts {
		if part.Path != splitPartName(filepath.Base(split.DatabasePath), index) || part.Bytes > partBudget ||
			split.ArchiveParts[index].Path != filepath.Join(split.OutputDir, part.Path) {
			t.Fatalf("part %d = %+v result=%+v", index, part, split.ArchiveParts[index])
		}
		data, err := os.ReadFile(filepath.Join(split.OutputDir, part.Path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != part.SHA256 || int64(len(data)) != part.Bytes {
			t.Fatalf("part %d digest/size mismatch", index)
		}
		whole.Write(data)
	}
	sum := sha256.Sum256(whole.Bytes())
	if hex.EncodeToString(sum[:]) != split.SHA256 || split.ArtifactID != baseline.ArtifactID {
		t.Fatalf("reassembled split database does not match its manifest")
	}
	if err := validateSplitArchive(split.OutputDir, manifest.ArchiveParts, manifest.ArchiveBytes, manifest.ArchiveSHA256, manifest.OutputBytes, manifest.SHA256, manifest.MaxArchiveBytes); err != nil {
		t.Fatalf("validate committed split archive: %v", err)
	}
	// Tampering, reordering and budget violations are each rejected.
	reordered := append([]ArchivePart(nil), manifest.ArchiveParts...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	small := int64(1)
	for name, check := range map[string]func() error{
		"reordered": func() error {
			return validateSplitArchive(split.OutputDir, reordered, manifest.ArchiveBytes, manifest.ArchiveSHA256, manifest.OutputBytes, manifest.SHA256, nil)
		},
		"budget": func() error {
			return validateSplitArchive(split.OutputDir, manifest.ArchiveParts, manifest.ArchiveBytes, manifest.ArchiveSHA256, manifest.OutputBytes, manifest.SHA256, &small)
		},
		"metadata": func() error {
			return validateSplitArchive(split.OutputDir, manifest.ArchiveParts, manifest.ArchiveBytes-1, manifest.ArchiveSHA256, manifest.OutputBytes, manifest.SHA256, nil)
		},
	} {
		if err := check(); err == nil {
			t.Fatalf("%s split archive accepted", name)
		}
	}
	lastPart := filepath.Join(split.OutputDir, manifest.ArchiveParts[2].Path)
	data, _ := os.ReadFile(lastPart)
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(lastPart, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateSplitArchive(split.OutputDir, manifest.ArchiveParts, manifest.ArchiveBytes, manifest.ArchiveSHA256, manifest.OutputBytes, manifest.SHA256, nil); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("tampered split archive error = %v", err)
	}

	tiny := int64(1024)
	failingDir := filepath.Join(dir, "too-many-parts")
	failing := testExportOptions(sourcePath, failingDir)
	failing.Compression = CompressionSplit
	failing.MaxArchiveBytes = &tiny
	if _, err := Export(ctx, failing); err == nil || !strings.Contains(err.Error(), "parts") {
		t.Fatalf("too many parts error = %v", err)
	}
	if _, err := os.Stat(failingDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed split export left target: %v", err)
	}
	assertNoExportTemps(t, dir)
}

func TestWriteSplitArchiveRefusesInvalidInputsAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.db")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := writeSplitArchive(empty, 16); err == nil {
		t.Fatal("empty database split")
	}
	data := filepath.Join(dir, "data.db")
	if err := os.WriteFile(data, bytes.Repeat([]byte("x"), 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := writeSplitArchive(data, 0); err == nil {
		t.Fatal("zero part size accepted")
	}
	if _, err := writeSplitArchive(filepath.Join(dir, "missing.db"), 16); err == nil {
		t.Fatal("missing database split")
	}
	// A pre-existing part blocks exclusive creation; earlier parts are removed.
	if err := os.WriteFile(filepath.Join(dir, splitPartName("data.db", 2)), []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := writeSplitArchive(data, 40); err == nil {
		t.Fatal("split overwrote an existing part")
	}
	for _, index := range []int{0, 1} {
		if _, err := os.Stat(filepath.Join(dir, splitPartName("data.db", index))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed split left part %d: %v", index, err)
		}
	}
	if _, err := writeSplitArchive(data, 1); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("too many parts error = %v", err)
	}
	if err := validateSplitArchive(dir, []ArchivePart{{Path: "../x", Bytes: 1, SHA256: "a"}}, 1, "s", 1, "s", nil); err == nil {
		t.Fatal("escaping part path accepted")
	}
	if err := validateSplitArchive(dir, []ArchivePart{{Path: "data.db", Bytes: 0}}, 1, "s", 1, "s", nil); err == nil {
		t.Fatal("incomplete part metadata accepted")
	}
	if err := validateSplitArchive(dir, nil, 1, "s", 1, "s", nil); err == nil {
		t.Fatal("empty part list accepted")
	}
}
