package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	portableexport "github.com/openclaw/gitcrawl/internal/portable"
)

type portableDBManifest struct {
	Schema            string `json:"schema,omitempty"`
	Profile           string `json:"profile,omitempty"`
	ExportedAt        string `json:"exportedAt,omitempty"`
	OutputPath        string `json:"outputPath,omitempty"`
	OutputBytes       int64  `json:"outputBytes,omitempty"`
	SHA256            string `json:"sha256,omitempty"`
	ArtifactID        string `json:"artifactId,omitempty"`
	ArtifactIDProfile string `json:"artifactIdProfile,omitempty"`
	QuickCheck        string `json:"quickCheck,omitempty"`
	Compression       string `json:"compression,omitempty"`
	ArchivePath       string `json:"archivePath,omitempty"`
	ArchiveBytes      int64  `json:"archiveBytes,omitempty"`
	ArchiveSHA256     string `json:"archiveSha256,omitempty"`
}

func portableDBManifestPath(dbPath string) string {
	return dbPath + ".manifest.json"
}

func validatePortableSQLiteFile(ctx context.Context, dbPath, manifestDBPath string) error {
	if err := sqliteStoreHealth(ctx, dbPath); err != nil {
		return err
	}
	return validatePortableDBManifest(ctx, dbPath, portableDBManifestPath(manifestDBPath))
}

func validatePortableSQLiteSourceFile(ctx context.Context, dbPath, manifestDBPath string) error {
	_, _, compressed, err := portableSourceArtifact(dbPath)
	if err != nil {
		return err
	}
	if !compressed {
		if err := sqliteStoreImmutableHealth(ctx, dbPath); err != nil {
			return err
		}
		return validatePortableDBManifest(ctx, dbPath, portableDBManifestPath(manifestDBPath))
	}
	tempDir, err := os.MkdirTemp("", "gitcrawl-portable-source-*")
	if err != nil {
		return fmt.Errorf("create portable source validation dir: %w", err)
	}
	defer os.RemoveAll(tempDir)
	tempPath, err := stagePortableSQLiteSourceTempContext(ctx, dbPath, filepath.Join(tempDir, filepath.Base(dbPath)), 0o600)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(tempPath)
		removeSQLiteTempSidecars(tempPath)
	}()
	if err := sqliteStoreImmutableHealth(ctx, tempPath); err != nil {
		return err
	}
	return validatePortableDBManifest(ctx, tempPath, portableDBManifestPath(manifestDBPath))
}

func validatePortableDBManifest(ctx context.Context, dbPath, manifestPath string) error {
	manifest, ok, err := readPortableDBManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("portable manifest mismatch: %w", err)
	}
	if !ok {
		return nil
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(manifest.Schema) == "" {
		return fmt.Errorf("portable manifest mismatch: schema missing")
	}
	if manifest.OutputBytes <= 0 {
		return fmt.Errorf("portable manifest mismatch: outputBytes missing")
	}
	if strings.TrimSpace(manifest.SHA256) == "" {
		return fmt.Errorf("portable manifest mismatch: sha256 missing")
	}
	if strings.TrimSpace(manifest.QuickCheck) != "" && strings.TrimSpace(manifest.QuickCheck) != "ok" {
		return fmt.Errorf("portable manifest mismatch: quickCheck %q", manifest.QuickCheck)
	}
	if manifest.OutputBytes > 0 && info.Size() != manifest.OutputBytes {
		return fmt.Errorf("portable manifest mismatch: size %d != %d", info.Size(), manifest.OutputBytes)
	}
	sum, err := portableFileSHA256(ctx, dbPath)
	if err != nil {
		return err
	}
	sumText := fmt.Sprintf("%x", sum)
	if !strings.EqualFold(sumText, strings.TrimSpace(manifest.SHA256)) {
		return fmt.Errorf("portable manifest mismatch: sha256 %s != %s", sumText, manifest.SHA256)
	}
	artifactID := strings.TrimSpace(manifest.ArtifactID)
	artifactIDProfile := strings.TrimSpace(manifest.ArtifactIDProfile)
	if artifactIDProfile == "" {
		// Derived manifests published before semantic identity used artifactId as
		// an exact-SHA alias. Keep that additive manifest evolution readable.
		if artifactID != "" && !strings.EqualFold(artifactID, strings.TrimSpace(manifest.SHA256)) {
			return fmt.Errorf("portable manifest mismatch: legacy artifactId %s != sha256 %s", manifest.ArtifactID, manifest.SHA256)
		}
	} else {
		if manifest.Profile != portableexport.CurrentStateV1 {
			return fmt.Errorf("portable manifest mismatch: profile %q does not support semantic artifact identity", manifest.Profile)
		}
		if artifactIDProfile != portableexport.CurrentStateSemanticV1 {
			return fmt.Errorf("portable manifest mismatch: unsupported artifactIdProfile %q", manifest.ArtifactIDProfile)
		}
		tempParent, _ := ctx.Value(portableValidationDirKey{}).(string)
		computedArtifactID, err := portableexport.ComputeArtifactIDInDirectory(ctx, dbPath, artifactIDProfile, tempParent)
		if err != nil {
			return fmt.Errorf("portable manifest mismatch: recompute artifactId: %w", err)
		}
		if artifactID == "" || !strings.EqualFold(computedArtifactID, artifactID) {
			return fmt.Errorf("portable manifest mismatch: artifactId %s != %s", computedArtifactID, manifest.ArtifactID)
		}
	}
	return nil
}

func readPortableDBManifest(path string) (portableDBManifest, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return portableDBManifest{}, false, nil
		}
		return portableDBManifest{}, false, err
	}
	var manifest portableDBManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return portableDBManifest{}, true, fmt.Errorf("read portable manifest: %w", err)
	}
	return manifest, true, nil
}

func portableSourceArtifact(dbPath string) (string, portableDBManifest, bool, error) {
	manifestPath := portableDBManifestPath(dbPath)
	manifest, ok, err := readPortableDBManifest(manifestPath)
	if err != nil {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: %w", err)
	}
	if !ok || strings.TrimSpace(manifest.Compression) == "" {
		return dbPath, manifest, false, nil
	}
	if strings.TrimSpace(manifest.Compression) != "gzip" {
		return "", portableDBManifest{}, false, fmt.Errorf(
			"portable manifest mismatch: unsupported compression %q",
			manifest.Compression,
		)
	}
	archiveRaw := strings.TrimSpace(manifest.ArchivePath)
	if archiveRaw == "" ||
		strings.HasPrefix(archiveRaw, "/") ||
		strings.HasPrefix(archiveRaw, "\\") ||
		(len(archiveRaw) >= 2 && archiveRaw[1] == ':') {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath must be relative")
	}
	for _, component := range strings.FieldsFunc(archiveRaw, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if component == ".." {
			return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath escapes the store")
		}
	}
	archivePath := filepath.FromSlash(archiveRaw)
	archivePath = filepath.Clean(archivePath)
	if archivePath == "." || archivePath == ".." || strings.HasPrefix(archivePath, ".."+string(os.PathSeparator)) {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath escapes the store")
	}
	manifestDir := filepath.Dir(manifestPath)
	resolved := filepath.Join(manifestDir, archivePath)
	relative, err := filepath.Rel(manifestDir, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath escapes the store")
	}
	if manifest.ArchiveBytes <= 0 {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archiveBytes missing")
	}
	if strings.TrimSpace(manifest.ArchiveSHA256) == "" {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archiveSha256 missing")
	}
	return resolved, manifest, true, nil
}

func validatePortableArchive(path string, manifest portableDBManifest) error {
	return validatePortableArchiveContext(context.Background(), path, manifest)
}

func validatePortableArchiveContext(ctx context.Context, path string, manifest portableDBManifest) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() != manifest.ArchiveBytes {
		return fmt.Errorf(
			"portable manifest mismatch: archive size %d != %d",
			info.Size(),
			manifest.ArchiveBytes,
		)
	}
	sum, err := portableFileSHA256(ctx, path)
	if err != nil {
		return err
	}
	sumText := fmt.Sprintf("%x", sum)
	if !strings.EqualFold(sumText, strings.TrimSpace(manifest.ArchiveSHA256)) {
		return fmt.Errorf(
			"portable manifest mismatch: archive sha256 %s != %s",
			sumText,
			manifest.ArchiveSHA256,
		)
	}
	return nil
}
