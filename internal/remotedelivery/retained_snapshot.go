package remotedelivery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

var ErrSnapshotExport = errors.New("retained snapshot export failed; preserve partial directory for reconciliation")

type SnapshotExportRequest struct {
	Source                                store.RetainedSnapshotRequest
	Directory, Archive, BindingID, SeedID string
	MaxEncodedBytes                       int64
}

type SnapshotExportResult struct {
	Profile        string `json:"profile"`
	NativeThrough  string `json:"native_through"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Rows           int64  `json:"rows"`
	NativeBytes    int64  `json:"native_bytes"`
	EncodedBytes   int64  `json:"encoded_bytes"`
	Chunks         int    `json:"chunks"`
}

// ExportRetainedSnapshot writes exact transport requests, not an outbox. Only
// manifest.json commits a successful export. Failure leaves an exclusive partial
// directory; no automatic reuse, deletion, HTTP or source modification occurs.
func ExportRetainedSnapshot(ctx context.Context, path string, req SnapshotExportRequest) (SnapshotExportResult, error) {
	var result SnapshotExportResult
	if req.MaxEncodedBytes <= 0 || req.MaxEncodedBytes > int64(remote.MaxChangeBytes)*int64(remote.MaxSeedChunks+2) || !filepath.IsAbs(req.Directory) || filepath.Clean(req.Directory) != req.Directory {
		return result, ErrSnapshotExport
	}
	m := remote.GitHubSeedManifest{Contract: remote.GitHubSeedContract, App: "gitcrawl", Archive: req.Archive, BindingID: req.BindingID, SeedID: req.SeedID, Profile: remote.GitHubRetainedSnapshotProfile, NativeSchema: 16}
	// Validate transport identity without inventing an inventory/journal anchor.
	if _, err := remote.EncodeGitHubSeedComplete(remote.GitHubSeedComplete{Contract: m.Contract, App: m.App, Archive: m.Archive, BindingID: m.BindingID, SeedID: m.SeedID, ManifestSHA256: req.Source.SHA256, ExpectedCursor: "seed1:" + m.SeedID + ":begin"}); err != nil {
		return result, ErrSnapshotExport
	}
	if err := os.Mkdir(req.Directory, 0700); err != nil {
		return result, ErrSnapshotExport
	}
	write := func(name string, body []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return writeSnapshotFile(filepath.Join(req.Directory, name), body)
	}
	p := newSeedPacker(ctx, m, req.MaxEncodedBytes, func(index int, body []byte) error { return write(fmt.Sprintf("chunk-%06d.json", index), body) })
	tableIndex := 0
	started := false
	var integers []bool
	begin := func(t store.NativeReportTable) error {
		if tableIndex >= len(privateLayouts) {
			return ErrPrivateProjection
		}
		layout := privateLayouts[tableIndex]
		if t.Name != layout.name || !slices.Equal(t.Columns, strings.Fields(layout.columns)) || !slices.Equal(t.KeyColumns, strings.Fields(layout.keys)) {
			return ErrPrivateProjection
		}
		integers = make([]bool, len(t.Columns))
		for i, column := range t.Columns {
			integers[i] = strings.Contains(" "+layout.integers+" ", " "+column+" ")
		}
		p.begin(layout)
		started = true
		return nil
	}
	capture, err := store.StreamRetainedSnapshot(ctx, path, req.Source, func(t store.NativeReportTable, row []store.NativeReportValue) error {
		if !started {
			if err := begin(t); err != nil {
				return err
			}
		}
		values := make([]PrivateValue, len(row))
		for i, v := range row {
			value, err := privateValue(v, integers[i])
			if err != nil {
				return err
			}
			values[i] = value
		}
		return p.row(values)
	}, func(t store.NativeReportTable) error {
		if !started {
			if err := begin(t); err != nil {
				return err
			}
		}
		if err := p.end(); err != nil {
			return err
		}
		tableIndex++
		started = false
		return nil
	})
	if err != nil {
		return result, err
	}
	if tableIndex != len(privateLayouts) {
		return result, ErrPrivateProjection
	}
	a := capture.Capture.Anchor
	p.manifest.Repository = a.Repository
	p.manifest.RepositoryID = strconv.FormatInt(a.RepositoryID, 10)
	p.manifest.ScopeSHA256 = a.ScopeSHA256
	p.manifest.NativeSHA256 = capture.Capture.SHA256
	p.manifest.NativeThrough, err = remote.EncodeGitHubSnapshotAnchor(remote.GitHubSnapshotAnchor{FileSHA256: capture.FileSHA256, ScopeSHA256: a.ScopeSHA256, InventorySHA256: capture.Capture.SHA256})
	if err != nil {
		return result, err
	}
	seed, err := p.finish()
	if err != nil {
		return result, err
	}
	if err = write("complete.json", seed.Complete); err != nil {
		return result, err
	}
	// Source was rehashed successfully before making the manifest discoverable.
	if err = write("manifest.json", seed.Manifest); err != nil {
		return result, err
	}
	directory, err := os.Open(req.Directory)
	if err != nil {
		return result, err
	}
	err = directory.Sync()
	closeErr := directory.Close()
	if err != nil {
		return result, err
	}
	if closeErr != nil {
		return result, closeErr
	}
	return SnapshotExportResult{Profile: remote.GitHubRetainedSnapshotProfile, NativeThrough: p.manifest.NativeThrough, ManifestSHA256: seed.Plan.ManifestSHA256(), Rows: capture.Capture.Rows, NativeBytes: capture.Capture.Bytes, EncodedBytes: p.used, Chunks: p.index}, nil
}

func writeSnapshotFile(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(body)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
