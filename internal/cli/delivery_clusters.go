package cli

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

// Byte slices intentionally marshal as base64: decoding this envelope restores
// the exact bounded seed requests, without another JSON normalization pass.
type clusterExportOutput = remotedelivery.ClusterBundle

func (a *App) runClusterExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delivery export-clusters", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	db := fs.String("db", "", "explicit existing native archive")
	repo := fs.String("repo", "", "explicit owner/repository")
	archive := fs.String("archive", "", "registered archive ID")
	binding := fs.String("binding", "", "registered binding ID")
	seedID := fs.String("seed-id", "", "explicit 32-character lowercase hex seed identity")
	captured := fs.String("captured-at", "", "fixed nonfuture RFC3339Nano capture timestamp")
	maxRows := fs.Int64("max-rows", 0, "positive native row bound")
	maxNative := fs.Int64("max-native-bytes", 0, "positive native scalar read bound")
	maxEncoded := fs.Int64("max-encoded-bytes", 0, "positive exact request byte bound")
	timeout := fs.Duration("timeout", 0, "positive operation deadline")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("delivery")
		}
		return usageErr(errors.New("invalid export-clusters flags; see gitcrawl help delivery"))
	}
	identity, identityErr := hex.DecodeString(*seedID)
	at, timeErr := time.Parse(time.RFC3339Nano, *captured)
	if fs.NArg() != 0 || *db == "" || *repo == "" || *archive == "" || *binding == "" || *timeout <= 0 || *maxRows <= 0 || *maxNative <= 0 || *maxEncoded <= 0 || *maxEncoded > int64(remote.MaxChangeBytes)*int64(remote.MaxSeedChunks+2) || identityErr != nil || len(identity) != 16 || strings.ToLower(*seedID) != *seedID || timeErr != nil || at.IsZero() || at.After(time.Now()) {
		return usageErr(errors.New("export-clusters requires explicit db, repo, archive, binding, lowercase 32-hex seed-id, nonfuture RFC3339Nano captured-at, and positive row/byte/deadline bounds within wire limits"))
	}
	a.applyCommandJSON(true)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	file, err := store.NativeReportFileIdentity(*db)
	if err != nil {
		return clusterExportError(err)
	}
	capture, err := store.CaptureNativeClusterContext(ctx, *db, store.NativeClusterContextRequest{Repository: *repo, File: file, Limits: store.NativeReportLimits{MaxRows: *maxRows, MaxBytes: *maxNative}})
	if err != nil {
		return clusterExportError(err)
	}
	report, err := remotedelivery.ProjectClusterContext(ctx, capture, at)
	if err != nil {
		return clusterExportError(err)
	}
	seed, err := remotedelivery.PrepareClusterSeed(ctx, report, remotedelivery.PrivateSeedRequest{Archive: *archive, BindingID: *binding, SeedID: *seedID, MaxBytes: *maxEncoded})
	if err != nil {
		return clusterExportError(err)
	}
	return a.writeOutput("delivery", clusterExportOutput{Profile: report.Profile, Manifest: seed.Manifest, Chunks: seed.Chunks, Complete: seed.Complete}, false)
}

func clusterExportError(err error) error {
	for _, known := range []error{context.Canceled, context.DeadlineExceeded, store.ErrNativeClusterContext, store.ErrNativeReportLimit, remotedelivery.ErrClusterProjection, remotedelivery.ErrPrivateSeedLimit} {
		if errors.Is(err, known) {
			return known
		}
	}
	// SQLite, filesystem and codec diagnostics may contain source paths or
	// operator values. Export errors never quote that private evidence.
	return errors.New("cluster export failed; verify the native source, explicit identifiers and bounds")
}
