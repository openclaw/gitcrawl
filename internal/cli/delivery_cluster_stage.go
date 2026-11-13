package cli

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

type clusterStageOutput struct {
	Status          string `json:"status"`
	Profile         string `json:"profile"`
	SeedID          string `json:"seed_id"`
	NativeThrough   string `json:"native_through"`
	ManifestSHA256  string `json:"manifest_sha256"`
	InventorySHA256 string `json:"inventory_sha256"`
	Parts           int    `json:"parts"`
	Rows            int64  `json:"rows"`
	Bytes           int64  `json:"bytes"`
}

func (a *App) runClusterStage(ctx context.Context, args []string) error {
	status := args[0] == "cluster-status"
	fs := flag.NewFlagSet("delivery "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	outbox := fs.String("outbox", "", "explicit cluster outbox path")
	timeout := fs.Duration("timeout", 0, "positive operation deadline")
	jsonOut := fs.Bool("json", false, "write JSON output")
	var source, bundlePath, repo, origin, archive, binding string
	var maxRows, maxBytes int64
	if !status {
		fs.StringVar(&source, "db", "", "explicit existing native archive")
		fs.StringVar(&bundlePath, "bundle", "", "retained exact export envelope")
		fs.StringVar(&repo, "repo", "", "expected owner/repository")
		fs.StringVar(&origin, "origin", "", "registered service origin")
		fs.StringVar(&archive, "archive", "", "registered archive ID")
		fs.StringVar(&binding, "binding", "", "registered binding ID")
		fs.Int64Var(&maxRows, "max-rows", 0, "positive bundle row bound")
		fs.Int64Var(&maxBytes, "max-encoded-bytes", 0, "positive total decoded request byte bound")
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("delivery")
		}
		return usageErr(errors.New("invalid cluster staging flags; see gitcrawl help delivery"))
	}
	if fs.NArg() != 0 || *outbox == "" || *timeout <= 0 {
		return usageErr(errors.New("cluster staging requires explicit --outbox and positive --timeout"))
	}
	if !status && (source == "" || bundlePath == "" || repo == "" || origin == "" || archive == "" || binding == "" || maxRows <= 0 || maxBytes <= 0 || maxBytes > int64(remote.MaxChangeBytes)*int64(remote.MaxSeedChunks+2)) {
		return usageErr(errors.New("stage-clusters requires explicit db, bundle, repo, origin, archive, binding and positive row/byte bounds within wire limits"))
	}
	a.applyCommandJSON(*jsonOut)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var state remotedelivery.ClusterState
	var err error
	if status {
		state, err = remotedelivery.ReadClusterOutboxState(ctx, *outbox)
	} else {
		state, err = stageClusterFile(ctx, *outbox, source, bundlePath, remotedelivery.SeedRegistration{Origin: origin, Archive: archive, BindingID: binding, Repository: repo}, remotedelivery.ClusterStageLimits{MaxRows: maxRows, MaxEncodedBytes: maxBytes})
	}
	if err != nil {
		return clusterStageError(err)
	}
	return a.writeOutput("delivery", clusterStageOutput{Status: "staged", Profile: store.NativeClusterContextProfile, SeedID: state.SeedID, NativeThrough: state.NativeThrough, ManifestSHA256: state.ManifestSHA256, InventorySHA256: state.InventorySHA256, Parts: state.PartCount, Rows: state.Rows, Bytes: state.Bytes}, false)
}

func stageClusterFile(ctx context.Context, outbox, source, path string, r remotedelivery.SeedRegistration, limits remotedelivery.ClusterStageLimits) (remotedelivery.ClusterState, error) {
	var empty remotedelivery.ClusterState
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	f, err := os.Open(path)
	if err != nil {
		return empty, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return empty, remotedelivery.ErrClusterOutbox
	}
	bundle, err := remotedelivery.ReadClusterBundle(ctx, f, limits)
	if err != nil {
		return empty, err
	}
	r.File, err = store.NativeReportFileIdentity(source)
	if err != nil {
		return empty, err
	}
	return remotedelivery.StageClusterBundle(ctx, outbox, source, r, bundle, limits)
}

func clusterStageError(err error) error {
	for _, known := range []error{context.Canceled, context.DeadlineExceeded, remotedelivery.ErrClusterOutbox, remotedelivery.ErrSeedOutbox} {
		if errors.Is(err, known) {
			return known
		}
	}
	return errors.New("cluster staging failed; retain the bundle and any owned outbox for reconciliation")
}
