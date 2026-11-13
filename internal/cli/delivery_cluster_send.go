package cli

import (
	"context"
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
)

func (a *App) runClusterDeliver(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delivery deliver-clusters", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	source := fs.String("db", "", "explicit native source pinned at staging")
	path := fs.String("outbox", "", "existing cluster outbox")
	token := fs.String("token-env", "", "explicit service-token environment variable name")
	timeout := fs.Duration("timeout", 0, "positive operation deadline")
	parts := fs.Int("max-parts", 0, "positive request bound")
	rows := fs.Int64("max-rows", 0, "positive total snapshot row bound")
	size := fs.Int64("max-encoded-bytes", 0, "positive total saved request byte bound")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("delivery")
		}
		return usageErr(errors.New("invalid cluster delivery flags; see gitcrawl help delivery"))
	}
	if fs.NArg() != 0 || *source == "" || *path == "" || *timeout <= 0 || *parts <= 0 || *parts > remote.MaxSeedChunks+2 || *rows <= 0 || *size <= 0 || *size > int64(remote.MaxChangeBytes)*int64(remote.MaxSeedChunks+2) || strings.TrimSpace(*token) == "" || strings.ContainsAny(*token, "=\x00") {
		return usageErr(errors.New("deliver-clusters requires explicit db, outbox, token-env and positive part/row/byte/deadline bounds within wire limits"))
	}
	a.applyCommandJSON(*jsonOut)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	outbox, err := remotedelivery.OpenClusterOutbox(ctx, *path)
	if err != nil {
		return clusterStageError(err)
	}
	defer outbox.Close()
	result, err := outbox.DeliverClusterSeed(ctx, *source, remote.EnvTokenProvider{Name: *token}, nil, remotedelivery.DeliveryLimits{MaxParts: *parts, MaxRows: *rows, MaxBytes: *size})
	if err != nil {
		// An uncertain reply can follow a remote effect. Report only this call's
		// durable settlements; never attach a stale snapshot or completion claim.
		if writeErr := a.writeOutput("delivery", deliveryOutput{Status: "partial", Receipts: result.Receipts}, false); writeErr != nil {
			return writeErr
		}
		return clusterStageError(err)
	}
	state, err := outbox.ClusterState(ctx)
	if err != nil {
		return clusterStageError(err)
	}
	if err = outbox.Close(); err != nil {
		return clusterStageError(err)
	}
	return a.writeOutput("delivery", clusterStateOutput(state), false)
}
