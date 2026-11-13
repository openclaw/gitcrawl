package cli

import (
	"context"
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

type deliveryOutput struct {
	Status        string `json:"status"`
	SeedID        string `json:"seed_id,omitempty"`
	BaselineState string `json:"baseline_state,omitempty"`
	NativeThrough string `json:"native_through,omitempty"`
	Parts         int    `json:"parts,omitempty"`
	Receipts      int    `json:"receipts"`
	Rows          int64  `json:"rows,omitempty"`
	Bytes         int64  `json:"bytes,omitempty"`
}

func (a *App) runDelivery(ctx context.Context, args []string) error {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h")) {
		return a.printCommandUsage("delivery")
	}
	command := args[0]
	switch command {
	case "stage-clusters", "cluster-status":
		return a.runClusterStage(ctx, args)
	case "export-clusters":
		return a.runClusterExport(ctx, args[1:])
	case "report-status", "upgrade", "stage-changes", "deliver-changes":
		return a.runReportDelivery(ctx, args)
	}
	if command != "stage-seed" && command != "status" && command != "deliver" {
		return usageErr(errors.New("unknown delivery command; see gitcrawl help delivery"))
	}
	fs := flag.NewFlagSet("delivery", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	outboxPath := fs.String("outbox", "", "explicit private outbox path")
	jsonOut := fs.Bool("json", false, "write JSON output")
	timeout := fs.Duration("timeout", 0, "positive operation deadline")
	var db, repo, origin, archive, binding, tokenEnv string
	var maxRows, maxNativeBytes, maxEncodedBytes int64
	var maxParts int
	if command != "status" {
		fs.StringVar(&db, "db", "", "explicit existing native archive")
		fs.Int64Var(&maxRows, "max-rows", 0, "positive total native row bound")
		fs.Int64Var(&maxEncodedBytes, "max-encoded-bytes", 0, "positive total wire byte bound")
	}
	if command == "stage-seed" {
		fs.StringVar(&repo, "repo", "", "explicit owner/repository")
		fs.StringVar(&origin, "origin", "", "registered service origin")
		fs.StringVar(&archive, "archive", "", "registered archive ID")
		fs.StringVar(&binding, "binding", "", "registered binding ID")
		fs.Int64Var(&maxNativeBytes, "max-native-bytes", 0, "positive native scalar byte bound")
	}
	if command == "deliver" {
		fs.IntVar(&maxParts, "max-parts", 0, "positive request bound")
		fs.StringVar(&tokenEnv, "token-env", "", "explicit service-token environment variable name")
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("delivery")
		}
		// flag errors can quote operator-supplied paths or credential values.
		return usageErr(errors.New("invalid delivery flags; see gitcrawl help delivery"))
	}
	if fs.NArg() != 0 || *outboxPath == "" || *timeout <= 0 {
		return usageErr(errors.New("delivery requires explicit --outbox and positive --timeout, with no positional arguments"))
	}
	if command != "status" && (db == "" || maxRows <= 0 || maxEncodedBytes <= 0 || maxEncodedBytes > int64(remote.MaxChangeBytes)*int64(remote.MaxSeedChunks+2)) {
		return usageErr(errors.New("delivery requires explicit --db and positive --max-rows and --max-encoded-bytes within the wire maximum"))
	}
	if command == "stage-seed" && (repo == "" || origin == "" || archive == "" || binding == "" || maxNativeBytes <= 0) {
		return usageErr(errors.New("stage-seed requires --repo, --origin, --archive, --binding, and positive --max-native-bytes"))
	}
	if command == "deliver" && (maxParts <= 0 || maxParts > remote.MaxSeedChunks+2 || strings.TrimSpace(tokenEnv) == "" || strings.ContainsAny(tokenEnv, "=\x00")) {
		return usageErr(errors.New("deliver requires positive --max-parts within the wire maximum and an explicit --token-env name"))
	}
	a.applyCommandJSON(*jsonOut)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if command == "status" {
		state, err := remotedelivery.ReadSeedOutboxState(ctx, *outboxPath)
		if err != nil {
			return deliveryCommandError(err)
		}
		return a.writeOutput("delivery", deliveryStateOutput("status", state), false)
	}
	var outbox *remotedelivery.Outbox
	var err error
	if command == "stage-seed" {
		file, sourceErr := store.NativeReportFileIdentity(db)
		if sourceErr != nil {
			return deliveryCommandError(sourceErr)
		}
		capture, sourceErr := store.CaptureNativeReport(ctx, db, store.NativeReportRequest{Repository: repo, File: file,
			Limits: store.NativeReportLimits{MaxRows: maxRows, MaxBytes: maxNativeBytes}})
		if sourceErr != nil {
			return deliveryCommandError(sourceErr)
		}
		outbox, err = remotedelivery.CreateSeedOutbox(ctx, *outboxPath, remotedelivery.SeedRegistration{
			Origin: origin, Archive: archive, BindingID: binding, File: file, Repository: repo,
		}, capture, remotedelivery.StageLimits{MaxEncodedBytes: maxEncodedBytes})
	} else {
		outbox, err = remotedelivery.OpenSeedOutbox(ctx, *outboxPath)
	}
	if err != nil {
		return deliveryCommandError(err)
	}
	defer outbox.Close()
	status := "staged"
	if command == "deliver" {
		result, err := outbox.DeliverSeed(ctx, db, remote.EnvTokenProvider{Name: tokenEnv}, nil,
			remotedelivery.DeliveryLimits{MaxParts: maxParts, MaxRows: maxRows, MaxBytes: maxEncodedBytes})
		status = "part_limit"
		if result.Complete {
			status = "complete"
		}
		if err != nil {
			// A lost reply may follow a remote effect. Only this call's durable
			// settlements are known; never attach a pre-call baseline to failure.
			if writeErr := a.writeOutput("delivery", deliveryOutput{Status: "partial", Receipts: result.Receipts}, false); writeErr != nil {
				return writeErr
			}
			return deliveryCommandError(err)
		}
	}
	state, err := outbox.State(ctx)
	if err != nil {
		return deliveryCommandError(err)
	}
	if err := outbox.Close(); err != nil {
		return deliveryCommandError(err)
	}
	return a.writeOutput("delivery", deliveryStateOutput(status, state), false)
}

func deliveryStateOutput(status string, state remotedelivery.SeedState) deliveryOutput {
	return deliveryOutput{Status: status, SeedID: state.SeedID, BaselineState: state.BaselineState,
		NativeThrough: state.NativeThrough, Parts: state.PartCount, Receipts: state.Receipts,
		Rows: state.NativeRows, Bytes: state.NativeBytes}
}

func deliveryCommandError(err error) error {
	for _, known := range []error{context.Canceled, context.DeadlineExceeded, store.ErrNativeReportSource,
		store.ErrNativeReportLimit, store.ErrNativeReportBlob, remotedelivery.ErrPrivateProjection,
		remotedelivery.ErrPrivateSeed, remotedelivery.ErrPrivateSeedLimit, remotedelivery.ErrSeedOutbox, remote.ErrMissingToken} {
		if errors.Is(err, known) {
			return known
		}
	}
	return errors.New("private delivery failed; retained source and outbox require reconciliation")
}
