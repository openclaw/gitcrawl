package cli

import (
	"context"
	"errors"
	"flag"
	"io"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

type reportDeliveryOutput struct {
	Status           string `json:"status"`
	Phase            string `json:"phase,omitempty"`
	ToVersion        int    `json:"to_version,omitempty"`
	CaptureID        string `json:"capture_id,omitempty"`
	ActiveCaptureID  string `json:"active_capture_id,omitempty"`
	NativeThrough    string `json:"native_through,omitempty"`
	InventorySHA256  string `json:"inventory_sha256,omitempty"`
	InventoryLayout  string `json:"inventory_layout,omitempty"`
	ServerCursor     string `json:"server_cursor,omitempty"`
	ReceiverSequence string `json:"receiver_sequence,omitempty"`
	Receipts         *int   `json:"receipts,omitempty"`
	Rows             int64  `json:"rows,omitempty"`
	NativeBytes      int64  `json:"native_bytes,omitempty"`
}

func (a *App) runReportDelivery(ctx context.Context, args []string) error {
	command := args[0]
	staging := command == "stage-changes" || command == "cycle"
	sending := command == "deliver-changes" || command == "cycle"
	fs := flag.NewFlagSet("delivery "+command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	outboxPath := fs.String("outbox", "", "explicit private outbox path")
	timeout := fs.Duration("timeout", 0, "positive operation deadline")
	jsonOut := fs.Bool("json", false, "write JSON output")
	var db, tokenEnv string
	var toVersion, maxParts int
	var maxRows, maxEncodedBytes, maxNativeBytes, maxJournal, maxKeys, maxLookups int64
	if command == "upgrade" {
		fs.IntVar(&toVersion, "to-version", 0, "explicit next outbox schema version")
	}
	if command != "report-status" {
		fs.Int64Var(&maxRows, "max-rows", 0, "positive captured row bound")
		fs.Int64Var(&maxEncodedBytes, "max-encoded-bytes", 0, "positive saved/request byte bound")
	}
	if staging || sending {
		fs.StringVar(&db, "db", "", "explicit existing native archive")
		fs.IntVar(&maxParts, "max-parts", 0, "positive change-part bound")
	}
	if staging {
		fs.Int64Var(&maxJournal, "max-journal-records", 0, "positive journal record bound")
		fs.Int64Var(&maxKeys, "max-affected-keys", 0, "positive affected-key bound")
		fs.Int64Var(&maxLookups, "max-lookup-rows", 0, "positive dependency lookup bound")
		fs.Int64Var(&maxNativeBytes, "max-native-bytes", 0, "positive native capture work byte bound")
	}
	if sending {
		fs.StringVar(&tokenEnv, "token-env", "", "explicit service-token environment variable name")
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("delivery")
		}
		return usageErr(errors.New("invalid delivery flags; see gitcrawl help delivery"))
	}
	if fs.NArg() != 0 || *outboxPath == "" || *timeout <= 0 {
		return usageErr(errors.New("delivery requires explicit --outbox and positive --timeout, with no positional arguments"))
	}
	if command == "upgrade" {
		if toVersion != 2 && toVersion != 3 {
			return usageErr(errors.New("upgrade requires explicit --to-version 2 or 3"))
		}
		if toVersion == 2 && (maxRows <= 0 || maxEncodedBytes <= 0) {
			return usageErr(errors.New("upgrade to version 2 requires positive --max-rows and --max-encoded-bytes"))
		}
		if toVersion == 3 && (maxRows != 0 || maxEncodedBytes != 0) {
			return usageErr(errors.New("upgrade to version 3 is schema-only; omit row and byte bounds"))
		}
	}
	if staging || sending {
		if db == "" || maxRows <= 0 || maxEncodedBytes <= 0 || maxEncodedBytes > int64(remote.MaxChangeBytes)*remote.MaxChangeParts || maxParts <= 0 || maxParts > remote.MaxChangeParts {
			return usageErr(errors.New("changes require --db, positive --max-rows and --max-encoded-bytes within the wire maximum, and --max-parts between 1 and 64"))
		}
	}
	if staging && (maxJournal <= 0 || maxKeys <= 0 || maxLookups <= 0 || maxNativeBytes <= 0) {
		return usageErr(errors.New("stage-changes requires positive journal, affected-key, lookup-row, and native-byte bounds"))
	}
	if sending && (strings.TrimSpace(tokenEnv) == "" || strings.ContainsAny(tokenEnv, "=\x00")) {
		return usageErr(errors.New("deliver-changes requires an explicit --token-env name"))
	}
	a.applyCommandJSON(*jsonOut)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if command == "report-status" {
		s, err := remotedelivery.ReadReportOutboxState(ctx, *outboxPath)
		if err != nil {
			return reportDeliveryCommandError(err)
		}
		return a.writeOutput("delivery", reportDeliveryOutput{Status: "status", ActiveCaptureID: s.ActiveChangeID, NativeThrough: s.BaselineNative, InventorySHA256: s.InventorySHA256, InventoryLayout: s.InventoryLayout, ServerCursor: s.ServerCursor, ReceiverSequence: strconv.FormatInt(s.ReceiverSequence, 10)}, false)
	}
	if command == "upgrade" {
		var err error
		if toVersion == 2 {
			err = remotedelivery.MigrateSeedOutboxV2(ctx, *outboxPath, remotedelivery.InventoryMigrationLimits{MaxRows: maxRows, MaxBytes: maxEncodedBytes})
		} else {
			err = remotedelivery.MigrateReportOutboxV3(ctx, *outboxPath)
		}
		if err != nil {
			return reportDeliveryCommandError(err)
		}
		return a.writeOutput("delivery", reportDeliveryOutput{Status: "upgraded", ToVersion: toVersion}, false)
	}
	request := reportDeliveryRequest{
		outbox: *outboxPath, db: db, tokenEnv: tokenEnv,
		capture:  store.NativeReportDeltaLimits{MaxJournalRecords: maxJournal, MaxAffectedKeys: maxKeys, MaxLookupRows: maxLookups, MaxRows: maxRows, MaxNativeBytes: maxNativeBytes},
		stage:    remotedelivery.ChangeStageLimits{MaxRows: maxRows, MaxBytes: maxEncodedBytes, MaxParts: maxParts},
		delivery: remotedelivery.DeliveryLimits{MaxParts: maxParts, MaxRows: maxRows, MaxBytes: maxEncodedBytes},
	}
	if command != "cycle" {
		output, err := executeReportDelivery(ctx, command, request, false)
		if output.Status != "" {
			if writeErr := a.writeOutput("delivery", output, false); writeErr != nil {
				return writeErr
			}
		}
		if err != nil {
			return reportDeliveryCommandError(err)
		}
		return nil
	}
	for _, phase := range []string{"resume", "capture", "deliver"} {
		operation := "deliver-changes"
		if phase == "capture" {
			operation = "stage-changes"
		}
		var output reportDeliveryOutput
		err := ctx.Err()
		if err == nil {
			output, err = executeReportDelivery(ctx, operation, request, true)
		}
		if output.Status == "" {
			output.Status = "failed"
		}
		output.Phase = phase
		if writeErr := a.writeOutput("delivery", output, false); writeErr != nil {
			return writeErr
		}
		if err != nil {
			return reportDeliveryCommandError(err)
		}
		// A bounded drain may leave saved parts. Never capture over them.
		if output.Status == "part_limit" {
			return nil
		}
	}
	return nil
}

type reportDeliveryRequest struct {
	outbox, db, tokenEnv string
	capture              store.NativeReportDeltaLimits
	stage                remotedelivery.ChangeStageLimits
	delivery             remotedelivery.DeliveryLimits
}

func executeReportDelivery(ctx context.Context, command string, request reportDeliveryRequest, skipStableIdle bool) (reportDeliveryOutput, error) {
	outbox, err := remotedelivery.OpenReportOutbox(ctx, request.outbox)
	if err != nil {
		return reportDeliveryOutput{}, err
	}
	defer outbox.Close()
	if command == "stage-changes" {
		// This read-only owner call admits schema3 before any native capture.
		pending, err := outbox.NextChangePart(ctx)
		if err != nil {
			return reportDeliveryOutput{}, err
		}
		if pending != nil {
			return reportDeliveryOutput{}, remotedelivery.ErrSeedOutbox
		}
		var expected remotedelivery.ReportOutboxState
		var delta store.NativeReportDelta
		err = outbox.WithQualifiedReportIndex(ctx, func(state remotedelivery.ReportOutboxState, index store.NativeReportOldIndex) error {
			if state.ActiveChangeID != "" {
				return remotedelivery.ErrSeedOutbox
			}
			expected = state
			var err error
			delta, err = store.CaptureNativeReportChanges(ctx, request.db, store.NativeReportDeltaRequest{Baseline: state.BaselineAnchor, Limits: request.capture}, index)
			return err
		})
		if err != nil {
			return reportDeliveryOutput{}, err
		}
		if skipStableIdle && delta.Rows == 0 && delta.From == delta.Through {
			anchor, err := remote.ParseGitHubNativeAnchor(expected.BaselineNative)
			if err != nil {
				return reportDeliveryOutput{}, err
			}
			// The first empty capture still upgrades the v1 wire anchor to v2.
			if anchor.Version == remote.GitHubNativeV2 {
				return reportDeliveryOutput{Status: "idle"}, outbox.Close()
			}
		}
		// Both native and outbox read transactions have closed. Staging compares
		// their exact predecessor again under the outbox's writer transaction.
		id, err := outbox.StageReportChanges(ctx, expected, delta, request.stage)
		if err != nil {
			return reportDeliveryOutput{}, err
		}
		if err := outbox.Close(); err != nil {
			return reportDeliveryOutput{}, err
		}
		return reportDeliveryOutput{Status: "staged", CaptureID: id, Rows: delta.Rows, NativeBytes: delta.Bytes}, nil
	}
	// The token is resolved only inside the sender's actual request path.
	result, sendErr := outbox.DeliverReportChanges(ctx, request.db, remote.EnvTokenProvider{Name: request.tokenEnv}, nil, request.delivery)
	if closeErr := outbox.Close(); sendErr == nil {
		sendErr = closeErr
	}
	status := "part_limit"
	if result.CaptureID == "" {
		status = "idle"
	} else if result.Complete {
		status = "complete"
	}
	if sendErr != nil {
		status = "partial"
	}
	// Use only this invocation's pinned capture and durable settlement count.
	return reportDeliveryOutput{Status: status, CaptureID: result.CaptureID, Receipts: &result.Receipts}, sendErr
}

func reportDeliveryCommandError(err error) error {
	for _, known := range []error{remotedelivery.ErrPrivateChanges, remotedelivery.ErrPrivateChangesLimit, remotedelivery.ErrReportDelivery, store.ErrThreadExcluded} {
		if errors.Is(err, known) {
			return known
		}
	}
	return deliveryCommandError(err)
}
