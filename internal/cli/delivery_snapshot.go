package cli

import (
	"context"
	"errors"
	"flag"
	"io"
	"math"

	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runRetainedSnapshot(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delivery "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	db := fs.String("db", "", "explicit stopped native archive")
	repo := fs.String("repo", "", "explicit owner/repository")
	sha := fs.String("sha256", "", "expected whole-file SHA-256")
	timeout := fs.Duration("timeout", 0, "positive operation deadline")
	jsonOut := fs.Bool("json", false, "write JSON output")
	var req remotedelivery.SnapshotExportRequest
	if args[0] == "export-snapshot" {
		fs.StringVar(&req.Directory, "directory", "", "new exclusive output directory")
		fs.StringVar(&req.Archive, "archive", "", "destination archive identity")
		fs.StringVar(&req.BindingID, "binding", "", "destination binding identity")
		fs.StringVar(&req.SeedID, "seed", "", "explicit seed identity")
		fs.Int64Var(&req.Source.Limits.MaxRows, "max-rows", 0, "positive total row bound")
		fs.Int64Var(&req.Source.Limits.MaxBytes, "max-native-bytes", 0, "positive native scalar byte bound")
		fs.Int64Var(&req.MaxEncodedBytes, "max-encoded-bytes", 0, "positive transport byte bound")
	} else {
		req.Source.Limits = store.NativeReportLimits{MaxRows: math.MaxInt64, MaxBytes: math.MaxInt64}
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("delivery")
		}
		return usageErr(errors.New("invalid retained snapshot flags; see gitcrawl help delivery"))
	}
	if fs.NArg() != 0 || *db == "" || *repo == "" || *sha == "" || *timeout <= 0 {
		return usageErr(errors.New("retained snapshot requires --db, --repo, --sha256 and positive --timeout"))
	}
	if args[0] == "export-snapshot" && (req.Directory == "" || req.Archive == "" || req.BindingID == "" || req.SeedID == "" || req.Source.Limits.MaxRows <= 0 || req.Source.Limits.MaxBytes <= 0 || req.MaxEncodedBytes <= 0) {
		return usageErr(errors.New("export-snapshot requires --directory, --archive, --binding, --seed and positive --max-rows, --max-native-bytes, --max-encoded-bytes"))
	}
	a.applyCommandJSON(*jsonOut)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	file, err := store.NativeReportFileIdentity(*db)
	if err != nil {
		return deliveryCommandError(err)
	}
	req.Source.File = file
	req.Source.Repository = *repo
	req.Source.SHA256 = *sha
	if args[0] == "measure-snapshot" {
		result, err := store.MeasureRetainedSnapshot(ctx, *db, req.Source)
		if err != nil {
			return deliveryCommandError(err)
		}
		return a.writeOutput("delivery", result, false)
	}
	result, err := remotedelivery.ExportRetainedSnapshot(ctx, *db, req)
	if err != nil {
		return deliveryCommandError(err)
	}
	return a.writeOutput("delivery", result, false)
}
