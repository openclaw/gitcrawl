package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
)

func (a *App) runMintAcquisitionToken(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mint-acquisition-token", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	appID := fs.Int64("app-id", 0, "expected GitHub App ID")
	installation := fs.Int64("installation-id", 0, "expected installation ID")
	repoID := fs.Int64("repository-id", 0, "expected immutable public repository ID")
	repo := fs.String("repo", "", "exact owner/repository")
	key := fs.String("private-key-file", "", "root-owned private RSA key file")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 || *appID <= 0 || *installation <= 0 || *repoID <= 0 || *repo == "" || *key == "" {
		return usageErr(errors.New("mint-acquisition-token requires App, installation, repository and private-key-file; stdout is a credential"))
	}
	keyPEM, err := readAcquisitionRootKey(*key)
	if err != nil {
		return err
	}
	defer clear(keyPEM)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	credential, err := github.MintAcquisitionToken(ctx, github.AcquisitionApp{AppID: *appID, InstallationID: *installation, RepositoryID: *repoID, Repository: *repo}, keyPEM)
	if err != nil {
		return err
	}
	// Deliberately bypass general formatters, status reports and release checks.
	// A rooted supervisor consumes this pipe privately; no credential is logged.
	return json.NewEncoder(a.Stdout).Encode(credential)
}
