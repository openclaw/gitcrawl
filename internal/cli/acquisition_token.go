package cli

import (
	"bytes"
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
	bulk := fs.Bool("cohort-stdin", false, "read an approved JSON array of public repository names and IDs from stdin")
	key := fs.String("private-key-file", "", "root-owned private RSA key file")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 0 || *appID <= 0 || *installation <= 0 || *key == "" {
		return usageErr(errors.New("mint-acquisition-token requires App, installation and private-key-file; stdout is a credential"))
	}
	if (*bulk && (*repoID != 0 || *repo != "")) || (!*bulk && (*repoID <= 0 || *repo == "")) {
		return usageErr(errors.New("mint-acquisition-token requires repo and repository-id or cohort-stdin"))
	}
	var cohort []github.AcquisitionRepository
	if *bulk {
		const maxCohortBytes = 128 << 10
		raw, err := io.ReadAll(io.LimitReader(a.Stdin, maxCohortBytes+1))
		if err != nil || len(raw) > maxCohortBytes {
			return usageErr(errors.New("could not read acquisition cohort within its size bound"))
		}
		cohort, err = decodeAcquisitionCohort(raw)
		if err != nil {
			return usageErr(err)
		}
	}
	keyPEM, err := readAcquisitionRootKey(*key)
	if err != nil {
		return err
	}
	defer clear(keyPEM)
	if *bulk {
		// Finish the cohort within the five-minute App JWT lifetime.
		ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
		defer cancel()
		credentials, err := github.MintAcquisitionCohort(ctx, *appID, *installation, cohort, keyPEM)
		if err != nil {
			return err
		}
		return json.NewEncoder(a.Stdout).Encode(credentials)
	}
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

// A permission-bearing roster accepts only exact, unique JSON field names.
// Ordinary struct decoding silently accepts duplicate and case-aliased keys.
func decodeAcquisitionCohort(raw []byte) ([]github.AcquisitionRepository, error) {
	invalid := errors.New("expected a nonempty acquisition cohort JSON array with unique id and name fields")
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if start, err := decoder.Token(); err != nil || start != json.Delim('[') {
		return nil, invalid
	}
	var repositories []github.AcquisitionRepository
	for decoder.More() {
		if start, err := decoder.Token(); err != nil || start != json.Delim('{') {
			return nil, invalid
		}
		var repo github.AcquisitionRepository
		var idSeen, nameSeen bool
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, invalid
			}
			switch key {
			case "id":
				if idSeen || decoder.Decode(&repo.ID) != nil {
					return nil, invalid
				}
				idSeen = true
			case "name":
				if nameSeen || decoder.Decode(&repo.Name) != nil {
					return nil, invalid
				}
				nameSeen = true
			default:
				return nil, invalid
			}
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim('}') || !idSeen || !nameSeen || repo.ID <= 0 || repo.Name == "" {
			return nil, invalid
		}
		repositories = append(repositories, repo)
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim(']') || len(repositories) == 0 {
		return nil, invalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, invalid
	}
	return repositories, nil
}
