package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// AcquisitionRepository is one explicitly admitted repository in a mint cohort.
type AcquisitionRepository struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// MintAcquisitionCohort verifies the public catalog once, then mints a separate
// single-repository credential for each approved identity in input order.
func MintAcquisitionCohort(ctx context.Context, appID, installationID int64, repos []AcquisitionRepository, keyPEM []byte) ([]AcquisitionToken, error) {
	return mintAcquisitionCohort(ctx, appID, installationID, repos, keyPEM, &http.Client{Timeout: 30 * time.Second, CheckRedirect: appNoRedirect})
}

func mintAcquisitionCohort(ctx context.Context, appID, installationID int64, repos []AcquisitionRepository, keyPEM []byte, client *http.Client) ([]AcquisitionToken, error) {
	if appID <= 0 || installationID <= 0 || len(repos) < 1 || len(repos) > 500 {
		return nil, errors.New("mint cohort requires exact App and installation IDs and 1..500 public repository identities")
	}
	owner, _, _ := strings.Cut(repos[0].Name, "/")
	pending := make(map[int64]AcquisitionApp, len(repos))
	names := make(map[string]bool, len(repos))
	apps := make([]AcquisitionApp, 0, len(repos))
	for _, repo := range repos {
		repoOwner, _, _ := strings.Cut(repo.Name, "/")
		name := strings.ToLower(repo.Name)
		_, duplicateID := pending[repo.ID]
		if repo.ID <= 0 || !validAcquisitionRepository(repo.Name) || !strings.EqualFold(repoOwner, owner) || names[name] || duplicateID {
			return nil, errors.New("mint cohort requires unique public repository names and IDs under one owner")
		}
		app := AcquisitionApp{AppID: appID, InstallationID: installationID, RepositoryID: repo.ID, Repository: repo.Name}
		pending[repo.ID], names[name] = app, true
		apps = append(apps, app)
	}
	jwt, err := acquisitionJWT(appID, keyPEM, time.Now())
	if err != nil {
		return nil, err
	}
	if err := verifyAcquisitionInstallation(ctx, client, jwt, owner, apps[0]); err != nil {
		return nil, err
	}
	// Catalog rows qualify only exact allowlisted identities. Every returned
	// installation credential is still checked against its own repository.
	const pageSize, maxPages = 100, 30
	for page := 1; len(pending) > 0; page++ {
		var catalog []appRepository
		path := fmt.Sprintf("/orgs/%s/repos?type=public&sort=full_name&direction=asc&per_page=%d&page=%d", owner, pageSize, page)
		header, err := appJSONResponse(ctx, client, "", http.MethodGet, path, nil, &catalog)
		if err != nil {
			return nil, err
		}
		for _, entry := range catalog {
			if app, ok := pending[entry.ID]; ok && entry.matches(app) {
				delete(pending, entry.ID)
			}
		}
		if len(pending) == 0 {
			break
		}
		next := nextPage(header.Get("Link"), "https://api.github.com")
		if next == "" {
			break
		}
		parsed, err := url.Parse(next)
		if err != nil || parsed.Query().Get("page") != strconv.Itoa(page+1) {
			return nil, errors.New("organization public catalog pagination did not advance")
		}
		limit, limitErr := strconv.Atoi(header.Get("X-RateLimit-Limit"))
		remaining, remainingErr := strconv.Atoi(header.Get("X-RateLimit-Remaining"))
		if page == maxPages || limitErr != nil || remainingErr != nil || limit <= 0 || remaining > limit || remaining <= limit/2+limit%2 {
			return nil, errors.New("organization public catalog pagination would exceed its anonymous request budget")
		}
	}
	if len(pending) > 0 {
		for _, app := range apps {
			if _, ok := pending[app.RepositoryID]; ok {
				return nil, fmt.Errorf("repository %q is not an admitted public identity in the organization catalog", app.Repository)
			}
		}
	}
	out := make([]AcquisitionToken, 0, len(apps))
	for _, app := range apps {
		token, err := mintRepositoryToken(ctx, client, jwt, app)
		if err != nil {
			return nil, fmt.Errorf("mint for %s: %w", app.Repository, err)
		}
		out = append(out, token)
	}
	return out, nil
}
