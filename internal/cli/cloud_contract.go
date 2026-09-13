package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	crawlremote "github.com/openclaw/crawlkit/remote"
)

func remoteNotFound(err error) bool {
	var remoteErr *crawlremote.Error
	return errors.As(err, &remoteErr) && remoteErr.Status == http.StatusNotFound
}

func remoteSnapshotIncomplete(err error) bool {
	var remoteErr *crawlremote.Error
	return errors.As(err, &remoteErr) &&
		remoteErr.Status == http.StatusConflict &&
		remoteErr.Code == "snapshot_mismatch"
}

func recoverConcurrentGitcrawlSnapshot(
	ctx context.Context,
	client *crawlremote.Client,
	archive string,
	snapshot gitcrawlCloudSnapshot,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
	cause error,
) (string, error) {
	var remoteErr *crawlremote.Error
	if !errors.As(cause, &remoteErr) ||
		remoteErr.Status != http.StatusConflict ||
		remoteErr.Code != "snapshot_active" {
		return "", nil
	}
	status, err := client.PublishStatusForSnapshot(
		ctx,
		"gitcrawl",
		archive,
		snapshot.ID,
	)
	if err != nil {
		return "", fmt.Errorf("re-probe concurrent snapshot completion: %w", err)
	}
	if !gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) {
		return "", fmt.Errorf(
			"concurrent active snapshot %s does not match the requested digest, profile, and coverage",
			snapshot.ID,
		)
	}
	return status.Snapshot.DatasetGeneratedAt, nil
}

func requireGitcrawlSnapshotPublishContract(
	ctx context.Context,
	client *crawlremote.Client,
	snapshot gitcrawlCloudSnapshot,
	cutover bool,
) error {
	contract, err := client.Contract(ctx)
	if err != nil {
		return fmt.Errorf("read remote snapshot publish contract: %w", err)
	}
	if err := contract.Validate(); err != nil {
		return fmt.Errorf("validate remote snapshot publish contract: %w", err)
	}
	var appSpec *crawlremote.AppSpec
	for index := range contract.Apps {
		app := &contract.Apps[index]
		if app.App == "gitcrawl" {
			appSpec = app
			break
		}
	}
	if appSpec == nil {
		return fmt.Errorf("remote contract does not advertise the gitcrawl app")
	}
	requiredCapabilities := []string{
		gitcrawlSnapshotAtomicCapability,
		gitcrawlSnapshotProvenanceCapability,
		gitcrawlSnapshotStagingCapability,
		sqliteBundleGzipUploadCapability,
	}
	requiredCapabilities = append(requiredCapabilities, snapshot.Capabilities...)
	if cutover {
		requiredCapabilities = append(
			requiredCapabilities,
			gitcrawlSnapshotCutoverCapability,
		)
	}
	for _, capability := range requiredCapabilities {
		if !slices.Contains(appSpec.Capabilities, capability) {
			return fmt.Errorf(
				"remote does not advertise required snapshot publish capability %s",
				capability,
			)
		}
	}
	requiredRoutes := []crawlremote.RouteSpec{
		{
			Method: http.MethodGet,
			Path:   "/v1/whoami",
			Auth:   crawlremote.AuthReader,
		},
		{
			Method: http.MethodGet,
			Path:   "/v1/apps/:app/archives/:archive/publish-status",
			Auth:   crawlremote.AuthPublisher,
		},
		{
			Method: http.MethodPost,
			Path:   "/v1/apps/:app/archives/:archive/query",
			Auth:   crawlremote.AuthReader,
		},
		{
			Method: http.MethodPost,
			Path:   "/v1/apps/:app/archives/:archive/ingest",
			Auth:   crawlremote.AuthPublisher,
		},
		{
			Method: http.MethodPut,
			Path:   "/v1/apps/:app/archives/:archive/sqlite",
			Auth:   crawlremote.AuthPublisher,
		},
	}
	if cutover {
		requiredRoutes = append(
			requiredRoutes,
			crawlremote.RouteSpec{
				Method: http.MethodGet,
				Path:   "/v1/apps/:app/archives/:archive/status",
				Auth:   crawlremote.AuthReader,
			},
			crawlremote.RouteSpec{
				Method: http.MethodPost,
				Path:   "/v1/apps/:app/archives/:archive/cutover",
				Auth:   crawlremote.AuthPublisher,
			},
			crawlremote.RouteSpec{
				Method: http.MethodGet,
				Path:   "/v1/apps/:app/archives/:archive/sqlite",
				Auth:   crawlremote.AuthReader,
			},
		)
	}
	for _, required := range requiredRoutes {
		if !slices.ContainsFunc(contract.Routes, func(route crawlremote.RouteSpec) bool {
			return route == required
		}) {
			return fmt.Errorf(
				"remote contract does not advertise required snapshot publish route %s %s with %s auth",
				required.Method,
				required.Path,
				required.Auth,
			)
		}
	}
	for _, required := range gitcrawlCloudReaderQuerySpecs() {
		queryIndex := -1
		for index, query := range appSpec.Queries {
			if query.Name != required.Name {
				continue
			}
			if queryIndex >= 0 {
				return fmt.Errorf(
					"remote contract advertises required reader query %s more than once",
					required.Name,
				)
			}
			queryIndex = index
		}
		if queryIndex < 0 {
			return fmt.Errorf(
				"remote contract does not advertise required reader query %s",
				required.Name,
			)
		}
		remoteArgs := appSpec.Queries[queryIndex].Args
		if !uniqueStringSuperset(remoteArgs, required.Args) {
			return fmt.Errorf(
				"remote contract reader query %s has arguments %v, missing required arguments from %v",
				required.Name,
				remoteArgs,
				required.Args,
			)
		}
	}
	requiredTables := make([]crawlremote.IngestTableSpec, 0, len(snapshot.Datasets)+1)
	for _, dataset := range snapshot.Datasets {
		requiredTables = append(requiredTables, crawlremote.IngestTableSpec{
			Name:    dataset.Name,
			Columns: dataset.Columns,
		})
	}
	requiredTables = append(requiredTables, crawlremote.IngestTableSpec{
		Name:    "dataset_coverage",
		Columns: gitcrawlCloudCoverageColumns,
	})
	for _, required := range requiredTables {
		tableIndex := slices.IndexFunc(appSpec.IngestTables, func(table crawlremote.IngestTableSpec) bool {
			return table.Name == required.Name
		})
		if tableIndex < 0 {
			return fmt.Errorf(
				"remote contract does not advertise required snapshot ingest table %s",
				required.Name,
			)
		}
		remoteColumns := appSpec.IngestTables[tableIndex].Columns
		for _, column := range required.Columns {
			if !slices.Contains(remoteColumns, column) {
				return fmt.Errorf(
					"remote contract snapshot ingest table %s is missing required column %s",
					required.Name,
					column,
				)
			}
		}
	}
	return nil
}

func requireGitcrawlCloudPublishRoles(ctx context.Context, client *crawlremote.Client) error {
	preflightCtx, cancel := context.WithTimeout(ctx, gitcrawlCloudPublishPreflightTimeout)
	defer cancel()
	identity, err := client.Whoami(preflightCtx)
	if err != nil {
		return fmt.Errorf("read remote identity before snapshot publication: %w", err)
	}
	if slices.Contains(identity.Roles, "admin") {
		return nil
	}
	missing := make([]string, 0, 2)
	for _, role := range []string{crawlremote.AuthPublisher, crawlremote.AuthReader} {
		if !slices.Contains(identity.Roles, role) {
			missing = append(missing, role)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"remote token must have publisher and reader roles before snapshot publication; missing %s",
			strings.Join(missing, ", "),
		)
	}
	return nil
}

func equalUniqueStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, value := range left {
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	for _, value := range right {
		if _, duplicate := seen[value]; !duplicate {
			return false
		}
		delete(seen, value)
	}
	return len(seen) == 0
}

func uniqueStringSuperset(values, required []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	for _, value := range required {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}
