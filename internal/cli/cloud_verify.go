package cli

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
)

func gitcrawlPublisherStatusMatches(
	status crawlremote.PublisherStatus,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
) bool {
	snapshot := status.Snapshot
	if status.App != manifest.App ||
		status.Archive != manifest.Archive ||
		status.ActiveSnapshotID != manifest.SnapshotID ||
		snapshot == nil ||
		snapshot.ID != manifest.SnapshotID ||
		snapshot.SourceSHA256 != manifest.SourceSHA256 ||
		snapshot.SourceSyncAt != manifest.SourceSyncAt ||
		snapshot.SchemaName != manifest.SchemaName ||
		snapshot.SchemaVersion != manifest.SchemaVersion ||
		snapshot.SchemaHash != manifest.SchemaHash ||
		strings.TrimSpace(snapshot.DatasetGeneratedAt) == "" {
		return false
	}
	if status.CoverageComplete != snapshot.CoverageComplete ||
		!status.CoverageComplete {
		return false
	}
	if !equalUniqueStringSet(snapshot.Capabilities, publicationCapabilities) {
		return false
	}
	return true
}

func gitcrawlReaderStatusMatches(
	status crawlremote.Status,
	snapshot gitcrawlCloudSnapshot,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
	expectedCutoverAt string,
) bool {
	if status.App != manifest.App ||
		status.Archive != manifest.Archive ||
		status.Mode != "cloud" ||
		status.SnapshotMode != "snapshot" ||
		status.ActiveSnapshotID != snapshot.ID ||
		status.SchemaName != manifest.SchemaName ||
		status.SchemaVersion != manifest.SchemaVersion ||
		status.SchemaHash != manifest.SchemaHash ||
		status.SourceSyncAt != snapshot.SourceSyncAt ||
		status.DatasetGeneratedAt != snapshot.DatasetGeneratedAt ||
		!status.CoverageComplete ||
		!equalUniqueStringSet(status.Capabilities, publicationCapabilities) {
		return false
	}
	if !gitcrawlPublisherStatusMatches(crawlremote.PublisherStatus{
		App:              status.App,
		Archive:          status.Archive,
		ActiveSnapshotID: status.ActiveSnapshotID,
		CoverageComplete: status.CoverageComplete,
		Snapshot:         status.Snapshot,
	}, manifest, publicationCapabilities) ||
		status.Snapshot.DatasetGeneratedAt != snapshot.DatasetGeneratedAt {
		return false
	}
	return gitcrawlReaderCutoverMatches(status, expectedCutoverAt) &&
		gitcrawlDatasetCoverageMatches(status.Datasets, snapshot)
}

func gitcrawlReaderCutoverMatches(status crawlremote.Status, expectedCutoverAt string) bool {
	if status.Snapshot == nil {
		return false
	}
	if _, err := time.Parse(time.RFC3339Nano, status.SnapshotCutoverAt); err != nil {
		return false
	}
	if _, err := time.Parse(time.RFC3339Nano, status.Snapshot.CutoverAt); err != nil ||
		status.SnapshotCutoverAt != status.Snapshot.CutoverAt {
		return false
	}
	if expectedCutoverAt == "" {
		return true
	}
	if _, err := time.Parse(time.RFC3339Nano, expectedCutoverAt); err != nil {
		return false
	}
	return status.SnapshotCutoverAt == expectedCutoverAt
}

func validateGitcrawlCutoverResult(
	result crawlremote.CutoverResult,
	archive, snapshotID string,
) error {
	if result.Archive != archive {
		return fmt.Errorf("cutover returned archive %q, want %q", result.Archive, archive)
	}
	if result.SnapshotID != snapshotID {
		return fmt.Errorf("cutover returned snapshot %q, want %q", result.SnapshotID, snapshotID)
	}
	if result.SnapshotMode != "snapshot" {
		return fmt.Errorf("cutover returned snapshot mode %q, want snapshot", result.SnapshotMode)
	}
	if _, err := time.Parse(time.RFC3339Nano, result.CutoverAt); err != nil {
		return fmt.Errorf("cutover returned invalid timestamp %q: %w", result.CutoverAt, err)
	}
	return nil
}

func verifyGitcrawlReaderProjection(
	ctx context.Context,
	client *crawlremote.Client,
	archive string,
	snapshot gitcrawlCloudSnapshot,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
	expectedCutoverAt string,
) error {
	return verifyGitcrawlReaderProjectionWithRetry(
		ctx,
		client,
		archive,
		snapshot,
		manifest,
		publicationCapabilities,
		expectedCutoverAt,
		gitcrawlCloudPostCutoverStatusAttempts,
		gitcrawlCloudPostCutoverStatusRetryDelay,
	)
}

func verifyGitcrawlReaderProjectionWithRetry(
	ctx context.Context,
	client *crawlremote.Client,
	archive string,
	snapshot gitcrawlCloudSnapshot,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
	expectedCutoverAt string,
	attempts int,
	retryDelay time.Duration,
) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		status, err := client.Status(ctx, "gitcrawl", archive)
		if err == nil && gitcrawlReaderStatusMatches(
			status,
			snapshot,
			manifest,
			publicationCapabilities,
			expectedCutoverAt,
		) {
			return nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if lastErr != nil {
		return fmt.Errorf(
			"read post-cutover reader status after %d attempts: %w",
			attempts,
			lastErr,
		)
	}
	return fmt.Errorf(
		"post-cutover reader status does not match snapshot %s digest, profile, generation, and coverage after %d attempts",
		snapshot.ID,
		attempts,
	)
}

func gitcrawlDatasetCoverageMatches(
	actual []crawlremote.DatasetCoverage,
	snapshot gitcrawlCloudSnapshot,
) bool {
	if len(actual) != len(snapshot.Datasets) {
		return false
	}
	expected := make(map[string]gitcrawlCloudDataset, len(snapshot.Datasets))
	for _, dataset := range snapshot.Datasets {
		if _, duplicate := expected[dataset.Name]; duplicate {
			return false
		}
		expected[dataset.Name] = dataset
	}
	for _, dataset := range actual {
		want, ok := expected[dataset.Dataset]
		if !ok ||
			dataset.RowCount != want.RowCount ||
			dataset.EligibleCount != want.EligibleCount ||
			dataset.CoveredCount != want.CoveredCount ||
			dataset.FreshCount != want.CoveredCount ||
			dataset.MaxSourceAt != want.MaxSourceAt ||
			dataset.DatasetGeneratedAt != snapshot.DatasetGeneratedAt ||
			dataset.Complete != want.Complete {
			return false
		}
		delete(expected, dataset.Dataset)
	}
	return len(expected) == 0
}

func verifyGitcrawlSnapshotPublication(
	ctx context.Context,
	client *crawlremote.Client,
	httpClient *http.Client,
	tokenProvider crawlremote.TokenProvider,
	endpoint, archive string,
	snapshot gitcrawlCloudSnapshot,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
	expectedCutoverAt string,
	sourceSize int64,
) error {
	if err := verifyGitcrawlReaderProjection(
		ctx,
		client,
		archive,
		snapshot,
		manifest,
		publicationCapabilities,
		expectedCutoverAt,
	); err != nil {
		return err
	}
	status, err := client.PublishStatusForSnapshot(
		ctx,
		"gitcrawl",
		archive,
		snapshot.ID,
	)
	if err != nil {
		return fmt.Errorf("read post-cutover publisher status: %w", err)
	}
	if !gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) ||
		status.Snapshot.DatasetGeneratedAt != snapshot.DatasetGeneratedAt {
		return fmt.Errorf(
			"post-cutover publisher status does not match snapshot %s digest, profile, generation, and coverage",
			snapshot.ID,
		)
	}

	token, err := tokenProvider.Token(ctx)
	if err != nil {
		return fmt.Errorf("read remote token for snapshot hydration: %w", err)
	}
	sqliteURL := strings.TrimRight(endpoint, "/") +
		"/v1/apps/" + url.PathEscape("gitcrawl") +
		"/archives/" + url.PathEscape(archive) +
		"/sqlite"
	hydrationCtx, cancel := context.WithTimeout(ctx, gitcrawlCloudHydrationTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(hydrationCtx, http.MethodGet, sqliteURL, nil)
	if err != nil {
		return fmt.Errorf("build snapshot hydration request: %w", err)
	}
	request.Header.Set("accept", "application/vnd.sqlite3, application/octet-stream")
	request.Header.Set("authorization", "Bearer "+token)
	request.Header.Set("user-agent", "gitcrawl/"+version)
	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("download bound SQLite snapshot: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return fmt.Errorf(
			"download bound SQLite snapshot: status=%d body=%s",
			response.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}
	return verifyGitcrawlSQLiteHydration(response, snapshot.ID, sourceSize)
}

func verifyGitcrawlSQLiteHydration(
	response *http.Response,
	expectedDigest string,
	expectedSize int64,
) error {
	advertised := strings.TrimSpace(response.Header.Get("x-crawl-content-sha256"))
	if advertised == "" {
		return fmt.Errorf("downloaded SQLite snapshot is missing x-crawl-content-sha256")
	}
	if !strings.EqualFold(advertised, expectedDigest) {
		return fmt.Errorf(
			"downloaded SQLite snapshot advertises digest %s, want %s",
			advertised,
			expectedDigest,
		)
	}
	if expectedSize <= 0 {
		return fmt.Errorf("uploaded SQLite manifest source size must be positive, got %d", expectedSize)
	}
	contentLength := int64(-1)
	header := strings.TrimSpace(response.Header.Get("content-length"))
	if header != "" {
		parsed, err := strconv.ParseInt(header, 10, 64)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("downloaded SQLite snapshot has invalid Content-Length %q", header)
		}
		contentLength = parsed
	} else if response.ContentLength >= 0 {
		contentLength = response.ContentLength
	}
	if contentLength >= 0 && contentLength != expectedSize {
		return fmt.Errorf(
			"downloaded SQLite snapshot Content-Length %d does not match uploaded source size %d",
			contentLength,
			expectedSize,
		)
	}
	hash := sha256.New()
	written, err := io.CopyN(hash, response.Body, expectedSize)
	if err != nil {
		return fmt.Errorf(
			"downloaded SQLite snapshot truncated after %d of %d bytes: %w",
			written,
			expectedSize,
			err,
		)
	}
	var extra [1]byte
	n, err := response.Body.Read(extra[:])
	if n > 0 {
		return fmt.Errorf(
			"downloaded SQLite snapshot exceeds uploaded source size %d",
			expectedSize,
		)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("check downloaded SQLite snapshot boundary: %w", err)
	}
	actual := fmt.Sprintf("%x", hash.Sum(nil))
	if actual != expectedDigest {
		return fmt.Errorf(
			"downloaded SQLite snapshot digest %s does not match source %s",
			actual,
			expectedDigest,
		)
	}
	return nil
}
