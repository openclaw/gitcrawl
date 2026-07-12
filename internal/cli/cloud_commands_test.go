package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
)

func TestUniqueStringSupersetAllowsAdditiveArguments(t *testing.T) {
	if !uniqueStringSuperset(
		[]string{"owner", "repo", "query", "limit", "cursor"},
		[]string{"owner", "repo", "query", "limit"},
	) {
		t.Fatal("additive optional remote argument was rejected")
	}
}

func TestUniqueStringSupersetRejectsMissingOrDuplicateArguments(t *testing.T) {
	for _, values := range [][]string{
		{"owner", "repo"},
		{"owner", "repo", "query", "query"},
	} {
		if uniqueStringSuperset(values, []string{"owner", "repo", "query"}) {
			t.Fatalf("invalid remote arguments accepted: %v", values)
		}
	}
}

func TestVerifyGitcrawlSnapshotPublicationRejectsUnreadableOrMismatchedSQLite(t *testing.T) {
	source := []byte("SQLite format 3\x00bound source")
	snapshotID := fmt.Sprintf("%x", sha256.Sum256(source))
	snapshot := gitcrawlCloudSnapshot{
		ID:                 snapshotID,
		SourceSyncAt:       "2026-07-12T12:00:00Z",
		DatasetGeneratedAt: "2026-07-12T12:01:00Z",
	}
	manifest := gitcrawlCloudManifest("gitcrawl/openclaw__openclaw", snapshot)
	publicationCapabilities := gitcrawlCloudPublicationCapabilities(snapshot.Capabilities)

	for _, test := range []struct {
		name       string
		statusCode int
		body       []byte
		want       string
	}{
		{
			name:       "bound snapshot unavailable",
			statusCode: http.StatusConflict,
			body:       []byte(`{"error":"snapshot_bundle_required"}`),
			want:       "status=409",
		},
		{
			name:       "downloaded digest mismatch",
			statusCode: http.StatusOK,
			body:       []byte("different sqlite image"),
			want:       "does not match source",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/publish-status"):
					w.Header().Set("content-type", "application/json")
					_ = json.NewEncoder(w).Encode(crawlremote.PublisherStatus{
						App:              "gitcrawl",
						Archive:          manifest.Archive,
						ActiveSnapshotID: snapshotID,
						CoverageComplete: true,
						Snapshot: &crawlremote.ArchiveSnapshot{
							ID:                 snapshotID,
							SourceSHA256:       snapshotID,
							SourceSyncAt:       snapshot.SourceSyncAt,
							DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
							SchemaName:         manifest.SchemaName,
							SchemaVersion:      manifest.SchemaVersion,
							SchemaHash:         manifest.SchemaHash,
							Capabilities:       publicationCapabilities,
						},
					})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/sqlite"):
					w.WriteHeader(test.statusCode)
					_, _ = w.Write(test.body)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			httpClient := &http.Client{Timeout: time.Second}
			tokenProvider := crawlremote.StaticToken("reader-publisher-token")
			client, err := crawlremote.NewClient(crawlremote.Options{
				Endpoint:      server.URL,
				HTTPClient:    httpClient,
				TokenProvider: tokenProvider,
			})
			if err != nil {
				t.Fatalf("remote client: %v", err)
			}
			err = verifyGitcrawlSnapshotPublication(
				context.Background(),
				client,
				httpClient,
				tokenProvider,
				server.URL,
				manifest.Archive,
				snapshot,
				manifest,
				publicationCapabilities,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verification error = %v, want %q", err, test.want)
			}
		})
	}
}
