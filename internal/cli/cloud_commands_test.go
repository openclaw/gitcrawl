package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
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
			body:       bytes.Repeat([]byte("x"), len(source)),
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
							CoverageComplete:   true,
						},
					})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/sqlite"):
					if test.statusCode == http.StatusOK {
						w.Header().Set("content-length", fmt.Sprintf("%d", len(test.body)))
						w.Header().Set("x-crawl-content-sha256", snapshotID)
					}
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
				int64(len(source)),
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verification error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestVerifyGitcrawlSQLiteHydrationRejectsInvalidFramingAndDigest(t *testing.T) {
	source := []byte("SQLite format 3\x00bound source")
	snapshotID := fmt.Sprintf("%x", sha256.Sum256(source))
	different := bytes.Repeat([]byte("x"), len(source))

	tests := []struct {
		name          string
		body          []byte
		digest        string
		contentLength int64
		want          string
	}{
		{
			name:          "valid",
			body:          source,
			digest:        snapshotID,
			contentLength: int64(len(source)),
		},
		{
			name:          "missing digest",
			body:          source,
			contentLength: int64(len(source)),
			want:          "missing x-crawl-content-sha256",
		},
		{
			name:          "wrong digest",
			body:          source,
			digest:        strings.Repeat("f", 64),
			contentLength: int64(len(source)),
			want:          "advertises digest",
		},
		{
			name:          "missing length",
			body:          source,
			digest:        snapshotID,
			contentLength: -1,
			want:          "missing a positive Content-Length",
		},
		{
			name:          "wrong length",
			body:          source,
			digest:        snapshotID,
			contentLength: int64(len(source) + 1),
			want:          "does not match uploaded source size",
		},
		{
			name:          "truncated body",
			body:          source[:len(source)-1],
			digest:        snapshotID,
			contentLength: int64(len(source)),
			want:          "truncated",
		},
		{
			name:          "oversized body",
			body:          append(append([]byte(nil), source...), 'x'),
			digest:        snapshotID,
			contentLength: int64(len(source)),
			want:          "exceeds Content-Length",
		},
		{
			name:          "body digest mismatch",
			body:          different,
			digest:        snapshotID,
			contentLength: int64(len(source)),
			want:          "does not match source",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := &http.Response{
				Header:        make(http.Header),
				Body:          io.NopCloser(bytes.NewReader(test.body)),
				ContentLength: test.contentLength,
			}
			if test.digest != "" {
				response.Header.Set("x-crawl-content-sha256", test.digest)
			}
			err := verifyGitcrawlSQLiteHydration(response, snapshotID, int64(len(source)))
			if test.want == "" {
				if err != nil {
					t.Fatalf("verify hydration: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verification error = %v, want %q", err, test.want)
			}
		})
	}
}
