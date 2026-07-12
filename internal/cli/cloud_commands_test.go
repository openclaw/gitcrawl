package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
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

func TestRequireGitcrawlCloudPublishRolesAcceptsAdmin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/v1/whoami" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(crawlremote.Identity{
			Login: "admin",
			Roles: []string{"admin"},
		})
	}))
	defer server.Close()

	client, err := crawlremote.NewClient(crawlremote.Options{
		Endpoint:      server.URL,
		HTTPClient:    server.Client(),
		TokenProvider: crawlremote.StaticToken("admin-token"),
	})
	if err != nil {
		t.Fatalf("remote client: %v", err)
	}
	if err := requireGitcrawlCloudPublishRoles(context.Background(), client); err != nil {
		t.Fatalf("admin role preflight: %v", err)
	}
}

func TestSendSnapshotIngestDatasetStreamsBoundedBatches(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		create table rows(id integer primary key, value text not null);
		with recursive sequence(id) as (
			select 1
			union all
			select id + 1 from sequence where id < 501
		)
		insert into rows(id, value)
		select id, printf('row-%03d', id) from sequence;
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	var requests []crawlremote.IngestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request crawlremote.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests = append(requests, request)
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{
			Table:         request.Table,
			SnapshotID:    request.Manifest.SnapshotID,
			MutationToken: fmt.Sprintf("mutation-%d", len(requests)),
			RowsAccepted:  int64(len(request.Rows)),
		})
	}))
	defer server.Close()
	client, err := crawlremote.NewClient(crawlremote.Options{
		Endpoint:      server.URL,
		HTTPClient:    server.Client(),
		TokenProvider: crawlremote.StaticToken("publisher-token"),
	})
	if err != nil {
		t.Fatalf("remote client: %v", err)
	}
	dataset := gitcrawlCloudDataset{
		Name:     "bounded",
		Columns:  []string{"id", "value"},
		Query:    `select id, value from rows order by id`,
		RowCount: 501,
	}
	progress, err := sendSnapshotIngestDataset(
		context.Background(),
		db,
		client,
		"gitcrawl",
		"gitcrawl/openclaw__gitcrawl",
		crawlremote.IngestManifest{
			App:        "gitcrawl",
			Archive:    "gitcrawl/openclaw__gitcrawl",
			SnapshotID: strings.Repeat("a", 64),
		},
		dataset,
		"",
	)
	if err != nil {
		t.Fatalf("stream dataset: %v", err)
	}
	if progress.RowsAccepted != 501 || progress.MutationToken != "mutation-3" {
		t.Fatalf("progress = %#v", progress)
	}
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(requests))
	}
	for index, wantRows := range []int{250, 250, 1} {
		if got := len(requests[index].Rows); got != wantRows {
			t.Fatalf("request %d rows = %d, want %d", index, got, wantRows)
		}
	}
	for index, wantCursor := range []string{"", "250", "500"} {
		if requests[index].Cursor != wantCursor {
			t.Fatalf("request %d cursor = %q, want %q", index, requests[index].Cursor, wantCursor)
		}
	}
}

func TestCompleteGitcrawlSnapshotStagingRequiresExactAcknowledgement(t *testing.T) {
	snapshotID := strings.Repeat("a", 64)
	snapshot := gitcrawlCloudSnapshot{
		ID:                 snapshotID,
		DatasetGeneratedAt: "2026-07-12T12:01:00Z",
		Datasets: []gitcrawlCloudDataset{
			{Name: "repositories", RowCount: 1, EligibleCount: 1, CoveredCount: 1, Complete: true},
			{Name: "threads", RowCount: 3, EligibleCount: 3, CoveredCount: 3, Complete: true},
		},
	}
	manifest := crawlremote.IngestManifest{
		App:        "gitcrawl",
		Archive:    "gitcrawl/openclaw__gitcrawl",
		SnapshotID: snapshotID,
	}
	const mutationToken = "mutation-final"

	for _, test := range []struct {
		name   string
		mutate func(*crawlremote.IngestResult)
		want   string
	}{
		{name: "exact acknowledgement"},
		{
			name: "wrong table",
			mutate: func(result *crawlremote.IngestResult) {
				result.Table = "threads"
			},
			want: "want dataset_coverage",
		},
		{
			name: "wrong snapshot",
			mutate: func(result *crawlremote.IngestResult) {
				result.SnapshotID = strings.Repeat("b", 64)
			},
			want: "want \"" + snapshotID + "\"",
		},
		{
			name: "wrong dataset count",
			mutate: func(result *crawlremote.IngestResult) {
				result.RowsAccepted--
			},
			want: "want 2 datasets",
		},
		{
			name: "wrong mutation token",
			mutate: func(result *crawlremote.IngestResult) {
				result.MutationToken = "other-mutation"
			},
			want: "want \"mutation-final\"",
		},
		{
			name: "partial candidate",
			mutate: func(result *crawlremote.IngestResult) {
				result.Complete = false
			},
			want: "did not complete snapshot",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request crawlremote.IngestRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if request.Table != "dataset_coverage" ||
					!request.Final ||
					request.MutationToken != mutationToken ||
					len(request.Rows) != len(snapshot.Datasets) {
					http.Error(w, "invalid final coverage request", http.StatusBadRequest)
					return
				}
				result := crawlremote.IngestResult{
					Table:         request.Table,
					SnapshotID:    request.Manifest.SnapshotID,
					MutationToken: request.MutationToken,
					RowsAccepted:  int64(len(request.Rows)),
					Complete:      true,
				}
				if test.mutate != nil {
					test.mutate(&result)
				}
				_ = json.NewEncoder(w).Encode(result)
			}))
			defer server.Close()
			client, err := crawlremote.NewClient(crawlremote.Options{
				Endpoint:      server.URL,
				HTTPClient:    server.Client(),
				TokenProvider: crawlremote.StaticToken("publisher-token"),
			})
			if err != nil {
				t.Fatalf("remote client: %v", err)
			}

			_, err = completeGitcrawlSnapshotStaging(
				context.Background(),
				client,
				manifest.App,
				manifest.Archive,
				manifest,
				snapshot,
				mutationToken,
			)
			if test.want == "" {
				if err != nil {
					t.Fatalf("complete staging: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("complete staging error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateGitcrawlSQLiteBundleUploadRequiresFinalSnapshotDigest(t *testing.T) {
	expected := crawlremote.SQLiteBundleManifest{
		SnapshotID: strings.Repeat("a", 64),
		Object: crawlremote.SQLiteBundleObject{
			Size:   123,
			SHA256: strings.Repeat("a", 64),
		},
	}
	validResult := func() crawlremote.SQLiteBundleUploadResult {
		manifest := expected
		return crawlremote.SQLiteBundleUploadResult{
			App:      "gitcrawl",
			Archive:  "gitcrawl/openclaw__gitcrawl",
			Complete: true,
			Bundle: &crawlremote.SQLiteBundle{
				Manifest: &manifest,
			},
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*crawlremote.SQLiteBundleUploadResult)
		want   string
	}{
		{name: "exact acknowledgement"},
		{
			name: "not finalized",
			mutate: func(result *crawlremote.SQLiteBundleUploadResult) {
				result.Complete = false
			},
			want: "not finalized",
		},
		{
			name: "wrong snapshot",
			mutate: func(result *crawlremote.SQLiteBundleUploadResult) {
				result.Bundle.Manifest.SnapshotID = strings.Repeat("b", 64)
			},
			want: "acknowledged snapshot",
		},
		{
			name: "wrong digest",
			mutate: func(result *crawlremote.SQLiteBundleUploadResult) {
				result.Bundle.Manifest.Object.SHA256 = strings.Repeat("b", 64)
			},
			want: "acknowledged digest",
		},
		{
			name: "wrong source size",
			mutate: func(result *crawlremote.SQLiteBundleUploadResult) {
				result.Bundle.Manifest.Object.Size++
			},
			want: "acknowledged source size",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := validResult()
			if test.mutate != nil {
				test.mutate(&result)
			}
			_, err := validateGitcrawlSQLiteBundleUpload(
				result,
				"gitcrawl",
				"gitcrawl/openclaw__gitcrawl",
				expected,
			)
			if test.want == "" {
				if err != nil {
					t.Fatalf("validate bundle upload: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("bundle validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestGitcrawlReaderStatusMatchesCompleteServingSnapshot(t *testing.T) {
	snapshotID := strings.Repeat("a", 64)
	snapshot := gitcrawlCloudSnapshot{
		ID:                 snapshotID,
		SourceSyncAt:       "2026-07-12T12:00:00Z",
		DatasetGeneratedAt: "2026-07-12T12:01:00Z",
		Datasets: []gitcrawlCloudDataset{{
			Name:          "repositories",
			RowCount:      1,
			EligibleCount: 1,
			CoveredCount:  1,
			MaxSourceAt:   "2026-07-12T12:00:00Z",
			Complete:      true,
		}},
	}
	manifest := gitcrawlCloudManifest("gitcrawl/openclaw__gitcrawl", snapshot)
	capabilities := gitcrawlCloudPublicationCapabilities(snapshot.Capabilities)
	status := crawlremote.Status{
		App:                manifest.App,
		Archive:            manifest.Archive,
		SchemaName:         manifest.SchemaName,
		SchemaVersion:      manifest.SchemaVersion,
		SchemaHash:         manifest.SchemaHash,
		Capabilities:       capabilities,
		ActiveSnapshotID:   snapshotID,
		SourceSyncAt:       snapshot.SourceSyncAt,
		DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
		CoverageComplete:   true,
		Datasets: []crawlremote.DatasetCoverage{{
			Dataset:            "repositories",
			RowCount:           1,
			EligibleCount:      1,
			CoveredCount:       1,
			FreshCount:         1,
			MaxSourceAt:        "2026-07-12T12:00:00Z",
			DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
			Complete:           true,
		}},
		Snapshot: &crawlremote.ArchiveSnapshot{
			ID:                 snapshotID,
			SourceSHA256:       snapshotID,
			SchemaName:         manifest.SchemaName,
			SchemaVersion:      manifest.SchemaVersion,
			SchemaHash:         manifest.SchemaHash,
			Capabilities:       capabilities,
			SourceSyncAt:       snapshot.SourceSyncAt,
			DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
			CoverageComplete:   true,
		},
	}
	if !gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities) {
		t.Fatal("complete serving snapshot did not match")
	}

	status.CoverageComplete = false
	if gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities) {
		t.Fatal("top-level incomplete reader status matched")
	}
	status.CoverageComplete = true
	status.Snapshot.CoverageComplete = false
	if gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities) {
		t.Fatal("nested incomplete reader status matched")
	}
	status.Snapshot.CoverageComplete = true
	status.Datasets[0].CoveredCount = 0
	if gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities) {
		t.Fatal("reader dataset count drift matched")
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
		name                string
		body                []byte
		digest              string
		contentLength       int64
		contentLengthHeader string
		want                string
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
			name:          "chunked response",
			body:          source,
			digest:        snapshotID,
			contentLength: -1,
		},
		{
			name:                "malformed length",
			body:                source,
			digest:              snapshotID,
			contentLength:       -1,
			contentLengthHeader: "not-a-number",
			want:                "invalid Content-Length",
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
			want:          "exceeds uploaded source size",
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
			if test.contentLengthHeader != "" {
				response.Header.Set("content-length", test.contentLengthHeader)
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

func TestVerifyGitcrawlSQLiteHydrationAcceptsChunkedResponse(t *testing.T) {
	source := []byte("SQLite format 3\x00chunked source")
	snapshotID := fmt.Sprintf("%x", sha256.Sum256(source))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-crawl-content-sha256", snapshotID)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write(source)
	}))
	defer server.Close()

	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatalf("download chunked hydration: %v", err)
	}
	defer response.Body.Close()
	if response.ContentLength != -1 || !slices.Contains(response.TransferEncoding, "chunked") {
		t.Fatalf(
			"response framing = length %d transfer %v, want chunked",
			response.ContentLength,
			response.TransferEncoding,
		)
	}
	if err := verifyGitcrawlSQLiteHydration(
		response,
		snapshotID,
		int64(len(source)),
	); err != nil {
		t.Fatalf("verify chunked hydration: %v", err)
	}
}
