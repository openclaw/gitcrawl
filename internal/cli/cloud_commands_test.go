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

func TestGitcrawlCloudManifestAlwaysOptsIntoStaging(t *testing.T) {
	snapshot := gitcrawlCloudSnapshot{
		ID:           strings.Repeat("a", 64),
		Capabilities: []string{gitcrawlObservationOrderCapability},
	}

	manifest := gitcrawlCloudManifest("gitcrawl/openclaw__gitcrawl", snapshot)

	if !slices.Equal(manifest.Capabilities, []string{
		gitcrawlObservationOrderCapability,
		gitcrawlSnapshotStagingCapability,
	}) {
		t.Fatalf("manifest capabilities = %v", manifest.Capabilities)
	}
	if !slices.Equal(snapshot.Capabilities, []string{gitcrawlObservationOrderCapability}) {
		t.Fatalf("snapshot capabilities mutated = %v", snapshot.Capabilities)
	}

	manifest = gitcrawlCloudManifest("gitcrawl/openclaw__gitcrawl", gitcrawlCloudSnapshot{
		ID: strings.Repeat("b", 64),
		Capabilities: []string{
			gitcrawlSnapshotStagingCapability,
		},
	})
	if !slices.Equal(manifest.Capabilities, []string{gitcrawlSnapshotStagingCapability}) {
		t.Fatalf("staging capability duplicated = %v", manifest.Capabilities)
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

func TestSendSnapshotIngestDatasetFlushesBeforeEncodedByteLimit(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`create table rows(id integer primary key, value text not null)`); err != nil {
		t.Fatalf("create rows: %v", err)
	}
	value := strings.Repeat("x", int(gitcrawlCloudIngestRequestMaxBytes/2))
	for id := 1; id <= 3; id++ {
		if _, err := db.Exec(`insert into rows(id, value) values(?, ?)`, id, value); err != nil {
			t.Fatalf("seed row %d: %v", id, err)
		}
	}

	var requests []crawlremote.IngestRequest
	var encodedSizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if int64(len(body)) > gitcrawlCloudIngestRequestMaxBytes {
			http.Error(w, "encoded request exceeded byte budget", http.StatusRequestEntityTooLarge)
			return
		}
		var request crawlremote.IngestRequest
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests = append(requests, request)
		encodedSizes = append(encodedSizes, len(body))
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
		gitcrawlCloudDataset{
			Name:     "bounded_bytes",
			Columns:  []string{"id", "value"},
			Query:    `select id, value from rows order by id`,
			RowCount: 3,
		},
		"",
	)
	if err != nil {
		t.Fatalf("stream byte-bounded dataset: %v", err)
	}
	if progress.RowsAccepted != 3 || progress.MutationToken != "mutation-3" {
		t.Fatalf("progress = %#v", progress)
	}
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want one per large row", len(requests))
	}
	for index := range requests {
		if len(requests[index].Rows) != 1 {
			t.Fatalf("request %d rows = %d, want 1", index, len(requests[index].Rows))
		}
		if int64(encodedSizes[index]) > gitcrawlCloudIngestRequestMaxBytes {
			t.Fatalf(
				"request %d encoded bytes = %d, limit %d",
				index,
				encodedSizes[index],
				gitcrawlCloudIngestRequestMaxBytes,
			)
		}
	}
}

func TestSendSnapshotIngestRowsFlushesBeforeEncodedByteLimit(t *testing.T) {
	var requests []crawlremote.IngestRequest
	var encodedSizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if int64(len(body)) > gitcrawlCloudIngestRequestMaxBytes {
			http.Error(w, "encoded request exceeded byte budget", http.StatusRequestEntityTooLarge)
			return
		}
		var request crawlremote.IngestRequest
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests = append(requests, request)
		encodedSizes = append(encodedSizes, len(body))
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{
			Table:         request.Table,
			SnapshotID:    request.Manifest.SnapshotID,
			MutationToken: fmt.Sprintf("mutation-%d", len(requests)),
			RowsAccepted:  int64(len(request.Rows)),
			Complete:      request.Final,
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
	value := strings.Repeat("x", int(gitcrawlCloudIngestRequestMaxBytes/2))
	progress, err := sendSnapshotIngestRows(
		context.Background(),
		client,
		"gitcrawl",
		"gitcrawl/openclaw__gitcrawl",
		crawlremote.IngestManifest{
			App:        "gitcrawl",
			Archive:    "gitcrawl/openclaw__gitcrawl",
			SnapshotID: strings.Repeat("a", 64),
		},
		"threads",
		[]string{"body"},
		[][]any{{value}, {value}, {value}},
		"",
		true,
	)
	if err != nil {
		t.Fatalf("send byte-bounded rows: %v", err)
	}
	if progress.RowsAccepted != 3 || progress.MutationToken != "mutation-3" {
		t.Fatalf("progress = %#v", progress)
	}
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want one per large row", len(requests))
	}
	for index := range requests {
		if len(requests[index].Rows) != 1 {
			t.Fatalf("request %d rows = %d, want 1", index, len(requests[index].Rows))
		}
		if int64(encodedSizes[index]) > gitcrawlCloudIngestRequestMaxBytes {
			t.Fatalf(
				"request %d encoded bytes = %d, limit %d",
				index,
				encodedSizes[index],
				gitcrawlCloudIngestRequestMaxBytes,
			)
		}
		wantCursor := ""
		if index > 0 {
			wantCursor = fmt.Sprint(index)
		}
		if requests[index].Cursor != wantCursor {
			t.Fatalf("request %d cursor = %q, want %q", index, requests[index].Cursor, wantCursor)
		}
		if requests[index].Final != (index == len(requests)-1) {
			t.Fatalf("request %d final = %v", index, requests[index].Final)
		}
	}
}

func TestIngestBatchingRejectsSingleOversizedRowBeforeRequest(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "unexpected request", http.StatusInternalServerError)
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
	_, err = sendSnapshotIngestRows(
		context.Background(),
		client,
		"gitcrawl",
		"gitcrawl/openclaw__gitcrawl",
		crawlremote.IngestManifest{
			App:        "gitcrawl",
			Archive:    "gitcrawl/openclaw__gitcrawl",
			SnapshotID: strings.Repeat("a", 64),
		},
		"threads",
		[]string{"body"},
		[][]any{{strings.Repeat("x", int(gitcrawlCloudIngestRequestMaxBytes))}},
		"",
		false,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "ingest table threads row 0 encoded request") ||
		!strings.Contains(err.Error(), fmt.Sprintf("limit %d", gitcrawlCloudIngestRequestMaxBytes)) {
		t.Fatalf("oversized row error = %v", err)
	}
	if requests != 0 {
		t.Fatalf("remote requests = %d, want 0", requests)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`create table rows(value text not null)`); err != nil {
		t.Fatalf("create rows: %v", err)
	}
	if _, err := db.Exec(
		`insert into rows(value) values(?)`,
		strings.Repeat("x", int(gitcrawlCloudIngestRequestMaxBytes)),
	); err != nil {
		t.Fatalf("seed oversized row: %v", err)
	}
	_, err = sendSnapshotIngestDataset(
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
		gitcrawlCloudDataset{
			Name:     "oversized",
			Columns:  []string{"value"},
			Query:    `select value from rows`,
			RowCount: 1,
		},
		"",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "dataset oversized row 0 encoded ingest request") {
		t.Fatalf("oversized dataset row error = %v", err)
	}
	if requests != 0 {
		t.Fatalf("remote requests after streamed row = %d, want 0", requests)
	}
}

func TestIngestBatchingMatchesCrawlkitEscapingAtByteBoundary(t *testing.T) {
	manifest := crawlremote.IngestManifest{
		App:           "gitcrawl",
		Archive:       "gitcrawl/openclaw__gitcrawl",
		SchemaName:    gitcrawlCloudSchemaName,
		SchemaVersion: gitcrawlCloudSchemaVersion,
		SchemaHash:    gitcrawlCloudSchemaHash,
		SnapshotID:    strings.Repeat("a", 64),
		SourceSHA256:  strings.Repeat("a", 64),
	}
	columns := []string{"value"}
	var bodies [][]byte
	var requests []crawlremote.IngestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if int64(len(body)) > gitcrawlCloudIngestRequestMaxBytes {
			http.Error(w, "encoded request exceeded byte budget", http.StatusRequestEntityTooLarge)
			return
		}
		var request crawlremote.IngestRequest
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		bodies = append(bodies, body)
		requests = append(requests, request)
		_ = json.NewEncoder(w).Encode(crawlremote.IngestResult{
			Table:         request.Table,
			SnapshotID:    request.Manifest.SnapshotID,
			MutationToken: fmt.Sprintf("mutation-%d", len(requests)),
			RowsAccepted:  int64(len(request.Rows)),
			Complete:      request.Final,
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

	fitRows, oversizedRows := escapedIngestBoundaryValues(
		t,
		manifest,
		"escaped_rows",
		columns,
		true,
	)
	if _, err := sendSnapshotIngestRows(
		context.Background(),
		client,
		manifest.App,
		manifest.Archive,
		manifest,
		"escaped_rows",
		columns,
		[][]any{{fitRows}},
		"",
		true,
	); err != nil {
		t.Fatalf("send escaped in-memory boundary row: %v", err)
	}
	assertCrawlkitWireSizeParity(t, requests[0], bodies[0])
	assertEscapedIngestWireFragments(t, bodies[0])
	requestCount := len(requests)
	if _, err := sendSnapshotIngestRows(
		context.Background(),
		client,
		manifest.App,
		manifest.Archive,
		manifest,
		"escaped_rows",
		columns,
		[][]any{{oversizedRows}},
		"",
		true,
	); err == nil || !strings.Contains(err.Error(), "encoded request") {
		t.Fatalf("escaped in-memory oversized row error = %v", err)
	}
	if len(requests) != requestCount {
		t.Fatalf("escaped in-memory oversized row sent %d requests", len(requests)-requestCount)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open escaped stream database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`create table rows(value text not null)`); err != nil {
		t.Fatalf("create escaped stream rows: %v", err)
	}
	fitStream, oversizedStream := escapedIngestBoundaryValues(
		t,
		manifest,
		"escaped_stream",
		columns,
		false,
	)
	if _, err := db.Exec(`insert into rows(value) values(?)`, fitStream); err != nil {
		t.Fatalf("seed escaped stream boundary row: %v", err)
	}
	dataset := gitcrawlCloudDataset{
		Name:     "escaped_stream",
		Columns:  columns,
		Query:    `select value from rows`,
		RowCount: 1,
	}
	if _, err := sendSnapshotIngestDataset(
		context.Background(),
		db,
		client,
		manifest.App,
		manifest.Archive,
		manifest,
		dataset,
		"",
	); err != nil {
		t.Fatalf("send escaped stream boundary row: %v", err)
	}
	assertCrawlkitWireSizeParity(t, requests[requestCount], bodies[requestCount])
	assertEscapedIngestWireFragments(t, bodies[requestCount])
	requestCount = len(requests)
	if _, err := db.Exec(`update rows set value = ?`, oversizedStream); err != nil {
		t.Fatalf("seed escaped stream oversized row: %v", err)
	}
	if _, err := sendSnapshotIngestDataset(
		context.Background(),
		db,
		client,
		manifest.App,
		manifest.Archive,
		manifest,
		dataset,
		"",
	); err == nil || !strings.Contains(err.Error(), "encoded ingest request") {
		t.Fatalf("escaped stream oversized row error = %v", err)
	}
	if len(requests) != requestCount {
		t.Fatalf("escaped stream oversized row sent %d requests", len(requests)-requestCount)
	}
}

func escapedIngestBoundaryValues(
	t *testing.T,
	manifest crawlremote.IngestManifest,
	table string,
	columns []string,
	final bool,
) (fit string, oversized string) {
	t.Helper()
	const pattern = "\"\\\x00\b\f\n\r\t<>&\u2028\u2029\u00e9\u65e5\u672c\u8a9e\U0001F600"
	fits := func(repeats int) bool {
		t.Helper()
		sizer, err := newGitcrawlIngestBatchSizer(
			manifest.App,
			manifest.Archive,
			manifest,
			table,
			columns,
			"",
			"",
		)
		if err != nil {
			t.Fatalf("create escaped ingest batch sizer: %v", err)
		}
		ok, _, err := sizer.add([]any{strings.Repeat(pattern, repeats)}, final)
		if err != nil {
			t.Fatalf("size escaped ingest row: %v", err)
		}
		return ok
	}
	low, high := 0, 1
	for fits(high) {
		low = high
		high *= 2
	}
	for low+1 < high {
		middle := low + (high-low)/2
		if fits(middle) {
			low = middle
		} else {
			high = middle
		}
	}
	return strings.Repeat(pattern, low), strings.Repeat(pattern, high)
}

func assertCrawlkitWireSizeParity(
	t *testing.T,
	request crawlremote.IngestRequest,
	body []byte,
) {
	t.Helper()
	expected, err := encodedGitcrawlIngestRequestBytes(request)
	if err != nil {
		t.Fatalf("size CrawlKit ingest request: %v", err)
	}
	if int64(len(body)) != expected {
		t.Fatalf("CrawlKit wire bytes = %d, batch sizer = %d", len(body), expected)
	}
	if int64(len(body)) > gitcrawlCloudIngestRequestMaxBytes {
		t.Fatalf(
			"CrawlKit wire bytes = %d, limit %d",
			len(body),
			gitcrawlCloudIngestRequestMaxBytes,
		)
	}
}

func assertEscapedIngestWireFragments(t *testing.T, body []byte) {
	t.Helper()
	for _, fragment := range []string{
		`\"`,
		`\\`,
		`\u0000`,
		`\b`,
		`\f`,
		`\n`,
		`\r`,
		`\t`,
		`\u003c`,
		`\u003e`,
		`\u0026`,
		`\u2028`,
		`\u2029`,
	} {
		if !bytes.Contains(body, []byte(fragment)) {
			t.Fatalf("CrawlKit wire body is missing escaped fragment %q", fragment)
		}
	}
	for _, value := range []string{
		"\u00e9",
		"\u65e5\u672c\u8a9e",
		"\U0001F600",
	} {
		if !bytes.Contains(body, []byte(value)) {
			t.Fatalf("CrawlKit wire body is missing UTF-8 value %q", value)
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
	const cutoverAt = "2026-07-12T12:02:00.123456789Z"
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
	capabilities := gitcrawlCloudPublicationCapabilities(manifest.Capabilities)
	status := crawlremote.Status{
		App:                manifest.App,
		Archive:            manifest.Archive,
		Mode:               "cloud",
		SchemaName:         manifest.SchemaName,
		SchemaVersion:      manifest.SchemaVersion,
		SchemaHash:         manifest.SchemaHash,
		Capabilities:       capabilities,
		SnapshotMode:       "snapshot",
		SnapshotCutoverAt:  cutoverAt,
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
			CutoverAt:          cutoverAt,
		},
	}
	if !gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities, cutoverAt) {
		t.Fatal("complete serving snapshot did not match")
	}

	status.CoverageComplete = false
	if gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities, cutoverAt) {
		t.Fatal("top-level incomplete reader status matched")
	}
	status.CoverageComplete = true
	status.Snapshot.CoverageComplete = false
	if gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities, cutoverAt) {
		t.Fatal("nested incomplete reader status matched")
	}
	status.Snapshot.CoverageComplete = true
	status.Datasets[0].CoveredCount = 0
	if gitcrawlReaderStatusMatches(status, snapshot, manifest, capabilities, cutoverAt) {
		t.Fatal("reader dataset count drift matched")
	}
}

func TestGitcrawlReaderStatusMatchesRequiresCutoverAttestation(t *testing.T) {
	snapshotID := strings.Repeat("a", 64)
	const cutoverAt = "2026-07-12T12:02:00.123456789Z"
	snapshot := gitcrawlCloudSnapshot{
		ID:                 snapshotID,
		SourceSyncAt:       "2026-07-12T12:00:00Z",
		DatasetGeneratedAt: "2026-07-12T12:01:00Z",
	}
	manifest := gitcrawlCloudManifest("gitcrawl/openclaw__gitcrawl", snapshot)
	capabilities := gitcrawlCloudPublicationCapabilities(manifest.Capabilities)
	valid := crawlremote.Status{
		App:                manifest.App,
		Archive:            manifest.Archive,
		Mode:               "cloud",
		SchemaName:         manifest.SchemaName,
		SchemaVersion:      manifest.SchemaVersion,
		SchemaHash:         manifest.SchemaHash,
		Capabilities:       capabilities,
		SnapshotMode:       "snapshot",
		SnapshotCutoverAt:  cutoverAt,
		ActiveSnapshotID:   snapshotID,
		SourceSyncAt:       snapshot.SourceSyncAt,
		DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
		CoverageComplete:   true,
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
			CutoverAt:          cutoverAt,
		},
	}

	for _, test := range []struct {
		name              string
		expectedCutoverAt string
		mutate            func(*crawlremote.Status)
	}{
		{name: "resumed cutover", expectedCutoverAt: ""},
		{name: "fresh cutover", expectedCutoverAt: cutoverAt},
		{
			name: "missing cloud mode",
			mutate: func(status *crawlremote.Status) {
				status.Mode = ""
			},
		},
		{
			name: "missing snapshot mode",
			mutate: func(status *crawlremote.Status) {
				status.SnapshotMode = ""
			},
		},
		{
			name: "legacy snapshot mode",
			mutate: func(status *crawlremote.Status) {
				status.SnapshotMode = "legacy"
			},
		},
		{
			name: "missing top-level cutover",
			mutate: func(status *crawlremote.Status) {
				status.SnapshotCutoverAt = ""
			},
		},
		{
			name: "missing nested cutover",
			mutate: func(status *crawlremote.Status) {
				status.Snapshot.CutoverAt = ""
			},
		},
		{
			name: "invalid top-level cutover",
			mutate: func(status *crawlremote.Status) {
				status.SnapshotCutoverAt = "later"
			},
		},
		{
			name: "inconsistent nested cutover",
			mutate: func(status *crawlremote.Status) {
				status.Snapshot.CutoverAt = "2026-07-12T12:02:01Z"
			},
		},
		{
			name: "equivalent nested cutover encoding",
			mutate: func(status *crawlremote.Status) {
				status.Snapshot.CutoverAt = "2026-07-12T12:02:00.123456789+00:00"
			},
		},
		{
			name:              "fresh acknowledgement mismatch",
			expectedCutoverAt: "2026-07-12T12:02:01Z",
		},
		{
			name:              "equivalent acknowledgement encoding",
			expectedCutoverAt: "2026-07-12T12:02:00.123456789+00:00",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := valid
			snapshotEnvelope := *valid.Snapshot
			status.Snapshot = &snapshotEnvelope
			if test.mutate != nil {
				test.mutate(&status)
			}
			matched := gitcrawlReaderStatusMatches(
				status,
				snapshot,
				manifest,
				capabilities,
				test.expectedCutoverAt,
			)
			want := test.mutate == nil &&
				(test.expectedCutoverAt == "" || test.expectedCutoverAt == cutoverAt)
			if matched != want {
				t.Fatalf("cutover status match = %v, want %v", matched, want)
			}
		})
	}
}

func TestValidateGitcrawlCutoverResultRequiresExactAcknowledgement(t *testing.T) {
	const archive = "gitcrawl/openclaw__gitcrawl"
	snapshotID := strings.Repeat("a", 64)
	valid := crawlremote.CutoverResult{
		Archive:      archive,
		SnapshotID:   snapshotID,
		SnapshotMode: "snapshot",
		CutoverAt:    "2026-07-12T12:00:00.123456789Z",
	}
	for _, test := range []struct {
		name   string
		mutate func(*crawlremote.CutoverResult)
		want   string
	}{
		{name: "exact acknowledgement"},
		{
			name: "wrong archive",
			mutate: func(result *crawlremote.CutoverResult) {
				result.Archive = "gitcrawl/other"
			},
			want: "want \"" + archive + "\"",
		},
		{
			name: "wrong snapshot",
			mutate: func(result *crawlremote.CutoverResult) {
				result.SnapshotID = strings.Repeat("b", 64)
			},
			want: "want \"" + snapshotID + "\"",
		},
		{
			name: "wrong mode",
			mutate: func(result *crawlremote.CutoverResult) {
				result.SnapshotMode = "mutable"
			},
			want: "want snapshot",
		},
		{
			name: "invalid timestamp",
			mutate: func(result *crawlremote.CutoverResult) {
				result.CutoverAt = "later"
			},
			want: "invalid timestamp",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := valid
			if test.mutate != nil {
				test.mutate(&result)
			}
			err := validateGitcrawlCutoverResult(result, archive, snapshotID)
			if test.want == "" {
				if err != nil {
					t.Fatalf("validate cutover: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestVerifyGitcrawlReaderProjectionUsesBoundedExactRetries(t *testing.T) {
	snapshotID := strings.Repeat("a", 64)
	const cutoverAt = "2026-07-12T12:02:00Z"
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
	capabilities := gitcrawlCloudPublicationCapabilities(manifest.Capabilities)
	exactStatus := crawlremote.Status{
		App:                manifest.App,
		Archive:            manifest.Archive,
		Mode:               "cloud",
		SchemaName:         manifest.SchemaName,
		SchemaVersion:      manifest.SchemaVersion,
		SchemaHash:         manifest.SchemaHash,
		Capabilities:       capabilities,
		SnapshotMode:       "snapshot",
		SnapshotCutoverAt:  cutoverAt,
		ActiveSnapshotID:   snapshot.ID,
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
			ID:                 snapshot.ID,
			SourceSHA256:       snapshot.ID,
			SchemaName:         manifest.SchemaName,
			SchemaVersion:      manifest.SchemaVersion,
			SchemaHash:         manifest.SchemaHash,
			Capabilities:       capabilities,
			SourceSyncAt:       snapshot.SourceSyncAt,
			DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
			CoverageComplete:   true,
			CutoverAt:          cutoverAt,
		},
	}

	for _, test := range []struct {
		name      string
		exactAt   int
		want      string
		wantCalls int
	}{
		{
			name:      "eventually exact",
			exactAt:   3,
			wantCalls: 3,
		},
		{
			name:      "retry bound exhausted",
			want:      "after 3 attempts",
			wantCalls: 3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				status := exactStatus
				if test.exactAt == 0 || calls < test.exactAt {
					status.ActiveSnapshotID = strings.Repeat("b", 64)
				}
				_ = json.NewEncoder(w).Encode(status)
			}))
			defer server.Close()
			client, err := crawlremote.NewClient(crawlremote.Options{
				Endpoint:      server.URL,
				HTTPClient:    server.Client(),
				TokenProvider: crawlremote.StaticToken("reader-token"),
			})
			if err != nil {
				t.Fatalf("remote client: %v", err)
			}
			err = verifyGitcrawlReaderProjectionWithRetry(
				context.Background(),
				client,
				manifest.Archive,
				snapshot,
				manifest,
				capabilities,
				cutoverAt,
				3,
				0,
			)
			if test.want == "" {
				if err != nil {
					t.Fatalf("verify reader projection: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verification error = %v, want %q", err, test.want)
			}
			if calls != test.wantCalls {
				t.Fatalf("status calls = %d, want %d", calls, test.wantCalls)
			}
		})
	}
}

func TestRecoverConcurrentGitcrawlSnapshotAdoptsOnlyMatchingCompletedSnapshot(t *testing.T) {
	snapshotID := strings.Repeat("a", 64)
	snapshot := gitcrawlCloudSnapshot{
		ID:                 snapshotID,
		SourceSyncAt:       "2026-07-12T12:00:00Z",
		DatasetGeneratedAt: "2026-07-12T12:01:00Z",
	}
	manifest := gitcrawlCloudManifest("gitcrawl/openclaw__openclaw", snapshot)
	publicationCapabilities := gitcrawlCloudPublicationCapabilities(manifest.Capabilities)
	const winnerGeneration = "2026-07-12T12:02:00Z"

	for _, test := range []struct {
		name             string
		activeSnapshotID string
		coverageComplete bool
		wantGeneration   string
		want             string
	}{
		{
			name:             "independent winner generation",
			activeSnapshotID: snapshotID,
			coverageComplete: true,
			wantGeneration:   winnerGeneration,
		},
		{
			name:             "unrelated active candidate",
			activeSnapshotID: strings.Repeat("b", 64),
			coverageComplete: true,
			want:             "does not match the requested digest, profile, and coverage",
		},
		{
			name:             "incomplete candidate",
			activeSnapshotID: snapshotID,
			want:             "does not match the requested digest, profile, and coverage",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("snapshot_id"); got != snapshotID {
					http.Error(w, fmt.Sprintf("snapshot_id = %q, want %q", got, snapshotID), http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(crawlremote.PublisherStatus{
					App:              manifest.App,
					Archive:          manifest.Archive,
					ActiveSnapshotID: test.activeSnapshotID,
					CoverageComplete: test.coverageComplete,
					Snapshot: &crawlremote.ArchiveSnapshot{
						ID:                 snapshotID,
						SourceSHA256:       snapshotID,
						SourceSyncAt:       snapshot.SourceSyncAt,
						DatasetGeneratedAt: winnerGeneration,
						SchemaName:         manifest.SchemaName,
						SchemaVersion:      manifest.SchemaVersion,
						SchemaHash:         manifest.SchemaHash,
						Capabilities:       publicationCapabilities,
						CoverageComplete:   test.coverageComplete,
					},
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

			generation, err := recoverConcurrentGitcrawlSnapshot(
				context.Background(),
				client,
				manifest.Archive,
				snapshot,
				manifest,
				publicationCapabilities,
				&crawlremote.Error{
					Status:  http.StatusConflict,
					Code:    "snapshot_active",
					Message: "a concurrent publisher completed the snapshot",
				},
			)
			if test.want != "" {
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("recovery error = %v, want %q", err, test.want)
				}
				if generation != "" {
					t.Fatalf("recovered generation = %q, want none", generation)
				}
				return
			}
			if err != nil {
				t.Fatalf("recover concurrent snapshot: %v", err)
			}
			if generation != test.wantGeneration {
				t.Fatalf("recovered generation = %q, want %q", generation, test.wantGeneration)
			}
		})
	}
}

func TestVerifyGitcrawlSnapshotPublicationRejectsUnreadableOrMismatchedSQLite(t *testing.T) {
	source := []byte("SQLite format 3\x00bound source")
	snapshotID := fmt.Sprintf("%x", sha256.Sum256(source))
	const cutoverAt = "2026-07-12T12:02:00Z"
	snapshot := gitcrawlCloudSnapshot{
		ID:                 snapshotID,
		SourceSyncAt:       "2026-07-12T12:00:00Z",
		DatasetGeneratedAt: "2026-07-12T12:01:00Z",
	}
	manifest := gitcrawlCloudManifest("gitcrawl/openclaw__openclaw", snapshot)
	publicationCapabilities := gitcrawlCloudPublicationCapabilities(manifest.Capabilities)

	for _, test := range []struct {
		name             string
		statusGeneration string
		statusCode       int
		body             []byte
		want             string
	}{
		{
			name:             "publisher generation mismatch",
			statusGeneration: "2026-07-12T12:02:00Z",
			statusCode:       http.StatusOK,
			body:             source,
			want:             "post-cutover publisher status does not match",
		},
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
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/status"):
					_ = json.NewEncoder(w).Encode(crawlremote.Status{
						App:                manifest.App,
						Archive:            manifest.Archive,
						Mode:               "cloud",
						SchemaName:         manifest.SchemaName,
						SchemaVersion:      manifest.SchemaVersion,
						SchemaHash:         manifest.SchemaHash,
						Capabilities:       publicationCapabilities,
						SnapshotMode:       "snapshot",
						SnapshotCutoverAt:  cutoverAt,
						ActiveSnapshotID:   snapshot.ID,
						SourceSyncAt:       snapshot.SourceSyncAt,
						DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
						CoverageComplete:   true,
						Snapshot: &crawlremote.ArchiveSnapshot{
							ID:                 snapshot.ID,
							SourceSHA256:       snapshot.ID,
							SourceSyncAt:       snapshot.SourceSyncAt,
							DatasetGeneratedAt: snapshot.DatasetGeneratedAt,
							SchemaName:         manifest.SchemaName,
							SchemaVersion:      manifest.SchemaVersion,
							SchemaHash:         manifest.SchemaHash,
							Capabilities:       publicationCapabilities,
							CoverageComplete:   true,
							CutoverAt:          cutoverAt,
						},
					})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.EscapedPath(), "/publish-status"):
					if got := r.URL.Query().Get("snapshot_id"); got != snapshotID {
						http.Error(
							w,
							fmt.Sprintf("snapshot_id = %q, want %q", got, snapshotID),
							http.StatusBadRequest,
						)
						return
					}
					w.Header().Set("content-type", "application/json")
					statusGeneration := snapshot.DatasetGeneratedAt
					if test.statusGeneration != "" {
						statusGeneration = test.statusGeneration
					}
					_ = json.NewEncoder(w).Encode(crawlremote.PublisherStatus{
						App:              "gitcrawl",
						Archive:          manifest.Archive,
						ActiveSnapshotID: snapshotID,
						CoverageComplete: true,
						Snapshot: &crawlremote.ArchiveSnapshot{
							ID:                 snapshotID,
							SourceSHA256:       snapshotID,
							SourceSyncAt:       snapshot.SourceSyncAt,
							DatasetGeneratedAt: statusGeneration,
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
				cutoverAt,
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
