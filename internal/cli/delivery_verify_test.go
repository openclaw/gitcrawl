//go:build darwin || linux

package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestVerifyReportCLICompleteReadOnlyParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var state remotedelivery.ReportOutboxState
	var seed string
	var report remotedelivery.PrivateReport
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/apps/gitcrawl/archives/fixture%2Farchive/query" && r.URL.Path != "/v1/apps/gitcrawl/archives/fixture/archive/query" || r.Header.Get("Authorization") != "Bearer fixture-service-secret" {
			t.Errorf("unexpected reader request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
			return
		}
		var request remote.QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		queries = append(queries, request.Name)
		sequence := strconv.FormatInt(state.ReceiverSequence, 10)
		pin := "pg-gitcrawl-current-v1:" + seed + ":" + sequence
		if request.SnapshotID != pin || request.Cursor != "" {
			t.Error("query was not pinned")
		}
		result := remote.QueryResult{Stats: remote.QueryStats{SnapshotID: pin, SchemaVersion: 1, ObservationOrder: "completed-publication/observation-sequence-ordinal"}}
		if request.Name == "gitcrawl.report-inputs.binding" {
			result.Columns = strings.Fields("profile archive binding_id seed_id native_profile native_schema repository repository_id scope_sha256 published_native inventory_sha256 published_sequence transport_sequence transport_cursor publication_batch_id partial_capture pending_capture pending_native_from pending_native_through pending_part_count pending_next_part")
			result.Rows = [][]any{{"gitcrawl-report-inputs/v1", state.Registration.Archive, state.Registration.BindingID, seed, remotedelivery.PrivateReportProfile, "16", state.Registration.Repository, strconv.FormatInt(state.BaselineAnchor.RepositoryID, 10), state.BaselineAnchor.ScopeSHA256, state.BaselineNative, state.InventorySHA256, sequence, sequence, state.ServerCursor, state.ServerCursor, false, nil, nil, nil, nil, nil}}
		} else {
			for _, table := range report.Tables {
				if request.Name != "gitcrawl."+table.Name+".current" {
					continue
				}
				result.Columns = append(table.Columns, "sequence", "ordinal")
				for ordinal, cells := range table.Rows {
					row := make([]any, len(cells))
					for i, cell := range cells {
						if cell.Value != nil {
							row[i] = *cell.Value
						}
					}
					result.Rows = append(result.Rows, append(row, "1", strconv.Itoa(ordinal)))
				}
			}
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()
	native, path, original := reportCLISeed(t, ctx, server.URL, false)
	seed = original.SeedID
	if err := remotedelivery.MigrateSeedOutboxV2(ctx, path, remotedelivery.InventoryMigrationLimits{MaxRows: 2000, MaxBytes: 16 << 20}); err != nil {
		t.Fatal(err)
	}
	var err error
	state, err = remotedelivery.ReadReportOutboxState(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := store.CaptureNativeReport(ctx, native, store.NativeReportRequest{Repository: "fixture/repo", File: original.Registration.File, Limits: store.NativeReportLimits{MaxRows: 2000, MaxBytes: 16 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	report, err = remotedelivery.ProjectPrivateReport(capture)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_REPORT_VERIFY_TOKEN", "fixture-service-secret")
	result, err := reportDeliveryApp(t, ctx, "delivery", "verify-report", "--outbox", path, "--timeout", "10s", "--max-rows", "2000", "--max-encoded-bytes", "16777216", "--token-env", "GITCRAWL_REPORT_VERIFY_TOKEN", "--json")
	if err != nil || result.Status != "verified" || result.Rows != capture.Rows || len(queries) != 9 {
		t.Fatalf("complete verification: status=%s rows=%d queries=%d err=%v", result.Status, result.Rows, len(queries), err)
	}
	current, err := remotedelivery.ReadReportOutboxState(ctx, path)
	if err != nil || current != state {
		t.Fatalf("verification changed the outbox: %v", err)
	}
}
