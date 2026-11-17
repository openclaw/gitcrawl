//go:build darwin || linux

package remotedelivery

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestVerifyReportFullCurrentParity(t *testing.T) {
	for _, scenario := range []string{"complete", "body suffix", "null becomes empty", "int64 differs", "numeric scalar", "missing", "extra", "duplicate", "wrong columns", "wrong pin", "unordered", "repeated cursor", "remote pending", "binding changes", "outbox changes", "outbox replaced", "local pending", "inventory differs", "row bound", "byte bound", "oversized response", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, native := nativeFixture(t)
			longBody := strings.Repeat("retained body ", 1000) + "original suffix"
			if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "9007199254740993", CommentType: "issue_comment", Body: longBody, RawJSON: "{}"}); err != nil {
				t.Fatal(err)
			}
			if _, err := native.DB().ExecContext(ctx, `update threads set title='',body=NULL,observation_sequence=-9223372036854775807,evidence_observation_sequence=9223372036854775807 where id=1; update comments set is_bot=-9223372036854775808`); err != nil {
				t.Fatal(err)
			}
			if _, err := native.ReserveThreadChildObservation(ctx, 1, store.ThreadChildComments, "2026-01-01", 1); err != nil {
				t.Fatal(err)
			}
			if err := native.ReplaceThreadChildObservationMembers(ctx, 1, store.ThreadChildComments, 1, []int64{1}); err != nil {
				t.Fatal(err)
			}
			if _, err := native.DB().ExecContext(ctx, `insert into thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,plan_id) values('fixture/repo',999,'issue',999,'gone','2026-01-01','owner_requested','fixture'); insert into thread_excluded_nodes(node_id,repository,number) values('absent-node','fixture/repo',999)`); err != nil {
				t.Fatal(err)
			}
			capture, report := projected(t, ctx, native)
			var state ReportOutboxState
			var seed string
			var outbox *Outbox
			var path string
			calls, bindings, threadPages := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/query") || r.Header.Get("Authorization") != "Bearer fixture-reader" {
					t.Error("verification used a non-reader request")
					w.WriteHeader(400)
					return
				}
				var request remote.QueryRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				sequence := strconv.FormatInt(state.ReceiverSequence, 10)
				pin := "pg-gitcrawl-current-v1:" + seed + ":" + sequence
				if request.App != "gitcrawl" || request.Archive != state.Registration.Archive || request.SnapshotID != pin {
					t.Error("reader request changed the source or snapshot")
				}
				result := remote.QueryResult{Stats: remote.QueryStats{SnapshotID: pin, SchemaVersion: 1, ObservationOrder: "completed-publication/observation-sequence-ordinal"}}
				if request.Name == "gitcrawl.report-inputs.binding" {
					bindings++
					result.Columns = strings.Fields("profile archive binding_id seed_id native_profile native_schema repository repository_id scope_sha256 published_native inventory_sha256 published_sequence transport_sequence transport_cursor publication_batch_id partial_capture pending_capture pending_native_from pending_native_through pending_part_count pending_next_part")
					result.Rows = [][]any{{"gitcrawl-report-inputs/v1", state.Registration.Archive, state.Registration.BindingID, seed, PrivateReportProfile, "16", state.Registration.Repository, strconv.FormatInt(state.BaselineAnchor.RepositoryID, 10), state.BaselineAnchor.ScopeSHA256, state.BaselineNative, state.InventorySHA256, sequence, sequence, state.ServerCursor, state.ServerCursor, false, nil, nil, nil, nil, nil}}
					if scenario == "remote pending" {
						result.Rows[0][15] = true
					}
					if scenario == "binding changes" && bindings == 2 {
						result.Rows[0][2] = "other-binding"
					}
					if scenario == "outbox changes" && bindings == 2 {
						// All row comparisons have finished; the final connection
						// check must still reject a changed compact inventory.
						if _, err := outbox.base.DB().ExecContext(ctx, `update inventory_rows set row_sha256=zeroblob(32)`); err != nil {
							t.Error(err)
						}
					}
					if scenario == "outbox replaced" && bindings == 2 {
						if err := os.WriteFile(path+".replacement", []byte("replacement"), 0o600); err != nil {
							t.Error(err)
						}
						if err := os.Rename(path+".replacement", path); err != nil {
							t.Error(err)
						}
					}
				} else {
					name := strings.TrimSuffix(strings.TrimPrefix(request.Name, "gitcrawl."), ".current")
					for _, table := range report.Tables {
						if table.Name != name {
							continue
						}
						result.Columns = append(slices.Clone(table.Columns), "sequence", "ordinal")
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
					if name == "threads" {
						threadPages++
						if request.Cursor == "" {
							result.Rows, result.Cursor = result.Rows[:1], "next-thread"
						} else if request.Cursor == "next-thread" {
							result.Rows = result.Rows[1:]
							if scenario == "repeated cursor" {
								result.Cursor = "next-thread"
							}
							if scenario == "duplicate" {
								result.Rows[0][0] = *report.Tables[1].Rows[0][0].Value
							}
							if scenario == "unordered" {
								result.Rows[0][len(result.Columns)-1] = "0"
							}
						} else {
							t.Error("unexpected pagination cursor")
						}
						if threadPages == 1 {
							switch scenario {
							case "null becomes empty":
								result.Rows[0][slices.Index(result.Columns, "body")] = ""
							case "int64 differs":
								result.Rows[0][slices.Index(result.Columns, "evidence_observation_sequence")] = "9223372036854775806"
							case "numeric scalar":
								result.Rows[0][0] = 1
							case "extra":
								result.Rows[0][0] = "9007199254740993"
							case "wrong columns":
								result.Columns[0] = "wrong"
							case "wrong pin":
								result.Stats.SnapshotID = "later-publication"
							}
						}
					}
					if name == "comments" {
						switch scenario {
						case "body suffix":
							result.Rows[0][slices.Index(result.Columns, "body")] = strings.TrimSuffix(longBody, "original suffix") + "changed suffix"
						case "missing":
							result.Rows = nil
						}
					}
				}
				if scenario == "oversized response" {
					_, _ = w.Write([]byte(strings.Repeat(" ", (8<<20)+1)))
					return
				}
				_ = json.NewEncoder(w).Encode(result)
			}))
			defer server.Close()
			path = filepath.Join(t.TempDir(), "report.sqlite")
			registration := seedRegistration(capture)
			registration.Origin = server.URL
			var err error
			outbox, err = CreateSeedOutbox(ctx, path, registration, capture, StageLimits{MaxEncodedBytes: 32 << 20})
			if err != nil {
				t.Fatal(err)
			}
			seed = outboxState(t, ctx, outbox).SeedID
			completeInventorySeed(t, ctx, outbox)
			outbox.Close()
			if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
				t.Fatal(err)
			}
			if err := MigrateReportOutboxV3(ctx, path); err != nil {
				t.Fatal(err)
			}
			outbox, err = OpenReportOutbox(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			state, err = ReadReportOutboxState(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "local pending" {
				if _, err := outbox.base.DB().ExecContext(ctx, `update parts set receipt_json=null where part_index=0`); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "inventory differs" {
				if _, err := outbox.base.DB().ExecContext(ctx, `update inventory_rows set row_sha256=zeroblob(32) where table_name='threads'`); err != nil {
					t.Fatal(err)
				}
			}
			limits := VerificationLimits{MaxRows: 1000, MaxBytes: 32 << 20}
			if scenario == "row bound" {
				limits.MaxRows = 1
			}
			if scenario == "byte bound" {
				limits.MaxBytes = 2000
			}
			if scenario == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			// The full verifier never opens the native source.
			native.Close()
			if err := os.Remove(native.Path()); err != nil {
				t.Fatal(err)
			}
			result, err := VerifyReport(ctx, path, remote.StaticToken("fixture-reader"), nil, limits)
			if scenario == "complete" {
				if err != nil || result.Rows != capture.Rows || result.InventorySHA256 != state.InventorySHA256 || threadPages != 2 || bindings != 2 || calls != 10 || result.EncodedBytes <= int64(len(longBody)) {
					t.Fatalf("full pinned parity failed: result=%+v calls=%d pages=%d bindings=%d error=%v", result, calls, threadPages, bindings, err)
				}
			} else if err == nil || result != (ReportVerification{}) {
				t.Fatalf("unqualified proof escaped: %+v error=%v", result, err)
			}
			if scenario == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if (scenario == "local pending" || scenario == "inventory differs" || scenario == "row bound" || scenario == "cancelled") && calls != 0 {
				t.Fatalf("invalid outbox acquired reader credentials: calls=%d", calls)
			}
		})
	}
}

func TestVerifyReportRefusesActiveCaptureBeforeCredentials(t *testing.T) {
	ctx, native, outbox, path, _ := changesFixture(t)
	state, delta := captureChanges(t, ctx, native, outbox)
	if _, err := outbox.StageReportChanges(ctx, state, delta, changeTestLimits); err != nil {
		t.Fatal(err)
	}
	result, err := VerifyReport(ctx, path, nil, nil, VerificationLimits{MaxRows: 1000, MaxBytes: 32 << 20})
	if !errors.Is(err, ErrSeedOutbox) || result != (ReportVerification{}) {
		t.Fatalf("active capture reached credentials or produced proof: %+v %v", result, err)
	}
}
