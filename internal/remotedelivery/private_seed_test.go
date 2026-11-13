//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

func seedRequest() PrivateSeedRequest {
	return PrivateSeedRequest{Archive: "fixture/archive", BindingID: "fixture-binding", SeedID: "0123456789abcdef0123456789abcdef", MaxBytes: 32 << 20}
}

func seedWireBytes(seed PrivateSeed) int64 {
	n := int64(len(seed.Manifest) + len(seed.Complete))
	for _, body := range seed.Chunks {
		n += int64(len(body))
	}
	return n
}

func verifyPrivateSeed(t *testing.T, report PrivateReport, seed PrivateSeed) {
	t.Helper()
	plan, err := remote.DecodeGitHubSeedManifest(seed.Manifest)
	if err != nil || !reflect.DeepEqual(plan.Manifest(), seed.Plan.Manifest()) {
		t.Fatal("manifest failed independent codec validation", err)
	}
	if _, err := plan.ValidateComplete(seed.Complete); err != nil {
		t.Fatal(err)
	}
	m := plan.Manifest()
	if len(m.Tables) != 7 || m.RepositoryID != report.RepositoryID || m.ScopeSHA256 != report.ScopeSHA256 || m.NativeSHA256 != report.SHA256 {
		t.Fatal("native capture identity changed")
	}
	chunkIndex := 0
	for tableIndex, table := range m.Tables {
		native := report.Tables[tableIndex]
		rowIndex := 0
		if table.Name != native.Name || table.Rows != strconv.Itoa(len(native.Rows)) || table.Chunks == nil {
			t.Fatal("table order/count/empty descriptor lost")
		}
		for range table.Chunks {
			body := seed.Chunks[chunkIndex]
			chunk, err := plan.ValidateChunk(body)
			if err != nil || chunk.Index != chunkIndex || len(body) > remote.MaxChangeBytes || len(chunk.Changes) > remote.MaxChanges {
				t.Fatal("chunk failed independent codec validation", err)
			}
			if bytes.Contains(body, []byte(`"value":null`)) {
				t.Fatal("DTO NULL representation leaked into wire")
			}
			for _, change := range chunk.Changes {
				if change.Table != table.Name || change.Operation != "upsert" || len(change.Row) != len(native.Columns) || len(change.Key) != len(native.KeyColumns) {
					t.Fatal("table/row/key layout changed")
				}
				for i, value := range change.Row {
					want := native.Rows[rowIndex][i]
					if value.Kind != want.Kind || !reflect.DeepEqual(value.Value, want.Value) {
						t.Fatal("native scalar/null/int64/text changed")
					}
				}
				for i, name := range native.KeyColumns {
					if !reflect.DeepEqual(change.Key[i], change.Row[slices.Index(native.Columns, name)]) {
						t.Fatal("native key order changed")
					}
				}
				rowIndex++
			}
			chunkIndex++
		}
		if rowIndex != len(native.Rows) {
			t.Fatal("native rows skipped")
		}
	}
	if chunkIndex != len(seed.Chunks) {
		t.Fatal("extra chunk")
	}
}

func TestPrivateSeedNativeCaptureToCodec(t *testing.T) {
	ctx, s := nativeFixture(t)
	_, h0 := projected(t, ctx, s)
	empty, err := PreparePrivateSeed(ctx, h0, seedRequest())
	if err != nil || !strings.Contains(empty.Plan.Manifest().NativeThrough, ":0:0:") {
		t.Fatal("H0 with empty content layouts refused", err)
	}
	verifyPrivateSeed(t, h0, empty)
	if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "9223372036854775808", CommentType: "pull_review", Body: "private\x00☃", RawJSON: `{"submitted_at":"2026-01-03T12:34:56.123456789Z","state":"APPROVED","private":"raw\u0000data"}`}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `update threads set title='',body=NULL,evidence_observation_sequence=9223372036854775807 where id=1; update comments set is_bot=-9223372036854775808`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveThreadChildObservation(ctx, 1, store.ThreadChildComments, "2026-01-03", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, store.ThreadChildComments, 1, []int64{1}); err != nil {
		t.Fatal(err)
	}
	_, report := projected(t, ctx, s)
	seed, err := PreparePrivateSeed(ctx, report, seedRequest())
	if err != nil {
		t.Fatal(err)
	}
	verifyPrivateSeed(t, report, seed)
	again, err := PreparePrivateSeed(ctx, report, seedRequest())
	if err != nil || !bytes.Equal(seed.Manifest, again.Manifest) || !reflect.DeepEqual(seed.Chunks, again.Chunks) || !bytes.Equal(seed.Complete, again.Complete) {
		t.Fatal("same report/seed ID changed exact bytes", err)
	}
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, store.ThreadChildComments, 1, []int64{}); err != nil {
		t.Fatal(err)
	}
	_, changed := projected(t, ctx, s)
	newSeed, err := PreparePrivateSeed(ctx, changed, seedRequest())
	if err != nil || changed.Cursor.Sequence != report.Cursor.Sequence || newSeed.Plan.Manifest().NativeThrough == seed.Plan.Manifest().NativeThrough {
		t.Fatal("same-head inventory change was collapsed", err)
	}
}

func TestPrivateSeedRowAndWireBounds(t *testing.T) {
	ctx, s := nativeFixture(t)
	err := s.WithTx(ctx, func(tx *store.Store) error {
		for i := range 501 {
			if _, err := tx.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: strconv.Itoa(i + 1), CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, report := projected(t, ctx, s)
	seed, err := PreparePrivateSeed(ctx, report, seedRequest())
	if err != nil {
		t.Fatal(err)
	}
	verifyPrivateSeed(t, report, seed)
	chunks := seed.Plan.Manifest().Tables[2].Chunks
	if len(chunks) != 2 || chunks[0].Rows != 500 || chunks[1].Rows != 1 {
		t.Fatal("500-row boundary changed")
	}
	request := seedRequest()
	request.MaxBytes = seedWireBytes(seed)
	if _, err := PreparePrivateSeed(ctx, report, request); err != nil {
		t.Fatal("exact total wire budget refused", err)
	}
	request.MaxBytes--
	if got, err := PreparePrivateSeed(ctx, report, request); !errors.Is(err, ErrPrivateSeedLimit) || !reflect.DeepEqual(got, PrivateSeed{}) {
		t.Fatal("partial output escaped total byte limit", err)
	}
}

func TestPrivateSeedEscapingAndOversizedNativeRow(t *testing.T) {
	ctx, s := nativeFixture(t)
	if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "first", CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	_, base := projected(t, ctx, s)
	small, err := PreparePrivateSeed(ctx, base, seedRequest())
	if err != nil {
		t.Fatal(err)
	}
	padding := remote.MaxChangeBytes - small.Plan.Manifest().Tables[2].Chunks[0].Bytes
	// JSON expands NUL to six bytes; account for the full envelope, not native
	// body length. The following small row must start another chunk.
	body := strings.Repeat("\x00", padding/6) + strings.Repeat("x", padding%6)
	if _, err := s.DB().ExecContext(ctx, `update comments set body=? where id=1`, body); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "second", CommentType: "issue_comment", Body: "next", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	_, report := projected(t, ctx, s)
	seed, err := PreparePrivateSeed(ctx, report, seedRequest())
	if err != nil {
		t.Fatal(err)
	}
	verifyPrivateSeed(t, report, seed)
	chunks := seed.Plan.Manifest().Tables[2].Chunks
	if len(chunks) != 2 || chunks[0].Rows != 1 || chunks[0].Bytes != remote.MaxChangeBytes || chunks[1].Rows != 1 {
		t.Fatal("exact escaped-byte chunk boundary changed")
	}
	if _, err := s.DB().ExecContext(ctx, `update comments set body=? where id=1`, strings.Repeat("x", remote.MaxChangeBytes+1)); err != nil {
		t.Fatal(err)
	}
	_, oversized := projected(t, ctx, s)
	if got, err := PreparePrivateSeed(ctx, oversized, seedRequest()); err == nil || !reflect.DeepEqual(got, PrivateSeed{}) {
		t.Fatal("oversized native cell yielded partial requests")
	}
}

func TestPrivateSeedErrorsAreZeroAndContentFree(t *testing.T) {
	ctx, s := nativeFixture(t)
	_, report := projected(t, ctx, s)
	for _, mutate := range []func(*PrivateReport){
		func(r *PrivateReport) { r.Tables = r.Tables[:6] },
		func(r *PrivateReport) { r.Tables[0].Columns = []string{"PRIVATE_INVALID"} },
		func(r *PrivateReport) { r.Tables[0].KeyColumns = []string{"PRIVATE_INVALID"} },
		func(r *PrivateReport) { r.Tables[0].Rows[0] = r.Tables[0].Rows[0][:1] },
		func(r *PrivateReport) { r.Rows = "0" },
		func(r *PrivateReport) { r.Rows = "4" },
		func(r *PrivateReport) { r.Cursor.Generation = "PRIVATE_INVALID" },
		func(r *PrivateReport) { value := "PRIVATE_INVALID\xff"; r.Tables[0].Rows[0][1].Value = &value },
		func(r *PrivateReport) { r.Tables[0].Rows[0][0].Kind = "PRIVATE_INVALID" },
	} {
		encoded, _ := json.Marshal(report)
		var bad PrivateReport
		_ = json.Unmarshal(encoded, &bad)
		mutate(&bad)
		got, err := PreparePrivateSeed(ctx, bad, seedRequest())
		if err == nil || !reflect.DeepEqual(got, PrivateSeed{}) || strings.Contains(err.Error(), "PRIVATE_INVALID") {
			t.Fatal("invalid report exposed partial result or content", err)
		}
	}
	for _, canceled := range []bool{false, true} {
		deadline := time.Now().Add(-time.Second)
		if canceled {
			deadline = time.Now().Add(time.Hour)
		}
		c, cancel := context.WithDeadline(ctx, deadline)
		if canceled {
			cancel()
		}
		got, err := PreparePrivateSeed(c, report, seedRequest())
		cancel()
		if !errors.Is(err, c.Err()) || !reflect.DeepEqual(got, PrivateSeed{}) {
			t.Fatal("context cause lost")
		}
	}
}
