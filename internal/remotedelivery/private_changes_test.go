package remotedelivery

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/remote"
)

func changesRequest() PrivateChangesRequest {
	return PrivateChangesRequest{Archive: "fixture/archive", BindingID: "fixture-binding", CaptureID: strings.Repeat("3", 32),
		NativeFrom:     "gitcrawl-native-v1:" + strings.Repeat("1", 32) + ":1:7:0:0:" + strings.Repeat("2", 64) + ":" + strings.Repeat("a", 64),
		NativeThrough:  "gitcrawl-native-v2:" + strings.Repeat("1", 32) + ":1:7:1:0:" + strings.Repeat("2", 64) + ":" + strings.Repeat("b", 64),
		ExpectedCursor: "seed1:fixture:complete", MaxBytes: 16 << 20, MaxParts: 4}
}

func changeValue(kind, value string) remote.ChangeValue {
	return remote.ChangeValue{Kind: kind, Value: &value}
}

func repositoryChange(id string, raw string) remote.Change {
	key := changeValue("integer", id)
	return remote.Change{Table: "repositories", Operation: "upsert", Key: []remote.ChangeValue{key},
		Row: []remote.ChangeValue{key, changeValue("text", "fixture"), changeValue("text", "repo"), changeValue("text", "fixture/repo"),
			{Kind: "null"}, changeValue("text", raw), changeValue("text", "2026-01-01")}}
}

func TestPrivateChangesMultipartRetainsRowsAndCursors(t *testing.T) {
	req := changesRequest()
	changes := []remote.Change{repositoryChange("9007199254740993", "raw\x00☃")}
	for i := 0; i < 500; i++ {
		changes = append(changes, remote.Change{Table: "threads", Operation: "remove", Key: []remote.ChangeValue{changeValue("integer", strconv.Itoa(i+1))}})
	}
	got, err := PreparePrivateChanges(context.Background(), changes, req)
	if err != nil || len(got.Parts) != 2 {
		t.Fatal("multipart preparation", len(got.Parts), err)
	}
	var recovered []remote.Change
	var total int64
	cursor := req.ExpectedCursor
	for i, body := range got.Parts {
		batch, err := remote.DecodeChangeBatch(body)
		if err != nil {
			t.Fatal(err)
		}
		if batch.App != "gitcrawl" || batch.Archive != req.Archive || batch.BindingID != req.BindingID || batch.CaptureID != req.CaptureID || batch.NativeFrom != req.NativeFrom || batch.NativeThrough != req.NativeThrough || batch.PartIndex != i || batch.PartCount != 2 || batch.ExpectedCursor != cursor {
			t.Fatalf("incorrect part identity: %+v", batch)
		}
		recovered = append(recovered, batch.Changes...)
		total += int64(len(body))
		cursor = batch.BatchID()
	}
	if !reflect.DeepEqual(recovered, changes) || total != got.Bytes {
		t.Fatal("typed values, order or byte accounting changed")
	}
	again, err := PreparePrivateChanges(context.Background(), changes, req)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("preparation is not deterministic", err)
	}
	req.MaxBytes = got.Bytes - 1
	if partial, err := PreparePrivateChanges(context.Background(), changes, req); !errors.Is(err, ErrPrivateChangesLimit) || !reflect.DeepEqual(partial, PrivateChanges{}) {
		t.Fatal("byte limit exposed partial output", err)
	}
	req = changesRequest()
	req.MaxParts = 1
	if _, err := PreparePrivateChanges(context.Background(), changes, req); !errors.Is(err, ErrPrivateChangesLimit) {
		t.Fatal("part limit accepted", err)
	}
}

func TestPrivateChangesEmptyTransitionAndByteSplit(t *testing.T) {
	req := changesRequest()
	req.NativeThrough = strings.Replace(req.NativeThrough, ":1:7:1:0:", ":1:7:0:0:", 1)
	empty, err := PreparePrivateChanges(context.Background(), nil, req)
	if err != nil || len(empty.Parts) != 1 {
		t.Fatal("empty v1 to v2 transition", err)
	}
	batch, err := remote.DecodeChangeBatch(empty.Parts[0])
	if err != nil || batch.Changes == nil || len(batch.Changes) != 0 || batch.ExpectedCursor != req.ExpectedCursor {
		t.Fatal("empty checkpoint lost", err)
	}
	// JSON escaping, rather than row count, forces two independently valid parts.
	changes := []remote.Change{repositoryChange("1", strings.Repeat("\x00", 400000)), repositoryChange("2", strings.Repeat("\x00", 400000))}
	packed, err := PreparePrivateChanges(context.Background(), changes, changesRequest())
	if err != nil || len(packed.Parts) != 2 {
		t.Fatal("encoded-byte split", len(packed.Parts), err)
	}
	for _, body := range packed.Parts {
		if len(body) > remote.MaxChangeBytes {
			t.Fatal("oversized wire part")
		}
		if _, err := remote.DecodeChangeBatch(body); err != nil {
			t.Fatal(err)
		}
	}
	req = changesRequest()
	req.MaxParts = 1
	if _, err := PreparePrivateChanges(context.Background(), changes, req); !errors.Is(err, ErrPrivateChangesLimit) {
		t.Fatal("byte-driven part limit accepted", err)
	}
}

func TestPrivateChangesRefusesInvalidRangesAndRows(t *testing.T) {
	base := repositoryChange("1", "raw")
	for name, change := range map[string]remote.Change{
		"unknown table": {Table: "future", Operation: "remove", Key: base.Key},
		"null key":      {Table: "repositories", Operation: "remove", Key: []remote.ChangeValue{{Kind: "null"}}},
		"removal body":  {Table: "repositories", Operation: "remove", Key: base.Key, Row: base.Row},
		"row width":     {Table: "repositories", Operation: "upsert", Key: base.Key, Row: base.Row[:2]},
		"invalid utf8":  repositoryChange("1", string([]byte{0xff})),
	} {
		t.Run(name, func(t *testing.T) {
			if out, err := PreparePrivateChanges(context.Background(), []remote.Change{change}, changesRequest()); err == nil || !reflect.DeepEqual(out, PrivateChanges{}) {
				t.Fatal("invalid row accepted", err)
			}
		})
	}
	if _, err := PreparePrivateChanges(context.Background(), []remote.Change{base, base}, changesRequest()); !errors.Is(err, ErrPrivateChanges) {
		t.Fatal("duplicate native key accepted", err)
	}
	for name, modify := range map[string]func(*PrivateChangesRequest){
		"malformed": func(r *PrivateChangesRequest) { r.NativeFrom = "invalid" },
		"generation": func(r *PrivateChangesRequest) {
			r.NativeThrough = strings.Replace(r.NativeThrough, ":1:7:", ":2:7:", 1)
		},
		"v2 same H different digest": func(r *PrivateChangesRequest) {
			r.NativeFrom = strings.Replace(r.NativeFrom, "native-v1", "native-v2", 1)
			r.NativeThrough = strings.Replace(r.NativeThrough, ":1:7:1:0:", ":1:7:0:0:", 1)
		},
		"empty binding":   func(r *PrivateChangesRequest) { r.BindingID = "" },
		"empty cursor":    func(r *PrivateChangesRequest) { r.ExpectedCursor = "" },
		"invalid capture": func(r *PrivateChangesRequest) { r.CaptureID = "invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			r := changesRequest()
			modify(&r)
			if _, err := PreparePrivateChanges(context.Background(), nil, r); !errors.Is(err, ErrPrivateChanges) {
				t.Fatal("invalid metadata accepted", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PreparePrivateChanges(ctx, nil, changesRequest()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	changed := base
	changed.Row = slices.Clone(base.Row)
	changed.Row[0] = changeValue("integer", "2")
	if _, err := PreparePrivateChanges(context.Background(), []remote.Change{changed}, changesRequest()); !errors.Is(err, ErrPrivateChanges) {
		t.Fatal("mismatched row key accepted", err)
	}
}
