//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

func reportCycleArgs(db, path string, parts int) []string {
	args := reportStageArgs(db, path)
	args[1] = "cycle"
	for i := range args {
		if args[i] == "--max-parts" {
			args[i+1] = strconv.Itoa(parts)
		}
	}
	return append(args, "--token-env", "GITCRAWL_REPORT_CLI_TOKEN")
}

func reportCycleApp(t *testing.T, ctx context.Context, args ...string) ([]reportDeliveryOutput, error) {
	t.Helper()
	a := New()
	var out, diagnostic bytes.Buffer
	a.Stdout, a.Stderr = &out, &diagnostic
	a.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Error("cycle acquired GitHub credentials")
		return "", errors.New("must not run")
	}
	err := a.Run(ctx, args)
	all := out.String() + diagnostic.String()
	if err != nil {
		all += err.Error()
	}
	for _, private := range []string{deliveryPrivate, "fixture-service-secret", "private-fixture-response"} {
		if strings.Contains(all, private) {
			t.Fatal("private value escaped cycle")
		}
	}
	if len(all) > 4096 {
		t.Fatal("unbounded cycle output")
	}
	var outputs []reportDeliveryOutput
	decoder := json.NewDecoder(&out)
	for {
		var output reportDeliveryOutput
		if decodeErr := decoder.Decode(&output); errors.Is(decodeErr, io.EOF) {
			break
		} else if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		phases := []string{"resume", "capture", "deliver"}
		if len(outputs) >= len(phases) || output.Phase != phases[len(outputs)] {
			t.Fatal("invalid cycle phase", output.Phase)
		}
		outputs = append(outputs, output)
	}
	return outputs, err
}

func TestReportDeliveryCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var mu sync.Mutex
	var path, cursor string
	var sequence int
	calls := 0
	failReply := false
	var cancelRequest context.CancelFunc
	seen := map[string]remote.ChangeReceipt{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		body, err := io.ReadAll(io.LimitReader(r.Body, remote.MaxChangeBytes+1))
		batch, decodeErr := remote.DecodeChangeBatch(body)
		if err != nil || decodeErr != nil || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer fixture-service-secret" {
			t.Error("invalid change request", err, decodeErr)
			w.WriteHeader(400)
			return
		}
		saved, err := reportSavedChangeBodies(ctx, path)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		exact := false
		for _, candidate := range saved {
			exact = exact || bytes.Equal(candidate, body)
		}
		if !exact {
			t.Error("request differs from saved bytes")
			w.WriteHeader(400)
			return
		}
		receipt, replayed := seen[batch.BatchID()]
		if !replayed {
			if batch.ExpectedCursor != cursor {
				t.Error("wrong predecessor")
				w.WriteHeader(409)
				return
			}
			sequence++
			cursor = batch.BatchID()
			digest := sha256.Sum256(body)
			receipt = remote.ChangeReceipt{App: batch.App, Archive: batch.Archive, BindingID: batch.BindingID, BatchID: batch.BatchID(), Cursor: cursor, Sequence: strconv.Itoa(sequence), PayloadSHA256: hex.EncodeToString(digest[:])}
			seen[batch.BatchID()] = receipt
		}
		if cancelRequest != nil {
			cancelRequest()
			cancelRequest = nil
		}
		if failReply {
			failReply = false
			w.WriteHeader(503)
			io.WriteString(w, "private-fixture-response")
			return
		}
		receipt.Replayed = replayed
		json.NewEncoder(w).Encode(receipt)
	}))
	defer server.Close()
	native, outbox, seed := reportCLISeed(t, ctx, server.URL, true)
	path, cursor, sequence = outbox, seed.ServerCursor, seed.PartCount
	for _, version := range []int{2, 3} {
		if _, err := reportDeliveryApp(t, ctx, reportUpgradeArgs(path, version)...); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("GITCRAWL_REPORT_CLI_TOKEN", "fixture-service-secret")
	cycle, err := reportCycleApp(t, ctx, reportCycleArgs(native, path, 10)...)
	if err != nil || len(cycle) != 3 || cycle[0].Status != "idle" || cycle[1].Status != "staged" || cycle[2].Status != "complete" {
		t.Fatal("initial v1 to v2 transition", cycle, err)
	}
	status, err := reportDeliveryApp(t, ctx, reportStatusArgs(path)...)
	if err != nil || !strings.HasPrefix(status.NativeThrough, remote.GitHubNativeV2+":") {
		t.Fatal("missing v2 baseline", status, err)
	}
	cycle, err = reportCycleApp(t, ctx, reportCycleArgs(native, path, 10)...)
	mu.Lock()
	initialCalls := calls
	mu.Unlock()
	if err != nil || len(cycle) != 3 || cycle[1].Status != "idle" || cycle[2].Status != "idle" || initialCalls != 1 {
		t.Fatal("stable cycle published empty capture", cycle, initialCalls, err)
	}
	mutate := func(query string) {
		t.Helper()
		s, err := store.Open(ctx, native)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.DB().ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	mutate(`with recursive n(i) as (values(1) union all select i+1 from n where i<501) insert into comments(thread_id,github_id,comment_type,body,raw_json) select 1,'cycle-'||i,'issue_comment','initial','{}' from n`)
	staged, err := reportDeliveryApp(t, ctx, reportStageArgs(native, path)...)
	if err != nil {
		t.Fatal(err)
	}
	mutate(`update comments set body='newer' where github_id='cycle-1'`)
	cycle, err = reportCycleApp(t, ctx, reportCycleArgs(native, path, 1)...)
	if err != nil || len(cycle) != 1 || cycle[0].Status != "part_limit" || cycle[0].CaptureID != staged.CaptureID {
		t.Fatal("part limit captured successor", cycle, err)
	}
	mu.Lock()
	failReply = true
	mu.Unlock()
	cycle, err = reportCycleApp(t, ctx, reportCycleArgs(native, path, 10)...)
	if err == nil || len(cycle) != 1 || cycle[0].Status != "partial" || cycle[0].CaptureID != staged.CaptureID {
		t.Fatal("failed resume captured successor", cycle, err)
	}
	cycle, err = reportCycleApp(t, ctx, reportCycleArgs(native, path, 10)...)
	if err != nil || len(cycle) != 3 || cycle[0].CaptureID != staged.CaptureID || cycle[0].Status != "complete" || cycle[1].Rows != 1 || cycle[2].Status != "complete" || cycle[2].CaptureID == staged.CaptureID {
		t.Fatal("resume then successor", cycle, err)
	}
	mutate(`insert into repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) values(2,'other','repo','other/repo','200','{}','now')`)
	cycle, err = reportCycleApp(t, ctx, reportCycleArgs(native, path, 10)...)
	if err != nil || len(cycle) != 3 || cycle[1].Rows != 0 || cycle[1].Status != "staged" || cycle[2].Status != "complete" {
		t.Fatal("advancing empty checkpoint lost", cycle, err)
	}
	mutate(`update comments set body='cancelled' where github_id='cycle-1'`)
	if _, err := reportDeliveryApp(t, ctx, reportStageArgs(native, path)...); err != nil {
		t.Fatal(err)
	}
	mutate(`update comments set body='after-cancel' where github_id='cycle-1'`)
	cancelCtx, stop := context.WithCancel(ctx)
	defer stop()
	mu.Lock()
	cancelRequest = stop
	mu.Unlock()
	cycle, err = reportCycleApp(t, cancelCtx, reportCycleArgs(native, path, 10)...)
	if err == nil || len(cycle) != 1 {
		t.Fatal("cancelled resume captured successor", cycle, err)
	}
}
