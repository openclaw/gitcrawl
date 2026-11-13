//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

func reportDeliveryApp(t *testing.T, ctx context.Context, args ...string) (reportDeliveryOutput, error) {
	t.Helper()
	a := New()
	var out, diagnostic bytes.Buffer
	a.Stdout, a.Stderr = &out, &diagnostic
	a.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Error("report delivery acquired GitHub credentials")
		return "", errors.New("must not run")
	}
	err := a.Run(ctx, args)
	all := out.String() + diagnostic.String()
	if err != nil {
		all += err.Error()
	}
	for _, private := range []string{deliveryPrivate, "fixture-service-secret", "private-fixture-response", "credential-command-secret"} {
		if strings.Contains(all, private) {
			t.Fatal("private value escaped report CLI")
		}
	}
	if len(all) > 2048 {
		t.Fatal("unbounded report output")
	}
	var result reportDeliveryOutput
	if out.Len() > 0 {
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatalf("invalid report JSON: %v", err)
		}
	}
	return result, err
}

func reportUpgradeArgs(path string, version int) []string {
	args := []string{"delivery", "upgrade", "--outbox", path, "--to-version", strconv.Itoa(version), "--timeout", "10s", "--json"}
	if version == 2 {
		args = append(args, "--max-rows", "2000", "--max-encoded-bytes", "16777216")
	}
	return args
}

func reportStageArgs(db, path string) []string {
	return []string{"delivery", "stage-changes", "--db", db, "--outbox", path, "--max-journal-records", "2000", "--max-affected-keys", "2000", "--max-lookup-rows", "10000", "--max-rows", "2000", "--max-native-bytes", "16777216", "--max-encoded-bytes", "16777216", "--max-parts", "10", "--timeout", "10s", "--json"}
}

func reportSendArgs(db, path string, parts int) []string {
	return []string{"delivery", "deliver-changes", "--db", db, "--outbox", path, "--max-rows", "2000", "--max-encoded-bytes", "16777216", "--max-parts", strconv.Itoa(parts), "--timeout", "10s", "--token-env", "GITCRAWL_REPORT_CLI_TOKEN", "--json"}
}

func reportStatusArgs(path string) []string {
	return []string{"delivery", "report-status", "--outbox", path, "--timeout", "10s", "--json"}
}

func reportCLISeed(t *testing.T, ctx context.Context, origin string, indexes bool) (string, string, remotedelivery.SeedState) {
	t.Helper()
	native := deliveryNativeFixture(t)
	if indexes {
		s, err := store.Open(ctx, native)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.PrepareNativeReportIndexes(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "report-outbox.sqlite")
	if _, _, err := deliveryApp(t, ctx, deliveryStageArgs(native, path, origin)...); err != nil {
		t.Fatal(err)
	}
	// A trusted local receiver receipt fixture qualifies the existing seed;
	// its HTTP path is independently covered by TestDeliveryCLIExactHTTPResumeStatus.
	o, err := remotedelivery.OpenSeedOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	for {
		p, err := o.NextPart(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			break
		}
		suffix := strconv.Itoa(p.Ref.Index - 1)
		if p.Ref.Index == 0 {
			suffix = "begin"
		} else if p.Ref.Index == p.PartCount-1 {
			suffix = "complete"
		}
		id := "seed1:" + p.Ref.SeedID + ":" + suffix
		r := remote.SeedReceipt{App: "gitcrawl", Archive: "fixture/archive", BindingID: "fixture-binding", BatchID: id, Cursor: id, Sequence: strconv.Itoa(p.Ref.Index + 1), PayloadSHA256: p.SHA256}
		if err := o.RecordReceipt(ctx, p.Ref, r); err != nil {
			t.Fatal(err)
		}
	}
	s, err := o.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	return native, path, s
}

func TestReportDeliveryCLIUpgradeStageHTTPResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var mu sync.Mutex
	var sequence int64
	var cursor string
	calls := 0
	failLast := true
	seen := map[string]remote.ChangeReceipt{}
	saved := map[string][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		body, err := io.ReadAll(io.LimitReader(r.Body, remote.MaxChangeBytes+1))
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		batch, err := remote.DecodeChangeBatch(body)
		if err != nil || r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/changes") || r.Header.Get("Authorization") != "Bearer fixture-service-secret" {
			t.Error("invalid private change request", err)
			w.WriteHeader(400)
			return
		}
		id := batch.BatchID()
		if !bytes.Equal(body, saved[id]) {
			t.Error("request differs from saved exact bytes")
			w.WriteHeader(400)
			return
		}
		receipt, replayed := seen[id]
		if !replayed {
			if batch.ExpectedCursor != cursor {
				t.Error("receiver predecessor differs")
				w.WriteHeader(400)
				return
			}
			sequence++
			cursor = id
			hash := sha256.Sum256(body)
			receipt = remote.ChangeReceipt{App: batch.App, Archive: batch.Archive, BindingID: batch.BindingID, BatchID: id, Cursor: id, Sequence: strconv.FormatInt(sequence, 10), PayloadSHA256: hex.EncodeToString(hash[:])}
			seen[id] = receipt
		}
		if batch.PartIndex == batch.PartCount-1 && failLast {
			failLast = false
			w.WriteHeader(503)
			io.WriteString(w, "fixture-service-secret private-fixture-response")
			return
		}
		receipt.Replayed = replayed
		json.NewEncoder(w).Encode(receipt)
	}))
	defer server.Close()
	native, path, seed := reportCLISeed(t, ctx, server.URL, true)
	sequence, cursor = int64(seed.PartCount), seed.ServerCursor
	if _, err := reportDeliveryApp(t, ctx, reportUpgradeArgs(path, 3)...); err == nil {
		t.Fatal("version1 silently upgraded through2")
	}
	if _, err := remotedelivery.ReadSeedOutboxState(ctx, path); err != nil {
		t.Fatal("refused upgrade changed seed", err)
	}
	// Neither upgrade reads the native source; the unchanged inode is restored
	// before later capture. All paths here belong to this disposable fixture.
	if err := os.Rename(native, native+".held"); err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{2, 3} {
		result, err := reportDeliveryApp(t, ctx, reportUpgradeArgs(path, version)...)
		if err != nil || result.Status != "upgraded" || result.ToVersion != version {
			t.Fatal("explicit upgrade", result, err)
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the read-only image separate: SQLite may create read-only WAL/SHM
	// coordination files, so chmodding only its main file back is not a valid
	// fixture transition to writable custody.
	readonly := filepath.Join(t.TempDir(), "readonly-outbox.sqlite")
	if err := os.WriteFile(readonly, before, 0o400); err != nil {
		t.Fatal(err)
	}
	status, err := reportDeliveryApp(t, ctx, reportStatusArgs(readonly)...)
	if err != nil || status.NativeThrough != seed.BaselineNative || status.ReceiverSequence != strconv.Itoa(seed.PartCount) || status.ActiveCaptureID != "" || status.Receipts != nil {
		t.Fatal("readonly status", status, err)
	}
	after, err := os.ReadFile(readonly)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(readonly)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || info.Mode().Perm() != 0o400 {
		t.Fatal("status wrote or chmodded outbox")
	}
	if err := os.Rename(native+".held", native); err != nil {
		t.Fatal(err)
	}
	// Idle delivery needs neither a valid source pathname nor an available token.
	t.Setenv("GITCRAWL_REPORT_CLI_TOKEN", "")
	idle, err := reportDeliveryApp(t, ctx, reportSendArgs(filepath.Join(t.TempDir(), "absent"), path, 1)...)
	if err != nil || idle.Status != "idle" || idle.CaptureID != "" {
		t.Fatal("idle sender acquired token/source", idle, err)
	}
	s, err := store.Open(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `with recursive n(i) as (values(1) union all select i+1 from n where i<501) insert into comments(thread_id,github_id,comment_type,body,raw_json) select 1,'new-'||i,'issue_comment',?,'{}' from n`, deliveryPrivate); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	broken := filepath.Join(t.TempDir(), "broken.toml")
	if err := os.WriteFile(broken, []byte("not valid ["), 0o600); err != nil {
		t.Fatal(err)
	}
	staged, err := reportDeliveryApp(t, ctx, append([]string{"--config", broken}, reportStageArgs(native, path)...)...)
	if err != nil || staged.Status != "staged" || staged.CaptureID == "" || staged.Rows < 501 {
		t.Fatal("stage CLI", staged, err)
	}
	if _, err := reportDeliveryApp(t, ctx, reportStageArgs("absent", path)...); !errors.Is(err, remotedelivery.ErrSeedOutbox) {
		t.Fatal("active capture touched source or got replaced", err)
	}
	rows, err := reportSavedChangeBodies(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range rows {
		batch, err := remote.DecodeChangeBatch(body)
		if err != nil {
			t.Fatal(err)
		}
		saved[batch.BatchID()] = body
	}
	if len(saved) != 2 {
		t.Fatal("fixture did not produce two parts", len(saved))
	}
	t.Setenv("GITCRAWL_REPORT_CLI_TOKEN", "fixture-service-secret")
	partial, err := reportDeliveryApp(t, ctx, reportSendArgs(native, path, 1)...)
	if err != nil || partial.Status != "part_limit" || partial.CaptureID != staged.CaptureID || partial.Receipts == nil || *partial.Receipts != 1 {
		t.Fatal("part limit", partial, err)
	}
	status, err = reportDeliveryApp(t, ctx, reportStatusArgs(path)...)
	if err != nil || status.ActiveCaptureID != staged.CaptureID || status.NativeThrough != seed.BaselineNative {
		t.Fatal("partial send advanced qualified baseline", status, err)
	}
	lost, err := reportDeliveryApp(t, ctx, reportSendArgs(native, path, 2)...)
	if err == nil || lost.Status != "partial" || lost.CaptureID != staged.CaptureID || lost.Receipts == nil || *lost.Receipts != 0 || lost.NativeThrough != "" {
		t.Fatal("lost reply misreported custody", lost, err)
	}
	complete, err := reportDeliveryApp(t, ctx, reportSendArgs(native, path, 2)...)
	if err != nil || complete.Status != "complete" || complete.CaptureID != staged.CaptureID || complete.Receipts == nil || *complete.Receipts != 1 {
		t.Fatal("resume", complete, err)
	}
	final, err := reportDeliveryApp(t, ctx, reportStatusArgs(path)...)
	if err != nil || final.ActiveCaptureID != "" || !strings.HasPrefix(final.NativeThrough, remote.GitHubNativeV2+":") || final.ReceiverSequence != strconv.Itoa(seed.PartCount+2) {
		t.Fatal("final status", final, err)
	}
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 3 {
		t.Fatal("unexpected retries", gotCalls)
	}
	// A source work bound must fail without creating another active capture.
	s, err = store.Open(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `update threads set number=2 where id=1`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	limited := reportStageArgs(native, path)
	for i := range limited {
		if limited[i] == "--max-affected-keys" {
			limited[i+1] = "1"
		}
	}
	if _, err := reportDeliveryApp(t, ctx, limited...); !errors.Is(err, store.ErrNativeReportLimit) {
		t.Fatal("capture bound ignored", err)
	}
	unchanged, err := reportDeliveryApp(t, ctx, reportStatusArgs(path)...)
	if err != nil || !reflect.DeepEqual(final, unchanged) {
		t.Fatal("failed stage changed custody", unchanged, err)
	}
}

func reportSavedChangeBodies(ctx context.Context, path string) ([][]byte, error) {
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `select body from change_parts order by part_index`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, rows.Err()
}

func TestReportDeliveryCLIRejectsFlagsAndMissingPreparation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	missing := filepath.Join(t.TempDir(), "absent.sqlite")
	for _, args := range [][]string{
		{"delivery", "report-status", "--outbox", missing},
		{"delivery", "report-status", "--outbox", missing, "--timeout", "1s", "--token-env", "credential-command-secret"},
		{"delivery", "upgrade", "--outbox", missing, "--timeout", "1s", "--to-version", "4"},
		{"delivery", "upgrade", "--outbox", missing, "--timeout", "1s", "--to-version", "2"},
		{"delivery", "upgrade", "--outbox", missing, "--timeout", "1s", "--to-version", "3", "--max-rows", "1"},
		{"delivery", "stage-changes", "--outbox", missing, "--timeout", "1s"},
		{"delivery", "deliver-changes", "--outbox", missing, "--timeout", "1s", "--token-env", "credential-command-secret"},
	} {
		if _, err := reportDeliveryApp(t, ctx, args...); err == nil {
			t.Fatal("invalid command accepted", args[1])
		}
		if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("rejection created outbox", err)
		}
	}
	native, path, _ := reportCLISeed(t, ctx, "http://127.0.0.1:8123", false)
	if _, err := reportDeliveryApp(t, ctx, reportUpgradeArgs(path, 2)...); err != nil {
		t.Fatal(err)
	}
	if _, err := reportDeliveryApp(t, ctx, reportStageArgs("absent", path)...); !errors.Is(err, remotedelivery.ErrSeedOutbox) {
		t.Fatal("schema2 stage read source", err)
	}
	if _, err := reportDeliveryApp(t, ctx, reportUpgradeArgs(path, 3)...); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reportDeliveryApp(t, ctx, reportStageArgs(native, path)...); !errors.Is(err, store.ErrNativeReportSource) {
		t.Fatal("missing native preparation accepted", err)
	}
	after, err := os.ReadFile(native)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("CLI rebuilt or changed native source", err)
	}
	if result, err := reportDeliveryApp(t, ctx, reportStatusArgs(path)...); err != nil || result.ActiveCaptureID != "" {
		t.Fatal("failed capture staged data", result, err)
	}
}
