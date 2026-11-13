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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

const deliveryPrivate = "private-fixture-α\x00end"

func deliveryNativeFixture(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "native.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	repo, err := s.UpsertRepository(ctx, store.Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := s.UpsertThread(ctx, store.Thread{RepoID: repo, GitHubID: "one", Number: 1, Kind: "issue", State: "open",
		Title: deliveryPrivate, Body: deliveryPrivate, RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: thread, GitHubID: "comment", CommentType: "issue_comment", Body: deliveryPrivate, RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnableChangeContract(ctx, store.ChangeContractEnableOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return s.Path()
}

func deliveryApp(t *testing.T, ctx context.Context, args ...string) (deliveryOutput, string, error) {
	t.Helper()
	a := New()
	var out, diagnostic bytes.Buffer
	a.Stdout, a.Stderr = &out, &diagnostic
	a.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Error("delivery invoked GitHub auth")
		return "", errors.New("must not run")
	}
	err := a.Run(ctx, args)
	all := out.String() + diagnostic.String()
	if err != nil {
		all += err.Error()
	}
	for _, secret := range []string{deliveryPrivate, "private-fixture-", "fixture-service-secret", "credential-command-secret"} {
		if strings.Contains(all, secret) {
			t.Fatal("private value escaped CLI output")
		}
	}
	if len(all) > 2048 {
		t.Fatal("delivery output is not bounded")
	}
	var result deliveryOutput
	if out.Len() > 0 && json.Unmarshal(out.Bytes(), &result) != nil {
		t.Fatalf("non-JSON delivery output: %q", out.String())
	}
	return result, all, err
}

func deliveryStageArgs(db, path, origin string) []string {
	return []string{"delivery", "stage-seed", "--db", db, "--repo", "fixture/repo", "--outbox", path,
		"--origin", origin, "--archive", "fixture/archive", "--binding", "fixture-binding",
		"--max-rows", "100", "--max-native-bytes", "1048576", "--max-encoded-bytes", "1048576", "--timeout", "10s", "--json"}
}

func deliverySendArgs(db, path string, parts int) []string {
	return []string{"delivery", "deliver", "--db", db, "--outbox", path, "--max-parts", strconv.Itoa(parts),
		"--max-rows", "100", "--max-encoded-bytes", "1048576", "--timeout", "10s", "--token-env", "GITCRAWL_DELIVERY_FIXTURE_TOKEN", "--json"}
}

func deliverySavedBodies(t *testing.T, path string) [][]byte {
	t.Helper()
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("select body from parts order by part_index")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var bodies [][]byte
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return bodies
}

func TestDeliveryCLIExactHTTPResumeStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	native := deliveryNativeFixture(t)
	before, err := os.ReadFile(native)
	if err != nil {
		t.Fatal(err)
	}
	outbox := filepath.Join(t.TempDir(), "outbox.sqlite")
	brokenConfig := filepath.Join(t.TempDir(), "broken.toml")
	if err := os.WriteFile(brokenConfig, []byte("not valid ["), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_DELIVERY_FIXTURE_TOKEN", "fixture-service-secret")
	var mu sync.Mutex
	var bodies [][]byte
	seen := map[int]bool{}
	calls, fail := 0, true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		body, err := io.ReadAll(io.LimitReader(r.Body, remote.MaxChangeBytes+1))
		if err != nil || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer fixture-service-secret" {
			t.Error("invalid authenticated request")
			w.WriteHeader(400)
			return
		}
		index := -1
		for i, saved := range bodies {
			if bytes.Equal(body, saved) {
				index = i
				break
			}
		}
		if index < 0 {
			t.Error("wire bytes differ from persisted request")
			w.WriteHeader(400)
			return
		}
		replayed := seen[index]
		seen[index] = true
		if index == 1 && fail {
			// The receiver retained this request but the caller gets an error.
			// Resume must replay these exact bytes, not capture a new seed.
			w.WriteHeader(503)
			_, _ = io.WriteString(w, "fixture-service-secret private-fixture-response")
			return
		}
		var manifest remote.GitHubSeedManifest
		if err := json.Unmarshal(bodies[0], &manifest); err != nil {
			t.Error(err)
			return
		}
		suffix := strconv.Itoa(index - 1)
		if index == 0 {
			suffix = "begin"
		} else if index == len(bodies)-1 {
			suffix = "complete"
		}
		batch := "seed1:" + manifest.SeedID + ":" + suffix
		hash := sha256.Sum256(body)
		_ = json.NewEncoder(w).Encode(remote.SeedReceipt{App: "gitcrawl", Archive: manifest.Archive, BindingID: manifest.BindingID,
			BatchID: batch, Cursor: batch, Sequence: strconv.Itoa(index + 1), PayloadSHA256: hex.EncodeToString(hash[:]), Replayed: replayed})
	}))
	defer server.Close()
	args := append([]string{"--config", brokenConfig}, deliveryStageArgs(native, outbox, server.URL)...)
	staged, output, err := deliveryApp(t, ctx, args...)
	if err != nil || staged.Status != "staged" || staged.BaselineState != "unqualified" || staged.Rows != 3 || staged.Receipts != 0 || staged.SeedID == "" {
		t.Fatalf("stage: %+v %v", staged, err)
	}
	if strings.Contains(output, native) || strings.Contains(output, server.URL) {
		t.Fatal("private location escaped output")
	}
	bodies = deliverySavedBodies(t, outbox)
	if len(bodies) != staged.Parts {
		t.Fatal("part count differs")
	}
	partial, _, err := deliveryApp(t, ctx, deliverySendArgs(native, outbox, 100)...)
	if err == nil || partial.Status != "partial" || partial.Receipts != 1 || partial.BaselineState != "" || partial.NativeThrough != "" || partial.SeedID != "" {
		t.Fatalf("partial: %+v %v", partial, err)
	}
	mu.Lock()
	observedCalls := calls
	fail = false
	mu.Unlock()
	if observedCalls != 2 {
		t.Fatal("request was implicitly retried")
	}
	limited, _, err := deliveryApp(t, ctx, deliverySendArgs(native, outbox, 1)...)
	if err != nil || limited.Status != "part_limit" || limited.Receipts != 2 || limited.SeedID != staged.SeedID {
		t.Fatalf("resume: %+v %v", limited, err)
	}
	complete, _, err := deliveryApp(t, ctx, deliverySendArgs(native, outbox, 100)...)
	if err != nil || complete.Status != "complete" || complete.BaselineState != "qualified" || complete.Receipts != staged.Parts {
		t.Fatalf("completion: %+v %v", complete, err)
	}
	// Completed delivery needs neither source access nor credentials.
	t.Setenv("GITCRAWL_DELIVERY_FIXTURE_TOKEN", "")
	again, _, err := deliveryApp(t, ctx, deliverySendArgs(filepath.Join(t.TempDir(), "absent.sqlite"), outbox, 1)...)
	if err != nil || again != complete {
		t.Fatalf("completed replay: %+v %v", again, err)
	}
	status, _, err := deliveryApp(t, ctx, "--config", brokenConfig, "--format", "json", "delivery", "status", "--outbox", outbox, "--timeout", "10s")
	if err != nil || status.BaselineState != "qualified" || status.SeedID != staged.SeedID || status.Receipts != staged.Parts {
		t.Fatalf("status: %+v %v", status, err)
	}
	after, err := os.ReadFile(native)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("native main database changed")
	}
}

func TestDeliveryCLIRejectsBeforeEffects(t *testing.T) {
	native := deliveryNativeFixture(t)
	ctx := context.Background()
	for _, flag := range []string{"--db", "--repo", "--outbox", "--origin", "--archive", "--binding", "--max-rows", "--max-native-bytes", "--max-encoded-bytes", "--timeout"} {
		t.Run(flag, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "outbox.sqlite")
			args := deliveryStageArgs(native, path, "http://127.0.0.1:1")
			for i, value := range args {
				if value == flag {
					args = append(args[:i], args[i+2:]...)
					break
				}
			}
			if _, _, err := deliveryApp(t, ctx, args...); err == nil || ExitCode(err) != 2 {
				t.Fatal("missing bound accepted", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("outbox created before admission")
			}
		})
	}
	for _, flag := range []string{"--db", "--outbox", "--max-parts", "--max-rows", "--max-encoded-bytes", "--timeout", "--token-env"} {
		t.Run("deliver/"+flag, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "outbox.sqlite")
			args := deliverySendArgs(native, path, 1)
			for i, value := range args {
				if value == flag {
					args = append(args[:i], args[i+2:]...)
					break
				}
			}
			if _, _, err := deliveryApp(t, ctx, args...); err == nil || ExitCode(err) != 2 {
				t.Fatal("missing delivery bound accepted", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("delivery created missing outbox")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "outbox.sqlite")
	for _, change := range []struct{ flag, value string }{{"--repo", "other/repo"}, {"--max-rows", "1"}, {"--max-native-bytes", "1"}, {"--max-encoded-bytes", "1"}, {"--origin", "https://user:private-fixture-password@example.invalid"}} {
		args := deliveryStageArgs(native, path, "http://127.0.0.1:1")
		for i := range args {
			if args[i] == change.flag {
				args[i+1] = change.value
			}
		}
		if _, _, err := deliveryApp(t, ctx, args...); err == nil {
			t.Fatal("invalid admission accepted")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("rejected staging created outbox")
		}
	}
	args := append([]string{"--github-token-command", "/credential-command-secret"}, deliveryStageArgs(native, path, "http://127.0.0.1:1")...)
	if _, _, err := deliveryApp(t, ctx, args...); err == nil || ExitCode(err) != 2 {
		t.Fatal("GitHub credential command accepted")
	}
	if _, _, err := deliveryApp(t, ctx, "delivery", "status", "--outbox", path, "--timeout", "10s", "--json"); err == nil {
		t.Fatal("status created missing outbox")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("status had filesystem effects")
	}
}

func TestDeliveryCLIRefusesOldSchemaAndSourceMismatch(t *testing.T) {
	ctx := context.Background()
	native := deliveryNativeFixture(t)
	outbox := filepath.Join(t.TempDir(), "outbox.sqlite")
	if _, _, err := deliveryApp(t, ctx, deliveryStageArgs(native, outbox, "http://127.0.0.1:1")...); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_DELIVERY_FIXTURE_TOKEN", "fixture-service-secret")
	other := deliveryNativeFixture(t)
	if result, _, err := deliveryApp(t, ctx, deliverySendArgs(other, outbox, 1)...); !errors.Is(err, store.ErrNativeReportSource) || result.Receipts != 0 {
		t.Fatal("source mismatch did not stop before HTTP", err)
	}
	db, err := sql.Open("sqlite", native)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("pragma user_version=1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(native)
	newPath := filepath.Join(t.TempDir(), "outbox.sqlite")
	if _, _, err := deliveryApp(t, ctx, deliveryStageArgs(native, newPath, "http://127.0.0.1:1")...); err == nil {
		t.Fatal("old native schema accepted")
	}
	after, _ := os.ReadFile(native)
	if !bytes.Equal(before, after) {
		t.Fatal("old schema upgraded")
	}
	for _, known := range []error{context.Canceled, context.DeadlineExceeded, remotedelivery.ErrSeedOutbox} {
		if !errors.Is(deliveryCommandError(fmt.Errorf("private-fixture-wrapper: %w", known)), known) {
			t.Fatal("known error classification lost")
		}
	}
}
