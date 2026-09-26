package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestThreadPurgeCLIPlansThenRequiresIdleOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "archive.db")
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf("db_path=%q\n", db)), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','{}','2026-01-01');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES
 (1,1,'one',10,'pull_request','open','PRIVATE_SENTINEL','','[]','[]','{}','h','2026-01-01'),
 (2,1,'two',20,'pull_request','open','keeper','','[]','[]','{}','h','2026-01-01')`)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	run := func(args ...string) (string, error) {
		a := New()
		var out, stderr bytes.Buffer
		a.Stdout = &out
		a.Stderr = &stderr
		err := a.Run(ctx, append([]string{"--config", cfg, "purge-threads", "fixture/repo", "--numbers", "10", "--json"}, args...))
		return out.String(), err
	}
	out, err := run()
	if err != nil || !strings.Contains(out, `"applied": false`) || strings.Contains(out, "PRIVATE_SENTINEL") {
		t.Fatalf("plan=%s err=%v", out, err)
	}
	if _, err = run("--apply"); err == nil {
		t.Fatal("missing owner request accepted")
	}
	lock, err := os.OpenFile(filepath.Join(dir, "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = lockPortableFile(lock); err != nil {
		t.Fatal(err)
	}
	_, err = run("--apply", "--request-id", "fixture-request")
	if err == nil || !strings.Contains(err.Error(), "ownership lock busy") {
		t.Fatalf("owner bypass: %v", err)
	}
	lock.Close()
	out, err = run("--apply", "--request-id", "fixture-request")
	if err != nil || !strings.Contains(out, `"applied": true`) {
		t.Fatalf("apply=%s %v", out, err)
	}
	out, err = run()
	if err != nil || !strings.Contains(out, `"already_excluded": true`) {
		t.Fatalf("repeat plan=%s %v", out, err)
	}
}

func TestThreadPurgeManagedMirrorSurvivesRefresh(t *testing.T) {
	ctx := context.Background()
	fixture := newPortableRefreshFixture(t, false)
	fixture.advance(t, false)
	if _, err := fixture.refresh(t); err != nil {
		t.Fatal(err)
	}
	// The fixture publisher is a native archive; compact only the disposable
	// runtime copy to exercise the live sparse portable schema.
	mirrorStore, e := store.Open(ctx, fixture.mirror)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = mirrorStore.PrunePortablePayloads(ctx, store.PortablePruneOptions{BodyChars: 32, RetainSanitizedPayloadColumns: true}); e != nil {
		t.Fatal(e)
	}
	mirrorStore.Close()
	beforeHead := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD")
	beforeSource, err := fileSHA256(filepath.Join(fixture.checkout, fixture.relative))
	if err != nil {
		t.Fatal(err)
	}
	run := func(cfg string, extra ...string) (string, error) {
		a := New()
		var out, stderr bytes.Buffer
		a.Stdout = &out
		a.Stderr = &stderr
		args := []string{"--config", cfg, "purge-threads", "openclaw/openclaw", "--numbers", "1", "--json"}
		err := a.Run(ctx, append(args, extra...))
		return out.String(), err
	}
	if _, err = run(fixture.configPath); err == nil {
		t.Fatal("publisher path accepted without mirror flag")
	}
	alias := filepath.Join(t.TempDir(), "alias.toml")
	if err = os.WriteFile(alias, []byte(fmt.Sprintf("db_path=%q\n", fixture.mirror)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = run(alias); err == nil {
		t.Fatal("portable alias accepted without managed owner")
	}
	if _, err = run(alias, "--runtime-mirror"); err == nil {
		t.Fatal("unbound mirror accepted")
	}
	out, err := run(fixture.configPath, "--runtime-mirror")
	if err != nil || !strings.Contains(out, `"applied": false`) {
		t.Fatalf("plan: %s %v", out, err)
	}
	out, err = run(fixture.configPath, "--runtime-mirror", "--apply", "--request-id", "mirror-fixture")
	if err != nil || !strings.Contains(out, `"applied": true`) {
		t.Fatalf("apply: %s %v", out, err)
	}
	if !readPortableStoreRefreshState(portableStoreRefreshStatePath(fixture.mirror)).MirrorWritable {
		t.Fatal("mirror replacement not fenced")
	}
	if got := portableTestGit(t, fixture.checkout, "rev-parse", "HEAD"); got != beforeHead {
		t.Fatal("purge changed publisher checkout")
	}
	if got, e := fileSHA256(filepath.Join(fixture.checkout, fixture.relative)); e != nil || got != beforeSource {
		t.Fatal("purge changed source bytes", e)
	}
	result, err := fixture.refresh(t)
	if err != nil || result.MirrorResult != "preserved-local" {
		t.Fatalf("refresh lost owner policy: %+v %v", result, err)
	}
	s, err := store.OpenReadOnly(ctx, fixture.mirror)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var removed, kept, excluded int
	s.DB().QueryRow("SELECT count(*) FROM threads WHERE number=1").Scan(&removed)
	s.DB().QueryRow("SELECT count(*) FROM threads WHERE number=2").Scan(&kept)
	s.DB().QueryRow("SELECT count(*) FROM thread_exclusions WHERE number=1 AND reason='owner_requested'").Scan(&excluded)
	if removed != 0 || kept != 1 || excluded != 1 {
		t.Fatalf("refresh restored target or lost peer: %d %d %d", removed, kept, excluded)
	}
}

func TestThreadPurgeFailedMirrorApplyDoesNotFreezeRefresh(t *testing.T) {
	ctx := context.Background()
	f := newPortableRefreshFixture(t, false)
	f.advance(t, false)
	if _, err := f.refresh(t); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(ctx, f.mirror)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PrunePortablePayloads(ctx, store.PortablePruneOptions{BodyChars: 32, RetainSanitizedPayloadColumns: true}); err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().Exec(`CREATE TRIGGER fixture_purge_failure BEFORE DELETE ON threads WHEN OLD.number=1 BEGIN SELECT RAISE(ABORT,'fixture purge rollback'); END`)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	before, err := fileSHA256(f.mirror)
	if err != nil {
		t.Fatal(err)
	}
	statePath := portableStoreRefreshStatePath(f.mirror)
	state := readPortableStoreRefreshState(statePath)
	state.MirrorWritable = false
	state.MirrorHealthSourceSHA256 = fmt.Sprintf("%x", before)
	if err = writePortableStoreRefreshState(statePath, state); err != nil {
		t.Fatal(err)
	}
	run := func(request string) error {
		a := New()
		a.Stdout = &bytes.Buffer{}
		a.Stderr = &bytes.Buffer{}
		return a.Run(ctx, []string{"--config", f.configPath, "purge-threads", "openclaw/openclaw", "--numbers", "1", "--runtime-mirror", "--apply", "--request-id", request, "--json"})
	}
	for _, request := range []string{strings.Repeat("x", 129), "rollback"} {
		if err = run(request); err == nil {
			t.Fatal("failed request accepted")
		}
		if readPortableStoreRefreshState(statePath).MirrorWritable {
			t.Fatal("failed apply froze mirror")
		}
		if after, e := fileSHA256(f.mirror); e != nil || after != before {
			t.Fatal("failed apply changed mirror bytes", e)
		}
		inspect, e := inspectPortableMirror(ctx, f.mirror, filepath.Join(f.checkout, f.relative))
		if e != nil || inspect.preserve {
			t.Fatal("failed apply blocks ordinary replacement", e)
		}
	}
	s, err = store.OpenThreadPurgeMirror(ctx, f.mirror)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec("DROP TRIGGER fixture_purge_failure"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err = run("success"); err != nil {
		t.Fatal(err)
	}
	// Simulate interruption between SQLite commit and the advisory state stamp:
	// refresh must preserve committed owner policy based on changed bytes/WAL.
	if err = writePortableStoreRefreshState(statePath, state); err != nil {
		t.Fatal(err)
	}
	inspect, err := inspectPortableMirror(ctx, f.mirror, filepath.Join(f.checkout, f.relative))
	if err != nil || !inspect.preserve {
		t.Fatal("commit-to-stamp interruption can restore removed content", err)
	}
}
