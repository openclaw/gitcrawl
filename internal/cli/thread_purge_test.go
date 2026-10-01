package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
	var plan store.ThreadPurgePlan
	if err := json.Unmarshal([]byte(out), &plan); err != nil {
		t.Fatal(err)
	}
	if _, err = run("--apply"); err == nil {
		t.Fatal("missing owner request accepted")
	}
	for _, args := range [][]string{{"--apply", ""}, {"--apply", strings.Repeat("0", 64)}, {"--numbers", ""}, {"--numbers", "10,10"}, {"--numbers", "other/repo#10"}, {"--numbers", "10,"}, {"--numbers", "999"}} {
		if _, err := run(args...); err == nil {
			t.Fatal("unsafe arguments accepted", args)
		}
	}
	lock, err := os.OpenFile(filepath.Join(dir, "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = lockPortableFile(lock); err != nil {
		t.Fatal(err)
	}
	_, err = run("--apply", plan.PlanID)
	if err == nil || !strings.Contains(err.Error(), "ownership lock busy") {
		t.Fatalf("owner bypass: %v", err)
	}
	lock.Close()
	out, err = run("--apply", plan.PlanID)
	if err != nil || !strings.Contains(out, `"applied": true`) {
		t.Fatalf("apply=%s %v", out, err)
	}
	out, err = run()
	if err == nil {
		t.Fatalf("repeat plan=%s %v", out, err)
	}
}

func TestThreadPurgeRefusesRemoteWithoutAccess(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf("db_path=%q\n[remote]\nmode=\"cloud\"\nendpoint=\"https://example.invalid\"\narchive=\"fixture\"\n", filepath.Join(filepath.Dir(cfg), "synthetic.db"))), 0600); err != nil {
		t.Fatal(err)
	}
	a := New()
	a.Stdout = &bytes.Buffer{}
	a.Stderr = &bytes.Buffer{}
	if err := a.Run(context.Background(), []string{"--config", cfg, "purge-threads", "fixture/repo", "--numbers", "1"}); err == nil || !strings.Contains(err.Error(), "native local") {
		t.Fatal("remote target admitted", err)
	}
}
