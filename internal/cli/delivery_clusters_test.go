//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

func clusterExportFixture(t *testing.T) string {
	t.Helper()
	path := deliveryNativeFixture(t)
	s, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().Exec(`update repositories set github_repo_id='9007199254740993',raw_json='{"private":false}';
	update threads set raw_json='{"node_id":"THREAD_1"}';
	insert into cluster_groups(id,repo_id,stable_key,stable_slug,status,representative_thread_id,created_at,updated_at) values(1,1,'one','one','active',1,'now','now');
	insert into cluster_memberships(cluster_id,thread_id,role,state,added_by,added_reason_json,created_at,updated_at) values(1,1,'canonical','active','fixture','{}','now','now')`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
func clusterExportArgs(path string) []string {
	return []string{"delivery", "export-clusters", "--db", path, "--repo", "fixture/repo", "--archive", "fixture/archive", "--binding", "fixture-binding", "--seed-id", strings.Repeat("a", 32), "--captured-at", "2026-01-01T00:00:00Z", "--max-rows", "1000", "--max-native-bytes", "1048576", "--max-encoded-bytes", "1048576", "--timeout", "20s"}
}
func runClusterExportCLI(t *testing.T, args []string) ([]byte, error) {
	t.Helper()
	a := New()
	var out, diagnostic bytes.Buffer
	a.Stdout, a.Stderr = &out, &diagnostic
	a.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Error("offline export invoked auth")
		return "", errors.New("must not run")
	}
	err := a.Run(context.Background(), args)
	if diagnostic.Len() != 0 {
		t.Fatal("unexpected offline diagnostic")
	}
	return out.Bytes(), err
}

func TestClusterExportCLIExactSeedAndSourceUnchanged(t *testing.T) {
	path := clusterExportFixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "broken.toml")
	if err = os.WriteFile(config, []byte("not valid ["), 0600); err != nil {
		t.Fatal(err)
	}
	args := append([]string{"--config", config}, clusterExportArgs(path)...)
	out, err := runClusterExportCLI(t, args)
	if err != nil {
		t.Fatal(err)
	}
	var got clusterExportOutput
	if err = json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if got.Profile != store.NativeClusterContextProfile || len(got.Chunks) != 4 {
		t.Fatal("wrong cluster envelope")
	}
	var raw map[string]json.RawMessage
	if err = json.Unmarshal(out, &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw["manifest"]) == 0 || raw["manifest"][0] != '"' || bytes.Contains(out, []byte(deliveryPrivate)) {
		t.Fatal("requests were not base64 exact bytes")
	}
	plan, err := remote.DecodeGitHubSeedManifest(got.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	m := plan.Manifest()
	if m.Profile != got.Profile || m.RepositoryID != "1" || !strings.HasPrefix(m.NativeThrough, "gitcrawl-cluster-v1:") || len(m.Tables) != 4 {
		t.Fatal("wrong profile, local identity or token")
	}
	for i, name := range []string{"selection", "groups", "members", "threads"} {
		if m.Tables[i].Name != name {
			t.Fatal("table order changed")
		}
	}
	for _, body := range got.Chunks {
		if _, err = plan.ValidateChunk(body); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = plan.ValidateComplete(got.Complete); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	file, err := store.NativeReportFileIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	c, err := store.CaptureNativeClusterContext(ctx, path, store.NativeClusterContextRequest{Repository: "fixture/repo", File: file, Limits: store.NativeReportLimits{MaxRows: 1000, MaxBytes: 1048576}})
	if err != nil {
		t.Fatal(err)
	}
	at, _ := time.Parse(time.RFC3339Nano, "2026-01-01T00:00:00Z")
	report, err := remotedelivery.ProjectClusterContext(ctx, c, at)
	if err != nil {
		t.Fatal(err)
	}
	want, err := remotedelivery.PrepareClusterSeed(ctx, report, remotedelivery.PrivateSeedRequest{Archive: "fixture/archive", BindingID: "fixture-binding", SeedID: strings.Repeat("a", 32), MaxBytes: 1048576})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Manifest, want.Manifest) || !reflect.DeepEqual(got.Chunks, want.Chunks) || !bytes.Equal(got.Complete, want.Complete) {
		t.Fatal("CLI reserialized exact requests")
	}
	again, err := runClusterExportCLI(t, args)
	if err != nil || !bytes.Equal(out, again) {
		t.Fatal("fixed observation export not reproducible", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("offline export wrote source")
	}
}

func TestClusterExportCLIRejectsBeforeOutput(t *testing.T) {
	path := clusterExportFixture(t)
	for _, tc := range []struct{ flag, value string }{{"--seed-id", strings.Repeat("A", 32)}, {"--seed-id", "bad-private-fixture-value"}, {"--captured-at", "2999-01-01T00:00:00Z"}, {"--captured-at", "bad-private-fixture-value"}, {"--max-native-bytes", "0"}, {"--max-encoded-bytes", "1"}, {"--timeout", "0s"}, {"--db", "/absent/private-fixture-source"}, {"--repo", "private-fixture-value"}} {
		t.Run(tc.flag+tc.value, func(t *testing.T) {
			args := clusterExportArgs(path)
			for i := range args {
				if args[i] == tc.flag {
					args[i+1] = tc.value
					break
				}
			}
			out, err := runClusterExportCLI(t, args)
			if err == nil || len(out) != 0 {
				t.Fatal("invalid export emitted data", err)
			}
			if strings.Contains(err.Error(), "private-fixture") || strings.Contains(err.Error(), path) {
				t.Fatal("source/operator value leaked")
			}
		})
	}
	args := append(clusterExportArgs(path), "--token-env", "private-fixture-token")
	if out, err := runClusterExportCLI(t, args); err == nil || len(out) != 0 || strings.Contains(err.Error(), "private-fixture") {
		t.Fatal("credential flag accepted or echoed", err)
	}
}
