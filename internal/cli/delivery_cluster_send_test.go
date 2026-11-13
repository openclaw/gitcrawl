//go:build darwin || linux

package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/openclaw/crawlkit/remote"
)

func clusterDeliverArgs(source, outbox string) []string {
	return []string{"delivery", "deliver-clusters", "--db", source, "--outbox", outbox, "--max-parts", "100", "--max-rows", "1000", "--max-encoded-bytes", "1048576", "--timeout", "20s", "--token-env", "CLUSTER_FIXTURE_TOKEN", "--json"}
}
func TestClusterDeliveryCLIResumeAndStatus(t *testing.T) {
	source := clusterExportFixture(t)
	export, err := runClusterExportCLI(t, clusterExportArgs(source))
	if err != nil {
		t.Fatal(err)
	}
	var bundle clusterExportOutput
	if json.Unmarshal(export, &bundle) != nil {
		t.Fatal("bad export fixture")
	}
	bodies := append([][]byte{bundle.Manifest}, bundle.Chunks...)
	bodies = append(bodies, bundle.Complete)
	plan, err := remote.DecodeGitHubSeedManifest(bundle.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	m := plan.Manifest()
	var mu sync.Mutex
	failed := false
	seen := map[int]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, err := io.ReadAll(io.LimitReader(r.Body, remote.MaxChangeBytes+1))
		if err != nil || r.Header.Get("Authorization") != "Bearer private-fixture-token" {
			t.Error("bad authenticated request")
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
			t.Error("changed persisted body")
			w.WriteHeader(400)
			return
		}
		replay := seen[index]
		seen[index] = true
		if index == 1 && !failed {
			failed = true
			w.WriteHeader(503)
			io.WriteString(w, "private-fixture-token private-fixture-response")
			return
		}
		suffix := strconv.Itoa(index - 1)
		if index == 0 {
			suffix = "begin"
		}
		if index == len(bodies)-1 {
			suffix = "complete"
		}
		batch := "seed1:" + m.SeedID + ":" + suffix
		sum := sha256.Sum256(body)
		hash := hex.EncodeToString(sum[:])
		json.NewEncoder(w).Encode(remote.SeedReceipt{App: "gitcrawl", Archive: m.Archive, BindingID: m.BindingID, BatchID: batch, Cursor: batch, Sequence: strconv.Itoa(101 + index), PayloadSHA256: hash, Replayed: replay})
	}))
	defer server.Close()
	dir := t.TempDir()
	input, outbox := filepath.Join(dir, "bundle.json"), filepath.Join(dir, "cluster.sqlite")
	if err = os.WriteFile(input, export, 0600); err != nil {
		t.Fatal(err)
	}
	args := clusterStageArgs(source, input, outbox)
	for i := range args {
		if args[i] == "--origin" {
			args[i+1] = server.URL
		}
	}
	if _, err = runClusterExportCLI(t, args); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLUSTER_FIXTURE_TOKEN", "private-fixture-token")
	before, _ := os.ReadFile(source)
	output, err := runClusterExportCLI(t, clusterDeliverArgs(source, outbox))
	var partial deliveryOutput
	if json.Unmarshal(output, &partial) != nil || err == nil || partial.Status != "partial" || partial.Receipts != 1 || partial.SeedID != "" || partial.NativeThrough != "" {
		t.Fatal("wrong uncertain receipt output", err)
	}
	if strings.Contains(err.Error(), "private-fixture") || strings.Contains(err.Error(), server.URL) || bytes.Contains(output, []byte(source)) {
		t.Fatal("private diagnostic escaped")
	}
	output, err = runClusterExportCLI(t, clusterDeliverArgs(source, outbox))
	if err != nil {
		t.Fatal(err)
	}
	var complete clusterStageOutput
	if json.Unmarshal(output, &complete) != nil || complete.Status != "complete" || complete.BeginSequence != "101" || complete.Receipts != len(bodies) {
		t.Fatal("completion offset lost")
	}
	status, err := runClusterExportCLI(t, []string{"delivery", "cluster-status", "--outbox", outbox, "--timeout", "5s", "--json"})
	if err != nil || !bytes.Equal(status, output) {
		t.Fatal("receipt-backed status differs", err)
	}
	after, _ := os.ReadFile(source)
	if !bytes.Equal(before, after) {
		t.Fatal("delivery changed native source")
	}
	for _, flag := range []string{"--token-env", "--max-parts", "--max-rows", "--max-encoded-bytes", "--timeout"} {
		args := clusterDeliverArgs(source, outbox)
		for i := range args {
			if args[i] == flag {
				args = append(args[:i], args[i+2:]...)
				break
			}
		}
		if output, err := runClusterExportCLI(t, args); err == nil || len(output) != 0 {
			t.Fatal("implicit delivery bound or token allowed")
		}
	}
}
