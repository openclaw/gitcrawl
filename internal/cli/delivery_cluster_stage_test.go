//go:build darwin || linux

package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func clusterStageArgs(source, bundle, outbox string) []string {
	return []string{"delivery", "stage-clusters", "--db", source, "--bundle", bundle, "--outbox", outbox, "--repo", "fixture/repo", "--origin", "https://archive.example", "--archive", "fixture/archive", "--binding", "fixture-binding", "--max-rows", "1000", "--max-encoded-bytes", "1048576", "--timeout", "20s", "--json"}
}

func TestClusterStageCLIExportAndReadOnlyStatus(t *testing.T) {
	source := clusterExportFixture(t)
	export, err := runClusterExportCLI(t, clusterExportArgs(source))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bundle, outbox := filepath.Join(dir, "bundle.json"), filepath.Join(dir, "new", "cluster.sqlite")
	if err = os.WriteFile(bundle, export, 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "broken.toml")
	if err = os.WriteFile(config, []byte("broken ["), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	output, err := runClusterExportCLI(t, append([]string{"--config", config}, clusterStageArgs(source, bundle, outbox)...))
	if err != nil {
		t.Fatal(err)
	}
	var state clusterStageOutput
	if err = json.Unmarshal(output, &state); err != nil {
		t.Fatal(err)
	}
	if state.Status != "staged" || state.SeedID != strings.Repeat("a", 32) || state.Parts != 6 || state.Rows != 4 || state.Bytes <= 0 || !strings.HasPrefix(state.NativeThrough, "gitcrawl-cluster-v1:") {
		t.Fatal("wrong staged receipt")
	}
	for _, private := range []string{source, bundle, "archive.example", deliveryPrivate, "baseline", "published", "qualified", "manifest\"", "chunks\""} {
		if bytes.Contains(output, []byte(private)) {
			t.Fatal("content/provenance escaped staged receipt")
		}
	}
	status, err := runClusterExportCLI(t, []string{"delivery", "cluster-status", "--outbox", outbox, "--timeout", "5s", "--json"})
	if err != nil || !bytes.Equal(status, output) {
		t.Fatal("status differs", err)
	}
	after, err := os.ReadFile(source)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("staging mutated native source", err)
	}
	if _, err = runClusterExportCLI(t, clusterStageArgs(source, bundle, outbox)); err == nil {
		t.Fatal("existing outbox replaced")
	}
	info, err := os.Stat(outbox)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("outbox mode", err)
	}
}

func TestClusterStageCLIRefusalNoEffects(t *testing.T) {
	source := clusterExportFixture(t)
	export, err := runClusterExportCLI(t, clusterExportArgs(source))
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "bundle.json")
	if err = os.WriteFile(bundle, export, 0600); err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"--db", "--bundle", "--outbox", "--repo", "--origin", "--archive", "--binding", "--max-rows", "--max-encoded-bytes", "--timeout"} {
		t.Run(flag, func(t *testing.T) {
			outbox := filepath.Join(t.TempDir(), "absent", "cluster.sqlite")
			args := clusterStageArgs(source, bundle, outbox)
			for i := range args {
				if args[i] == flag {
					args = append(args[:i], args[i+2:]...)
					break
				}
			}
			output, err := runClusterExportCLI(t, args)
			if err == nil || len(output) != 0 {
				t.Fatal("missing explicit flag admitted")
			}
			if _, err = os.Stat(filepath.Dir(outbox)); !os.IsNotExist(err) {
				t.Fatal("usage failure created directory")
			}
		})
	}
	for _, extra := range [][]string{{"--token-env", "private-fixture-token"}, {"--unknown", "private-fixture-value"}} {
		outbox := filepath.Join(t.TempDir(), "cluster.sqlite")
		output, err := runClusterExportCLI(t, append(clusterStageArgs(source, bundle, outbox), extra...))
		if err == nil || len(output) != 0 || strings.Contains(err.Error(), "private-fixture") {
			t.Fatal("credential/unknown flag accepted or echoed")
		}
	}
	if err = os.WriteFile(bundle, []byte(`{"private-fixture-secret":"value"}`), 0600); err != nil {
		t.Fatal(err)
	}
	outbox := filepath.Join(t.TempDir(), "cluster.sqlite")
	output, err := runClusterExportCLI(t, clusterStageArgs(source, bundle, outbox))
	if err == nil || len(output) != 0 || strings.Contains(err.Error(), "private-fixture") {
		t.Fatal("malformed bundle emitted data")
	}
}
