package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestIsDecimalString(t *testing.T) {
	if !isDecimalString("123") || isDecimalString("12a") {
		t.Fatalf("decimal classifier failed")
	}
}

func TestGHCompatFileHelpers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := writeAtomicFile(path, []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatalf("write atomic: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read atomic: %v", err)
	}
	if strings.TrimSpace(string(data)) != `{"ok":true}` {
		t.Fatalf("data = %q", data)
	}

	left := filepath.Join(dir, "left")
	right := filepath.Join(dir, "right")
	if err := os.WriteFile(left, []byte("same"), 0o755); err != nil {
		t.Fatalf("write left: %v", err)
	}
	if err := os.WriteFile(right, []byte("same"), 0o755); err != nil {
		t.Fatalf("write right: %v", err)
	}
	leftInfo, err := os.Stat(left)
	if err != nil {
		t.Fatalf("stat left: %v", err)
	}
	rightInfo, err := os.Stat(right)
	if err != nil {
		t.Fatalf("stat right: %v", err)
	}
	if !sameExecutableContents(left, right, leftInfo, rightInfo) {
		t.Fatalf("expected equal executable contents")
	}
	if !ghBackendModeUsable(0o755, "linux") || ghBackendModeUsable(0o644, "linux") || !ghBackendModeUsable(0, "windows") {
		t.Fatalf("mode usability failed")
	}
}

func TestWriteJSONValueWithoutJQ(t *testing.T) {
	app := New()
	var out bytes.Buffer
	app.Stdout = &out
	if err := app.writeJSONValue(map[string]any{"ok": true}, ""); err != nil {
		t.Fatalf("write json: %v", err)
	}
	if !strings.Contains(out.String(), `"ok": true`) {
		t.Fatalf("json output = %s", out.String())
	}
}

func TestWriteJSONValueErrors(t *testing.T) {
	app := New()
	if err := app.writeJSONValue(map[string]any{"bad": func() {}}, ""); err == nil {
		t.Fatalf("expected marshal error")
	}
	t.Setenv("PATH", "")
	if err := app.writeJSONValue(map[string]any{"ok": true}, ".ok"); !errors.Is(err, errLocalGHUnsupported) {
		t.Fatalf("jq error = %v, want local gh unsupported", err)
	}
}

func TestLocalGHUnsupported(t *testing.T) {
	if err := localGHUnsupported(nil); !errors.Is(err, errLocalGHUnsupported) {
		t.Fatalf("nil wrapped error = %v", err)
	}
	if err := localGHUnsupported(errors.New("missing")); !errors.Is(err, errLocalGHUnsupported) || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("wrapped error = %v", err)
	}
}

func TestClusterAndRuntimeHelperBranches(t *testing.T) {
	clusterCases := []struct {
		source string
		want   string
		ok     bool
	}{
		{source: "", want: "", ok: true},
		{source: " AUTO ", want: "", ok: true},
		{source: "raw", want: store.ClusterSourceRun, ok: true},
		{source: "durable", want: store.ClusterSourceDurable, ok: true},
		{source: "unknown", ok: false},
	}
	for _, tc := range clusterCases {
		got, err := parseClusterDetailSource(tc.source)
		if tc.ok && (err != nil || got != tc.want) {
			t.Fatalf("parseClusterDetailSource(%q) = %q, %v; want %q", tc.source, got, err, tc.want)
		}
		if !tc.ok && err == nil {
			t.Fatalf("parseClusterDetailSource(%q) unexpectedly succeeded", tc.source)
		}
	}

	err := neighborEmbeddingRecoveryError("openclaw", "gitcrawl", 42, true, fmt.Errorf("missing vector"))
	if !strings.Contains(err.Error(), "gitcrawl embed openclaw/gitcrawl --number 42 --limit 1 --include-closed") {
		t.Fatalf("neighbor recovery error = %v", err)
	}
	if isGitIndexLockError(nil) || isGitIndexLockError(errors.New("index.lock")) || !isGitIndexLockError(errors.New("fatal: Unable to create '.git/index.lock': File exists.")) {
		t.Fatalf("index lock classifier failed")
	}
}
