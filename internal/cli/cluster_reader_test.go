package cli

import (
	"bytes"
	"context"
	"testing"
)

func TestClusterReaderRefusesWritersBeforeOpeningRuntime(t *testing.T) {
	for _, args := range [][]string{{"sync", "openclaw/openclaw"}, {"embed", "openclaw/openclaw"}, {"cluster", "openclaw/openclaw"}, {"purge-threads", "openclaw/openclaw", "--numbers", "1"}, {"--github-token-command", "never-run", "clusters", "openclaw/openclaw"}} {
		a := New()
		a.Stdout = &bytes.Buffer{}
		a.Stderr = &bytes.Buffer{}
		if err := a.RunClusterReader(context.Background(), args); err == nil {
			t.Fatal("writer/provider route accepted", args)
		}
	}
}
