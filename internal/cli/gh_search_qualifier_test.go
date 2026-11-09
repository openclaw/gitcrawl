package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestGHSearchRejectsQualifiersLocalSearchDoesNotApply(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCommandFlowStore(t, dbPath)

	rejected := [][]string{
		{"search", "issues", "label:bug", "-R", "openclaw/openclaw"},
		{"search", "issues", "author:alice", "-R", "openclaw/openclaw"},
		{"search", "issues", "-R", "openclaw/openclaw", "--json", "number", "--", "gateway", "-label:bug"},
		{"search", "issues", "is:pr", "-R", "openclaw/openclaw"},
		{"search", "issues", "-R", "openclaw/openclaw", "--json", "number", "--", "-is:open"},
		{"search", "prs", "is:merged", "-R", "openclaw/openclaw"},
		{"search", "prs", "is:issue", "-R", "openclaw/openclaw"},
		{"search", "prs", "Label:Bug", "-R", "openclaw/openclaw"},
	}
	for _, args := range rejected {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout bytes.Buffer
			app := New()
			app.Stdout = &stdout
			if !strings.Contains(strings.Join(args, " "), " -- ") {
				args = append(args, "--json", "number")
			}
			err := app.Run(ctx, append([]string{"--config", configPath}, args...))
			if err == nil {
				t.Fatalf("search succeeded with output %s; want a usage error for the unapplied qualifier", strings.TrimSpace(stdout.String()))
			}
			if got := ExitCode(err); got != 2 {
				t.Fatalf("exit code = %d (%v), want usage error 2", got, err)
			}
			if !strings.Contains(err.Error(), "not supported by local search") {
				t.Fatalf("err = %v, want it to name the unsupported qualifier", err)
			}
		})
	}

	accepted := [][]string{
		{"search", "issues", "gateway", "is:open", "-R", "openclaw/openclaw"},
		{"search", "issues", "gateway", "is:issue", "repo:openclaw/openclaw"},
		{"search", "issues", "gateway", "state:open", "-R", "openclaw/openclaw"},
		{"search", "issues", "TypeError:", "gateway", "-R", "openclaw/openclaw"},
		{"search", "issues", "https://example.com/x", "-R", "openclaw/openclaw"},
		{"search", "prs", "is:pr", "is:closed", "-R", "openclaw/openclaw"},
	}
	for _, args := range accepted {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			app := New()
			app.Stdout = &bytes.Buffer{}
			if err := app.Run(ctx, append([]string{"--config", configPath}, append(args, "--json", "number")...)); err != nil {
				t.Fatalf("search: %v", err)
			}
		})
	}
}
