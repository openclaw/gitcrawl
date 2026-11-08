package cli

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// Local read commands must explain an owner/repo that is not in the archive
// instead of surfacing a raw SQL no-rows error.
func TestLocalReadsExplainRepositoryMissingFromArchive(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", filepath.Join(dir, "gitcrawl.db")}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for _, args := range [][]string{
		{"search", "issues", "foo", "-R", "never/synced", "--json", "number"},
		{"threads", "never/synced"},
		{"clusters", "never/synced"},
		{"fill-pr-details", "never/synced"},
	} {
		err := New().Run(ctx, append([]string{"--config", configPath}, args...))
		if err == nil || strings.Contains(err.Error(), "no rows in result set") ||
			!strings.Contains(err.Error(), "repository never/synced is not in this archive") ||
			!strings.Contains(err.Error(), "gitcrawl sync never/synced") {
			t.Fatalf("%s: err = %v, want an explanation naming the repository and the sync command", strings.Join(args, " "), err)
		}
		if strings.Count(err.Error(), "never/synced") != 2 {
			t.Fatalf("%s: err = %v repeats or omits the explanation", strings.Join(args, " "), err)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s: err = %v no longer matches sql.ErrNoRows for existing callers", strings.Join(args, " "), err)
		}
	}
}
