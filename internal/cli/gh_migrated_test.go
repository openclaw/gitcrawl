package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestGHShimPrintsOctopoolMigration(t *testing.T) {
	app := New()
	var stderr bytes.Buffer
	app.Stderr = &stderr

	err := app.Run(context.Background(), []string{"gh", "api", "repos/openclaw/openclaw/pulls/1"})
	if err == nil || !strings.Contains(err.Error(), "gitcrawl gh moved to octopool") {
		t.Fatalf("migration error = %v", err)
	}
	got := stderr.String()
	if !strings.Contains(got, "gitcrawl gh moved to octopool") {
		t.Fatalf("stderr = %q", got)
	}
	if !strings.Contains(got, "octopool login") {
		t.Fatalf("stderr = %q", got)
	}
}
