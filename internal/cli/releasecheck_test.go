package cli

import "testing"

func TestGitcrawlReleaseCheckOptions(t *testing.T) {
	opts := gitcrawlReleaseCheckOptions(true)
	if opts.AppName != "gitcrawl" || opts.Owner != "openclaw" || opts.Repo != "gitcrawl" {
		t.Fatalf("options = %#v", opts)
	}
	if !opts.Force || opts.CurrentVersion == "" || opts.CacheDir == "" {
		t.Fatalf("incomplete options = %#v", opts)
	}
}
