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

func TestReleaseNotificationAllowed(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "no command", want: true},
		{name: "ordinary command", args: []string{"sync", "openclaw/gitcrawl"}, want: true},
		{name: "fill without reserve", args: []string{"fill-pr-details", "openclaw/gitcrawl"}, want: true},
		{name: "fill with separate reserve", args: []string{"fill-pr-details", "openclaw/gitcrawl", "--reserve-rate-limit", "10"}, want: false},
		{name: "fill with joined reserve", args: []string{"fill-pr-details", "--reserve-rate-limit=10", "openclaw/gitcrawl"}, want: false},
		{name: "fill with single dash reserve", args: []string{"fill-pr-details", "openclaw/gitcrawl", "-reserve-rate-limit", "10"}, want: false},
		{name: "fill with joined single dash reserve", args: []string{"fill-pr-details", "-reserve-rate-limit=10", "openclaw/gitcrawl"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := releaseNotificationAllowed(test.args); got != test.want {
				t.Fatalf("releaseNotificationAllowed(%q) = %v, want %v", test.args, got, test.want)
			}
		})
	}
}
