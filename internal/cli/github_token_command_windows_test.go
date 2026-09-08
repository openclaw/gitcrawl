//go:build windows

package cli

import (
	"strings"
	"testing"
)

func TestGitHubTokenCommandUnsupportedWindows(t *testing.T) {
	provider, err := githubTokenProvider(`C:\unused\provider.exe`)
	if provider != nil || err == nil || !strings.Contains(err.Error(), "not supported on Windows") {
		t.Fatalf("managed credential eligibility: %v", err)
	}
}
