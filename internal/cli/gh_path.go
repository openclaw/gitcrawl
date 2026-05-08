package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func resolveRealGHPath() (string, error) {
	envPath := strings.TrimSpace(os.Getenv("GITCRAWL_GH_PATH"))
	candidates := []string{}
	if envPath != "" {
		candidates = append(candidates, envPath)
	}
	candidates = append(candidates,
		"/opt/homebrew/opt/gh/bin/gh",
		"/usr/local/opt/gh/bin/gh",
		"/usr/local/bin/gh",
		"/usr/bin/gh",
	)
	if lookPath, err := exec.LookPath("gh"); err == nil {
		candidates = append(candidates, lookPath)
	}

	seen := map[string]bool{}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || seen[candidate] {
			continue
		}
		seen[candidate] = true
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() {
			if envPath != "" && candidate == envPath {
				return "", fmt.Errorf("real gh not found at GITCRAWL_GH_PATH %q", envPath)
			}
			continue
		}
		if isGitcrawlShimPath(candidate) {
			if envPath != "" && candidate == envPath {
				return "", fmt.Errorf("GITCRAWL_GH_PATH points to the gitcrawl shim (%s); set it to the real gh binary", envPath)
			}
			continue
		}
		return candidate, nil
	}
	return "", fmt.Errorf("real gh not found; set GITCRAWL_GH_PATH")
}

func isGitcrawlShimPath(path string) bool {
	if path == "" {
		return false
	}
	resolved := path
	if eval, err := filepath.EvalSymlinks(path); err == nil {
		resolved = eval
	}
	for _, value := range []string{path, resolved} {
		base := strings.ToLower(filepath.Base(value))
		if base == "gitcrawl" || base == "gitcrawl-gh" {
			return true
		}
	}
	return false
}
