package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) resolveGitHubToken(ctx context.Context, cfg config.Config) config.TokenResolution {
	token := config.ResolveGitHubToken(cfg)
	if token.Value != "" {
		return token
	}
	if value, err := a.githubAuthToken(ctx); err == nil && value != "" {
		return config.TokenResolution{Value: value, Source: "gh auth token"}
	}
	return token
}

func (a *App) githubAuthToken(ctx context.Context) (string, error) {
	candidates := candidateRealGHPaths()
	var lastErr error
	for _, candidate := range candidates {
		if !usableRealGHPath(candidate) {
			continue
		}
		tokenCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		cmd := exec.CommandContext(tokenCtx, candidate, "auth", "token")
		out, err := cmd.Output()
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if token := strings.TrimSpace(string(out)); token != "" {
			return token, nil
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("real gh not found")
}

func candidateRealGHPaths() []string {
	var paths []string
	if envPath := strings.TrimSpace(os.Getenv("GITCRAWL_GH_PATH")); envPath != "" {
		paths = append(paths, envPath)
	}
	paths = append(paths,
		"/opt/homebrew/opt/gh/bin/gh",
		"/usr/local/bin/gh",
		"/usr/bin/gh",
	)
	if lookPath, err := exec.LookPath("gh"); err == nil {
		paths = append(paths, lookPath)
	}
	seen := map[string]bool{}
	unique := paths[:0]
	for _, path := range paths {
		if path = strings.TrimSpace(path); path != "" && !seen[path] && !isGitcrawlShimPath(path) {
			seen[path] = true
			unique = append(unique, path)
		}
	}
	return unique
}

func usableRealGHPath(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() || info.Mode()&0111 == 0 {
		return false
	}
	exe, err := os.Executable()
	if err != nil {
		return true
	}
	candidateReal, candidateErr := filepath.EvalSymlinks(path)
	exeReal, exeErr := filepath.EvalSymlinks(exe)
	if candidateErr == nil && exeErr == nil && candidateReal == exeReal {
		return false
	}
	return true
}
