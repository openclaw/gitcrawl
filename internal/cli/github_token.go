package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) resolveGitHubToken(ctx context.Context, cfg config.Config) config.TokenResolution {
	if a.githubTokenCommand != nil {
		a.githubTokenMu.Lock()
		defer a.githubTokenMu.Unlock()
		return config.TokenResolution{Value: a.observedGitHubToken, Source: "github-token-command"}
	}
	token := config.ResolveGitHubToken(cfg)
	if token.Value != "" {
		return token
	}
	lookup := a.githubAuthToken
	if a.githubAuthTokenLookup != nil {
		lookup = a.githubAuthTokenLookup
	}
	if value, err := lookup(ctx); err == nil && value != "" {
		return config.TokenResolution{Value: value, Source: "gh auth token"}
	}
	return token
}

func githubTokenProvider(path string) (func(context.Context) (string, error), error) {
	if runtime.GOOS == "windows" {
		return nil, errors.New("--github-token-command is not supported on Windows")
	}
	info, err := os.Stat(path)
	if !filepath.IsAbs(path) || err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, errors.New("--github-token-command requires an absolute executable file")
	}
	return func(ctx context.Context) (string, error) {
		tokenCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(tokenCtx, path)
		configureCommandGroup(cmd)
		cmd.Cancel = func() error { killCommandGroup(cmd); return nil }
		cmd.WaitDelay = 2 * time.Second
		output := &githubTokenOutput{cancel: cancel}
		cmd.Stdout, cmd.Stderr = output, io.Discard
		err := cmd.Run()
		// A successful helper may still have left descendants behind.
		killCommandGroup(cmd)
		cleanupCommandGroup(cmd)
		if output.overflow {
			return "", errors.New("GitHub token command output exceeded 4096 bytes")
		}
		if tokenCtx.Err() != nil {
			return "", errors.New("GitHub token command canceled or timed out")
		}
		if err != nil {
			return "", errors.New("GitHub token command failed")
		}
		token := strings.TrimSuffix(strings.TrimSuffix(output.buffer.String(), "\n"), "\r")
		if token == "" || !utf8.ValidString(token) || strings.IndexFunc(token, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
			return "", errors.New("GitHub token command must return one nonempty token line")
		}
		return token, nil
	}, nil
}

type githubTokenOutput struct {
	buffer   bytes.Buffer
	cancel   context.CancelFunc
	overflow bool
}

func (w *githubTokenOutput) Write(p []byte) (int, error) {
	if len(p) > 4096-w.buffer.Len() {
		w.overflow = true
		w.cancel()
		return 0, errors.New("token output limit")
	}
	return w.buffer.Write(p)
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
