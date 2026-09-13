package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/crawlkit/mirror"
)

func defaultPortableStoreDir(configPath, remoteURL string) string {
	base := filepath.Join(filepath.Dir(configPath), "stores")
	name := strings.TrimSuffix(remoteURL, ".git")
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = safePathName(name)
	if name == "" {
		name = "portable-store"
	}
	return filepath.Join(base, name)
}

func safePathName(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(value) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-.")
}

func syncPortableStore(ctx context.Context, remoteURL, dir string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, portableOperationTimeout)
	defer cancel()
	if strings.TrimSpace(remoteURL) == "" {
		return "", fmt.Errorf("portable store URL is required")
	}
	if strings.TrimSpace(dir) == "" {
		return "", fmt.Errorf("portable store directory is required")
	}
	if err := validatePortableRemote(remoteURL); err != nil {
		return "", err
	}
	ctx, release, err := acquirePortableOwner(ctx, dir)
	if err != nil {
		return "", err
	}
	defer release()
	dir = ctx.Value(portableOwnerKey{}).(*portableOwner).root
	gitDir := filepath.Join(dir, ".git")
	if info, err := os.Stat(gitDir); err == nil && info.IsDir() {
		if err := ensurePortableStoreRemote(ctx, remoteURL, dir); err != nil {
			return "", err
		}
		if err := markPortableStoreCheckout(dir); err != nil {
			return "", err
		}
		if !gitWorktreeClean(ctx, dir) {
			if resetErr := runGit(ctx, "", "-C", dir, "reset", "--hard", "HEAD"); resetErr != nil {
				return "", resetErr
			}
			if retryErr := fastForwardGitCheckout(ctx, dir, false); retryErr != nil {
				return "", retryErr
			}
			if err := removePortableSQLiteSidecars(dir); err != nil {
				return "", err
			}
			return "reset-pulled", nil
		}
		if err := fastForwardGitCheckout(ctx, dir, false); err != nil {
			if !isDirtyPortablePullError(err) {
				return "", err
			}
			if resetErr := runGit(ctx, "", "-C", dir, "reset", "--hard", "HEAD"); resetErr != nil {
				return "", resetErr
			}
			if retryErr := fastForwardGitCheckout(ctx, dir, false); retryErr != nil {
				return "", retryErr
			}
			if err := removePortableSQLiteSidecars(dir); err != nil {
				return "", err
			}
			return "reset-pulled", nil
		}
		if err := removePortableSQLiteSidecars(dir); err != nil {
			return "", err
		}
		return "pulled", nil
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return "", fmt.Errorf("portable store directory %s exists but is not a git checkout", dir)
	} else if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read portable store directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", fmt.Errorf("create portable store parent: %w", err)
	}
	if err := clonePortableStore(ctx, remoteURL, dir); err != nil {
		return "", err
	}
	return "cloned", nil
}

func markPortableStoreCheckout(dir string) error {
	infoDir := filepath.Join(dir, ".git", "info")
	if err := os.MkdirAll(infoDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(infoDir, portableStoreMarkerFile), []byte("gitcrawl portable store\n"), 0o644)
}

func ensurePortableStoreRemote(ctx context.Context, remoteURL, dir string) error {
	remote := gitBranchRemote(ctx, dir, currentGitBranch(ctx, dir))
	origin, err := gitConfigValue(ctx, dir, "remote."+remote+".url")
	if err != nil {
		return fmt.Errorf("read portable store remote %q: %w", remote, err)
	}
	if !sameGitRemote(origin, remoteURL) {
		return fmt.Errorf("portable store directory %s is a checkout of %q, not %q", dir, gitRemoteForMessage(origin), gitRemoteForMessage(remoteURL))
	}
	return nil
}

func sameGitRemote(left, right string) bool {
	left = canonicalGitRemote(left)
	right = canonicalGitRemote(right)
	return left != "" && left == right
}

func canonicalGitRemote(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" {
		parsed.Scheme = strings.ToLower(parsed.Scheme)
		parsed.Host = strings.ToLower(parsed.Host)
		if parsed.Scheme == "http" || parsed.Scheme == "https" {
			parsed.User = nil
		}
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
		if parsed.Scheme != "file" {
			parsed.Path = strings.TrimSuffix(parsed.Path, ".git")
		}
		return parsed.String()
	}
	if !filepath.IsAbs(value) && strings.Contains(value, ":") && !strings.Contains(value, "://") {
		value = strings.TrimSuffix(strings.TrimSuffix(value, "/"), ".git")
		return canonicalSCPGitRemote(value)
	}
	if abs, err := filepath.Abs(value); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(value)
}

func canonicalSCPGitRemote(value string) string {
	userHost, path, ok := strings.Cut(value, ":")
	if !ok {
		return value
	}
	user := ""
	host := userHost
	if before, after, ok := strings.Cut(userHost, "@"); ok {
		user = before + "@"
		host = after
	}
	return user + strings.ToLower(host) + ":" + path
}

func gitRemoteForMessage(value string) string {
	value = strings.TrimSpace(value)
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" {
		parsed.User = nil
		return parsed.String()
	}
	return value
}

func removePortableSQLiteSidecars(dir string) error {
	_, err := mirror.CleanSQLiteSidecars(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func isDirtyPortablePullError(err error) bool {
	message := err.Error()
	return strings.Contains(message, "Your local changes") || strings.Contains(message, "would be overwritten by merge")
}

func fastForwardGitCheckout(ctx context.Context, dir string, quiet bool) error {
	ctx, release, err := acquirePortableOwner(ctx, dir)
	if err != nil {
		return err
	}
	defer release()
	dir = ctx.Value(portableOwnerKey{}).(*portableOwner).root
	branch := currentGitBranch(ctx, dir)
	remote := gitBranchRemote(ctx, dir, branch)
	fetchArgs := []string{"-C", dir, "fetch", "--no-auto-maintenance", "--no-prune", "--no-prune-tags", "--no-tags", "--no-recurse-submodules"}
	if quiet {
		fetchArgs = append(fetchArgs, "--quiet")
	}
	fetchArgs = append(fetchArgs, "--", remote)
	if err := runGit(ctx, "", fetchArgs...); err != nil {
		return err
	}
	target := gitRemoteBranchRef(ctx, dir, remote, branch)
	if target == "" {
		var err error
		target, err = gitOutput(ctx, "", "-C", dir, "symbolic-ref", "--quiet", "--short", "refs/remotes/"+remote+"/HEAD")
		if err != nil {
			return fmt.Errorf("resolve portable store upstream branch: %w", err)
		}
		if strings.TrimSpace(target) == "" {
			return fmt.Errorf("resolve portable store upstream branch: remote %q has no HEAD", remote)
		}
	}
	mergeArgs := []string{"-C", dir, "merge", "--ff-only", "--no-autostash", "--no-overwrite-ignore"}
	if quiet {
		mergeArgs = append(mergeArgs, "--quiet")
	}
	mergeArgs = append(mergeArgs, "--", target)
	return runGit(ctx, "", mergeArgs...)
}

func gitBranchRemote(ctx context.Context, dir, branch string) string {
	if branch != "" {
		value, err := gitConfigValue(ctx, dir, "branch."+branch+".remote")
		if err == nil && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return "origin"
}

func currentGitBranch(ctx context.Context, dir string) string {
	branch, err := gitOutput(ctx, "", "-C", dir, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(branch)
}

func gitRemoteBranchRef(ctx context.Context, dir, remote, branch string) string {
	if strings.TrimSpace(remote) == "" || strings.TrimSpace(branch) == "" {
		return ""
	}
	ref := "refs/remotes/" + remote + "/" + branch
	if err := runGit(ctx, "", "-C", dir, "show-ref", "--verify", "--quiet", ref); err != nil {
		return ""
	}
	return ref
}

func gitConfigValue(ctx context.Context, dir, key string) (string, error) {
	value, err := gitOutput(ctx, "", "-C", dir, "config", "--get", key)
	return strings.TrimSpace(value), err
}

func runGit(ctx context.Context, workdir string, args ...string) error {
	out, err := runGitCommandOutput(ctx, workdir, args...)
	if err != nil {
		if _, portable := ctx.Value(portableGitKey{}).(portableGitExecutable); portable {
			return fmt.Errorf("portable git failed: %w", err)
		}
		return fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(out))
	}
	return nil
}

func runGitCommandOutput(ctx context.Context, workdir string, args ...string) (string, error) {
	return runGitCommandOutputWithEnv(ctx, workdir, os.Environ(), args...)
}

func runGitCommandOutputWithEnv(ctx context.Context, workdir string, env []string, args ...string) (string, error) {
	stdout, stderr, err := runGitCommandOutputWithEnvSeparate(ctx, workdir, env, args...)
	return stdout + stderr, err
}
