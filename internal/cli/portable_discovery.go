package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
)

func portableStoreRoot(ctx context.Context, dbPath string) (string, bool, error) {
	dir := filepath.Clean(filepath.Dir(dbPath))
	for {
		info, statErr := os.Stat(filepath.Join(dir, ".git"))
		if statErr == nil && info.IsDir() {
			isWorktree, err := probePortableStoreGitWorktree(ctx, dir)
			if err != nil {
				return "", false, fmt.Errorf("verify portable store candidate %s: %w", dir, err)
			}
			if isWorktree {
				return dir, true, nil
			}
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return "", false, fmt.Errorf("inspect portable store candidate %s: %w", dir, statErr)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false, nil
		}
		dir = parent
	}
}

func probePortableStoreGitWorktree(ctx context.Context, dir string) (bool, error) {
	initialized, err := gitMetadataLooksInitialized(filepath.Join(dir, ".git"))
	if err != nil {
		return false, err
	}
	if !initialized {
		return false, nil
	}
	ctx, err = portableGitContext(ctx, "")
	if err != nil {
		return false, err
	}

	topLevel, err := portableGitOutput(ctx, dir, "rev-parse", "--show-toplevel")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, fmt.Errorf("resolve portable store worktree: %w", err)
	}
	if !sameExistingPath(strings.TrimSpace(topLevel), dir) {
		return false, fmt.Errorf("Git resolved portable store candidate %s to worktree %s", dir, strings.TrimSpace(topLevel))
	}

	gitDir, err := portableGitOutput(ctx, dir, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return false, fmt.Errorf("resolve portable store Git directory: %w", err)
	}
	if !sameExistingPath(strings.TrimSpace(gitDir), filepath.Join(dir, ".git")) {
		return false, fmt.Errorf("Git resolved portable store candidate %s to Git directory %s", dir, strings.TrimSpace(gitDir))
	}
	return true, nil
}

func gitMetadataLooksInitialized(gitDir string) (bool, error) {
	for _, name := range []string{"HEAD", "config", "objects", "refs"} {
		_, err := os.Lstat(filepath.Join(gitDir, name))
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, os.ErrNotExist):
			continue
		default:
			return false, fmt.Errorf("inspect Git metadata %s: %w", gitDir, err)
		}
	}
	return false, nil
}

func sameExistingPath(left, right string) bool {
	leftInfo, err := os.Stat(left)
	if err != nil {
		return false
	}
	rightInfo, err := os.Stat(right)
	return err == nil && os.SameFile(leftInfo, rightInfo)
}

func portableStoreRemoteURL(ctx context.Context, root string) string {
	branch := currentGitBranch(ctx, root)
	remoteName := gitBranchRemote(ctx, root, branch)
	if remoteName != "" {
		remote, err := gitConfigValue(ctx, root, "remote."+remoteName+".url")
		if err == nil && strings.TrimSpace(remote) != "" {
			return strings.TrimSpace(remote)
		}
	}
	return gitRemoteURL(ctx, root)
}

func portableStoreRepairAllowed(root, configPath string) bool {
	if strings.TrimSpace(root) == "" {
		return false
	}
	if info, err := os.Stat(filepath.Join(root, ".git", "info", portableStoreMarkerFile)); err == nil && !info.IsDir() {
		return true
	}
	defaultStoresDir := filepath.Join(filepath.Dir(config.ResolvePath(configPath)), "stores")
	rel, err := filepath.Rel(defaultStoresDir, root)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel)
}

func gitWorktreeClean(ctx context.Context, dir string) bool {
	ctx, release, err := acquirePortableOwner(ctx, dir)
	if err != nil {
		return false
	}
	defer release()
	if err := runGit(ctx, "", "-C", dir, "update-index", "-q", "--refresh"); err != nil {
		return false
	}
	if err := runGit(ctx, "", "-C", dir, "diff", "--quiet", "--"); err != nil {
		return false
	}
	if err := runGit(ctx, "", "-C", dir, "diff", "--cached", "--quiet", "--"); err != nil {
		return false
	}
	return true
}
