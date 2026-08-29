package cli

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type portableCheckout struct {
	root       string
	branch     string
	remote     string
	head       string
	tracking   string
	configHash [32]byte
	rootInfo   os.FileInfo
	gitInfo    os.FileInfo
}

func portableRef(ctx context.Context, root, ref string) (string, error) {
	value, err := portableGitOutput(ctx, root, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve portable commit: %w", err)
	}
	value = strings.TrimSpace(value)
	if len(value) != 40 && len(value) != 64 {
		return "", fmt.Errorf("invalid portable commit identity")
	}
	for _, char := range value {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return "", fmt.Errorf("invalid portable commit identity")
		}
	}
	return value, nil
}

func inspectPortableCheckout(ctx context.Context, root, remote, branch string) (portableCheckout, error) {
	checkout := portableCheckout{root: root, remote: remote, branch: branch}
	var err error
	checkout.rootInfo, err = os.Lstat(root)
	if err != nil || !checkout.rootInfo.IsDir() {
		return checkout, fmt.Errorf("portable store must be an existing directory")
	}
	checkout.gitInfo, err = os.Lstat(filepath.Join(root, ".git"))
	if err != nil || !checkout.gitInfo.IsDir() {
		return checkout, fmt.Errorf("portable store requires its own .git directory")
	}
	if err := checkPortableMetadata(ctx, root); err != nil {
		return checkout, err
	}
	if ok, err := probePortableStoreGitWorktree(ctx, root); err != nil || !ok {
		return checkout, fmt.Errorf("portable store identity could not be verified")
	}
	checkout.configHash, err = portableConfigIdentity(ctx, root)
	if err != nil {
		return checkout, err
	}
	actualRemote, err := portableGitOutput(ctx, root, "config", "--get-all", "remote.origin.url")
	if err != nil || strings.Count(strings.TrimSpace(actualRemote), "\n") != 0 || !sameGitRemote(strings.TrimSpace(actualRemote), remote) {
		return checkout, fmt.Errorf("origin does not match --expected-remote")
	}
	actualBranch, err := portableGitOutput(ctx, root, "symbolic-ref", "--quiet", "HEAD")
	if err != nil || strings.TrimSpace(actualBranch) != "refs/heads/"+branch {
		return checkout, fmt.Errorf("portable checkout is not on the expected branch")
	}
	branchRemote, _ := portableGitOutput(ctx, root, "config", "--get-all", "branch."+branch+".remote")
	branchMerge, _ := portableGitOutput(ctx, root, "config", "--get-all", "branch."+branch+".merge")
	if strings.TrimSpace(branchRemote) != "origin" || strings.TrimSpace(branchMerge) != "refs/heads/"+branch {
		return checkout, fmt.Errorf("portable branch must track its matching origin branch")
	}
	checkout.head, err = portableRef(ctx, root, "HEAD")
	if err != nil {
		return checkout, err
	}
	checkout.tracking, err = portableRef(ctx, root, "refs/remotes/origin/"+branch)
	if err != nil {
		return checkout, err
	}
	if err := portableCheckoutClean(ctx, root); err != nil {
		return checkout, err
	}
	return checkout, nil
}

func portableCheckoutClean(ctx context.Context, root string) error {
	// Inspect index modes before status can descend into a submodule or apply
	// worktree attributes. No transport or checkout is needed for this check.
	flags, err := portableGitOutput(ctx, root, "ls-files", "--stage", "-v", "-z")
	if err != nil {
		return err
	}
	for _, entry := range strings.Split(flags, "\x00") {
		if entry == "" {
			continue
		}
		metadata, path, ok := strings.Cut(entry, "\t")
		fields := strings.Fields(metadata)
		if !ok || len(fields) != 4 || fields[0] != "H" || fields[3] != "0" || (fields[1] != "100644" && fields[1] != "100755") {
			return fmt.Errorf("portable index has hidden, unresolved, linked or submodule entries")
		}
		if filepath.Base(path) == ".gitattributes" || filepath.Base(path) == ".gitmodules" {
			return fmt.Errorf("strict portable refresh does not accept attributes or submodules")
		}
	}
	output, err := portableGitOutput(ctx, root, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignored=matching", "--ignore-submodules=all")
	if err != nil {
		return fmt.Errorf("inspect portable worktree: %w", err)
	}
	if output != "" {
		return fmt.Errorf("portable store is not clean (index, worktree, untracked or ignored files)")
	}
	return nil
}

func portableConfigIdentity(ctx context.Context, root string) ([32]byte, error) {
	var digest [32]byte
	output, err := portableGitOutput(ctx, root, "config", "--null", "--list", "--show-scope")
	if err != nil {
		return digest, fmt.Errorf("read portable Git configuration: %w", err)
	}
	parts := strings.Split(strings.TrimSuffix(output, "\x00"), "\x00")
	if len(parts)%2 != 0 {
		return digest, fmt.Errorf("unsupported Git configuration output")
	}
	for index := 0; index < len(parts); index += 2 {
		scope := parts[index]
		switch scope {
		case "command":
			continue
		case "system", "global", "local", "worktree":
		default:
			return digest, fmt.Errorf("unsupported Git configuration scope")
		}
		key, value, _ := strings.Cut(parts[index+1], "\n")
		key = strings.ToLower(key)
		// The runner overrides these single-valued settings on every call.
		if key == "core.hookspath" || key == "core.fsmonitor" || key == "core.attributesfile" || key == "core.sshcommand" {
			continue
		}
		if strings.HasPrefix(key, "filter.") {
			return digest, fmt.Errorf("unsupported portable Git configuration (%s scope: filters)", scope)
		}
		if key == "core.gitproxy" || key == "core.worktree" || key == "core.sparsecheckout" || key == "core.sparsecheckoutcone" ||
			strings.HasPrefix(key, "submodule.") || strings.HasPrefix(key, "extensions.") ||
			strings.HasPrefix(key, "include") || strings.HasPrefix(key, "url.") || key == "core.alternaterefscommand" ||
			(strings.HasPrefix(key, "remote.") && !strings.HasPrefix(key, "remote.origin.")) {
			// Never print key names: URL subsections can contain credentials.
			return digest, fmt.Errorf("unsupported portable Git configuration (%s scope: redirection, submodules or extensions)", scope)
		}
		if strings.HasPrefix(key, "remote.origin.") && key != "remote.origin.url" && key != "remote.origin.fetch" && key != "remote.origin.tagopt" {
			return digest, fmt.Errorf("unsupported portable Git configuration (%s scope: origin options)", scope)
		}
		if key == "core.bare" && value != "false" {
			return digest, fmt.Errorf("portable checkout cannot be bare")
		}
	}
	return sha256.Sum256([]byte(output)), nil
}

func checkPortableMetadata(ctx context.Context, root string) error {
	gitDir := filepath.Join(root, ".git")
	entries := 0
	return filepath.WalkDir(gitDir, func(path string, entry fs.DirEntry, err error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries++
		if entries > 200000 {
			return fmt.Errorf("portable metadata scan exceeds 200000 entries")
		}
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 || (!entry.IsDir() && !entry.Type().IsRegular()) {
			return fmt.Errorf("portable Git metadata contains a link or special file")
		}
		rel, err := filepath.Rel(gitDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		name := entry.Name()
		if strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".pid") || strings.HasPrefix(name, "tmp_") ||
			rel == "MERGE_HEAD" || rel == "CHERRY_PICK_HEAD" || rel == "REVERT_HEAD" || rel == "BISECT_LOG" ||
			rel == "rebase-apply" || rel == "rebase-merge" || rel == "sequencer" || rel == "worktrees" || rel == "modules" ||
			rel == "commondir" || rel == "info/grafts" || rel == "info/sparse-checkout" || rel == "info/attributes" ||
			rel == "objects/info/alternates" || rel == "objects/info/http-alternates" || strings.HasPrefix(rel, "refs/replace/") {
			return fmt.Errorf("portable Git metadata contains competing or unsupported state")
		}
		if strings.HasPrefix(rel, "hooks/") && !entry.IsDir() && !strings.HasSuffix(name, ".sample") {
			return fmt.Errorf("portable checkout contains a hook")
		}
		if strings.HasPrefix(rel, "objects/pack/") && (strings.HasSuffix(name, ".pack") || strings.HasSuffix(name, ".idx")) {
			paired := strings.TrimSuffix(path, filepath.Ext(path)) + ".idx"
			if strings.HasSuffix(name, ".idx") {
				paired = strings.TrimSuffix(path, ".idx") + ".pack"
			}
			if _, err := os.Lstat(paired); err != nil {
				return fmt.Errorf("portable store contains an unresolved orphan pack")
			}
		}
		return nil
	})
}

func (checkout portableCheckout) recheck(ctx context.Context, head, tracking string) error {
	owner, ok := ctx.Value(portableOwnerKey{}).(*portableOwner)
	if !ok {
		return fmt.Errorf("portable ownership missing")
	}
	if err := owner.check(); err != nil {
		return err
	}
	current, err := inspectPortableCheckout(ctx, checkout.root, checkout.remote, checkout.branch)
	if err != nil {
		return err
	}
	if !os.SameFile(checkout.rootInfo, current.rootInfo) || !os.SameFile(checkout.gitInfo, current.gitInfo) ||
		current.configHash != checkout.configHash || current.head != head || current.tracking != tracking {
		return fmt.Errorf("portable checkout identity or refs changed during refresh")
	}
	return nil
}

type portableTreeEntry struct {
	oid  string
	size int64
}

func portableCommitTree(ctx context.Context, root, commit string, limit int64) (map[string]portableTreeEntry, error) {
	output, err := portableGitOutput(ctx, root, "ls-tree", "-r", "-l", "-z", "--full-tree", commit)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]portableTreeEntry)
	var total int64
	for _, record := range strings.Split(output, "\x00") {
		if record == "" {
			continue
		}
		metadata, path, ok := strings.Cut(record, "\t")
		fields := strings.Fields(metadata)
		if !ok || len(fields) != 4 || (fields[0] != "100644" && fields[0] != "100755") || fields[1] != "blob" {
			return nil, fmt.Errorf("portable tree contains a symlink, submodule or unsupported entry")
		}
		if err := validatePortableRelativePath(path); err != nil {
			return nil, err
		}
		if filepath.Base(path) == ".gitattributes" || filepath.Base(path) == ".gitmodules" {
			return nil, fmt.Errorf("strict portable refresh does not accept attributes or submodules")
		}
		size, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil || size < 0 || size > limit-total {
			return nil, fmt.Errorf("portable checkout tree exceeds growth budget")
		}
		total += size
		entries[path] = portableTreeEntry{oid: fields[2], size: size}
	}
	return entries, nil
}
