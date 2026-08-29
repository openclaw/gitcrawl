package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

type portableRefreshOptions struct {
	StoreDir       string
	PortableDB     string
	ExpectedRemote string
	Branch         string
	Git            string
	Timeout        time.Duration
	Reserve        uint64
	Growth         int64
}

type portableRefreshResult struct {
	Stage         string           `json:"stage"`
	Result        string           `json:"result"`
	BeforeCommit  string           `json:"before_commit,omitempty"`
	AfterCommit   string           `json:"after_commit,omitempty"`
	TargetCommit  string           `json:"target_commit,omitempty"`
	ArtifactID    string           `json:"artifact_id,omitempty"`
	SHA256        string           `json:"sha256,omitempty"`
	ArtifactBytes int64            `json:"artifact_bytes,omitempty"`
	Mirror        string           `json:"mirror_destination,omitempty"`
	MirrorResult  string           `json:"mirror_result,omitempty"`
	Capacity      portableCapacity `json:"capacity"`
	ElapsedMS     int64            `json:"elapsed_ms"`
	Reason        string           `json:"reason,omitempty"`
}

func (a *App) runPortableRefresh(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("portable refresh", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	options := portableRefreshOptions{}
	fs.StringVar(&options.StoreDir, "store-dir", "", "configured store checkout")
	fs.StringVar(&options.PortableDB, "portable-db", "", "logical relative database path")
	fs.StringVar(&options.ExpectedRemote, "expected-remote", "", "required expected origin URL")
	fs.StringVar(&options.Branch, "branch", "main", "expected origin branch")
	fs.StringVar(&options.Git, "git", "", "absolute Git executable (or GITCRAWL_PORTABLE_GIT)")
	fs.DurationVar(&options.Timeout, "timeout", portableOperationTimeout, "total operation deadline")
	fs.Uint64Var(&options.Reserve, "min-free-bytes", portableDefaultReserve, "minimum free-space reserve")
	fs.Int64Var(&options.Growth, "max-growth-bytes", portableDefaultGrowth, "maximum observed temporary growth")
	jsonOut := fs.Bool("json", false, "write JSON output")
	values := map[string]bool{"store-dir": true, "portable-db": true, "expected-remote": true, "branch": true, "git": true, "timeout": true, "min-free-bytes": true, "max-growth-bytes": true}
	if err := fs.Parse(normalizeCommandArgs(args, values)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 || options.Timeout <= 0 || options.Reserve == 0 || options.Growth <= 0 {
		return usageErr(fmt.Errorf("portable refresh accepts no positional arguments and requires positive timeout and byte limits"))
	}
	if err := validatePortableRemote(options.ExpectedRemote); err != nil {
		return usageErr(fmt.Errorf("--expected-remote is required and must identify the intended origin"))
	}
	if options.PortableDB != "" {
		if err := validatePortableRelativePath(options.PortableDB); err != nil {
			return usageErr(err)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	ctx, err := portableGitContext(ctx, options.Git)
	if err != nil {
		return usageErr(err)
	}
	if strings.HasPrefix(options.Branch, "-") || strings.ContainsAny(options.Branch, "\x00\r\n") {
		return usageErr(fmt.Errorf("invalid --branch"))
	}
	result, refreshErr := a.refreshPortable(ctx, options)
	if err := a.writeOutput("portable refresh", result, true); err != nil {
		return errors.Join(refreshErr, err)
	}
	return refreshErr
}

func (a *App) refreshPortable(ctx context.Context, options portableRefreshOptions) (result portableRefreshResult, retErr error) {
	started := time.Now()
	result.Stage, result.Result = "admission", "refused"
	result.Capacity = portableCapacity{ReserveBytes: options.Reserve, GrowthLimit: options.Growth}
	var checkout portableCheckout
	advanced := false
	defer func() {
		result.ElapsedMS = time.Since(started).Milliseconds()
		exitCode := 0
		if retErr != nil {
			exitCode = 1
			// Never claim rollback: merge or promotion can have completed before
			// an external writer, deadline or I/O error interrupts the next stage.
			if advanced {
				result.Result = "partial"
			}
			result.Reason = portableRefreshReason(retErr)
			retErr = fmt.Errorf("portable refresh %s: %s", result.Stage, result.Reason)
		}
		fmt.Fprintf(a.Stderr, "gitcrawl: portable refresh: stage=%s result=%s elapsed=%s exit=%d\n", result.Stage, result.Result, time.Since(started).Round(time.Millisecond), exitCode)
	}()
	stage := func(name string) {
		result.Stage = name
		fmt.Fprintf(a.Stderr, "gitcrawl: portable refresh: stage=%s elapsed=%s\n", name, time.Since(started).Round(time.Millisecond))
	}
	if _, err := portableGitOutput(ctx, "", "check-ref-format", "refs/heads/"+options.Branch); err != nil {
		return result, fmt.Errorf("validate portable branch: %w", err)
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return result, fmt.Errorf("could not load configured portable database")
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode != "local" {
		return result, fmt.Errorf("portable refresh requires a local configured database")
	}
	configPath := config.ResolvePath(a.configPath)
	configStamp, err := portableFileSHA256(ctx, configPath)
	if err != nil {
		return result, err
	}
	source, err := canonicalPortablePath(cfg.DBPath)
	if err != nil {
		return result, err
	}
	root, ok, err := portableStoreRoot(ctx, source)
	if err != nil || !ok {
		return result, fmt.Errorf("configured database is not in a portable checkout")
	}
	root, err = canonicalPortablePath(root)
	if err != nil {
		return result, err
	}
	if options.StoreDir != "" {
		expected, err := a.absoluteInitPath(options.StoreDir)
		if err != nil {
			return result, err
		}
		expected, err = canonicalPortablePath(expected)
		if err != nil || expected != root {
			return result, fmt.Errorf("--store-dir does not match the configured database store")
		}
	}
	relative, err := filepath.Rel(root, source)
	if err != nil {
		return result, err
	}
	relative = filepath.ToSlash(relative)
	if err := validatePortableRelativePath(relative); err != nil {
		return result, err
	}
	if options.PortableDB != "" && options.PortableDB != relative {
		return result, fmt.Errorf("--portable-db does not match the configured logical database")
	}
	ctx, release, err := acquirePortableOwner(ctx, root)
	if err != nil {
		return result, err
	}
	defer release()
	checkout, err = inspectPortableCheckout(ctx, root, options.ExpectedRemote, options.Branch)
	result.BeforeCommit, result.AfterCommit = checkout.head, checkout.head
	if err != nil {
		return result, err
	}
	if _, err := portableCommitTree(ctx, root, checkout.head, options.Growth); err != nil {
		return result, err
	}
	// Keep the configured spelling for the established runtime destination;
	// canonicalization binds ownership, not a migration of writable mirrors.
	mirrorPath, err := a.portableRuntimeDBPath(ctx, cfg.DBPath)
	if err != nil {
		return result, err
	}
	mirrorPath, err = canonicalPortablePath(mirrorPath)
	if err != nil {
		return result, err
	}
	if pathWithin(root, mirrorPath) || pathWithin(filepath.Dir(mirrorPath), root) {
		return result, fmt.Errorf("runtime mirror must be outside the portable checkout")
	}
	result.Mirror = mirrorPath
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o700); err != nil {
		return result, err
	}
	budget, err := newPortableBudget(ctx, []string{root, filepath.Dir(mirrorPath)}, options.Reserve, options.Growth)
	if budget != nil {
		defer func() { result.Capacity = budget.snapshot() }()
	}
	if err != nil {
		return result, err
	}
	ctx, stopMonitor := budget.monitor(ctx)
	defer stopMonitor()
	defer func() {
		if retErr != nil && ctx.Err() != nil {
			retErr = context.Cause(ctx)
		}
	}()
	staging, err := os.MkdirTemp(filepath.Dir(mirrorPath), ".gitcrawl-refresh-*")
	if err != nil {
		return result, err
	}
	stagingInfo, err := os.Lstat(staging)
	if err != nil {
		return result, err
	}
	defer func() {
		if err := removeOwnedPortableStaging(staging, stagingInfo); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("remove owned staging: %w", err))
		}
	}()
	ctx = context.WithValue(ctx, portableValidationDirKey{}, staging)
	checkBoundary := func(head, tracking string) error {
		if err := budget.check(ctx); err != nil {
			return err
		}
		stamp, err := portableFileSHA256(ctx, configPath)
		if err != nil || stamp != configStamp {
			return fmt.Errorf("configuration changed during refresh")
		}
		configuredSource, err := canonicalPortablePath(cfg.DBPath)
		if err != nil || configuredSource != source {
			return fmt.Errorf("configured portable database path changed during refresh")
		}
		return checkout.recheck(ctx, head, tracking)
	}
	if err := checkBoundary(checkout.head, checkout.tracking); err != nil {
		return result, err
	}
	stage("fetch")
	if err := runPortableGit(ctx, root, io.Discard, "fetch", "--no-auto-maintenance", "--no-prune", "--no-prune-tags", "--no-tags", "--no-recurse-submodules", "--refmap=", "--", "origin", "refs/heads/"+options.Branch); err != nil {
		return result, fmt.Errorf("fetch failed: %w", err)
	}
	target, err := portableRef(ctx, root, "FETCH_HEAD")
	if err != nil {
		return result, err
	}
	result.TargetCommit = target
	for _, ancestor := range []string{checkout.head, checkout.tracking} {
		if err := runPortableGit(ctx, root, io.Discard, "merge-base", "--is-ancestor", ancestor, target); err != nil {
			return result, fmt.Errorf("fetched branch is not a fast-forward of HEAD and the tracking ref")
		}
	}
	stage("validate")
	tree, err := portableCommitTree(ctx, root, target, options.Growth)
	if err != nil {
		return result, err
	}
	stagedDB, manifest, err := stagePortableCommit(ctx, root, staging, relative, tree, options.Growth)
	if err != nil {
		return result, err
	}
	result.ArtifactID, result.SHA256, result.ArtifactBytes = manifest.ArtifactID, manifest.SHA256, manifest.OutputBytes
	if result.ArtifactID == "" {
		result.ArtifactID = manifest.SHA256
	}
	mirror, err := inspectPortableMirror(ctx, mirrorPath, source)
	if err != nil {
		return result, err
	}
	result.MirrorResult = "preserved-local"
	if mirror.exists && strings.EqualFold(fmt.Sprintf("%x", mirror.digest), manifest.SHA256) {
		mirror.preserve = true
		result.MirrorResult = "unchanged"
	}
	if err := checkBoundary(checkout.head, checkout.tracking); err != nil {
		return result, err
	}
	stage("advance")
	if target != checkout.head {
		advanced = true // Even a failed merge can have performed some writes.
		if err := runPortableGit(ctx, root, io.Discard, "merge", "--ff-only", "--no-autostash", "--no-overwrite-ignore", "--", target); err != nil {
			result.AfterCommit = "" // Unknown, not a claimed rollback.
			return result, fmt.Errorf("fast-forward failed; inspect checkout before retry: %w", err)
		}
		result.AfterCommit = target
	}
	if err := checkBoundary(target, checkout.tracking); err != nil {
		return result, err
	}
	if target != checkout.tracking {
		advanced = true
		if err := runPortableGit(ctx, root, io.Discard, "update-ref", "refs/remotes/origin/"+options.Branch, target, checkout.tracking); err != nil {
			return result, fmt.Errorf("tracking ref compare-and-swap failed: %w", err)
		}
	}
	if err := checkBoundary(target, target); err != nil {
		return result, err
	}
	stage("promote")
	if info, err := os.Lstat(staging); err != nil || !os.SameFile(info, stagingInfo) {
		return result, fmt.Errorf("owned staging directory changed during refresh")
	}
	if err := mirror.recheck(ctx); err != nil {
		return result, err
	}
	if !mirror.preserve {
		digest, err := portableFileSHA256(ctx, stagedDB)
		if err != nil || !strings.EqualFold(fmt.Sprintf("%x", digest), manifest.SHA256) {
			return result, fmt.Errorf("validated runtime generation changed before promotion")
		}
		if err := checkBoundary(target, target); err != nil {
			return result, err
		}
		if err := os.Rename(stagedDB, mirrorPath); err != nil {
			return result, fmt.Errorf("promote portable mirror: %w", err)
		}
		advanced = true
		result.MirrorResult = "promoted"
		if err := markPortableMirrorHealthVerified(mirrorPath, portableStoreRefreshStatePath(mirrorPath), source); err != nil {
			return result, err
		}
	}
	if err := budget.check(ctx); err != nil {
		return result, err
	}
	result.Stage, result.Result = "complete", "updated"
	if checkout.head == target && checkout.tracking == target {
		result.Result = "no-op"
	}
	return result, nil
}

func pathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) && !filepath.IsAbs(relative)
}

func removeOwnedPortableStaging(path string, original os.FileInfo) error {
	current, err := os.Lstat(path)
	if err != nil || !current.IsDir() || !os.SameFile(original, current) {
		return fmt.Errorf("owned staging directory changed; refusing cleanup")
	}
	return os.RemoveAll(path)
}

func stagePortableCommit(ctx context.Context, root, staging, relative string, tree map[string]portableTreeEntry, growth int64) (string, portableDBManifest, error) {
	logical := filepath.Join(staging, filepath.FromSlash(relative))
	manifestEntry, ok := tree[relative+".manifest.json"]
	if !ok || manifestEntry.size > 1<<20 {
		return "", portableDBManifest{}, fmt.Errorf("strict refresh requires a manifest no larger than 1 MiB")
	}
	if err := extractPortableBlob(ctx, root, manifestEntry, portableDBManifestPath(logical)); err != nil {
		return "", portableDBManifest{}, err
	}
	artifact, manifest, compressed, err := portableSourceArtifact(logical)
	if err != nil {
		return "", manifest, err
	}
	if manifest.OutputPath != relative && manifest.OutputPath != filepath.Base(relative) {
		return "", manifest, fmt.Errorf("manifest outputPath does not match the configured logical database")
	}
	artifactRelative, err := filepath.Rel(staging, artifact)
	if err != nil || !pathWithin(staging, artifact) {
		return "", manifest, fmt.Errorf("artifact path escapes staging")
	}
	entry, ok := tree[filepath.ToSlash(artifactRelative)]
	if !ok {
		return "", manifest, fmt.Errorf("manifest artifact is absent from fetched commit")
	}
	// Allow the extracted blob, inflated DB and both semantic-identity copies,
	// plus another artifact-sized checkout allocation. The monitor also counts
	// fetched objects and all other positive growth throughout the operation.
	if manifest.OutputBytes <= 0 || manifest.OutputBytes > growth/4 || entry.size > (growth-4*manifest.OutputBytes)/2 {
		return "", manifest, fmt.Errorf("artifact validation would exceed growth budget")
	}
	if compressed && entry.size != manifest.ArchiveBytes || !compressed && entry.size != manifest.OutputBytes {
		return "", manifest, fmt.Errorf("manifest artifact size does not match fetched blob")
	}
	if err := extractPortableBlob(ctx, root, entry, artifact); err != nil {
		return "", manifest, err
	}
	stagedDB, err := stagePortableSQLiteSourceTempContext(ctx, logical, filepath.Join(staging, "runtime.db"), 0o600)
	if err != nil {
		return "", manifest, err
	}
	if err := sqliteStoreImmutableHealth(ctx, stagedDB); err != nil {
		return "", manifest, err
	}
	if err := validatePortableDBManifest(ctx, stagedDB, portableDBManifestPath(logical)); err != nil {
		return "", manifest, err
	}
	file, err := os.OpenFile(stagedDB, os.O_RDWR, 0)
	if err != nil {
		return "", manifest, err
	}
	err = file.Sync()
	err = errors.Join(err, file.Close())
	return stagedDB, manifest, err
}

func extractPortableBlob(ctx context.Context, root string, entry portableTreeEntry, destination string) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writer := &portableLimitedWriter{writer: file, left: entry.size}
	err = runPortableGit(ctx, root, writer, "cat-file", "blob", entry.oid)
	err = errors.Join(err, file.Close())
	if err == nil && writer.left != 0 {
		return fmt.Errorf("incomplete fetched artifact")
	}
	return err
}

func portableRefreshReason(err error) string {
	if strings.Contains(err.Error(), "portable manifest mismatch") {
		return "portable artifact manifest verification failed"
	}
	// Our own errors carry stage context. File-system and process errors can
	// contain local paths; line breaks are escaped and diagnostics are bounded.
	reason := strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\n", " "), "\r", " ")
	if len(reason) > 512 {
		reason = reason[:512]
	}
	return reason
}
