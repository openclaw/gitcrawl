//go:build native_code_overlay

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/openclaw/gitcrawl/internal/codeindex"
	"github.com/openclaw/gitcrawl/internal/store"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

func main() {
	if e := codeRun(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func codeRun(args []string) error {
	fs := flag.NewFlagSet("code-index", flag.ContinueOnError)
	db := fs.String("db", "", "H database")
	source := fs.String("source", "", "owned Git object repository")
	commit := fs.String("commit", "", "full immutable commit ID")
	scanOnly := fs.Bool("scan-only", false, "read immutable source without opening H")
	manifest := fs.String("manifest", "", "optional new metadata manifest file")
	upstream := fs.String("upstream-receipt", "", "captured official-main receipt")
	gate := fs.String("commit-gate", "", "one-shot private gate file")
	writeLimit := fs.Duration("write-timeout", 90*time.Second, "atomic write deadline")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *source == "" || *commit == "" || (!*scanOnly && (*db == "" || *gate == "" || *upstream == "")) {
		return fmt.Errorf("source/commit and live db/commit-gate/upstream-receipt required")
	}
	if *writeLimit <= 0 || *writeLimit > 2*time.Minute {
		return fmt.Errorf("write-timeout must be positive and <=2m")
	}
	started := time.Now().UTC()
	scanCtx, done := context.WithTimeout(context.Background(), 45*time.Second)
	result, e := codeindex.ScanCommit(scanCtx, codeindex.Options{Path: *source, MaxFileBytes: 524288, MaxTotalBytes: 1 << 30, MaxFiles: 100000}, *commit)
	done()
	if e != nil {
		return e
	}
	var captured map[string]any
	if *upstream != "" {
		b, e := os.ReadFile(*upstream)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, &captured); e != nil {
			return e
		}
		if captured["format"] != "gitcrawl.code-upstream.v1" || captured["repository_url"] != "https://github.com/openclaw/openclaw.git" || captured["branch"] != "main" || captured["commit"] != result.GitSHA || captured["git_dir"] != result.SourceRoot {
			return fmt.Errorf("captured upstream source binding differs")
		}
		captured["receipt_sha256"] = store.NativeSHA256(b)
	}
	type documentMeta struct {
		Path     string `json:"path"`
		Language string `json:"language"`
		Hash     string `json:"sha256"`
		Bytes    int64  `json:"bytes"`
	}
	if *manifest != "" {
		entries := make([]documentMeta, 0, len(result.Documents))
		for _, d := range result.Documents {
			entries = append(entries, documentMeta{d.Path, d.Language, d.ContentHash, d.ByteSize})
		}
		raw, e := json.Marshal(entries)
		if e != nil {
			return e
		}
		f, e := os.OpenFile(*manifest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = f.Write(raw)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
	}
	if *scanOnly {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"format": "gitcrawl.code-scan.v1", "git_sha": result.GitSHA, "source_root": result.SourceRoot, "tracked_dirty": result.WorktreeDirty, "documents": result.FilesIndexed, "text_bytes": result.BytesIndexed, "skip_counts": map[string]int{"binary": result.SkippedBinary, "large": result.SkippedLarge, "nonregular": result.SkippedNonRegular}, "database_writes": false, "scanned_at": time.Now().UTC()})
	}
	readCtx, done := context.WithTimeout(context.Background(), 15*time.Second)
	ro, e := store.OpenReadOnly(readCtx, *db)
	if e != nil {
		done()
		return e
	}
	if e = ro.NativeCodeWriterReady(readCtx); e != nil {
		ro.Close()
		done()
		return e
	}
	var coreComplete int
	var coreObserved string
	if e = ro.DB().QueryRowContext(readCtx, "select complete,observed_at from analytics_coverage where repository='openclaw/openclaw'").Scan(&coreComplete, &coreObserved); e != nil {
		ro.Close()
		done()
		return fmt.Errorf("core publication baseline required: %w", e)
	}
	at, parseErr := time.Parse(time.RFC3339Nano, coreObserved)
	if parseErr != nil || coreComplete != 1 || time.Since(at) > 5*time.Minute {
		ro.Close()
		done()
		return fmt.Errorf("core collection must be complete and current before code handoff")
	}
	repo, e := ro.RepositoryByFullName(readCtx, "openclaw/openclaw")
	if e != nil {
		ro.Close()
		done()
		return e
	}
	snap := store.CodeSnapshot{RepoID: repo.ID, SourceRoot: result.SourceRoot, GitSHA: result.GitSHA, WorktreeDirty: result.WorktreeDirty, FileCount: result.FilesIndexed, ByteCount: result.BytesIndexed, IndexedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	docs := make([]store.CodeDocument, 0, len(result.Documents))
	type meta struct {
		Path, Language, Hash string
		Bytes                int64
	}
	metas := []meta{}
	for _, d := range result.Documents {
		docs = append(docs, store.CodeDocument{RepoID: repo.ID, Path: d.Path, Language: d.Language, ContentHash: d.ContentHash, Text: d.Text, ByteSize: d.ByteSize, UpdatedAt: snap.IndexedAt})
		metas = append(metas, meta{d.Path, d.Language, d.ContentHash, d.ByteSize})
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Path < metas[j].Path })
	material, _ := json.Marshal(struct {
		Root, SHA string
		Dirty     bool
		Documents []meta
	}{snap.SourceRoot, snap.GitSHA, snap.WorktreeDirty, metas})
	digest := store.NativeSHA256(material)
	var id int64
	var same bool
	e = ro.WithTx(readCtx, func(tx *store.Store) error {
		var err error
		id, same, err = tx.NativeCodeSnapshotMatches(readCtx, snap, docs)
		return err
	})
	closeErr := ro.Close()
	done()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	output := map[string]any{"format": "gitcrawl.code-producer.v1", "upstream": captured, "phase": "prepared", "started_at": started, "scanned_at": time.Now().UTC(), "git_sha": snap.GitSHA, "tracked_dirty": snap.WorktreeDirty, "source_root": snap.SourceRoot, "documents": len(docs), "text_bytes": snap.ByteCount, "content_digest": digest, "unchanged": same, "snapshot_id": id, "skip_counts": map[string]int{"binary": result.SkippedBinary, "large": result.SkippedLarge, "nonregular": result.SkippedNonRegular, "missing": result.SkippedMissing, "outside": result.SkippedOutsideRoot}}
	if same {
		output["phase"] = "unchanged"
		return json.NewEncoder(os.Stdout).Encode(output)
	}
	info, e := os.Stat(*db)
	if e != nil {
		return e
	}
	identity := info.Sys().(*syscall.Stat_t)
	output["source_device"] = fmt.Sprint(identity.Dev)
	output["source_inode"] = fmt.Sprint(identity.Ino)
	if e = json.NewEncoder(os.Stdout).Encode(output); e != nil {
		return e
	}
	// Gate is created by the reviewed owner wrapper only after ordinary settlement.
	parent := os.Getppid()
	until := time.Now().Add(60 * time.Second)
	for {
		if os.Getppid() != parent {
			return fmt.Errorf("owner wrapper exited before commit")
		}
		if time.Now().After(until) {
			return fmt.Errorf("commit gate timed out")
		}
		b, err := os.ReadFile(*gate)
		if err == nil {
			if strings.TrimSpace(string(b)) != digest {
				return fmt.Errorf("commit gate digest mismatch")
			}
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	lock, e := os.OpenFile(filepath.Join(filepath.Dir(*db), "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return fmt.Errorf("native owner lock busy: %w", e)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	now, e := os.Stat(*db)
	if e != nil {
		return e
	}
	current := now.Sys().(*syscall.Stat_t)
	if current.Dev != identity.Dev || current.Ino != identity.Ino {
		return fmt.Errorf("source file replaced after scan")
	}
	ctx, finish := context.WithTimeout(context.Background(), *writeLimit)
	defer finish()
	probe, e := store.OpenReadOnly(ctx, *db)
	if e != nil {
		return e
	}
	e = probe.NativeCodeWriterReady(ctx)
	probe.Close()
	if e != nil {
		return e
	}
	st, e := store.OpenNativeProducerWriter(ctx, *db)
	if e != nil {
		return e
	}
	defer st.Close()
	actualRepo, e := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if e != nil {
		return e
	}
	if actualRepo.ID != repo.ID || actualRepo.GitHubRepoID != repo.GitHubRepoID {
		return fmt.Errorf("native repository identity changed")
	}
	id, same, e = st.NativeCodeSnapshotMatches(ctx, snap, docs)
	if e != nil {
		return e
	}
	commitStart := time.Now()
	if !same {
		id, e = st.ReplaceCodeSnapshot(ctx, snap, docs)
		if e != nil {
			return e
		}
	}
	output["phase"] = "committed"
	output["unchanged"] = same
	output["snapshot_id"] = id
	output["write_seconds"] = time.Since(commitStart).Seconds()
	output["finished_at"] = time.Now().UTC()
	output["documents"] = len(docs)
	return json.NewEncoder(os.Stdout).Encode(output)
}
