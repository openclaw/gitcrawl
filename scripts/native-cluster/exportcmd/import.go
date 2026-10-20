//go:build native_cluster_overlay

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/openclaw/gitcrawl/internal/store"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func importRun(args []string) error {
	fs := flag.NewFlagSet("embedding-import", flag.ContinueOnError)
	db := fs.String("db", "", "H DB")
	request := fs.String("request", "", "native original packet")
	response := fs.String("response", "", "processor result")
	policy := fs.String("consumer-exclusions", "", "current consumer exclusions")
	receipt := fs.String("receipt", "", "new durable metadata receipt")
	gate := fs.String("commit-gate", "", "private owner gate")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *db == "" || *request == "" || *response == "" || *policy == "" || *receipt == "" || *gate == "" {
		return fmt.Errorf("all import paths required")
	}
	req, e := os.ReadFile(*request)
	if e != nil {
		return e
	}
	res, e := os.ReadFile(*response)
	if e != nil {
		return e
	}
	ex, e := os.ReadFile(*policy)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ro, e := store.OpenReadOnly(ctx, *db)
	if e != nil {
		return e
	}
	if e = ro.NativeCodeWriterReady(ctx); e != nil {
		ro.Close()
		return e
	}
	preview, e := ro.PreviewNativeEmbeddingResponse(ctx, req, res, ex)
	ro.Close()
	if e != nil {
		return e
	}
	digest := store.NativeSHA256(append(append(append([]byte{}, req...), res...), ex...))
	out := map[string]any{"format": "gitcrawl.embedding-import.v1", "phase": "prepared", "content_digest": digest, "request_sha256": preview.RequestSHA256, "would_import": preview.Imported, "unchanged": preview.Imported == 0}
	if preview.Imported == 0 {
		out["phase"] = "unchanged"
		out["validated_at"] = time.Now().UTC()
		if e = durableReceipt(*receipt, preview); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	info, e := os.Stat(*db)
	if e != nil {
		return e
	}
	identity := info.Sys().(*syscall.Stat_t)
	if e = json.NewEncoder(os.Stdout).Encode(out); e != nil {
		return e
	}
	parent := os.Getppid()
	until := time.Now().Add(60 * time.Second)
	for {
		if os.Getppid() != parent {
			return fmt.Errorf("native controller exited")
		}
		if time.Now().After(until) {
			return fmt.Errorf("import gate timeout")
		}
		b, e := os.ReadFile(*gate)
		if e == nil {
			if strings.TrimSpace(string(b)) != digest {
				return fmt.Errorf("import gate mismatch")
			}
			break
		}
		if !os.IsNotExist(e) {
			return e
		}
		time.Sleep(50 * time.Millisecond)
	}
	lock, e := os.OpenFile(filepath.Join(filepath.Dir(*db), "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return fmt.Errorf("native owner lock busy")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	current, e := os.Stat(*db)
	if e != nil {
		return e
	}
	stat := current.Sys().(*syscall.Stat_t)
	if stat.Dev != identity.Dev || stat.Ino != identity.Ino {
		return fmt.Errorf("source file replaced")
	}
	latestPolicy, e := os.ReadFile(*policy)
	if e != nil {
		return e
	}
	ctx2, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()
	probe, e := store.OpenReadOnly(ctx2, *db)
	if e != nil {
		return e
	}
	e = probe.NativeCodeWriterReady(ctx2)
	probe.Close()
	if e != nil {
		return e
	}
	s, e := store.OpenNativeProducerWriter(ctx2, *db)
	if e != nil {
		return e
	}
	defer s.Close()
	r, e := s.ImportNativeEmbeddingResponse(ctx2, req, res, latestPolicy)
	if e != nil {
		return e
	}
	if e = durableReceipt(*receipt, r); e != nil {
		return fmt.Errorf("source committed; durable receipt failed; retain broker results: %w", e)
	}
	out["phase"] = "committed"
	out["receipt"] = *receipt
	out["imported"] = r.Imported
	out["finished_at"] = time.Now().UTC()
	return json.NewEncoder(os.Stdout).Encode(out)
}
func durableReceipt(path string, value any) error {
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(append(b, '\n')); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
