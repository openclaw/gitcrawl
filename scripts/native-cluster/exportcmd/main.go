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
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) > 1 && os.Args[1] == "code-index" {
		return codeRun(os.Args[2:])
	}
	if len(os.Args) > 1 && os.Args[1] == "embedding-import" {
		return importRun(os.Args[2:])
	}
	if len(os.Args) < 2 {
		return fmt.Errorf("expected embedding-export")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	db := fs.String("db", "", "existing native source DB")
	out := fs.String("output", "", "new private request file")
	limit := fs.Int("limit", 32, "native task cap1..2000")
	exclusions := fs.String("consumer-exclusions", "", "required metadata-only consumer exclusion file")
	if e := fs.Parse(os.Args[2:]); e != nil {
		return e
	}
	if os.Args[1] != "embedding-export" {
		return fmt.Errorf("unsupported mode")
	}
	if *db == "" || *out == "" {
		return fmt.Errorf("db and output required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, e := store.OpenReadOnly(ctx, *db)
	if e != nil {
		return e
	}
	defer s.Close()
	policy, e := os.ReadFile(*exclusions)
	if e != nil {
		return e
	}
	r, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", *limit, policy)
	if e != nil {
		return e
	}
	if len(r.Tasks) == 0 {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "unchanged", "tasks": 0, "database_writes": false, "provider_calls": false})
	}
	raw, e := json.Marshal(r)
	if e != nil {
		return e
	}
	raw = append(raw, '\n')
	if e = os.MkdirAll(filepath.Dir(*out), 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(raw); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	var bytes, runes int
	for _, x := range r.Tasks {
		bytes += x.InputBytes
		runes += x.InputRunes
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"request_path": *out, "request_sha256": store.NativeSHA256(raw), "request_bytes": len(raw), "tasks": len(r.Tasks), "input_bytes": bytes, "input_runes": runes, "model": r.Model, "dimensions": r.Dimensions, "database_writes": false, "provider_calls": false})
}
