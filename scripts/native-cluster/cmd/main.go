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

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

type cacheReceipt struct {
	InputSHA256 string `json:"input_sha256"`
	StateSHA256 string `json:"state_sha256"`
	RunID       int64  `json:"run_id"`
}

func readSnapshot(db string, policy, request, response []byte) (store.NativeClusterSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, e := store.OpenReadOnly(ctx, db)
	if e != nil {
		return store.NativeClusterSnapshot{}, e
	}
	defer s.Close()
	if e = s.NativeCodeWriterReady(ctx); e != nil {
		return store.NativeClusterSnapshot{}, e
	}
	var complete int
	var observed string
	if e = s.DB().QueryRowContext(ctx, "select complete,observed_at from analytics_coverage where repository='openclaw/openclaw'").Scan(&complete, &observed); e != nil {
		return store.NativeClusterSnapshot{}, e
	}
	at, e := time.Parse(time.RFC3339Nano, observed)
	if e != nil || complete != 1 || time.Since(at) > 5*time.Minute {
		return store.NativeClusterSnapshot{}, fmt.Errorf("core must be complete and current")
	}
	return s.NativeCombinedSnapshot(ctx, policy, request, response)
}
func run() error {
	fs := flag.NewFlagSet("cluster", flag.ContinueOnError)
	db := fs.String("db", "", "H")
	policy := fs.String("consumer-exclusions", "", "consumer metadata")
	cache := fs.String("pair-cache-dir", "", "private complete pair cache")
	receipt := fs.String("receipt", "", "durable new receipt")
	request := fs.String("request", "", "native request")
	response := fs.String("response", "", "broker response")
	gate := fs.String("commit-gate", "", "owner gate")
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "combined" {
		args = args[1:]
	}
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *db == "" || *policy == "" || *cache == "" || *receipt == "" || *gate == "" || *request == "" || *response == "" {
		return fmt.Errorf("all paths required")
	}
	started := time.Now()
	p, e := os.ReadFile(*policy)
	if e != nil {
		return e
	}
	req, e := os.ReadFile(*request)
	if e != nil {
		return e
	}
	res, e := os.ReadFile(*response)
	if e != nil {
		return e
	}
	snap, e := readSnapshot(*db, p, req, res)
	if e != nil {
		return e
	}
	out := map[string]any{"format": "gitcrawl.combined-producer.v1", "input_sha256": snap.InputSHA256, "state_sha256": snap.StateSHA256, "eligible": snap.Eligible, "started_at": started.UTC(), "algorithm": store.NativeClusterAlgorithm}
	if snap.CommittedInput == snap.InputSHA256 && snap.CommittedState == snap.StateSHA256 && snap.PreviewReceipt.Imported == 0 {
		r := snap.PreviewReceipt
		out["phase"] = "unchanged"
		out["run_id"] = snap.LastRun
		out["request_sha256"] = r.RequestSHA256
		out["response_sha256"] = r.ResponseSHA256
		out["imported"] = 0
		out["unchanged"] = r.Unchanged
		out["receipt"] = *receipt
		out["finished_at"] = time.Now().UTC()
		if e = durable(*receipt, out, false); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	pairCache, parentHash, e := loadPairs(*cache, snap)
	if e != nil {
		return e
	}
	cachedNodes := len(pairCache.fingerprints)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	inputs, edges, e := buildWithCache(ctx, snap.RepoID, snap.Vectors, snap.Threads, "openclaw/openclaw", clusterBuildOptions{Threshold: .80, MinSize: 1, MaxClusterSize: 40, Fanout: 16, CrossKindThreshold: .93, RetireMissing: true}, pairCache)
	cancel()
	if e != nil {
		return e
	}
	cacheHash, e := savePairs(*cache, snap, pairCache, parentHash)
	if e != nil {
		return e
	}
	// Retain the completed native cache before the strict second preview. Clock
	// refresh may require a new broker envelope but never a repeated full pass.
	current, e := readSnapshot(*db, p, req, res)
	if e != nil {
		return e
	}
	if current.InputSHA256 != snap.InputSHA256 || current.StateSHA256 != snap.StateSHA256 {
		return fmt.Errorf("native combined input/curation changed during compute; cache retained")
	}
	out["pair_cache_sha256"] = cacheHash
	out["previous_cached_nodes"] = cachedNodes
	out["pair_dot_products"] = pairCache.recomputed
	out["pair_facts_reused"] = pairCache.reused
	raw, e := json.Marshal(inputs)
	if e != nil {
		return e
	}
	digest := store.NativeSHA256(append(append(append(append([]byte(snap.InputSHA256+snap.StateSHA256), raw...), req...), res...), p...))
	out["phase"] = "prepared"
	out["content_digest"] = digest
	out["edges"] = edges
	out["clusters"] = len(inputs)
	out["compute_seconds"] = time.Since(started).Seconds()
	if e = json.NewEncoder(os.Stdout).Encode(out); e != nil {
		return e
	}
	parent := os.Getppid()
	until := time.Now().Add(60 * time.Second)
	for {
		if os.Getppid() != parent || time.Now().After(until) {
			return fmt.Errorf("cluster handoff expired")
		}
		b, e := os.ReadFile(*gate)
		if e == nil {
			if strings.TrimSpace(string(b)) != digest {
				return fmt.Errorf("cluster gate mismatch")
			}
			break
		}
		if !os.IsNotExist(e) {
			return e
		}
		time.Sleep(50 * time.Millisecond)
	}
	lock, e := os.OpenFile(filepath.Join(filepath.Dir(*db), "runner.lock"), os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return e
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	p, e = os.ReadFile(*policy)
	if e != nil {
		return e
	}
	ctx, done := context.WithTimeout(context.Background(), 90*time.Second)
	defer done()
	s, e := store.OpenNativeProducerWriter(ctx, *db)
	if e != nil {
		return e
	}
	defer s.Close()
	writeStart := time.Now()
	imported, result, state, e := s.NativeCombinedPublish(ctx, snap, p, req, res, inputs)
	if e != nil {
		return e
	}
	out["phase"] = "committed"
	out["run_id"] = result.RunID
	out["cluster_unchanged"] = result.RunID == snap.LastRun
	out["members"] = result.MemberCount
	out["state_sha256"] = state
	out["write_seconds"] = time.Since(writeStart).Seconds()
	out["finished_at"] = time.Now().UTC()
	out["receipt"] = *receipt
	out["request_sha256"] = imported.RequestSHA256
	out["response_sha256"] = imported.ResponseSHA256
	out["imported"] = imported.Imported
	out["unchanged"] = imported.Unchanged
	out["committed_at"] = imported.CommittedAt
	out["consumer_exclusions_sha256"] = imported.ConsumerExclusionsSHA256
	out["vector_encoding"] = imported.VectorEncoding
	if e = durable(*receipt, out, false); e != nil {
		return fmt.Errorf("committed; receipt failed: %w", e)
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
func durable(path string, value any, replace bool) error {
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	target := path
	if replace {
		path = fmt.Sprintf("%s.tmp-%d-%d", path, os.Getpid(), time.Now().UnixNano())
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(append(b, '\n'))
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
	if replace {
		if e = os.Rename(path, target); e != nil {
			return e
		}
	}
	d, e := os.Open(filepath.Dir(target))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
