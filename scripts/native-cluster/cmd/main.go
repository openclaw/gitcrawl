//go:build native_cluster_overlay

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/openclaw/gitcrawl/internal/store"
	"io"
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
	if len(request) == 0 && len(response) == 0 {
		return s.NativeClusterSnapshot(ctx, policy)
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
	clusterOnly := len(args) > 0 && args[0] == "cluster"
	if len(args) > 0 && (args[0] == "combined" || clusterOnly) {
		args = args[1:]
	}
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *db == "" || *policy == "" || *cache == "" || *receipt == "" || *gate == "" || (!clusterOnly && (*request == "" || *response == "")) {
		return fmt.Errorf("all paths required")
	}
	started := time.Now()
	p, e := os.ReadFile(*policy)
	if e != nil {
		return e
	}
	var req, res []byte
	if !clusterOnly {
		req, e = os.ReadFile(*request)
		if e != nil {
			return e
		}
		res, e = os.ReadFile(*response)
		if e != nil {
			return e
		}
	}
	snap, e := readSnapshot(*db, p, req, res)
	if e != nil {
		return e
	}
	out := map[string]any{"format": "gitcrawl.combined-producer.v1", "input_sha256": snap.InputSHA256, "state_sha256": snap.StateSHA256, "eligible": snap.Eligible, "started_at": started.UTC(), "algorithm": store.NativeClusterAlgorithm, "cluster_scope": snap.Scope}
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
	planCtx, planDone := context.WithTimeout(context.Background(), 15*time.Second)
	planStore, planErr := store.OpenReadOnly(planCtx, *db)
	var plan store.ClusterQuarantinePlan
	if planErr == nil {
		plan, planErr = planStore.NativePlanClusters(planCtx, snap, inputs)
		planStore.Close()
	}
	planDone()
	if planErr != nil {
		return planErr
	}
	planRaw, planHash, e := plan.Proof()
	if e != nil {
		return e
	}
	planPath := filepath.Join(filepath.Dir(*receipt), planHash+"-quarantine-plan.json.gz")
	if e = retainPlan(planPath, planRaw); e != nil {
		return e
	}
	snap.QuarantinePlanSHA256 = planHash
	out["quarantine_proof_sha256"] = planHash
	out["quarantine_proof_path"] = planPath
	out["computed_clusters"] = len(inputs)
	out["quarantined_candidate_clusters"] = len(inputs) - len(plan.Published)
	out["pair_cache_sha256"] = cacheHash
	out["previous_cached_nodes"] = cachedNodes
	out["pair_dot_products"] = pairCache.recomputed
	out["pair_facts_reused"] = pairCache.reused
	raw, e := json.Marshal(inputs)
	if e != nil {
		return e
	}
	digest := store.NativeSHA256(append(append(append(append([]byte(snap.InputSHA256+snap.StateSHA256+snap.QuarantinePlanSHA256), raw...), req...), res...), p...))
	delete(out, "cluster_scope")
	out["admission"] = map[string]any{"mode": snap.Availability.Scope.Mode, "provider_complete": snap.Availability.Scope.ProviderComplete, "source_as_of": snap.Availability.Scope.SourceAsOf}
	out["phase"] = "prepared"
	out["content_digest"] = digest
	out["edges"] = edges
	out["clusters"] = len(plan.Published)
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
	var imported store.NativeImportReceipt
	var result store.SaveDurableClustersResult
	var state string
	if clusterOnly {
		result, state, e = s.NativePublishClusters(ctx, snap, p, inputs)
	} else {
		imported, result, state, e = s.NativeCombinedPublish(ctx, snap, p, req, res, inputs)
	}
	if e != nil {
		if strings.Contains(e.Error(), "partial_identity_conflict") {
			if save := durable(filepath.Join(*cache, "identity-conflict.json"), cacheReceipt{InputSHA256: snap.InputSHA256, StateSHA256: snap.StateSHA256}, true); save != nil {
				return fmt.Errorf("%v; retain conflict proof: %w", e, save)
			}
		}
		return e
	}
	scope, e := s.NativeCurrentClusterAnalysisScope(ctx, snap.RepoID)
	if e != nil {
		return e
	}
	out["cluster_scope"] = scope
	out["phase"] = "committed"
	out["run_id"] = result.RunID
	out["cluster_unchanged"] = result.RunID == snap.LastRun
	out["members"] = result.MemberCount
	out["clusters"] = result.ClusterCount
	out["publication_status"] = scope.PublicationStatus
	out["run_status"] = "success"
	if scope.PublicationStatus == "retained_only" {
		out["run_status"] = "blocked_identity"
	}
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

func checkIdentityConflict(dir string, snap store.NativeClusterSnapshot) error {
	b, e := os.ReadFile(filepath.Join(dir, "identity-conflict.json"))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	if len(b) > 4096 {
		return fmt.Errorf("identity-conflict proof exceeds bound")
	}
	var r cacheReceipt
	if e = json.Unmarshal(b, &r); e != nil {
		return e
	}
	if r.InputSHA256 == snap.InputSHA256 && r.StateSHA256 == snap.StateSHA256 {
		return fmt.Errorf("partial_identity_conflict: unchanged input/curation; operator resolution required")
	}
	return nil
}

func readRetainedPlan(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		return nil, e
	}
	defer gz.Close()
	raw, e := io.ReadAll(io.LimitReader(gz, (32<<20)+1))
	if e != nil {
		return nil, e
	}
	if len(raw) > 32<<20 {
		return nil, fmt.Errorf("quarantine proof logical size exceeds bound")
	}
	return raw, nil
}
func retainPlan(path string, raw []byte) error {
	if len(raw) > 32<<20 {
		return fmt.Errorf("quarantine proof exceeds bound")
	}
	if old, e := readRetainedPlan(path); e == nil {
		if !bytes.Equal(old, raw) {
			return fmt.Errorf("retained quarantine proof changed")
		}
		return nil
	} else if !os.IsNotExist(e) {
		return e
	}
	tmp := fmt.Sprintf("%s.tmp-%d-%d", path, os.Getpid(), time.Now().UnixNano())
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	gz := gzip.NewWriter(f)
	if _, e = gz.Write(raw); e == nil {
		e = gz.Close()
	}
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
	// Link publishes a complete fsynced file without replacing any old evidence.
	if e = os.Link(tmp, path); e != nil {
		if !os.IsExist(e) {
			return e
		}
		old, err := readRetainedPlan(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(old, raw) {
			return fmt.Errorf("concurrent quarantine proof differs")
		}
	}
	if e = os.Remove(tmp); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
