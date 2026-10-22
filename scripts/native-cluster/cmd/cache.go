//go:build native_cluster_overlay

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	clusterer "github.com/openclaw/gitcrawl/internal/cluster"
	"github.com/openclaw/gitcrawl/internal/store"
	"math"
	"os"
	"path/filepath"
)

const cacheFormat = "gitcrawl.native-pair-cache.v1"

// Exact previously reviewed producer; its pair algorithm/encoding is unchanged.
// A copied parent keeps this provenance, never relabels it as the new binary.
const reviewedSubsetCacheProducer = "db85fc43e9b05a709501e458b682da7b86c097c7e915985c6b3eda354851a36f"

// reviewedQuarantineCacheProducer is the deployed identity-quarantine combined
// producer (2026-10-20). Later producers change only what they write after the
// graph (for example v3 candidate generations), not the pair algorithm,
// encoding or lineage checks, so its complete cache remains reusable.
const reviewedQuarantineCacheProducer = "1b423688055470a1ef921d6ca2b237e670f528111c6132dd1d7772ace56a2ed8"

func reviewedCacheProducer(hash string) bool {
	return hash == reviewedSubsetCacheProducer || hash == reviewedQuarantineCacheProducer
}

type pairCacheFile struct {
	Producer     string                    `json:"producer_sha256"`
	Format       string                    `json:"format"`
	Algorithm    string                    `json:"algorithm"`
	Source       store.NativeSourceBinding `json:"source"`
	Model        string                    `json:"model"`
	Basis        string                    `json:"basis"`
	Dimensions   int                       `json:"dimensions"`
	Complete     bool                      `json:"complete"`
	Nodes        int                       `json:"nodes"`
	Pairs        int64                     `json:"pairs"`
	Snapshot     string                    `json:"snapshot_input_sha256"`
	Parent       string                    `json:"parent_sha256"`
	Fingerprints map[int64]string          `json:"fingerprints"`
	Edges        map[string]clusterer.Edge `json:"edges"`
}

func producerHash() (string, error) {
	path, e := os.Executable()
	if e != nil {
		return "", e
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	return store.NativeSHA256(raw), nil
}
func validHash(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func loadPairs(dir string, snap store.NativeClusterSnapshot) (*similarityCache, string, error) {
	empty := &similarityCache{}
	pointer, e := os.ReadFile(filepath.Join(dir, "current.json"))
	if os.IsNotExist(e) {
		return empty, "", nil
	}
	if e != nil {
		return nil, "", e
	}
	var id string
	if e = json.Unmarshal(pointer, &id); e != nil || !validHash(id) {
		return nil, "", fmt.Errorf("invalid pair-cache pointer")
	}
	path := filepath.Join(dir, id+".json")
	info, e := os.Stat(path)
	if e != nil {
		return nil, "", e
	}
	if info.Size() > 64<<20 {
		return nil, "", fmt.Errorf("pair cache exceeds64MiB")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		return nil, "", e
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != id {
		return nil, "", fmt.Errorf("pair-cache digest mismatch")
	}
	var c pairCacheFile
	if e = json.Unmarshal(raw, &c); e != nil {
		return nil, "", e
	}
	producer, e := producerHash()
	if e != nil {
		return nil, "", e
	}
	if c.Producer != producer && !reviewedCacheProducer(c.Producer) {
		return nil, "", fmt.Errorf("unrecognized complete-cache producer; evidence retained")
	}
	if c.Format != cacheFormat || c.Algorithm != store.NativeClusterAlgorithm || c.Model != "text-embedding-3-small" || c.Basis != "title_original" || c.Dimensions != 1536 {
		return nil, "", fmt.Errorf("complete-cache lineage mismatch; evidence retained")
	}
	if c.Source != snap.Source {
		// These are exclusively this native producer's derived candidate files, no
		// source history. A new exclusion/identity invalidates every old capability.
		if e = clearPairs(dir); e != nil {
			return nil, "", e
		}
		return empty, "", nil
	}
	if !c.Complete || c.Nodes != len(c.Fingerprints) || c.Nodes > 12000 || c.Nodes < 0 || c.Pairs != int64(c.Nodes)*int64(c.Nodes-1)/2 || int64(len(c.Edges)) > c.Pairs || !validHash(c.Snapshot) || (c.Parent != "" && !validHash(c.Parent)) {
		return nil, "", fmt.Errorf("pair-cache completeness/lineage invalid")
	}
	for node, h := range c.Fingerprints {
		if node <= 0 || !validHash(h) {
			return nil, "", fmt.Errorf("pair-cache node fingerprint invalid")
		}
	}
	for key, edge := range c.Edges {
		_, l := c.Fingerprints[edge.LeftThreadID]
		_, r := c.Fingerprints[edge.RightThreadID]
		if !l || !r || edge.LeftThreadID >= edge.RightThreadID || key != threadIDPairKey(edge.LeftThreadID, edge.RightThreadID) || math.IsNaN(edge.Score) || math.IsInf(edge.Score, 0) || edge.Score < .80 || edge.Score > 1 {
			return nil, "", fmt.Errorf("pair-cache edge shape invalid")
		}
	}
	return &similarityCache{fingerprints: c.Fingerprints, edges: c.Edges}, id, nil
}
func savePairs(dir string, snap store.NativeClusterSnapshot, cache *similarityCache, parent string) (string, error) {
	// Called only after buildWithCache returns success. That function updates its
	// fingerprint set/edges only after every pair has been computed or reused from
	// a validated complete predecessor; canceled passes are never checkpointed.
	producer, e := producerHash()
	if e != nil {
		return "", e
	}
	n := len(cache.fingerprints)
	c := pairCacheFile{Producer: producer, Format: cacheFormat, Algorithm: store.NativeClusterAlgorithm, Source: snap.Source, Model: "text-embedding-3-small", Basis: "title_original", Dimensions: 1536, Complete: true, Nodes: n, Pairs: int64(n) * int64(n-1) / 2, Snapshot: snap.InputSHA256, Parent: parent, Fingerprints: cache.fingerprints, Edges: cache.edges}
	raw, e := json.MarshalIndent(c, "", "  ")
	raw = append(raw, '\n')
	if e != nil {
		return "", e
	}
	if len(raw) > 64<<20 {
		return "", fmt.Errorf("complete pair cache exceeds64MiB; no truncation")
	}
	sum := sha256.Sum256(raw)
	id := hex.EncodeToString(sum[:])
	if e = os.MkdirAll(dir, 0700); e != nil {
		return "", e
	}
	target := filepath.Join(dir, id+".json")
	if _, e = os.Stat(target); os.IsNotExist(e) {
		if e = durable(target, c, false); e != nil {
			return "", e
		}
	} else if e != nil {
		return "", e
	}

	if e = durable(filepath.Join(dir, "current.json"), id, true); e != nil {
		return "", e
	}
	// At most current+parent; historical signatures remain metadata in receipts.
	entries, e := os.ReadDir(dir)
	if e != nil {
		return "", e
	}
	for _, entry := range entries {
		name := entry.Name()
		if len(name) == 69 && name[64:] == ".json" && validHash(name[:64]) && name[:64] != id && name[:64] != parent {
			if e = os.Remove(filepath.Join(dir, name)); e != nil {
				return "", e
			}
		}
	}
	return id, nil
}
func clearPairs(dir string) error {
	entries, e := os.ReadDir(dir)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "current.json" || (len(name) == 69 && name[64:] == ".json" && validHash(name[:64])) {
			if e = os.Remove(filepath.Join(dir, name)); e != nil {
				return e
			}
		}
	}
	return nil
}
