//go:build native_cluster_overlay

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const NativeClusterAlgorithm = "gitcrawl:6c9c1bea:threshold=.80,cross=.93,k=16,max=40,min=1;prepared-exact-cosine:v1"

type NativeClusterSnapshot struct {
	Availability         ClusterAvailability `json:"-"`
	Scope                *ClusterScope       `json:"cluster_scope,omitempty"`
	Source               NativeSourceBinding `json:"source"`
	RepoID               int64               `json:"repo_id"`
	Threads              map[int64]Thread    `json:"-"`
	Vectors              []ThreadVector      `json:"-"`
	InputSHA256          string              `json:"input_sha256"`
	StateSHA256          string              `json:"state_sha256"`
	Eligible             int                 `json:"eligible"`
	CommittedInput       string              `json:"committed_input"`
	CommittedState       string              `json:"committed_state"`
	LastRun              int64               `json:"last_run"`
	PreviewReceipt       NativeImportReceipt `json:"-"`
	QuarantinePlanSHA256 string              `json:"quarantine_plan_sha256,omitempty"`
}

func (s *Store) NativeClusterSnapshot(ctx context.Context, policy []byte) (NativeClusterSnapshot, error) {
	var out NativeClusterSnapshot
	err := s.WithTx(ctx, func(tx *Store) error { var e error; out, e = tx.nativeClusterSnapshot(ctx, policy); return e })
	return out, err
}
func (s *Store) nativeClusterSnapshot(ctx context.Context, policy []byte) (NativeClusterSnapshot, error) {
	return s.nativeClusterSnapshotOverlay(ctx, policy, nil)
}
func (s *Store) nativeClusterSnapshotOverlay(ctx context.Context, policy []byte, overlay map[int64]ThreadVector) (NativeClusterSnapshot, error) {
	var out NativeClusterSnapshot
	var p struct {
		Format  string   `json:"format"`
		Dataset string   `json:"dataset"`
		IDs     []string `json:"excluded_thread_ids"`
	}
	if len(policy) > 1<<20 {
		return out, fmt.Errorf("exclusion input too large")
	}
	if e := json.Unmarshal(policy, &p); e != nil {
		return out, e
	}
	if p.Format != "gitcrawl.embedding.exclusions.v1" || p.Dataset != "gitcrawl/history/threads" || p.IDs == nil {
		return out, fmt.Errorf("consumer policy required")
	}
	blocked := map[int64]bool{}
	for _, id := range p.IDs {
		n, e := strconv.ParseInt(id, 10, 64)
		if e != nil || n <= 0 || strconv.FormatInt(n, 10) != id {
			return out, fmt.Errorf("invalid consumer ID")
		}
		blocked[n] = true
	}
	var e error
	out.Source, e = s.nativeSourceBinding(ctx, "openclaw/openclaw")
	if e != nil {
		return out, e
	}
	out.Source.ConsumerExclusionsSHA256 = NativeSHA256(policy)
	out.RepoID, _ = strconv.ParseInt(out.Source.RepoID, 10, 64)
	out.Availability, e = s.ReadClusterAvailability(ctx, "openclaw/openclaw", time.Now().UTC())
	if e != nil {
		return out, e
	}
	// This is the exact native open/unclosed scope. Snapshot task projection uses
	// the same title_original builder; no summaries/fingerprints affect this basis.
	rows, e := s.q().QueryContext(ctx, `select t.id,t.number,t.github_id,t.kind,t.state,t.title,coalesce(t.body,''),t.content_hash,
 coalesce(d.body,t.body,''),coalesce(d.raw_text,t.body,''),coalesce(d.dedupe_text,t.title||' '||coalesce(t.body,'')),
 tv.content_hash,tv.dimensions,tv.vector_json,tv.vector_backend,exists(select 1 from thread_exclusions x where x.repository='openclaw/openclaw' and x.number=t.number)
 from threads t left join documents d on d.thread_id=t.id
 left join thread_vectors tv on tv.thread_id=t.id and tv.basis='title_original' and tv.model='text-embedding-3-small'
 where t.repo_id=? and t.state='open' and t.closed_at_local is null order by t.id`, out.RepoID)
	if e != nil {
		return out, e
	}
	h := sha256.New()
	encoder := json.NewEncoder(h)
	encoder.Encode(NativeClusterAlgorithm)
	encoder.Encode(out.Source)
	encoder.Encode(out.Availability.SemanticDigest)
	encoder.Encode(ClusterQuarantinePolicyDigest)
	out.Threads = map[int64]Thread{}
	missing := 0
	stale := 0
	for rows.Next() {
		var excluded bool
		var t Thread
		var body, raw, dedupe string
		var hash, backend *string
		var dims *int
		var vectorRaw []byte
		if e = rows.Scan(&t.ID, &t.Number, &t.GitHubID, &t.Kind, &t.State, &t.Title, &t.Body, &t.ContentHash, &body, &raw, &dedupe, &hash, &dims, &vectorRaw, &backend, &excluded); e != nil {
			rows.Close()
			return out, e
		}
		out.Eligible++
		if out.Eligible > 12000 {
			rows.Close()
			return out, fmt.Errorf("eligible nodes exceed reviewed12000 CPU bound")
		}
		if blocked[t.ID] || excluded {
			rows.Close()
			return out, fmt.Errorf("consumer-excluded native thread remains in cluster scope:%d", t.ID)
		}
		if out.Availability.Blocked[t.ID] {
			out.Availability.Scope.Population.UnavailableInScope++
			continue
		}
		if v, ok := overlay[t.ID]; ok {
			same := false
			if hash != nil && *hash == v.ContentHash && dims != nil && *dims == 1536 {
				old, err := decodeStoredVector(vectorRaw)
				same = err == nil && reflect.DeepEqual(old, v.Vector)
			}
			if !same {
				b := "exact"
				backend = &b
			}
			hash = &v.ContentHash
			d := v.Dimensions
			dims = &d
			vectorRaw, e = json.Marshal(v.Vector)
			if e != nil {
				rows.Close()
				return out, e
			}
		}
		text, _, e := embeddingTextForBasisWithMeta("title_original", t.Title, body, raw, dedupe, "")
		if e != nil {
			rows.Close()
			return out, e
		}
		want := embeddingContentHash("title_original", "text-embedding-3-small", text)
		if strings.TrimSpace(text) == "" || hash == nil || dims == nil || *dims != 1536 {
			missing++
			continue
		}
		if *hash != want {
			stale++
			continue
		}
		v, e := decodeStoredVector(vectorRaw)
		valid := e == nil && len(v) == 1536
		nonzero := false
		for _, n := range v {
			valid = valid && !math.IsNaN(n) && !math.IsInf(n, 0)
			nonzero = nonzero || n != 0
		}
		if !valid || !nonzero {
			missing++
			continue
		}
		t.RepoID = out.RepoID
		out.Threads[t.ID] = t
		item := ThreadVector{ThreadID: t.ID, Basis: "title_original", Model: "text-embedding-3-small", Dimensions: 1536, ContentHash: want, Vector: v}
		if backend != nil {
			item.Backend = *backend
		}
		out.Vectors = append(out.Vectors, item)
		// Provider/local observation clocks are intentionally absent. All native
		// graph inputs and capped embedding input values are represented exactly.
		if e = encoder.Encode([]any{t.ID, t.Number, t.GitHubID, t.Kind, t.State, t.Title, t.Body, want, item}); e != nil {
			rows.Close()
			return out, e
		}
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return out, e
	}
	rows.Close()
	if missing != 0 || stale != 0 {
		return out, fmt.Errorf("native cluster coverage incomplete: eligible=%d fresh=%d missing=%d stale=%d", out.Eligible, len(out.Vectors), missing, stale)
	}
	out.Availability.Scope.Population.NativeEligible = out.Eligible
	out.Availability.Scope.Population.Selected = len(out.Vectors)
	out.InputSHA256 = hex.EncodeToString(h.Sum(nil))
	out.StateSHA256, e = s.nativeClusterState(ctx, out.RepoID)
	if e != nil {
		return out, e
	}
	var lastStats *string
	e = s.q().QueryRowContext(ctx, "select id,stats_json from cluster_runs where repo_id=? order by id desc limit 1", out.RepoID).Scan(&out.LastRun, &lastStats)
	if e != nil && e != sql.ErrNoRows {
		return out, e
	}
	if lastStats != nil {
		var meta struct {
			Native struct {
				Input string `json:"input_sha256"`
				State string `json:"state_sha256"`
			} `json:"native_producer"`
		}
		if e = json.Unmarshal([]byte(*lastStats), &meta); e != nil {
			return out, e
		}
		out.CommittedInput = meta.Native.Input
		out.CommittedState = meta.Native.State
	}
	out.Scope, e = s.NativeCurrentClusterAnalysisScope(ctx, out.RepoID)
	if e != nil {
		return out, e
	}
	return out, nil
}
func (s *Store) nativeClusterState(ctx context.Context, repo int64) (string, error) {
	h := sha256.New()
	enc := json.NewEncoder(h)
	// Includes curation, closures, memberships and output generation. Source
	// collection does not update these tables; clocks here belong to curation.
	for _, q := range []string{
		"select * from cluster_runs where repo_id=? order by id",
		"select * from cluster_groups where repo_id=? order by id",
		"select * from cluster_memberships where cluster_id in(select id from cluster_groups where repo_id=?) order by cluster_id,thread_id",
		"select * from cluster_overrides where repo_id=? order by id",
		"select * from cluster_closures where cluster_id in(select id from cluster_groups where repo_id=?) order by cluster_id",
		"select * from cluster_aliases where cluster_id in(select id from cluster_groups where repo_id=?) order by cluster_id,alias_slug",
		"select * from clusters where repo_id=? order by id",
		"select * from cluster_members where cluster_id in(select id from clusters where repo_id=?) order by cluster_id,thread_id",
	} {
		enc.Encode(q)
		rows, e := s.q().QueryContext(ctx, q, repo)
		if e != nil {
			return "", e
		}
		cols, e := rows.Columns()
		if e != nil {
			rows.Close()
			return "", e
		}
		enc.Encode(cols)
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if e = rows.Scan(ptrs...); e != nil {
				rows.Close()
				return "", e
			}
			for i, col := range cols {
				if col == "stats_json" && vals[i] != nil {
					var obj map[string]any
					var raw []byte
					switch v := vals[i].(type) {
					case string:
						raw = []byte(v)
					case []byte:
						raw = v
					default:
						rows.Close()
						return "", fmt.Errorf("invalid stats type")
					}
					decoder := json.NewDecoder(bytes.NewReader(raw))
					decoder.UseNumber()
					if e = decoder.Decode(&obj); e != nil {
						rows.Close()
						return "", e
					}
					if native, ok := obj["native_producer"].(map[string]any); ok {
						delete(native, "state_sha256")
					}
					canonical, _ := json.Marshal(obj)
					vals[i] = string(canonical)
				}
			}
			if e = enc.Encode(vals); e != nil {
				rows.Close()
				return "", e
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return "", e
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func (s *Store) NativePublishClusters(ctx context.Context, want NativeClusterSnapshot, policy []byte, inputs []DurableClusterInput) (SaveDurableClustersResult, string, error) {
	var result SaveDurableClustersResult
	var state string
	e := s.nativeClusterWithTx(ctx, func(tx *Store) error {
		got, e := tx.nativeClusterSnapshot(ctx, policy)
		if e != nil {
			return e
		}
		if got.InputSHA256 != want.InputSHA256 || got.StateSHA256 != want.StateSHA256 {
			return fmt.Errorf("native cluster snapshot changed")
		}
		seen := map[int64]bool{}
		for _, in := range inputs {
			for _, m := range in.Members {
				if _, ok := got.Threads[m.ThreadID]; !ok || seen[m.ThreadID] {
					return fmt.Errorf("cluster membership outside exact input or duplicate")
				}
				seen[m.ThreadID] = true
			}
		}
		if len(seen) != len(got.Threads) {
			return fmt.Errorf("cluster membership incomplete")
		}
		if got.CommittedInput == got.InputSHA256 && got.CommittedState == got.StateSHA256 {
			result.RunID = got.LastRun
			state = got.StateSHA256
			if got.Scope != nil && got.Scope.Population.Published != nil {
				result.MemberCount = *got.Scope.Population.Published
			}
			return nil
		}
		plan, e := tx.NativePlanClusters(ctx, want, inputs)
		if e != nil {
			return e
		}
		_, proofHash, e := plan.Proof()
		if e != nil {
			return e
		}
		if want.QuarantinePlanSHA256 != "" && want.QuarantinePlanSHA256 != proofHash {
			return fmt.Errorf("native quarantine plan changed")
		}
		if len(plan.SelectedIDs) == 0 {
			return fmt.Errorf("no_publishable_input")
		}
		// Exact target pinning preserves the existing ID even if automatic
		// representative selection changes its candidate-derived key.
		publish := append([]DurableClusterInput(nil), plan.Published...)
		for i := range publish {
			target := plan.Targets[i]
			if target.CandidateKey != publish[i].StableKey {
				return fmt.Errorf("publication target ordering mismatch")
			}
			if target.ClusterID != 0 {
				resolved, e := tx.reconcilePartialDurableClusterInput(ctx, want.RepoID, publish[i])
				if e != nil {
					return e
				}
				var actual int64
				if e = tx.q().QueryRowContext(ctx, "select id from cluster_groups where repo_id=? and stable_key=?", want.RepoID, resolved.StableKey).Scan(&actual); e != nil {
					return e
				}
				if actual != target.ClusterID {
					return fmt.Errorf("resolved publication identity changed")
				}
				publish[i].StableKey = resolved.StableKey
				publish[i].StableSlug = resolved.StableSlug
				publish[i].Title = resolved.Title
			}
		}
		scope := plan.Scope(got.Availability.Scope, 0, proofHash)
		if len(publish) == 0 {
			result.RunID, e = tx.insertClusterRun(ctx, want.RepoID, time.Now().UTC().Format(time.RFC3339Nano))
			if e == nil {
				_, e = tx.q().ExecContext(ctx, "update cluster_runs set status='blocked_identity',stats_json='{}' where id=?", result.RunID)
			}
		} else if scope.Mode == "full-native" {
			result, e = tx.SaveCompleteScopedClusters(ctx, want.RepoID, publish, plan.PreservedGroupIDs...)
		} else {
			result, e = tx.SaveAvailableSubsetClusters(ctx, want.RepoID, publish)
		}
		if e != nil {
			return e
		}
		after, e := tx.ClusterQuarantineRowHash(ctx, want.RepoID, plan.PreservedGroupIDs)
		if e != nil {
			return e
		}
		if after != plan.ProtectedPreimageSHA256 {
			return fmt.Errorf("quarantined identity postimage changed; rollback")
		}
		scope.RunID = strconv.FormatInt(result.RunID, 10)
		if scope.Mode == "full-native" {
			scope.LastFullRunID = &scope.RunID
		} else {
			rows, e := tx.q().QueryContext(ctx, "select id,stats_json from cluster_runs where repo_id=? and id<? and scope='durable' and status in ('success','completed') order by id desc", want.RepoID, result.RunID)
			if e != nil {
				return e
			}
			for rows.Next() {
				var id int64
				var raw *string
				if e = rows.Scan(&id, &raw); e != nil {
					rows.Close()
					return e
				}
				if raw != nil {
					p, e := decodeClusterScope([]byte(*raw), id, scope.Repository)
					if e != nil {
						rows.Close()
						return e
					}
					if p != nil && p.Mode == "full-native" {
						scope.LastFullRunID = clusterRunKey(id)
						break
					}
				}
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return e
			}
		}
		if e = tx.q().QueryRowContext(ctx, "select coalesce(sum(cm.last_seen_run_id=?),0),coalesce(sum(cm.last_seen_run_id IS NOT ?),0) from cluster_memberships cm join cluster_groups cg on cg.id=cm.cluster_id where cg.repo_id=? and cg.status='active' and cm.state='active'", result.RunID, result.RunID, want.RepoID).Scan(&scope.RefreshedMemberships, &scope.RetainedPriorMemberships); e != nil {
			return e
		}
		if e = ValidateClusterScope(scope, result.RunID, scope.Repository); e != nil {
			return e
		}
		// Record the complete computed partition as this run's immutable v3
		// generation and select it in the same transaction. It never writes the
		// durable identity tables, so the quarantine postimage stays unchanged.
		// Resolve every published target to the durable ID actually written in
		// this run, including identities this publication just created.
		publishedIDs := map[string]int64{}
		for i, target := range plan.Targets {
			var id int64
			if e = tx.q().QueryRowContext(ctx, "select id from cluster_groups where repo_id=? and stable_key=?", want.RepoID, publish[i].StableKey).Scan(&id); e != nil {
				return fmt.Errorf("resolve published durable identity: %w", e)
			}
			publishedIDs[target.CandidateKey] = id
		}
		if _, e = tx.SaveClusterCandidateGeneration(ctx, ClusterCandidateGenerationInput{RepoID: want.RepoID, RunID: result.RunID,
			Algorithm: NativeClusterAlgorithm, InputSHA256: want.InputSHA256, QuarantinePlanSHA256: proofHash, Plan: plan,
			PublishedClusterIDs: publishedIDs}); e != nil {
			return e
		}
		if after, e = tx.ClusterQuarantineRowHash(ctx, want.RepoID, plan.PreservedGroupIDs); e != nil {
			return e
		}
		if after != plan.ProtectedPreimageSHA256 {
			return fmt.Errorf("computed candidate generation changed quarantined identities; rollback")
		}
		if _, e = tx.q().ExecContext(ctx, "update cluster_runs set started_at=?,finished_at=? where id=?", got.Availability.AdmissionAt, time.Now().UTC().Format(time.RFC3339Nano), result.RunID); e != nil {
			return e
		}

		// Persist the no-op proof with the cluster transaction. The state hash omits
		// only this self-referential receipt key, so a lost host receipt cannot cause
		// another identical graph run after crash recovery.
		var statsRaw string
		if e = tx.q().QueryRowContext(ctx, "select stats_json from cluster_runs where id=?", result.RunID).Scan(&statsRaw); e != nil {
			return e
		}
		var stats map[string]any
		if e = json.Unmarshal([]byte(statsRaw), &stats); e != nil {
			return e
		}
		stats["native_producer"] = map[string]any{"input_sha256": want.InputSHA256, "state_sha256": "", "algorithm": NativeClusterAlgorithm, "scope": scope, "availability_evidence": got.Availability.Unavailable, "quarantine_plan_sha256": proofHash}
		encoded, e := json.Marshal(stats)
		if e != nil {
			return e
		}
		if _, e = tx.q().ExecContext(ctx, "update cluster_runs set stats_json=? where id=?", string(encoded), result.RunID); e != nil {
			return e
		}
		state, e = tx.nativeClusterState(ctx, want.RepoID)
		if e != nil {
			return e
		}
		stats["native_producer"].(map[string]any)["state_sha256"] = state
		encoded, e = json.Marshal(stats)
		if e != nil {
			return e
		}
		_, e = tx.q().ExecContext(ctx, "update cluster_runs set stats_json=? where id=?", string(encoded), result.RunID)
		return e
	})
	return result, state, e
}
func (s *Store) nativeClusterWithTx(ctx context.Context, fn func(*Store) error) error {
	if s.queries != nil {
		return fn(s)
	}
	return s.WithTx(ctx, fn)
}

// Preview validates the exact current request first; overlay bytes are never
// admitted independently of the native broker importer contract.
func (s *Store) NativeCombinedSnapshot(ctx context.Context, policy, request, response []byte) (NativeClusterSnapshot, error) {
	var out NativeClusterSnapshot
	e := s.nativeClusterWithTx(ctx, func(tx *Store) error {
		preview, e := tx.PreviewNativeEmbeddingResponse(ctx, request, response, policy)
		if e != nil {
			return e
		}
		overlay, e := nativeResponseOverlay(request, response)
		if e != nil {
			return e
		}
		out, e = tx.nativeClusterSnapshotOverlay(ctx, policy, overlay)
		out.PreviewReceipt = preview
		return e
	})
	return out, e
}
func nativeResponseOverlay(request, response []byte) (map[int64]ThreadVector, error) {
	var req NativeEmbeddingRequest
	var res NativeEmbeddingResponse
	if e := json.Unmarshal(request, &req); e != nil {
		return nil, e
	}
	if e := json.Unmarshal(response, &res); e != nil {
		return nil, e
	}
	values := map[string][]float64{}
	for _, r := range res.Results {
		values[r.TaskID] = r.Vector
	}
	out := map[int64]ThreadVector{}
	for _, t := range req.Tasks {
		id, e := strconv.ParseInt(t.ThreadBinding.ThreadID, 10, 64)
		if e != nil {
			return nil, e
		}
		out[id] = ThreadVector{ThreadID: id, Basis: req.Basis, Model: req.Model, Dimensions: req.Dimensions, ContentHash: t.NativeContentHash, Vector: values[t.TaskID], Backend: "exact"}
	}
	return out, nil
}
func (s *Store) NativeCombinedPublish(ctx context.Context, want NativeClusterSnapshot, policy, request, response []byte, inputs []DurableClusterInput) (NativeImportReceipt, SaveDurableClustersResult, string, error) {
	var imported NativeImportReceipt
	var clusters SaveDurableClustersResult
	var state string
	e := s.WithTx(ctx, func(tx *Store) error {
		current, e := tx.NativeCombinedSnapshot(ctx, policy, request, response)
		if e != nil {
			return e
		}
		if current.InputSHA256 != want.InputSHA256 || current.StateSHA256 != want.StateSHA256 {
			return fmt.Errorf("native combined snapshot changed")
		}
		seen := map[int64]bool{}
		for _, in := range inputs {
			for _, member := range in.Members {
				if _, ok := current.Threads[member.ThreadID]; !ok || seen[member.ThreadID] {
					return fmt.Errorf("combined membership invalid")
				}
				seen[member.ThreadID] = true
			}
		}
		if len(seen) != len(current.Threads) {
			return fmt.Errorf("combined membership incomplete")
		}
		imported, e = tx.ImportNativeEmbeddingResponse(ctx, request, response, policy)
		if e != nil {
			return e
		}
		if want.CommittedInput == want.InputSHA256 && want.CommittedState == want.StateSHA256 {
			clusters = SaveDurableClustersResult{RunID: want.LastRun, ClusterCount: len(inputs), MemberCount: len(want.Threads)}
			state = want.StateSHA256
			return nil
		}
		clusters, state, e = tx.NativePublishClusters(ctx, want, policy, inputs)
		return e
	})
	return imported, clusters, state, e
}

func (s *Store) NativePublishedClusterScope(ctx context.Context, repoID int64) (*ClusterScope, error) {
	r, e := s.ClusterReadSelection(ctx, repoID, ClusterSourceDurable)
	return r.ClusterScope, e
}

// Preview partial identity reconciliation without settling the ordinary owner.
func (s *Store) NativeCheckClusterPlan(ctx context.Context, want NativeClusterSnapshot, inputs []DurableClusterInput) error {
	return s.nativeClusterWithTx(ctx, func(tx *Store) error {
		state, e := tx.nativeClusterState(ctx, want.RepoID)
		if e != nil {
			return e
		}
		if state != want.StateSHA256 {
			return fmt.Errorf("native combined input/curation changed during compute")
		}
		if want.Availability.Scope.Mode != "available-subset" {
			return nil
		}
		for _, input := range inputs {
			if _, e = tx.reconcilePartialDurableClusterInput(ctx, want.RepoID, input); e != nil {
				if strings.Contains(e.Error(), "overlaps multiple active identities") {
					return fmt.Errorf("partial_identity_conflict: %w", e)
				}
				return e
			}
		}
		return nil
	})
}

// NativePlanClusters constructs complete current relation closure without source
// writes. The exact same proof is recomputed inside the final transaction.
func (s *Store) NativePlanClusters(ctx context.Context, want NativeClusterSnapshot, inputs []DurableClusterInput) (ClusterQuarantinePlan, error) {
	var plan ClusterQuarantinePlan
	e := s.nativeClusterWithTx(ctx, func(tx *Store) error {
		state, e := tx.nativeClusterState(ctx, want.RepoID)
		if e != nil {
			return e
		}
		if state != want.StateSHA256 {
			return fmt.Errorf("native combined input/curation changed during compute")
		}
		ids := make([]int64, 0, len(want.Threads))
		for id := range want.Threads {
			ids = append(ids, id)
		}
		groups, e := tx.ClusterIdentityMetadata(ctx, want.RepoID)
		if e != nil {
			return e
		}
		plan, e = PlanClusterQuarantine(want.RepoID, inputs, ids, groups)
		if e != nil {
			return e
		}
		plan.InputSHA256 = want.InputSHA256
		plan.StateSHA256 = state
		raw, e := json.Marshal(want.Source)
		if e != nil {
			return e
		}
		plan.SourceIdentitySHA256 = NativeSHA256(raw)
		plan.AvailabilitySHA256 = want.Availability.SemanticDigest
		plan.ProtectedPreimageSHA256, e = tx.ClusterQuarantineRowHash(ctx, want.RepoID, plan.PreservedGroupIDs)
		if e != nil {
			return e
		}
		_, _, e = plan.Proof()
		return e
	})
	return plan, e
}
func (s *Store) NativeCurrentClusterAnalysisScope(ctx context.Context, repoID int64) (*ClusterScope, error) {
	r, e := s.ClusterReadSelection(ctx, repoID, ClusterSourceDurable)
	if e != nil {
		return nil, e
	}
	if r.Analysis != nil {
		return r.Analysis.ClusterScope, nil
	}
	return r.ClusterScope, nil
}
