package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

type ClusterAnalysis struct {
	Status       string        `json:"status"`
	RunID        string        `json:"run_id"`
	ScopeStatus  string        `json:"scope_status"`
	ClusterScope *ClusterScope `json:"cluster_scope"`
}
type ClusterIdentityQuarantine struct {
	AnalysisRunID string   `json:"analysis_run_id"`
	ComponentID   string   `json:"component_id"`
	Reasons       []string `json:"reasons"`
}
type ClusterIdentityContext struct {
	IdentityQuarantineStatus string                     `json:"identity_quarantine_status,omitempty"`
	IdentityQuarantine       *ClusterIdentityQuarantine `json:"identity_quarantine"`
}

func validClusterDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func safeClusterCount(n int) bool { return n >= 0 && uint64(n) <= 9007199254740991 }
func validateClusterScopeV2(s ClusterScope, id, last int64) error {
	p := s.Population
	if s.PolicyDigest != ClusterQuarantinePolicyDigest || s.IdentityReconciliationComplete == nil || s.PartialReasons == nil || s.Quarantine == nil || p.Published == nil || p.IdentityQuarantined == nil {
		return fmt.Errorf("v2 scope fields/policy missing")
	}
	if !safeClusterCount(*p.Published) || !safeClusterCount(*p.IdentityQuarantined) || p.Selected != *p.Published+*p.IdentityQuarantined || !validClusterDigest(s.Quarantine.ProofSHA256) {
		return fmt.Errorf("v2 publication/proof invalid")
	}
	q := s.Quarantine
	quarantined := 0
	seenGroups := map[int64]bool{}
	lastComponent := ""
	for _, c := range q.Components {
		if !validClusterDigest(c.ComponentID) || c.ComponentID <= lastComponent || c.CandidateCount <= 0 || c.SelectedCount <= 0 || c.CandidateCount > c.SelectedCount || !safeClusterCount(c.CandidateCount) || !safeClusterCount(c.SelectedCount) || !safeClusterCount(c.RetainedMembershipCount) || len(c.ClusterIDs) == 0 || len(c.Reasons) == 0 {
			return fmt.Errorf("invalid quarantine component")
		}
		lastComponent = c.ComponentID
		lastReason := ""
		for _, r := range c.Reasons {
			if r <= lastReason {
				return fmt.Errorf("quarantine reasons not unique ordered")
			}
			switch r {
			case "existing_identity_split", "multiple_existing_identities", "protected_curation", "retained_identity_target":
			default:
				return fmt.Errorf("unknown quarantine reason")
			}
			lastReason = r
		}
		prev := int64(0)
		for _, key := range c.ClusterIDs {
			gid, e := positiveClusterKey(key)
			if e != nil || gid <= prev || seenGroups[gid] {
				return fmt.Errorf("quarantine group IDs invalid/duplicate")
			}
			seenGroups[gid] = true
			prev = gid
		}
		if quarantined > 9007199254740991-c.SelectedCount {
			return fmt.Errorf("quarantine count overflow")
		}
		quarantined += c.SelectedCount
	}
	identityComplete := len(q.Components) == 0
	if *s.IdentityReconciliationComplete != identityComplete || quarantined != *p.IdentityQuarantined {
		return fmt.Errorf("quarantine population mismatch")
	}
	reasons := []string{}
	if !identityComplete {
		reasons = append(reasons, "identity_quarantine")
	}
	if !s.ProviderComplete {
		reasons = append(reasons, "provider_unavailable")
	}
	if len(reasons) != len(*s.PartialReasons) {
		return fmt.Errorf("partial reason derivation mismatch")
	}
	for i, r := range reasons {
		if (*s.PartialReasons)[i] != r {
			return fmt.Errorf("partial reason order/derivation mismatch")
		}
	}
	if s.ProviderComplete {
		if p.Unavailable != 0 || p.UnavailableInScope != 0 || p.UnmaterializedUnavailable != 0 {
			return fmt.Errorf("provider complete has unavailable obligations")
		}
	} else if p.Unavailable == 0 {
		return fmt.Errorf("partial provider has no qualified obligations")
	}
	full := s.ProviderComplete && identityComplete
	if s.FullNativeScopeComplete != full || (full && s.Mode != "full-native") || (!full && s.Mode != "available-subset") || (full && last != id) || (!full && last == id) {
		return fmt.Errorf("v2 completeness/anchor mismatch")
	}
	switch s.PublicationStatus {
	case "published":
		if *p.Published <= 0 {
			return fmt.Errorf("published scope has no published inputs")
		}
	case "retained_only":
		if full || *p.Published != 0 || p.Selected <= 0 || *p.IdentityQuarantined != p.Selected || s.RefreshedMemberships != 0 {
			return fmt.Errorf("retained-only scope claims refresh")
		}
	default:
		return fmt.Errorf("v2 publication status unknown")
	}
	return nil
}
func requiredClusterKeys(raw json.RawMessage, keys []string) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if e := json.Unmarshal(raw, &obj); e != nil {
		return nil, e
	}
	if len(obj) != len(keys) {
		return nil, fmt.Errorf("cluster scope object shape mismatch")
	}
	for _, k := range keys {
		v, ok := obj[k]
		if !ok || (k != "last_full_run_id" && bytes.Equal(bytes.TrimSpace(v), []byte("null"))) {
			return nil, fmt.Errorf("cluster scope missing/null %s", k)
		}
	}
	return obj, nil
}
func requireClusterScopeShape(raw json.RawMessage) error {
	var header struct {
		Format string `json:"format"`
	}
	if e := json.Unmarshal(raw, &header); e != nil {
		return e
	}
	v2 := header.Format == ClusterScopeFormatV2
	if !v2 && header.Format != ClusterScopeFormat {
		return fmt.Errorf("cluster scope version unsupported")
	}
	keys := []string{"format", "mode", "run_id", "repository", "policy_version", "policy_digest", "source_as_of", "provider_complete", "full_native_scope_complete", "selected_input_complete", "population", "refreshed_memberships", "retained_prior_memberships", "last_full_run_id"}
	pop := []string{"native_eligible", "selected", "unavailable", "unavailable_in_scope", "unmaterialized_unavailable", "missing_vectors", "stale_vectors"}
	if v2 {
		keys = append(keys, "identity_reconciliation_complete", "publication_status", "partial_reasons", "quarantine")
		pop = append(pop, "published", "identity_quarantined")
	}
	obj, e := requiredClusterKeys(raw, keys)
	if e != nil {
		return e
	}
	if _, e = requiredClusterKeys(obj["source_as_of"], []string{"through", "observed_at"}); e != nil {
		return e
	}
	if _, e = requiredClusterKeys(obj["population"], pop); e != nil {
		return e
	}
	if v2 {
		q, e := requiredClusterKeys(obj["quarantine"], []string{"proof_sha256", "components"})
		if e != nil {
			return e
		}
		var cs []json.RawMessage
		if e = json.Unmarshal(q["components"], &cs); e != nil {
			return e
		}
		for _, c := range cs {
			if _, e = requiredClusterKeys(c, []string{"component_id", "reasons", "cluster_ids", "candidate_count", "selected_count", "retained_membership_count"}); e != nil {
				return e
			}
		}
	}
	return nil
}
func (s *Store) validateQuarantineGroupReferences(ctx context.Context, repo int64, scopes []*ClusterScope) error {
	wanted := map[int64]bool{}
	for _, scope := range scopes {
		if scope == nil || scope.Quarantine == nil {
			continue
		}
		for _, c := range scope.Quarantine.Components {
			for _, key := range c.ClusterIDs {
				id, _ := positiveClusterKey(key)
				wanted[id] = true
			}
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	rows, e := s.q().QueryContext(ctx, "select id from cluster_groups where repo_id=?", repo)
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if e = rows.Scan(&id); e != nil {
			return e
		}
		delete(wanted, id)
	}
	if e = rows.Err(); e != nil {
		return e
	}
	if len(wanted) > 0 {
		return fmt.Errorf("quarantine references missing/wrong-repository group")
	}
	return nil
}
func clusterIdentityContext(id int64, read ClusterReadContext) ClusterIdentityContext {
	out := ClusterIdentityContext{IdentityQuarantineStatus: "not_recorded"}
	if read.Selection.Source == ClusterSourceRun || read.Analysis == nil || read.Analysis.ClusterScope == nil {
		return out
	}
	out.IdentityQuarantineStatus = "not_listed"
	key := strconv.FormatInt(id, 10)
	for _, c := range read.Analysis.ClusterScope.Quarantine.Components {
		for _, gid := range c.ClusterIDs {
			if gid == key {
				out.IdentityQuarantineStatus = "quarantined"
				out.IdentityQuarantine = &ClusterIdentityQuarantine{read.Analysis.RunID, c.ComponentID, c.Reasons}
				return out
			}
		}
	}
	return out
}
func clusterMemberRetention(marker *string, identity ClusterIdentityContext, generation string) (*string, *string, error) {
	if q := identity.IdentityQuarantine; q != nil {
		run, _ := positiveClusterKey(q.AnalysisRunID)
		if marker != nil {
			id, e := positiveClusterKey(*marker)
			if e != nil || id >= run {
				return nil, nil, fmt.Errorf("quarantined membership claims new analysis generation")
			}
		}
		reason := "identity_quarantine"
		return &reason, &q.AnalysisRunID, nil
	}
	if generation == "retained_prior" {
		reason := "prior_membership"
		return &reason, nil, nil
	}
	return nil, nil, nil
}
func intClusterPointer(n int) *int    { return &n }
func boolClusterPointer(v bool) *bool { return &v }
func (p ClusterQuarantinePlan) Scope(base ClusterScope, run int64, proof string) ClusterScope {
	base.Format = ClusterScopeFormatV2
	base.PolicyVersion = 2
	base.PolicyDigest = ClusterQuarantinePolicyDigest
	base.RunID = strconv.FormatInt(run, 10)
	base.IdentityReconciliationComplete = boolClusterPointer(len(p.Components) == 0)
	base.Population.Published = intClusterPointer(len(p.SelectedIDs) - len(p.QuarantinedIDs))
	base.Population.IdentityQuarantined = intClusterPointer(len(p.QuarantinedIDs))
	base.Quarantine = &ClusterQuarantineScope{proof, p.Components}
	base.FullNativeScopeComplete = base.ProviderComplete && len(p.Components) == 0
	base.Mode = "available-subset"
	if base.FullNativeScopeComplete {
		base.Mode = "full-native"
	}
	base.PublicationStatus = "published"
	if len(p.Published) == 0 {
		base.PublicationStatus = "retained_only"
	}
	reasons := []string{}
	if !base.ProviderComplete {
		reasons = append(reasons, "provider_unavailable")
	}
	if len(p.Components) > 0 {
		reasons = append(reasons, "identity_quarantine")
	}
	sort.Strings(reasons)
	base.PartialReasons = &reasons
	return base
}
