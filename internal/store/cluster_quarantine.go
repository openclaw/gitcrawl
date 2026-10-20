package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

const ClusterScopeFormatV2 = "gitcrawl.cluster-scope.v2"
const ClusterQuarantinePolicyDigest = "b6e52d586f9e0063b3aca46b34dffacd35a4bd458d6b89a0ab363f1d62cee48a"
const clusterPlanByteLimit = 32 << 20

type ClusterQuarantineComponent struct {
	ComponentID             string   `json:"component_id"`
	Reasons                 []string `json:"reasons"`
	ClusterIDs              []string `json:"cluster_ids"`
	CandidateCount          int      `json:"candidate_count"`
	SelectedCount           int      `json:"selected_count"`
	RetainedMembershipCount int      `json:"retained_membership_count"`
}
type ClusterQuarantineScope struct {
	ProofSHA256 string                       `json:"proof_sha256"`
	Components  []ClusterQuarantineComponent `json:"components"`
}
type ClusterIdentityMetadata struct {
	ID                      int64   `json:"id"`
	Key                     string  `json:"key"`
	Active                  bool    `json:"active"`
	Protected               bool    `json:"protected"`
	Representative          int64   `json:"representative"`
	ActiveMembers           []int64 `json:"active_members"`
	LocalRemoved            []int64 `json:"local_removed"`
	RetainedMembershipCount int     `json:"retained_membership_count"`
}
type ClusterPublicationTarget struct {
	CandidateKey string `json:"candidate_key"`
	ClusterID    int64  `json:"cluster_id"` // zero means a new identity
}
type ClusterQuarantinePlan struct {
	Format                  string                       `json:"format"`
	PolicyDigest            string                       `json:"policy_digest"`
	RepositoryID            int64                        `json:"repository_id"`
	SelectedIDs             []int64                      `json:"selected_ids"`
	Candidates              []DurableClusterInput        `json:"candidate_partition"`
	Identities              []ClusterIdentityMetadata    `json:"active_identity_metadata"`
	Targets                 []ClusterPublicationTarget   `json:"resolved_targets"`
	Components              []ClusterQuarantineComponent `json:"components"`
	QuarantinedIDs          []int64                      `json:"quarantined_selected_ids"`
	QuarantinedGroups       []int64                      `json:"quarantined_group_ids"`
	PreservedGroupIDs       []int64                      `json:"preserved_group_ids"`
	ProtectedPreimageSHA256 string                       `json:"protected_preimage_sha256"`
	InputSHA256             string                       `json:"input_sha256"`
	StateSHA256             string                       `json:"identity_state_sha256"`
	SourceIdentitySHA256    string                       `json:"source_identity_sha256"`
	AvailabilitySHA256      string                       `json:"availability_sha256"`
	Published               []DurableClusterInput        `json:"-"`
}

func clusterHash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func sortedClusterIDs(set map[int64]bool) []int64 {
	out := make([]int64, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
func clusterIDStrings(ids []int64) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strconv.FormatInt(id, 10)
	}
	return out
}
func (p ClusterQuarantinePlan) Proof() ([]byte, string, error) {
	raw, e := json.Marshal(p)
	if e != nil {
		return nil, "", e
	}
	if len(raw) > clusterPlanByteLimit {
		return nil, "", fmt.Errorf("identity metadata exceeds32MiB")
	}
	return raw, clusterHash(raw), nil
}

// PlanClusterQuarantine is pure metadata logic. Historical inactive memberships
// never become topology edges. Exact write targets still reserve old identities.
func PlanClusterQuarantine(repoID int64, inputs []DurableClusterInput, selected []int64, groups []ClusterIdentityMetadata) (ClusterQuarantinePlan, error) {
	p := ClusterQuarantinePlan{Format: "gitcrawl.identity-quarantine-plan.v1", PolicyDigest: ClusterQuarantinePolicyDigest, RepositoryID: repoID, Candidates: inputs, Identities: groups, Targets: []ClusterPublicationTarget{}, Components: []ClusterQuarantineComponent{}, Published: []DurableClusterInput{}}
	if repoID <= 0 || len(selected) > 12000 {
		return p, fmt.Errorf("invalid quarantine repository/node bound")
	}
	selectedSet := map[int64]bool{}
	for _, id := range selected {
		if id <= 0 || selectedSet[id] {
			return p, fmt.Errorf("invalid selected identity")
		}
		selectedSet[id] = true
	}
	p.SelectedIDs = sortedClusterIDs(selectedSet)
	byID := map[int64]ClusterIdentityMetadata{}
	byKey := map[string]int64{}
	memberships := map[int64][]int64{}
	parent := map[string]string{}
	var find func(string) string
	find = func(x string) string {
		v, ok := parent[x]
		if !ok {
			parent[x] = x
			return x
		}
		if v != x {
			parent[x] = find(v)
		}
		return parent[x]
	}
	union := func(a, b string) {
		a, b = find(a), find(b)
		if a != b {
			if a < b {
				parent[b] = a
			} else {
				parent[a] = b
			}
		}
	}
	gn := func(id int64) string { return "g:" + strconv.FormatInt(id, 10) }
	tn := func(id int64) string { return "t:" + strconv.FormatInt(id, 10) }
	for _, g := range groups {
		if g.ID <= 0 || g.Key == "" || byID[g.ID].ID != 0 || byKey[g.Key] != 0 || g.RetainedMembershipCount < 0 {
			return p, fmt.Errorf("unqualified or duplicate durable identity")
		}
		if g.RetainedMembershipCount < len(g.ActiveMembers) {
			return p, fmt.Errorf("retained count smaller than active memberships")
		}
		byID[g.ID] = g
		byKey[g.Key] = g.ID
		find(gn(g.ID))
		seen := map[int64]bool{}
		if g.Active {
			for _, tid := range g.ActiveMembers {
				if tid <= 0 || seen[tid] {
					return p, fmt.Errorf("invalid active membership")
				}
				seen[tid] = true
				memberships[tid] = append(memberships[tid], g.ID)
				union(gn(g.ID), tn(tid))
			}
			// Local exclusion of the last visible member legitimately leaves a
			// protected active identity with a NULL representative. It is reserved,
			// never repaired, and any incoming target is quarantined.
			if !(g.Protected && g.Representative == 0) && (g.Representative <= 0 || !seen[g.Representative]) {
				return p, fmt.Errorf("active identity representative is not an active member:%d", g.ID)
			}
		}
	}
	seen := map[int64]bool{}
	keys := map[string]bool{}
	targets := map[string]int64{}
	protectedTargets := map[int64]bool{}
	for _, in := range inputs {
		if in.StableKey == "" || keys[in.StableKey] || len(in.Members) == 0 {
			return p, fmt.Errorf("duplicate/empty candidate key or members")
		}
		keys[in.StableKey] = true
		cn := "c:" + in.StableKey
		find(cn)
		overlap := map[int64]bool{}
		representative := false
		for _, m := range in.Members {
			if !selectedSet[m.ThreadID] || seen[m.ThreadID] {
				return p, fmt.Errorf("candidate partition outside selected or duplicate")
			}
			seen[m.ThreadID] = true
			representative = representative || m.ThreadID == in.RepresentativeThreadID
			union(cn, tn(m.ThreadID))
			for _, g := range memberships[m.ThreadID] {
				overlap[g] = true
			}
		}
		if !representative {
			return p, fmt.Errorf("candidate representative outside partition")
		}
		var target int64
		if len(overlap) == 1 {
			for id := range overlap {
				target = id
			}
		} else if len(overlap) == 0 {
			target = byKey[in.StableKey]
		}
		targets[in.StableKey] = target
		if target != 0 {
			union(cn, gn(target))
			g := byID[target]
			for _, tid := range g.LocalRemoved {
				for _, m := range in.Members {
					if tid == m.ThreadID {
						protectedTargets[target] = true
					}
				}
			}
		}
	}
	if len(seen) != len(selectedSet) {
		return p, fmt.Errorf("candidate partition incomplete")
	}
	type component struct {
		inputs []DurableClusterInput
		groups []ClusterIdentityMetadata
	}
	comps := map[string]*component{}
	for _, in := range inputs {
		root := find("c:" + in.StableKey)
		if comps[root] == nil {
			comps[root] = &component{}
		}
		comps[root].inputs = append(comps[root].inputs, in)
	}
	for _, g := range groups {
		if c := comps[find(gn(g.ID))]; c != nil {
			c.groups = append(c.groups, g)
		}
	}
	qthreads := map[int64]bool{}
	qgroups := map[int64]bool{}
	for _, c := range comps {
		reasons := []string{}
		if len(c.groups) > 1 {
			reasons = append(reasons, "multiple_existing_identities")
		}
		if len(c.groups) > 0 && len(c.inputs) > 1 {
			reasons = append(reasons, "existing_identity_split")
		}
		protected, inactive := false, false
		for _, g := range c.groups {
			protected = protected || g.Protected || protectedTargets[g.ID]
			inactive = inactive || !g.Active
		}
		if protected {
			reasons = append(reasons, "protected_curation")
		}
		if inactive {
			reasons = append(reasons, "retained_identity_target")
		}
		sort.Strings(reasons)
		if len(reasons) == 0 {
			for _, in := range c.inputs {
				p.Published = append(p.Published, in)
				p.Targets = append(p.Targets, ClusterPublicationTarget{in.StableKey, targets[in.StableKey]})
			}
			continue
		}
		gs := map[int64]bool{}
		ts := map[int64]bool{}
		ks := []string{}
		reserved := [][]any{}
		protectedIDs := map[int64]bool{}
		retained := 0
		sort.Slice(c.groups, func(i, j int) bool { return c.groups[i].ID < c.groups[j].ID })
		for _, g := range c.groups {
			gs[g.ID] = true
			qgroups[g.ID] = true
			retained += g.RetainedMembershipCount
			if g.Protected || protectedTargets[g.ID] {
				protectedIDs[g.ID] = true
			}
			if g.Active {
				ms := map[int64]bool{}
				for _, id := range g.ActiveMembers {
					ms[id] = true
				}
				reserved = append(reserved, []any{strconv.FormatInt(g.ID, 10), clusterIDStrings(sortedClusterIDs(ms))})
			}
		}
		for _, in := range c.inputs {
			ks = append(ks, in.StableKey)
			for _, m := range in.Members {
				ts[m.ThreadID] = true
				qthreads[m.ThreadID] = true
			}
		}
		sort.Strings(ks)
		// The topology ID excludes observation clocks and inactive history. The plan
		// proof and complete protected preimage independently bind preservation.
		binding := map[string]any{"policy": "gitcrawl.identity-quarantine.v1", "candidate_keys": ks, "group_ids": clusterIDStrings(sortedClusterIDs(gs)), "selected_ids": clusterIDStrings(sortedClusterIDs(ts)), "reasons": reasons, "active_reservations": reserved, "protected_groups": clusterIDStrings(sortedClusterIDs(protectedIDs))}
		raw, e := json.Marshal(binding)
		if e != nil {
			return p, e
		}
		p.Components = append(p.Components, ClusterQuarantineComponent{clusterHash(raw), reasons, clusterIDStrings(sortedClusterIDs(gs)), len(c.inputs), len(ts), retained})
	}
	sort.Slice(p.Published, func(i, j int) bool { return p.Published[i].StableKey < p.Published[j].StableKey })
	sort.Slice(p.Targets, func(i, j int) bool { return p.Targets[i].CandidateKey < p.Targets[j].CandidateKey })
	sort.Slice(p.Components, func(i, j int) bool { return p.Components[i].ComponentID < p.Components[j].ComponentID })
	p.QuarantinedIDs = sortedClusterIDs(qthreads)
	p.QuarantinedGroups = sortedClusterIDs(qgroups)
	preserved := map[int64]bool{}
	for id := range qgroups {
		preserved[id] = true
	}
	for _, g := range groups {
		if g.Protected {
			preserved[g.ID] = true
		}
	}
	p.PreservedGroupIDs = sortedClusterIDs(preserved)
	for _, t := range p.Targets {
		if qgroups[t.ClusterID] {
			return p, fmt.Errorf("publication target crosses quarantine")
		}
	}
	for _, in := range p.Published {
		for _, m := range in.Members {
			if qthreads[m.ThreadID] {
				return p, fmt.Errorf("published member crosses quarantine")
			}
			for _, g := range memberships[m.ThreadID] {
				if qgroups[g] {
					return p, fmt.Errorf("active reservation crosses quarantine")
				}
			}
		}
	}
	if _, _, e := p.Proof(); e != nil {
		return p, e
	}
	return p, nil
}

func (s *Store) ClusterIdentityMetadata(ctx context.Context, repo int64) ([]ClusterIdentityMetadata, error) {
	rows, e := s.q().QueryContext(ctx, `select g.id,g.stable_key,g.status,coalesce(g.closed_at,''),coalesce(g.cluster_type,''),coalesce(g.representative_thread_id,0),
 exists(select 1 from cluster_overrides x where x.repo_id=g.repo_id and x.cluster_id=g.id),
 exists(select 1 from cluster_closures x where x.cluster_id=g.id and x.actor_kind='local'),
 exists(select 1 from cluster_aliases x where x.cluster_id=g.id),
 (select count(*) from cluster_memberships m where m.cluster_id=g.id)
 from cluster_groups g where g.repo_id=? order by g.id`, repo)
	if e != nil {
		return nil, e
	}
	out := []ClusterIdentityMetadata{}
	index := map[int64]int{}
	for rows.Next() {
		var g ClusterIdentityMetadata
		var status, closed, kind string
		var override, closure, alias bool
		if e = rows.Scan(&g.ID, &g.Key, &status, &closed, &kind, &g.Representative, &override, &closure, &alias, &g.RetainedMembershipCount); e != nil {
			rows.Close()
			return nil, e
		}
		g.Active = status == "active" && closed == "" && kind != "similarity"
		g.Protected = override || closure || alias
		g.ActiveMembers = []int64{}
		g.LocalRemoved = []int64{}
		index[g.ID] = len(out)
		out = append(out, g)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	rows, e = s.q().QueryContext(ctx, `select m.cluster_id,m.thread_id,m.state,coalesce(m.added_by,''),coalesce(m.removed_by,'') from cluster_memberships m join cluster_groups g on g.id=m.cluster_id where g.repo_id=? and (m.state='active' or m.removed_by='local') order by m.cluster_id,m.thread_id`, repo)
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		var gid, tid int64
		var state, added, removed string
		if e = rows.Scan(&gid, &tid, &state, &added, &removed); e != nil {
			rows.Close()
			return nil, e
		}
		i, ok := index[gid]
		if !ok {
			rows.Close()
			return nil, fmt.Errorf("membership identity absent")
		}
		if state == "active" {
			out[i].ActiveMembers = append(out[i].ActiveMembers, tid)
			if added != "cluster" {
				out[i].Protected = true
			}
		}
		if removed == "local" {
			out[i].LocalRemoved = append(out[i].LocalRemoved, tid)
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	raw, e := json.Marshal(out)
	if e != nil {
		return nil, e
	}
	if len(raw) > clusterPlanByteLimit {
		return nil, fmt.Errorf("identity metadata exceeds32MiB")
	}
	return out, nil
}

// ClusterQuarantineRowHash streams complete retained rows, including inactive
// history and group events. It never writes and excludes no mutable column.
func (s *Store) ClusterQuarantineRowHash(ctx context.Context, repo int64, ids []int64) (string, error) {
	h := sha256.New()
	enc := json.NewEncoder(h)
	wanted := map[int64]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	for _, table := range []struct{ name, key, order string }{{"cluster_groups", "id", "id"}, {"cluster_memberships", "cluster_id", "cluster_id,thread_id"}, {"cluster_overrides", "cluster_id", "cluster_id,id"}, {"cluster_closures", "cluster_id", "cluster_id"}, {"cluster_aliases", "cluster_id", "cluster_id,alias_slug"}, {"cluster_events", "cluster_id", "cluster_id,id"}} {
		if e := enc.Encode(table.name); e != nil {
			return "", e
		}
		for start := 0; start < len(ids); start += 300 {
			end := start + 300
			if end > len(ids) {
				end = len(ids)
			}
			marks := ""
			args := []any{repo}
			for _, id := range ids[start:end] {
				if !wanted[id] {
					return "", fmt.Errorf("quarantine ID missing")
				}
				if marks != "" {
					marks += ","
				}
				marks += "?"
				args = append(args, id)
			}
			query := "select * from " + table.name + " where " + table.key + " in(select id from cluster_groups where repo_id=?) and " + table.key + " in(" + marks + ") order by " + table.order
			rows, e := s.q().QueryContext(ctx, query, args...)
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
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
