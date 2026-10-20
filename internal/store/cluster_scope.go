package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

const ClusterScopeFormat = "gitcrawl.cluster-scope.v1"

type ClusterScopePopulation struct {
	NativeEligible            int  `json:"native_eligible"`
	Selected                  int  `json:"selected"`
	Unavailable               int  `json:"unavailable"`
	UnavailableInScope        int  `json:"unavailable_in_scope"`
	UnmaterializedUnavailable int  `json:"unmaterialized_unavailable"`
	MissingVectors            int  `json:"missing_vectors"`
	StaleVectors              int  `json:"stale_vectors"`
	Published                 *int `json:"published,omitempty"`
	IdentityQuarantined       *int `json:"identity_quarantined,omitempty"`
}
type ClusterScopeClock struct {
	Through    string `json:"through"`
	ObservedAt string `json:"observed_at"`
}
type ClusterScope struct {
	Format                         string                  `json:"format"`
	Mode                           string                  `json:"mode"`
	RunID                          string                  `json:"run_id"`
	Repository                     string                  `json:"repository"`
	PolicyVersion                  int                     `json:"policy_version"`
	PolicyDigest                   string                  `json:"policy_digest"`
	SourceAsOf                     ClusterScopeClock       `json:"source_as_of"`
	ProviderComplete               bool                    `json:"provider_complete"`
	FullNativeScopeComplete        bool                    `json:"full_native_scope_complete"`
	SelectedInputComplete          bool                    `json:"selected_input_complete"`
	Population                     ClusterScopePopulation  `json:"population"`
	RefreshedMemberships           int                     `json:"refreshed_memberships"`
	RetainedPriorMemberships       int                     `json:"retained_prior_memberships"`
	LastFullRunID                  *string                 `json:"last_full_run_id"`
	IdentityReconciliationComplete *bool                   `json:"identity_reconciliation_complete,omitempty"`
	PublicationStatus              string                  `json:"publication_status,omitempty"`
	PartialReasons                 *[]string               `json:"partial_reasons,omitempty"`
	Quarantine                     *ClusterQuarantineScope `json:"quarantine,omitempty"`
}
type ClusterSelection struct {
	Source string  `json:"source"`
	RunID  *string `json:"run_id"`
}
type ClusterReadContext struct {
	Selection    ClusterSelection `json:"selection"`
	ScopeStatus  string           `json:"scope_status"`
	ClusterScope *ClusterScope    `json:"cluster_scope"`
	Analysis     *ClusterAnalysis `json:"analysis"`
}

func clusterRunKey(id int64) *string { v := strconv.FormatInt(id, 10); return &v }
func positiveClusterKey(s string) (int64, error) {
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil || n <= 0 || strconv.FormatInt(n, 10) != s {
		return 0, fmt.Errorf("cluster scope invalid run ID")
	}
	return n, nil
}

// ValidateClusterScope validates historical metadata, not current readiness.
// Admission freshness is checked by the producer; old pinned runs remain readable.
func ValidateClusterScope(s ClusterScope, runID int64, repository string) error {
	if s.Format != ClusterScopeFormat && s.Format != ClusterScopeFormatV2 {
		return fmt.Errorf("cluster scope version unsupported")
	}
	id, e := positiveClusterKey(s.RunID)
	if e != nil || id != runID || s.Repository != repository || (s.Format == ClusterScopeFormat && s.PolicyVersion != 1) || (s.Format == ClusterScopeFormatV2 && s.PolicyVersion != 2) {
		return fmt.Errorf("cluster scope binding mismatch")
	}
	if len(s.PolicyDigest) != 64 {
		return fmt.Errorf("cluster scope policy digest invalid")
	}
	for _, c := range s.PolicyDigest {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return fmt.Errorf("cluster scope policy digest invalid")
		}
	}
	a, e := time.Parse(time.RFC3339Nano, s.SourceAsOf.Through)
	if e != nil {
		return fmt.Errorf("cluster scope frontier invalid")
	}
	b, e := time.Parse(time.RFC3339Nano, s.SourceAsOf.ObservedAt)
	if e != nil || a.After(b) {
		return fmt.Errorf("cluster scope clock order invalid")
	}
	p := s.Population
	for _, n := range []int{p.NativeEligible, p.Selected, p.Unavailable, p.UnavailableInScope, p.UnmaterializedUnavailable, p.MissingVectors, p.StaleVectors, s.RefreshedMemberships, s.RetainedPriorMemberships} {
		if n < 0 || uint64(n) > 9007199254740991 {
			return fmt.Errorf("cluster scope count invalid")
		}
	}
	if p.NativeEligible != p.Selected+p.UnavailableInScope || p.Unavailable < p.UnavailableInScope+p.UnmaterializedUnavailable || p.MissingVectors != 0 || p.StaleVectors != 0 || !s.SelectedInputComplete {
		return fmt.Errorf("cluster scope population invalid")
	}
	var last int64
	if s.LastFullRunID != nil {
		last, e = positiveClusterKey(*s.LastFullRunID)
		if e != nil || last > id {
			return fmt.Errorf("cluster scope full run invalid")
		}
	}
	if s.Format == ClusterScopeFormatV2 {
		return validateClusterScopeV2(s, id, last)
	}
	if s.IdentityReconciliationComplete != nil || s.PublicationStatus != "" || s.PartialReasons != nil || s.Quarantine != nil || p.Published != nil || p.IdentityQuarantined != nil {
		return fmt.Errorf("v1 scope contains v2 fields")
	}
	switch s.Mode {
	case "available-subset":
		if s.ProviderComplete || s.FullNativeScopeComplete || last == id {
			return fmt.Errorf("partial cluster scope claims completeness")
		}
	case "full-native":
		if !s.ProviderComplete || !s.FullNativeScopeComplete || p.Unavailable != 0 || last != id {
			return fmt.Errorf("full cluster scope incomplete")
		}
	default:
		return fmt.Errorf("cluster scope mode unsupported")
	}
	return nil
}

func decodeClusterScope(raw []byte, runID int64, repo string) (*ClusterScope, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var stats map[string]json.RawMessage
	if e := json.Unmarshal(raw, &stats); e != nil {
		return nil, fmt.Errorf("cluster stats invalid: %w", e)
	}
	v, ok := stats["native_producer"]
	if !ok {
		return nil, nil
	}
	var native map[string]json.RawMessage
	if e := json.Unmarshal(v, &native); e != nil {
		return nil, e
	}
	v, ok = native["scope"]
	if !ok {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		return nil, fmt.Errorf("cluster scope null is not legacy absence")
	}
	var required map[string]json.RawMessage
	if e := json.Unmarshal(v, &required); e != nil {
		return nil, e
	}
	if e := requireClusterScopeShape(v); e != nil {
		return nil, e
	}
	var scope ClusterScope
	d := json.NewDecoder(bytes.NewReader(v))
	d.DisallowUnknownFields()
	if e := d.Decode(&scope); e != nil {
		return nil, e
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return nil, fmt.Errorf("cluster scope trailing value")
	}
	if e := ValidateClusterScope(scope, runID, repo); e != nil {
		return nil, e
	}
	return &scope, nil
}

// ClusterReadSelection validates present scope before looking for an authority
// marker. A corrupt only marker cannot silently restore legacy raw-first reads.
func (s *Store) ClusterReadSelection(ctx context.Context, repoID int64, source string) (ClusterReadContext, error) {
	out := ClusterReadContext{ScopeStatus: "legacy_unknown", Selection: ClusterSelection{Source: "legacy_display"}}

	if !s.hasTable(ctx, "cluster_runs") {
		if source == ClusterSourceDurable {
			out.Selection.Source = source
		}
		return out, nil
	}
	rows, e := s.q().QueryContext(ctx, `SELECT cr.id,cr.stats_json,r.full_name,cr.started_at,cr.finished_at,cr.status FROM cluster_runs cr JOIN repositories r ON r.id=cr.repo_id WHERE cr.repo_id=? AND cr.scope='durable' AND cr.status IN ('success','completed','blocked_identity') ORDER BY cr.id DESC`, repoID)
	if e != nil {
		return out, e
	}
	anchors := map[int64]*ClusterScope{}
	all := []*ClusterScope{}
	authority := false
	first := true
	for rows.Next() {
		var id int64
		var raw, finished sql.NullString
		var repo, started, status string
		if e = rows.Scan(&id, &raw, &repo, &started, &finished, &status); e != nil {
			rows.Close()
			return out, e
		}
		scope, err := decodeClusterScope([]byte(raw.String), id, repo)
		if err != nil {
			rows.Close()
			return out, err
		}
		success := status == "success" || status == "completed"
		if scope != nil {
			at, err := time.Parse(time.RFC3339Nano, started)
			observed, _ := time.Parse(time.RFC3339Nano, scope.SourceAsOf.ObservedAt)
			through, _ := time.Parse(time.RFC3339Nano, scope.SourceAsOf.Through)
			if err != nil || observed.After(at) || at.Sub(observed) > 5*time.Minute || at.Sub(through) > 5*time.Minute {
				rows.Close()
				return out, fmt.Errorf("cluster scope admission clock invalid")
			}
			if scope.Format == ClusterScopeFormatV2 {
				end, err := time.Parse(time.RFC3339Nano, finished.String)
				if err != nil || end.Before(at) || (success && scope.PublicationStatus != "published") || (!success && scope.PublicationStatus != "retained_only") {
					rows.Close()
					return out, fmt.Errorf("v2 analysis status/admission mismatch")
				}
				if out.Analysis == nil {
					out.Analysis = &ClusterAnalysis{Status: scope.PublicationStatus, RunID: scope.RunID, ScopeStatus: "native_v2", ClusterScope: scope}
				}
			} else if !success {
				rows.Close()
				return out, fmt.Errorf("blocked analysis requires v2")
			}
			all = append(all, scope)
			if success {
				anchors[id] = scope
				authority = true
			}
		} else if !success {
			rows.Close()
			return out, fmt.Errorf("blocked analysis missing scope")
		}
		if first && success {
			out.Selection.RunID = clusterRunKey(id)
			out.ClusterScope = scope
			first = false
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	for _, scope := range all {
		if scope.LastFullRunID != nil {
			id, _ := positiveClusterKey(*scope.LastFullRunID)
			anchor := anchors[id]
			if anchor == nil || anchor.Mode != "full-native" {
				return out, fmt.Errorf("cluster scope full anchor missing or invalid")
			}
		}
	}
	if out.Analysis != nil && out.Selection.RunID != nil {
		a, _ := positiveClusterKey(out.Analysis.RunID)
		b, _ := positiveClusterKey(*out.Selection.RunID)
		if a < b {
			out.Analysis = nil
		}
	}
	if len(all) > 0 {
		if e = s.validateQuarantineGroupReferences(ctx, repoID, all); e != nil {
			return out, e
		}
	}
	if source == ClusterSourceRun {
		rawRead := ClusterReadContext{ScopeStatus: "legacy_unknown", Selection: ClusterSelection{Source: ClusterSourceRun}}
		id, ok, err := s.latestRawClusterRunID(ctx, repoID)
		if ok {
			rawRead.Selection.RunID = clusterRunKey(id)
		}
		return rawRead, err
	}
	if authority || source == ClusterSourceDurable {
		out.Selection.Source = ClusterSourceDurable
		if out.ClusterScope != nil {
			out.ScopeStatus = "native_v1"
			if out.ClusterScope.Format == ClusterScopeFormatV2 {
				out.ScopeStatus = "native_v2"
			}
		}
		return out, nil
	}
	out.ClusterScope = nil
	out.Selection.RunID = nil
	id, ok, e := s.latestRawClusterRunID(ctx, repoID)
	if ok {
		out.Selection.RunID = clusterRunKey(id)
	}
	return out, e
}

func clusterMemberGeneration(marker *int64, read ClusterReadContext) (*string, string, error) {
	var key *string
	if marker != nil {
		if *marker <= 0 {
			return nil, "", fmt.Errorf("invalid cluster membership generation")
		}
		key = clusterRunKey(*marker)
	}
	if read.ClusterScope == nil || marker == nil {
		return key, "legacy_unknown", nil
	}
	run, _ := positiveClusterKey(read.ClusterScope.RunID)
	if *marker > run {
		return nil, "", fmt.Errorf("cluster member generation exceeds selected scope")
	}
	if *marker == run {
		return key, "seen_in_run", nil
	}
	return key, "retained_prior", nil
}

// QualifiedClusterSummaries pins selection and rows to one read transaction.
func (s *Store) QualifiedClusterSummaries(ctx context.Context, o ClusterSummaryOptions, source string) ([]ClusterSummary, ClusterReadContext, error) {
	var rows []ClusterSummary
	var read ClusterReadContext
	work := func(tx *Store) error {
		var e error
		read, e = tx.ClusterReadSelection(ctx, o.RepoID, source)
		if e != nil {
			return e
		}
		switch source {
		case ClusterSourceRun:
			rows, e = tx.ListRunClusterSummaries(ctx, o)
		case ClusterSourceDurable:
			rows, e = tx.ListClusterSummaries(ctx, o)
		default:
			rows, e = tx.ListDisplayClusterSummaries(ctx, o)

		}
		if e == nil {
			for i := range rows {
				if rows[i].Source == ClusterSourceRun {
					rows[i].ClusterIdentityContext = ClusterIdentityContext{IdentityQuarantineStatus: "not_recorded"}
				} else {
					rows[i].ClusterIdentityContext = clusterIdentityContext(rows[i].ID, read)
				}
			}
		}
		return e
	}
	var err error
	if s.queries != nil {
		err = work(s)
	} else {
		err = s.WithTx(ctx, work)
	}
	return rows, read, err
}
