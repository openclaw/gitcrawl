package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func quarantineReaderFixture(t *testing.T, emptyRaw, newerLegacy, published bool) *Store {
	t.Helper()
	raw, e := os.ReadFile("testdata/cluster-selection-v2.json")
	if e != nil {
		t.Fatal(e)
	}
	var o selectionOracle
	if e = json.Unmarshal(raw, &o); e != nil {
		t.Fatal(e)
	}
	tables := o.Fixture["tables"].(map[string]any)
	for _, row := range tables["cluster_runs"].([]any) {
		delete(row.(map[string]any), "native_producer_scope")
	}
	zero, three := 0, 3
	incomplete := false
	reasons := []string{"identity_quarantine"}
	scope := ClusterScope{Format: ClusterScopeFormatV2, Mode: "available-subset", RunID: "11", Repository: "fixture/repo", PolicyVersion: 2, PolicyDigest: ClusterQuarantinePolicyDigest, SourceAsOf: ClusterScopeClock{Through: "2026-01-01T11:59:50Z", ObservedAt: "2026-01-01T11:59:55Z"}, ProviderComplete: true, SelectedInputComplete: true, Population: ClusterScopePopulation{NativeEligible: 3, Selected: 3, Published: &zero, IdentityQuarantined: &three}, RetainedPriorMemberships: 3, IdentityReconciliationComplete: &incomplete, PublicationStatus: "retained_only", PartialReasons: &reasons, Quarantine: &ClusterQuarantineScope{ProofSHA256: StableHash("proof"), Components: []ClusterQuarantineComponent{{ComponentID: StableHash("component"), Reasons: []string{"multiple_existing_identities"}, ClusterIDs: []string{"7", "11"}, CandidateCount: 1, SelectedCount: 3, RetainedMembershipCount: 3}}}}
	status := "blocked_identity"
	if published {
		status = "success"
		scope.Mode = "full-native"
		scope.FullNativeScopeComplete = true
		scope.LastFullRunID = clusterRunKey(11)
		scope.IdentityReconciliationComplete = boolClusterPointer(true)
		scope.Population.Published = intClusterPointer(3)
		scope.Population.IdentityQuarantined = intClusterPointer(0)
		empty := []string{}
		scope.PartialReasons = &empty
		scope.PublicationStatus = "published"
		scope.Quarantine.Components = []ClusterQuarantineComponent{}
	}
	encoded, _ := json.Marshal(scope)
	var obj any
	json.Unmarshal(encoded, &obj)
	tables["cluster_runs"] = append(tables["cluster_runs"].([]any), map[string]any{"id": float64(11), "repo_id": float64(1), "scope": "durable", "status": status, "finished_at": "2026-01-01T12:00:00Z", "native_producer_scope": obj})
	if newerLegacy {
		tables["cluster_runs"] = append(tables["cluster_runs"].([]any), map[string]any{"id": float64(12), "repo_id": float64(1), "scope": "durable", "status": "success", "finished_at": "2026-01-01T12:01:00Z"})
	}
	if emptyRaw {
		tables["clusters"] = []any{}
		tables["cluster_members"] = []any{}
	}
	return loadSelectionFixture(t, o.Fixture)
}
func TestQuarantineAnalysisNamespaces(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		empty, newer, published bool
	}{{"mixed", false, false, false}, {"empty_raw", true, false, false}, {"new_legacy_floor", true, true, false}, {"v2_authority", false, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			s := quarantineReaderFixture(t, tc.empty, tc.newer, tc.published)
			rows, read, e := s.QualifiedClusterSummaries(context.Background(), ClusterSummaryOptions{RepoID: 1, IncludeClosed: true}, "")
			if e != nil {
				t.Fatal(e)
			}
			if tc.newer {
				if read.Analysis != nil {
					t.Fatal("stale analysis crossed newer durable floor")
				}
			} else if read.Analysis == nil || read.Analysis.RunID != "11" {
				t.Fatal("analysis missing")
			}
			if tc.empty && (read.Selection.Source != "legacy_display" || read.Selection.RunID != nil) {
				t.Fatal("empty raw changed legacy selection", read.Selection)
			}
			if tc.published && read.Selection.Source != ClusterSourceDurable {
				t.Fatal("v2 success not authoritative")
			}
			for _, r := range rows {
				if r.Source == ClusterSourceRun && (r.IdentityQuarantineStatus != "not_recorded" || r.IdentityQuarantine != nil) {
					t.Fatal("raw row inherited colliding durable ID")
				}
			}
			if tc.empty && !tc.newer {
				found := false
				for _, r := range rows {
					if r.ID == 7 {
						found = true
						if r.IdentityQuarantineStatus != "quarantined" {
							t.Fatal("durable fallback lost analysis")
						}
					}
				}
				if !found {
					t.Fatal("durable fallback absent")
				}
			}
		})
	}
	s := quarantineReaderFixture(t, false, false, false)
	read, e := s.ClusterReadSelection(context.Background(), 1, ClusterSourceRun)
	if e != nil || read.Analysis != nil {
		t.Fatal("explicit raw analysis", e)
	}
	d, e := s.ClusterDetail(context.Background(), ClusterDetailOptions{RepoID: 1, ClusterID: 7, Source: ClusterSourceRun, IncludeClosed: true})
	if e != nil || d.Analysis != nil || d.Cluster.IdentityQuarantineStatus != "not_recorded" {
		t.Fatal("raw detail namespace", e)
	}
	d, e = s.ClusterDetail(context.Background(), ClusterDetailOptions{RepoID: 1, ClusterID: 7, Source: ClusterSourceDurable, IncludeClosed: true})
	if e != nil || d.Analysis == nil || d.Cluster.IdentityQuarantineStatus != "quarantined" {
		t.Fatal("durable detail analysis", e)
	}
	if _, e = s.DB().Exec("update cluster_memberships set last_seen_run_id=11 where cluster_id=7"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ClusterDetail(context.Background(), ClusterDetailOptions{RepoID: 1, ClusterID: 7, Source: ClusterSourceDurable, IncludeClosed: true}); e == nil {
		t.Fatal("new-generation marker admitted in quarantine")
	}
}
func TestQuarantineMalformedBlockedScopeRefused(t *testing.T) {
	s := quarantineReaderFixture(t, false, false, false)
	s.DB().Exec("update cluster_runs set finished_at='2025-01-01T00:00:00Z' where id=11")
	for _, source := range []string{"", ClusterSourceDurable, ClusterSourceRun} {
		if _, e := s.ClusterReadSelection(context.Background(), 1, source); e == nil {
			t.Fatal("invalid blocked admission admitted", source)
		}
	}
}

func TestHistoricalQuarantineLegacyReferenceSurvivesFullPersistence(t *testing.T) {
	s := quarantineReaderFixture(t, false, false, false)
	ctx := context.Background()
	// The historical v2 scope references group11. A legacy similarity identity
	// must remain addressable after the complete v2 persistence path.
	if _, e := s.DB().Exec("update cluster_groups set cluster_type='similarity' where id=11"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.SaveCompleteScopedClusters(ctx, 1, []DurableClusterInput{{StableKey: "fresh", StableSlug: "fresh", RepresentativeThreadID: 1, Members: []DurableClusterMemberInput{{ThreadID: 1, Role: "canonical"}}}}); e != nil {
		t.Fatal(e)
	}
	var count int
	if e := s.DB().QueryRow("select count(*) from cluster_groups where id=11 and repo_id=1").Scan(&count); e != nil || count != 1 {
		t.Fatal("referenced legacy identity deleted", e)
	}
	// Validates all historical v2 references, not just the now-ineligible analysis.
	if _, e := s.ClusterReadSelection(ctx, 1, ClusterSourceDurable); e != nil {
		t.Fatal("old scope poisoned later reader", e)
	}
}
