package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestClusterQuarantineSharedOracle(t *testing.T) {
	raw, e := os.ReadFile("testdata/cluster-quarantine-v2.json")
	if e != nil {
		t.Fatal(e)
	}
	var all struct {
		Cases []struct {
			Name  string `json:"name"`
			Input struct {
				Admitted   bool     `json:"admitted"`
				Fresh      bool     `json:"fresh_vectors"`
				Selected   []string `json:"selected"`
				Candidates []struct {
					Key            string
					Members        []string
					Representative string
				}
				Groups []struct {
					ID, Key, Kind string
					Active        bool
					Protected     *bool
					ActiveMembers []string `json:"active_members"`
					Retained      int      `json:"retained_membership_count"`
				}
			}
			Expected struct {
				Published []string `json:"published_ids"`
				Q         []string `json:"quarantined_ids"`
				Groups    []string `json:"quarantined_group_ids"`
				Error     string
			}
		}
	}
	if e = json.Unmarshal(raw, &all); e != nil {
		t.Fatal(e)
	}
	for _, tc := range all.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			var inputs []DurableClusterInput
			var selected []int64
			var groups []ClusterIdentityMetadata
			invalid := !tc.Input.Admitted || !tc.Input.Fresh
			parse := func(s string) int64 {
				n, e := strconv.ParseInt(s, 10, 64)
				if e != nil {
					t.Fatal(e)
				}
				return n
			}
			for _, id := range tc.Input.Selected {
				selected = append(selected, parse(id))
			}
			for _, c := range tc.Input.Candidates {
				in := DurableClusterInput{StableKey: c.Key, RepresentativeThreadID: parse(c.Representative)}
				for _, id := range c.Members {
					in.Members = append(in.Members, DurableClusterMemberInput{ThreadID: parse(id)})
				}
				inputs = append(inputs, in)
			}
			for _, g := range tc.Input.Groups {
				if g.Protected == nil {
					invalid = true
					continue
				}
				meta := ClusterIdentityMetadata{ID: parse(g.ID), Key: g.Key, Active: g.Active && g.Kind != "similarity", Protected: *g.Protected, RetainedMembershipCount: g.Retained}
				for _, id := range g.ActiveMembers {
					meta.ActiveMembers = append(meta.ActiveMembers, parse(id))
				}
				if len(meta.ActiveMembers) > 0 {
					meta.Representative = meta.ActiveMembers[0]
				}
				groups = append(groups, meta)
			}
			if invalid {
				if tc.Expected.Error == "" {
					t.Fatal("unexpected invalid fixture")
				}
				return
			}
			p, e := PlanClusterQuarantine(1, inputs, selected, groups)
			if tc.Expected.Error != "" {
				if e == nil {
					t.Fatal("expected refusal", tc.Expected.Error)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			pub := map[int64]bool{}
			for _, c := range p.Published {
				for _, m := range c.Members {
					pub[m.ThreadID] = true
				}
			}
			for name, pair := range map[string][2][]string{"published": {clusterIDStrings(sortedClusterIDs(pub)), tc.Expected.Published}, "quarantine": {clusterIDStrings(p.QuarantinedIDs), tc.Expected.Q}, "groups": {clusterIDStrings(p.QuarantinedGroups), tc.Expected.Groups}} {
				if !reflect.DeepEqual(pair[0], pair[1]) {
					t.Fatalf("%s got %v want %v", name, pair[0], pair[1])
				}
			}
		})
	}
}
func TestClusterQuarantineLocalRemovalProtectsOnlyActualTarget(t *testing.T) {
	inputs := []DurableClusterInput{{StableKey: "candidate", RepresentativeThreadID: 1, Members: []DurableClusterMemberInput{{ThreadID: 1}, {ThreadID: 2}}}}
	groups := []ClusterIdentityMetadata{{ID: 5, Key: "old", Active: true, Representative: 1, ActiveMembers: []int64{1}, LocalRemoved: []int64{2}, RetainedMembershipCount: 2}}
	p, e := PlanClusterQuarantine(1, inputs, []int64{1, 2}, groups)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Published) != 0 || len(p.Components) != 1 || !strings.Contains(strings.Join(p.Components[0].Reasons, ","), "protected_curation") {
		t.Fatalf("local removed target unprotected: %+v", p)
	}
}

func TestQuarantinePolicyDigestMatchesWire(t *testing.T) {
	raw, e := os.ReadFile("testdata/cluster-policy-v2.json")
	if e != nil {
		t.Fatal(e)
	}
	if clusterHash(raw) != ClusterQuarantinePolicyDigest {
		t.Fatal("policy constant differs from frozen wire")
	}
}

func TestScopedCompletePreservesUntouchedCurationAndLegacy(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	for n := 1; n <= 2; n++ {
		if _, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: strconv.Itoa(n), Number: n, Kind: "issue", State: "open", Title: "fixture", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"}); e != nil {
			t.Fatal(e)
		}
	}
	in := func(key string, id int64) DurableClusterInput {
		return DurableClusterInput{StableKey: key, StableSlug: key, RepresentativeThreadID: id, Members: []DurableClusterMemberInput{{ThreadID: id, Role: "canonical"}}}
	}
	if _, e = s.SaveCompleteDurableClusters(ctx, repo, []DurableClusterInput{in("a", 1), in("b", 2)}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.SetClusterCanonicalLocally(ctx, repo, 2, 2, "keep local canonical"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB().Exec("insert into cluster_groups(repo_id,stable_key,stable_slug,status,cluster_type,created_at,updated_at) values(?,'legacy','legacy','active','similarity','t','t')", repo); e != nil {
		t.Fatal(e)
	}
	meta, e := s.ClusterIdentityMetadata(ctx, repo)
	if e != nil {
		t.Fatal(e)
	}
	if !meta[1].Protected {
		t.Fatal("curation protection missing")
	}
	before, e := s.ClusterQuarantineRowHash(ctx, repo, []int64{2, 3})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.SaveAvailableSubsetClusters(ctx, repo, []DurableClusterInput{in("new-a", 1)}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.SaveCompleteScopedClusters(ctx, repo, []DurableClusterInput{in("a", 1)}, 2, 3); e != nil {
		t.Fatal(e)
	}
	after, e := s.ClusterQuarantineRowHash(ctx, repo, []int64{2, 3})
	if e != nil || after != before {
		t.Fatal("protected/legacy history changed", e)
	}
}

func TestQuarantineEmptyProtectedIdentityDoesNotBlockIndependentWork(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "empty-protected.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	for n := 1; n <= 2; n++ {
		_, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: strconv.Itoa(n), Number: n, Kind: "issue", State: "open", Title: "fixture", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"})
		if e != nil {
			t.Fatal(e)
		}
	}
	existing := DurableClusterInput{StableKey: "existing", StableSlug: "existing", RepresentativeThreadID: 1, Members: []DurableClusterMemberInput{{ThreadID: 1, Role: "canonical"}}}
	if _, e = s.SaveCompleteDurableClusters(ctx, repo, []DurableClusterInput{existing}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ExcludeClusterMemberLocally(ctx, repo, 1, 1, "exclude last visible member"); e != nil {
		t.Fatal(e)
	}
	groups, e := s.ClusterIdentityMetadata(ctx, repo)
	if e != nil {
		t.Fatal(e)
	}
	if len(groups) != 1 || !groups[0].Protected || groups[0].Representative != 0 || len(groups[0].ActiveMembers) != 0 {
		t.Fatal("supported empty protected state absent", groups)
	}
	independent := DurableClusterInput{StableKey: "independent", RepresentativeThreadID: 2, Members: []DurableClusterMemberInput{{ThreadID: 2, Role: "canonical"}}}
	plan, e := PlanClusterQuarantine(repo, []DurableClusterInput{existing, independent}, []int64{1, 2}, groups)
	if e != nil {
		t.Fatal("empty protected identity blocked unrelated publication", e)
	}
	if len(plan.Published) != 1 || plan.Published[0].StableKey != "independent" || len(plan.Components) != 1 || plan.Components[0].ClusterIDs[0] != "1" {
		t.Fatal("empty protected target not isolated", plan)
	}
	plan, e = PlanClusterQuarantine(repo, []DurableClusterInput{independent}, []int64{2}, groups)
	if e != nil || len(plan.Published) != 1 || len(plan.PreservedGroupIDs) != 1 {
		t.Fatal("unrelated empty protected identity not preserved", e)
	}
}
