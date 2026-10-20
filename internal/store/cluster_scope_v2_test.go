package store

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

type recordedV2Case struct {
	Row struct {
		ID     int64  `json:"id"`
		Status string `json:"status"`
	} `json:"row"`
	Read struct {
		Analysis struct {
			ClusterScope json.RawMessage `json:"cluster_scope"`
		} `json:"analysis"`
	} `json:"read"`
}

func recordedV2Scopes(t *testing.T) map[string]recordedV2Case {
	t.Helper()
	raw, e := os.ReadFile("testdata/cluster-scope-v2-writer.json")
	if e != nil {
		t.Fatal(e)
	}
	var all map[string]json.RawMessage
	if e = json.Unmarshal(raw, &all); e != nil {
		t.Fatal(e)
	}
	out := map[string]recordedV2Case{}
	for _, name := range []string{"partial", "provider_complete_identity_partial", "retained_only", "full_recovery"} {
		var c recordedV2Case
		if e = json.Unmarshal(all[name], &c); e != nil {
			t.Fatal(e)
		}
		out[name] = c
	}
	return out
}
func decodeRecordedScope(raw []byte, id int64) (*ClusterScope, error) {
	return decodeClusterScope(append(append([]byte(`{"native_producer":{"scope":`), raw...), []byte(`}}`)...), id, "openclaw/openclaw")
}
func TestClusterScopeV2AcceptsRecordedNativeWriterShapes(t *testing.T) {
	for name, fixture := range recordedV2Scopes(t) {
		t.Run(name, func(t *testing.T) {
			scope, e := decodeRecordedScope(fixture.Read.Analysis.ClusterScope, fixture.Row.ID)
			if e != nil {
				t.Fatal(e)
			}
			if scope.Format != ClusterScopeFormatV2 || scope.RunID != strconv.FormatInt(fixture.Row.ID, 10) {
				t.Fatal("recorded scope binding lost")
			}
			if name == "retained_only" && (scope.PublicationStatus != "retained_only" || fixture.Row.Status != "blocked_identity" || *scope.Population.Published != 0) {
				t.Fatal("blocked analysis became publication")
			}
			if name == "provider_complete_identity_partial" && (!scope.ProviderComplete || scope.FullNativeScopeComplete) {
				t.Fatal("provider recovery concealed unresolved identities")
			}
		})
	}
}
func TestClusterScopeV2RejectsCorruptWireClaims(t *testing.T) {
	good := recordedV2Scopes(t)["partial"]
	type object = map[string]any
	population := func(s object) object { return s["population"].(object) }
	quarantine := func(s object) object { return s["quarantine"].(object) }
	component := func(s object) object { return quarantine(s)["components"].([]any)[0].(object) }
	cases := []struct {
		name   string
		change func(object)
	}{
		{"unsupported_version", func(s object) { s["format"] = "gitcrawl.cluster-scope.v99" }},
		{"wrong_policy", func(s object) { s["policy_digest"] = strings.Repeat("0", 64) }},
		{"missing_identity_flag", func(s object) { delete(s, "identity_reconciliation_complete") }},
		{"null_quarantine", func(s object) { s["quarantine"] = nil }},
		{"unknown_field", func(s object) { s["all_fresh"] = true }},
		{"population_unknown_field", func(s object) { population(s)["hidden_filtered"] = 1 }},
		{"missing_published", func(s object) { delete(population(s), "published") }},
		{"fractional_published", func(s object) { population(s)["published"] = 1.5 }},
		{"unsafe_count", func(s object) { population(s)["published"] = float64(9007199254740992) }},
		{"negative_count", func(s object) { population(s)["identity_quarantined"] = -1 }},
		{"publication_partition", func(s object) { population(s)["published"] = 0 }},
		{"missing_proof", func(s object) { delete(quarantine(s), "proof_sha256") }},
		{"uppercase_proof", func(s object) { quarantine(s)["proof_sha256"] = strings.Repeat("A", 64) }},
		{"invalid_component_id", func(s object) { component(s)["component_id"] = "not-a-digest" }},
		{"empty_reason", func(s object) { component(s)["reasons"] = []any{} }},
		{"unknown_reason", func(s object) { component(s)["reasons"] = []any{"erase_history"} }},
		{"duplicate_reason", func(s object) { component(s)["reasons"] = []any{"protected_curation", "protected_curation"} }},
		{"unordered_reasons", func(s object) { component(s)["reasons"] = []any{"protected_curation", "existing_identity_split"} }},
		{"duplicate_group", func(s object) { component(s)["cluster_ids"] = []any{"1", "1"} }},
		{"unordered_groups", func(s object) { component(s)["cluster_ids"] = []any{"2", "1"} }},
		{"noncanonical_group", func(s object) { component(s)["cluster_ids"] = []any{"01", "2"} }},
		{"group_overflows_int64", func(s object) { component(s)["cluster_ids"] = []any{"9223372036854775808"} }},
		{"missing_component_count", func(s object) { delete(component(s), "selected_count") }},
		{"more_candidates_than_members", func(s object) { component(s)["candidate_count"] = 99 }},
		{"component_partition", func(s object) { component(s)["selected_count"] = 1 }},
		{"duplicate_component", func(s object) { c := component(s); quarantine(s)["components"] = []any{c, c} }},
		{"identity_false_complete", func(s object) { s["identity_reconciliation_complete"] = true }},
		{"provider_false_complete", func(s object) { s["provider_complete"] = true }},
		{"provider_counter_falsification", func(s object) { s["provider_complete"] = true; s["partial_reasons"] = []any{"identity_quarantine"} }},
		{"incomplete_provider_without_obligation", func(s object) {
			p := population(s)
			p["native_eligible"] = p["selected"]
			p["unavailable"] = 0
			p["unavailable_in_scope"] = 0
		}},
		{"same_group_in_distinct_components", func(s object) {
			c := component(s)
			b, _ := json.Marshal(c)
			var other object
			json.Unmarshal(b, &other)
			c["component_id"] = strings.Repeat("0", 64)
			other["component_id"] = strings.Repeat("f", 64)
			quarantine(s)["components"] = []any{c, other}
		}},
		{"component_total_overflow", func(s object) {
			c := component(s)
			b, _ := json.Marshal(c)
			var other object
			json.Unmarshal(b, &other)
			c["component_id"] = strings.Repeat("0", 64)
			c["selected_count"] = float64(9007199254740991)
			other["component_id"] = strings.Repeat("f", 64)
			other["cluster_ids"] = []any{"999"}
			other["selected_count"] = 1
			quarantine(s)["components"] = []any{c, other}
		}},
		{"false_full_scope", func(s object) { s["full_native_scope_complete"] = true }},
		{"unknown_mode", func(s object) { s["mode"] = "pretend-complete" }},
		{"self_partial_anchor", func(s object) { s["last_full_run_id"] = s["run_id"] }},
		{"wrong_partial_reasons", func(s object) { s["partial_reasons"] = []any{"provider_unavailable"} }},
		{"unknown_publication_status", func(s object) { s["publication_status"] = "complete" }},
		{"retained_only_claims_refresh", func(s object) { s["publication_status"] = "retained_only" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s object
			if e := json.Unmarshal(good.Read.Analysis.ClusterScope, &s); e != nil {
				t.Fatal(e)
			}
			tc.change(s)
			raw, e := json.Marshal(s)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = decodeRecordedScope(raw, good.Row.ID); e == nil {
				t.Fatal("corrupt claim admitted")
			}
		})
	}
}
func TestClusterScopeV2DerivesProviderAndIdentityCoverageSeparately(t *testing.T) {
	inputs := []DurableClusterInput{{StableKey: "merge", RepresentativeThreadID: 1, Members: []DurableClusterMemberInput{{ThreadID: 1}, {ThreadID: 2}}}, {StableKey: "independent", RepresentativeThreadID: 3, Members: []DurableClusterMemberInput{{ThreadID: 3}}}}
	groups := []ClusterIdentityMetadata{{ID: 1, Key: "a", Active: true, Representative: 1, ActiveMembers: []int64{1}, RetainedMembershipCount: 1}, {ID: 2, Key: "b", Active: true, Representative: 2, ActiveMembers: []int64{2}, RetainedMembershipCount: 1}}
	plan, e := PlanClusterQuarantine(1, inputs, []int64{1, 2, 3}, groups)
	if e != nil {
		t.Fatal(e)
	}
	base := ClusterScope{Repository: "openclaw/openclaw", SelectedInputComplete: true, SourceAsOf: ClusterScopeClock{Through: "2026-01-01T00:00:00Z", ObservedAt: "2026-01-01T00:00:01Z"}, Population: ClusterScopePopulation{NativeEligible: 4, Selected: 3, Unavailable: 1, UnavailableInScope: 1}}
	partial := plan.Scope(base, 5, StableHash("proof"))
	if e = ValidateClusterScope(partial, 5, base.Repository); e != nil {
		t.Fatal(e)
	}
	if *partial.Population.Published != 1 || *partial.Population.IdentityQuarantined != 2 || partial.FullNativeScopeComplete {
		t.Fatal("wrong partial partition")
	}
	base.ProviderComplete = true
	base.Population.NativeEligible = 3
	base.Population.Unavailable = 0
	base.Population.UnavailableInScope = 0
	stillPartial := plan.Scope(base, 6, StableHash("proof"))
	if e = ValidateClusterScope(stillPartial, 6, base.Repository); e != nil {
		t.Fatal(e)
	}
	if stillPartial.Mode != "available-subset" || len(*stillPartial.PartialReasons) != 1 || (*stillPartial.PartialReasons)[0] != "identity_quarantine" {
		t.Fatal("provider completion erased identity hold")
	}
}
