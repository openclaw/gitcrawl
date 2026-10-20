package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestClusterScopeStrictNullsAndAnchors(t *testing.T) {
	raw, e := os.ReadFile("testdata/cluster-selection-v2.json")
	if e != nil {
		t.Fatal(e)
	}
	var oracle selectionOracle
	if e = json.Unmarshal(raw, &oracle); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"null_count", "null_flag", "missing_anchor", "anchor_other_repo", "wrong_admission"} {
		t.Run(name, func(t *testing.T) {
			b, _ := json.Marshal(oracle.Fixture)
			var f map[string]any
			json.Unmarshal(b, &f)
			runs := f["tables"].(map[string]any)["cluster_runs"].([]any)
			scope := runs[3].(map[string]any)["native_producer_scope"].(map[string]any)
			switch name {
			case "null_count":
				scope["population"].(map[string]any)["unavailable"] = nil
			case "null_flag":
				scope["provider_complete"] = nil
			case "missing_anchor":
				scope["last_full_run_id"] = "9"
			case "anchor_other_repo":
				scope["last_full_run_id"] = "99"
			case "wrong_admission":
				scope["source_as_of"].(map[string]any)["observed_at"] = "2026-01-01T12:00:00Z"
			}
			s := loadSelectionFixture(t, f)
			if _, e = s.ClusterReadSelection(context.Background(), 1, ""); e == nil {
				t.Fatal("invalid scope accepted")
			}
		})
	}
}
