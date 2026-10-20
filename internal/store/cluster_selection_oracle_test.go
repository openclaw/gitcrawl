package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type selectionOracle struct {
	Fixture map[string]any `json:"fixture"`
	Cases   []struct {
		Name     string         `json:"name"`
		Phase    string         `json:"phase"`
		Applies  []string       `json:"applies_to"`
		Request  map[string]any `json:"request"`
		Expected map[string]any `json:"expected"`
		Patch    []struct {
			Op, Path string
			Value    any
		} `json:"fixture_patch"`
	} `json:"cases"`
}

func loadSelectionFixture(t *testing.T, fixture map[string]any) *Store {
	t.Helper()
	s, e := Open(context.Background(), filepath.Join(t.TempDir(), "oracle.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	tables := fixture["tables"].(map[string]any)
	for _, table := range []string{"repositories", "threads", "cluster_runs", "clusters", "cluster_members", "cluster_groups", "cluster_memberships", "cluster_overrides", "cluster_closures"} {
		for _, value := range tables[table].([]any) {
			r := value.(map[string]any)
			switch table {
			case "repositories":
				r["owner"] = "fixture"
				r["name"] = "repo"
				r["raw_json"] = "{}"
				r["updated_at"] = "2026-01-01T00:00:00Z"
			case "threads":
				r["github_id"] = fmt.Sprint(r["id"])
				r["html_url"] = "https://example.invalid"
				r["labels_json"] = "[]"
				r["assignees_json"] = "[]"
				r["raw_json"] = "{}"
				r["content_hash"] = "fixture"
				r["updated_at"] = r["updated_at_gh"]
			case "cluster_runs":
				r["started_at"] = r["finished_at"]
				if scope, ok := r["native_producer_scope"]; ok {
					b, _ := json.Marshal(map[string]any{"native_producer": map[string]any{"scope": scope}})
					r["stats_json"] = string(b)
					delete(r, "native_producer_scope")
				}
			case "cluster_members":
				r["created_at"] = "2026-01-01T10:00:00Z"
			case "clusters":
				delete(r, "locally_closed")
			case "cluster_groups":
				r["stable_key"] = "fixture-" + fmt.Sprint(r["id"])
				r["cluster_type"] = "duplicate_candidate"
				r["created_at"] = "2026-01-01T10:00:00Z"
			case "cluster_memberships":
				r["added_by"] = "fixture"
				r["added_reason_json"] = "{}"
				r["first_seen_run_id"] = r["last_seen_run_id"]
				r["created_at"] = "2026-01-01T10:00:00Z"
				r["updated_at"] = "2026-01-01T11:00:00Z"
			}
			cols := []string{}
			for k := range r {
				cols = append(cols, k)
			}
			sort.Strings(cols)
			args := []any{}
			quoted := []string{}
			marks := []string{}
			for _, k := range cols {
				args = append(args, r[k])
				quoted = append(quoted, `"`+k+`"`)
				marks = append(marks, "?")
			}
			if _, e = s.DB().Exec("INSERT INTO "+table+"("+strings.Join(quoted, ",")+") VALUES("+strings.Join(marks, ",")+")", args...); e != nil {
				t.Fatalf("fixture %s: %v", table, e)
			}
		}
	}
	return s
}

func TestNativeClusterSelectionSharedOracle(t *testing.T) {
	raw, e := os.ReadFile("testdata/cluster-selection-v2.json")
	if e != nil {
		t.Fatal(e)
	}
	var all selectionOracle
	if e = json.Unmarshal(raw, &all); e != nil {
		t.Fatal(e)
	}
	for _, tc := range all.Cases {
		native := false
		for _, a := range tc.Applies {
			native = native || a == "native_reader"
		}
		if !native || tc.Phase == "baseline_reference" {
			continue
		}
		t.Run(tc.Name, func(t *testing.T) {
			b, _ := json.Marshal(all.Fixture)
			var fixture map[string]any
			json.Unmarshal(b, &fixture)
			for _, p := range tc.Patch {
				parts := strings.Split(strings.TrimPrefix(p.Path, "/"), "/")
				var cur any = fixture
				for _, key := range parts[:len(parts)-1] {
					if a, ok := cur.([]any); ok {
						i, _ := strconv.Atoi(key)
						cur = a[i]
					} else {
						cur = cur.(map[string]any)[key]
					}
				}
				last := parts[len(parts)-1]
				if last == "-" {
					parent := fixture["tables"].(map[string]any)
					table := parts[1]
					parent[table] = append(cur.([]any), p.Value)
				} else {
					obj := cur.(map[string]any)
					if p.Op == "remove" {
						delete(obj, last)
					} else {
						obj[last] = p.Value
					}
				}
			}
			s := loadSelectionFixture(t, fixture)
			if path := os.Getenv("GITCRAWL_READER_FIXTURE"); path != "" && tc.Name == "v1_default_durable_authority" {
				if _, err := s.DB().Exec("VACUUM INTO ?", path); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			req := tc.Request
			source, _ := req["source"].(string)
			if source == "auto" || source == "display" {
				source = ""
			}
			var read ClusterReadContext
			var rows []ClusterSummary
			var detail ClusterDetail
			if req["operation"] == "detail" {
				id, _ := strconv.ParseInt(req["id"].(string), 10, 64)
				detail, e = s.ClusterDetail(ctx, ClusterDetailOptions{RepoID: 1, ClusterID: id, Source: source, IncludeClosed: req["include_closed"].(bool), MemberLimit: 100})
				read = detail.ClusterReadContext
			} else {
				rows, read, e = s.QualifiedClusterSummaries(ctx, ClusterSummaryOptions{RepoID: 1, IncludeClosed: req["include_closed"].(bool), MinSize: int(req["min_size"].(float64)), Sort: req["sort"].(string)}, source)
			}
			if want, ok := tc.Expected["error"]; ok {
				if e == nil {
					t.Fatal("expected", want)
				}
				if want == "not_found" && !strings.Contains(e.Error(), "not found") {
					t.Fatal(e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if want, ok := tc.Expected["selection"].(map[string]any); ok {
				if read.Selection.Source != want["source"] || read.Selection.RunID == nil || *read.Selection.RunID != want["run_id"] {
					t.Fatalf("selection %+v want%v", read.Selection, want)
				}
			}
			if want, ok := tc.Expected["scope_status"]; ok && read.ScopeStatus != want {
				t.Fatalf("scope status %s want%v", read.ScopeStatus, want)
			}
			if want, ok := tc.Expected["rows"].([]any); ok {
				got := []any{}
				for _, r := range rows {
					got = append(got, []any{r.Source, strconv.FormatInt(r.ID, 10)})
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("rows %v want%v", got, want)
				}
			}
			if want, ok := tc.Expected["representative_thread_id"]; ok && fmt.Sprint(detail.Cluster.RepresentativeThreadID) != fmt.Sprint(want) {
				t.Fatalf("representative %d want%v", detail.Cluster.RepresentativeThreadID, want)
			}
			if want, ok := tc.Expected["member_thread_ids"].([]any); ok {
				got := []any{}
				for _, m := range detail.Members {
					got = append(got, strconv.FormatInt(m.Thread.ID, 10))
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("members %v want%v", got, want)
				}
			}
			if want, ok := tc.Expected["members"].([]any); ok {
				if len(want) != len(detail.Members) {
					t.Fatal("member length")
				}
				for i, m := range detail.Members {
					x := want[i].(map[string]any)
					if strconv.FormatInt(m.Thread.ID, 10) != x["thread_id"] || m.LastSeenRunID == nil || *m.LastSeenRunID != x["last_seen_run_id"] || m.GenerationStatus != x["generation_status"] {
						t.Fatalf("member %+v want%v", m, x)
					}
				}
			}
			if want, ok := tc.Expected["all_member_generation_status"]; ok {
				for _, m := range detail.Members {
					if m.GenerationStatus != want {
						t.Fatal(m.GenerationStatus, want)
					}
				}
			}
		})
	}
}
