package remotedelivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

const clusterInventoryLayoutID = "gitcrawl-cluster-context-16-v1"
const clusterMaxScalarBytes = 16 << 10

var ErrClusterProjection = errors.New("cluster projection requires a complete native durable-context capture")

var clusterLayouts = []privateLayout{
	{"selection", "repository_id repository github_repo_id captured_at view scope_status selection_source selection_run_id scope_mode policy_digest source_through source_observed_at provider_complete full_native_scope_complete selected_input_complete retained_prior_memberships identity_reconciliation_complete publication_status analysis_status analysis_run_id analysis_scope_mode analysis_policy_digest analysis_source_through analysis_source_observed_at analysis_provider_complete analysis_full_native_scope_complete analysis_selected_input_complete analysis_retained_prior_memberships analysis_identity_reconciliation_complete analysis_publication_status", "repository_id", "repository_id provider_complete full_native_scope_complete selected_input_complete retained_prior_memberships identity_reconciliation_complete analysis_provider_complete analysis_full_native_scope_complete analysis_selected_input_complete analysis_retained_prior_memberships analysis_identity_reconciliation_complete"},
	{"groups", "id status closed_at representative_thread_id member_count quarantined", "id", "id representative_thread_id member_count quarantined"},
	{"members", "group_id thread_id state role last_seen_run_id generation_status", "group_id thread_id", "group_id thread_id"},
	{"threads", "id number kind state title labels_json updated_at body node_id", "id", "id number"},
}

// ClusterReport is a compact public projection, not a provider checkpoint.
// Capture time is supplied once by the caller so retrying the same capture keeps
// its exact bytes. The original capture hash and physical path stay local.
type ClusterReport struct {
	Profile, Repository, RepositoryID, ScopeSHA256, SHA256 string
	Rows                                                   int64
	Tables                                                 []PrivateTable
}

func ProjectClusterContext(ctx context.Context, c store.NativeClusterContext, capturedAt time.Time) (ClusterReport, error) {
	if err := ctx.Err(); err != nil {
		return ClusterReport{}, err
	}
	if c.Profile != store.NativeClusterContextProfile || c.View != store.NativeClusterContextView || c.RepositoryID <= 0 || c.Repository == "" || c.GitHubRepositoryID == "" || !digest(c.SHA256) || c.Rows != 1+int64(len(c.Groups)+len(c.Members)+len(c.Threads)) || capturedAt.IsZero() {
		return ClusterReport{}, ErrClusterProjection
	}
	out := ClusterReport{Profile: c.Profile, Repository: c.Repository, RepositoryID: strconv.FormatInt(c.RepositoryID, 10), Rows: c.Rows}
	for _, l := range clusterLayouts {
		out.Tables = append(out.Tables, PrivateTable{Name: l.name, Columns: strings.Fields(l.columns), KeyColumns: strings.Fields(l.keys), Rows: [][]PrivateValue{}})
	}
	row := []PrivateValue{clusterInteger(c.RepositoryID), clusterText(c.Repository), clusterText(c.GitHubRepositoryID), clusterText(capturedAt.UTC().Format(time.RFC3339Nano)), clusterText(c.View), clusterText(c.Selection.ScopeStatus), clusterText(c.Selection.Source), clusterRunID(c.Selection.RunID)}
	row = append(row, clusterScopeValues(c.Selection.Scope)...)
	status := clusterText(c.Selection.AnalysisStatus)
	if c.Selection.AnalysisRunID == nil {
		status = clusterNull()
	}
	row = append(row, status, clusterRunID(c.Selection.AnalysisRunID))
	row = append(row, clusterScopeValues(c.Selection.AnalysisScope)...)
	out.Tables[0].Rows = append(out.Tables[0].Rows, row)
	for _, g := range c.Groups {
		out.Tables[1].Rows = append(out.Tables[1].Rows, []PrivateValue{clusterInteger(g.ID), clusterText(g.Status), clusterNullableText(g.ClosedAt), clusterNullableInteger(g.RepresentativeThreadID), clusterInteger(g.MemberCount), clusterBoolean(g.Quarantined)})
	}
	for _, m := range c.Members {
		out.Tables[2].Rows = append(out.Tables[2].Rows, []PrivateValue{clusterInteger(m.GroupID), clusterInteger(m.ThreadID), clusterText(m.State), clusterText(m.Role), clusterRunID(m.LastSeenRunID), clusterText(m.Generation)})
	}
	for _, t := range c.Threads {
		out.Tables[3].Rows = append(out.Tables[3].Rows, []PrivateValue{clusterInteger(t.ID), clusterInteger(t.Number), clusterText(t.Kind), clusterText(t.State), clusterText(t.Title), clusterText(t.LabelsJSON), clusterNullableText(t.UpdatedAt), clusterNullableText(t.Body), clusterNullableText(t.NodeID)})
	}
	codec, err := clusterInventoryCodec()
	if err != nil {
		return ClusterReport{}, err
	}
	var entries []remote.InventoryEntry
	var counts []remote.InventoryCount
	for _, table := range out.Tables {
		counts = append(counts, remote.InventoryCount{Table: table.Name, Rows: uint64(len(table.Rows))})
		for _, r := range table.Rows {
			values := make([]remote.ChangeValue, len(r))
			for i, v := range r {
				if v.Kind == "text" && v.Value != nil && len(*v.Value) > clusterMaxScalarBytes {
					return ClusterReport{}, ErrClusterProjection
				}
				values[i] = remote.ChangeValue{Kind: v.Kind, Value: v.Value}
			}
			keys := make([]remote.ChangeValue, len(table.KeyColumns))
			for i, k := range table.KeyColumns {
				keys[i] = values[slices.Index(table.Columns, k)]
			}
			key, e := codec.EncodeKey(table.Name, keys)
			if e != nil {
				return ClusterReport{}, e
			}
			hash, e := codec.RowDigest(table.Name, keys, values)
			if e != nil {
				return ClusterReport{}, e
			}
			entries = append(entries, remote.InventoryEntry{Table: table.Name, Key: key, RowSHA256: hash})
		}
	}
	slices.SortFunc(entries, func(a, b remote.InventoryEntry) int {
		if n := strings.Compare(a.Table, b.Table); n != 0 {
			return n
		}
		return bytes.Compare(a.Key, b.Key)
	})
	scope := []byte(c.Profile + "\n" + c.View + "\n" + c.Repository + "\n")
	scopeHash := sha256.Sum256(scope)
	out.ScopeSHA256 = hex.EncodeToString(scopeHash[:])
	i := 0
	hash, err := codec.Digest(ctx, scope, counts, remote.InventoryLimits{MaxEntries: uint64(c.Rows), MaxKeyBytes: 1024}, func(context.Context) (remote.InventoryEntry, error) {
		if i == len(entries) {
			return remote.InventoryEntry{}, io.EOF
		}
		e := entries[i]
		i++
		return e, nil
	})
	if err != nil {
		return ClusterReport{}, err
	}
	out.SHA256 = hex.EncodeToString(hash[:])
	return out, nil
}

func clusterInventoryCodec() (*remote.InventoryCodec, error) {
	var tables []remote.InventoryTable
	for _, l := range clusterLayouts {
		table := remote.InventoryTable{Name: l.name, KeyColumns: strings.Fields(l.keys)}
		for _, name := range strings.Fields(l.columns) {
			kind := "text"
			if slices.Contains(strings.Fields(l.integers), name) {
				kind = "integer"
			}
			table.Columns = append(table.Columns, remote.InventoryColumn{Name: name, Kind: kind, Nullable: !slices.Contains(table.KeyColumns, name)})
		}
		tables = append(tables, table)
	}
	return remote.NewInventoryCodec(store.NativeClusterContextProfile, clusterInventoryLayoutID, tables)
}
func clusterNull() PrivateValue         { return PrivateValue{Kind: "null"} }
func clusterText(v string) PrivateValue { return PrivateValue{Kind: "text", Value: &v} }
func clusterInteger(v int64) PrivateValue {
	s := strconv.FormatInt(v, 10)
	return PrivateValue{Kind: "integer", Value: &s}
}
func clusterBoolean(v bool) PrivateValue {
	if v {
		return clusterInteger(1)
	}
	return clusterInteger(0)
}
func clusterNullableText(v *string) PrivateValue {
	if v == nil {
		return clusterNull()
	}
	return clusterText(*v)
}
func clusterNullableInteger(v *int64) PrivateValue {
	if v == nil {
		return clusterNull()
	}
	return clusterInteger(*v)
}
func clusterRunID(v *int64) PrivateValue {
	if v == nil {
		return clusterNull()
	}
	return clusterText(strconv.FormatInt(*v, 10))
}
func clusterScopeValues(s *store.NativeClusterContextScope) []PrivateValue {
	if s == nil {
		return []PrivateValue{clusterNull(), clusterNull(), clusterNull(), clusterNull(), clusterNull(), clusterNull(), clusterNull(), clusterNull(), clusterNull(), clusterNull()}
	}
	identity := clusterNull()
	if s.IdentityReconciliationComplete != nil {
		identity = clusterBoolean(*s.IdentityReconciliationComplete)
	}
	publication := clusterNull()
	if s.PublicationStatus != "" {
		publication = clusterText(s.PublicationStatus)
	}
	return []PrivateValue{clusterText(s.Mode), clusterText(s.PolicyDigest), clusterText(s.SourceThrough), clusterText(s.SourceObservedAt), clusterBoolean(s.ProviderComplete), clusterBoolean(s.FullNativeScopeComplete), clusterBoolean(s.SelectedInputComplete), clusterInteger(s.RetainedPriorMemberships), identity, publication}
}
