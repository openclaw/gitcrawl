// Package remotedelivery owns private native-report projection, seed packing,
// receipt-backed seed custody and explicit policy-checked delivery. It does not
// acquire upstream data or schedule collection.
package remotedelivery

import (
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/openclaw/gitcrawl/internal/store"
)

const PrivateReportProfile = "gitcrawl-private-report-v1"

var ErrPrivateProjection = errors.New("private report projection requires a supported native capture")

// PrivateValue keeps SQL NULL distinct from empty text and encodes signed
// integers as decimal strings, without JSON number precision loss.
type PrivateValue struct {
	Kind  string  `json:"kind"`
	Value *string `json:"value"`
}

type PrivateTable struct {
	Name       string           `json:"name"`
	Columns    []string         `json:"columns"`
	KeyColumns []string         `json:"key_columns"`
	Rows       [][]PrivateValue `json:"rows"`
	SHA256     string           `json:"native_sha256"`
}

type PrivateCursor struct {
	Epoch         string `json:"epoch"`
	Generation    string `json:"generation"`
	SchemaVersion string `json:"schema_version"`
	Sequence      string `json:"sequence"`
	PrunedThrough string `json:"pruned_through"`
}

// PrivateReport retains the logical native anchor and inventory digests. The
// physical file identity stays with the local capture owner. Native hashes and
// byte counts describe the captured values, not this JSON representation.
type PrivateReport struct {
	Profile      string         `json:"profile"`
	Repository   string         `json:"repository"`
	RepositoryID string         `json:"repository_id"`
	ScopeSHA256  string         `json:"scope_sha256"`
	Cursor       PrivateCursor  `json:"cursor"`
	SHA256       string         `json:"native_sha256"`
	Rows         string         `json:"rows"`
	Bytes        string         `json:"native_bytes"`
	Tables       []PrivateTable `json:"tables"`
}

type privateLayout struct{ name, columns, keys, integers string }

// Fixed native-report/v1 layouts: no portable-export sanitization, column
// discovery or payload flattening. New native columns require profile review.
var privateLayouts = []privateLayout{
	{"repositories", "id owner name full_name github_repo_id raw_json updated_at", "id", "id"},
	{"threads", "id repo_id github_id number kind state title body author_login author_type author_association html_url labels_json assignees_json raw_json content_hash is_draft created_at_gh updated_at_gh closed_at_gh merged_at_gh closed_at_local close_reason_local first_pulled_at last_pulled_at observation_sequence evidence_observation_sequence evidence_source_updated_at updated_at", "id", "id repo_id number is_draft observation_sequence evidence_observation_sequence"},
	{"comments", "id thread_id github_id comment_type author_login author_type body is_bot raw_json raw_json_blob_id created_at_gh updated_at_gh deleted_at deletion_reason submitted_at_gh publication_at_gh", "id", "id thread_id is_bot raw_json_blob_id"},
	{"thread_child_observation_reservations", "thread_id family source_updated_at observation_sequence", "thread_id family", "thread_id observation_sequence"},
	{"thread_child_observation_memberships", "thread_id family observation_sequence member_ids_json", "thread_id family", "thread_id observation_sequence"},
	{"thread_exclusions", "repository number kind original_thread_id github_id excluded_at reason plan_id", "repository number", "number original_thread_id"},
	{"thread_excluded_nodes", "node_id repository number", "node_id", "number"},
}

// ProjectPrivateReport consumes a successful CaptureNativeReport result. Store
// remains the exclusion-policy and consistent-read owner; an unsuccessful or
// partial capture is not a private report. This full inventory does not infer
// removals, provider freshness, body completeness or delivery eligibility.
func ProjectPrivateReport(capture store.NativeReportCapture) (PrivateReport, error) {
	a, c := capture.Anchor, capture.Anchor.Cursor
	if a.Repository == "" || !utf8.ValidString(a.Repository) || a.RepositoryID <= 0 || c.Epoch == "" || !utf8.ValidString(c.Epoch) ||
		c.Generation < 1 || c.SchemaVersion < 1 || c.Sequence < 0 || c.PrunedThrough < 0 || c.PrunedThrough > c.Sequence ||
		!digest(a.ScopeSHA256) || !digest(capture.SHA256) || capture.Rows < 1 || capture.Bytes < 1 || len(capture.Tables) != len(privateLayouts) {
		return PrivateReport{}, ErrPrivateProjection
	}
	out := PrivateReport{Profile: PrivateReportProfile, Repository: a.Repository, RepositoryID: strconv.FormatInt(a.RepositoryID, 10),
		ScopeSHA256: a.ScopeSHA256, SHA256: capture.SHA256, Rows: strconv.FormatInt(capture.Rows, 10), Bytes: strconv.FormatInt(capture.Bytes, 10),
		Cursor: PrivateCursor{Epoch: c.Epoch, Generation: strconv.FormatInt(c.Generation, 10), SchemaVersion: strconv.FormatInt(c.SchemaVersion, 10),
			Sequence: strconv.FormatInt(c.Sequence, 10), PrunedThrough: strconv.FormatInt(c.PrunedThrough, 10)}}
	var rows int64
	for i, layout := range privateLayouts {
		table := capture.Tables[i]
		columns, keys := strings.Fields(layout.columns), strings.Fields(layout.keys)
		if table.Name != layout.name || !slices.Equal(table.Columns, columns) || !slices.Equal(table.KeyColumns, keys) || !digest(table.SHA256) {
			return PrivateReport{}, ErrPrivateProjection
		}
		projected := PrivateTable{Name: layout.name, Columns: columns, KeyColumns: keys, SHA256: table.SHA256, Rows: make([][]PrivateValue, 0, len(table.Rows))}
		for _, row := range table.Rows {
			if len(row) != len(columns) || rows >= capture.Rows {
				return PrivateReport{}, ErrPrivateProjection
			}
			values := make([]PrivateValue, len(row))
			for j, native := range row {
				if layout.name == "comments" && columns[j] == "raw_json_blob_id" && native.Kind != store.NativeReportNull {
					return PrivateReport{}, store.ErrNativeReportBlob
				}
				if native.Kind == store.NativeReportNull && slices.Contains(keys, columns[j]) {
					return PrivateReport{}, ErrPrivateProjection
				}
				var err error
				values[j], err = privateValue(native, strings.Contains(" "+layout.integers+" ", " "+columns[j]+" "))
				if err != nil {
					return PrivateReport{}, err
				}
			}
			projected.Rows = append(projected.Rows, values)
			rows++
		}
		out.Tables = append(out.Tables, projected)
	}
	if rows != capture.Rows {
		return PrivateReport{}, ErrPrivateProjection
	}
	return out, nil
}

func privateValue(native store.NativeReportValue, integer bool) (PrivateValue, error) {
	switch native.Kind {
	case store.NativeReportNull:
		return PrivateValue{Kind: "null"}, nil
	case store.NativeReportInteger:
		if integer {
			value := strconv.FormatInt(native.Integer, 10)
			return PrivateValue{Kind: "integer", Value: &value}, nil
		}
	case store.NativeReportText:
		if !integer && utf8.ValidString(native.Text) {
			return PrivateValue{Kind: "text", Value: &native.Text}, nil
		}
	}
	return PrivateValue{}, ErrPrivateProjection
}

func digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}
