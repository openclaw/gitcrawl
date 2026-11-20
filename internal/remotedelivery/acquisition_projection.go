package remotedelivery

import (
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

// AcquisitionParent decodes an exact-ID current-row query using the same native
// layout as snapshot delivery. No reader metadata or SQL columns become facts.
func AcquisitionParent(result remote.QueryResult, id int64) (*remote.Change, error) {
	if len(result.Rows) > 1 || result.Cursor != "" || result.Stats.NextCursor != "" {
		return nil, ErrPrivateProjection
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	layout := privateLayouts[1]
	columns := strings.Fields(layout.columns)
	if !slices.Equal(result.Columns, append(slices.Clone(columns), "sequence", "ordinal")) || len(result.Rows[0]) != len(result.Columns) {
		return nil, ErrPrivateProjection
	}
	row := make([]remote.ChangeValue, len(columns))
	for i, name := range columns {
		value := result.Rows[0][i]
		if value == nil {
			row[i] = remote.ChangeValue{Kind: "null"}
			continue
		}
		text, ok := value.(string)
		if !ok {
			return nil, ErrPrivateProjection
		}
		kind := "text"
		if strings.Contains(" "+layout.integers+" ", " "+name+" ") {
			kind = "integer"
		}
		row[i] = remote.ChangeValue{Kind: kind, Value: &text}
	}
	change := remote.Change{Table: "threads", Operation: "upsert", Key: row[:1], Row: row}
	codec, err := privateInventoryCodec()
	if err != nil {
		return nil, err
	}
	if _, err = codec.RowDigest(change.Table, change.Key, change.Row); err != nil {
		return nil, err
	}
	if row[0].Value == nil || *row[0].Value != strconv.FormatInt(id, 10) {
		return nil, ErrPrivateProjection
	}
	return &change, nil
}

// ProjectAcquiredThread preserves local and earlier evidence fields from the
// current published parent. The acquisition owner alone allocates observations.
func ProjectAcquiredThread(repoID, id int64, raw map[string]any, prior *remote.Change, at string) (remote.Change, error) {
	t := syncer.MapIssueToThread(repoID, raw, at)
	t.ID = id
	layout := privateLayouts[1]
	cols := strings.Fields(layout.columns)
	row := make([]remote.ChangeValue, len(cols))
	for i := range row {
		row[i].Kind = "null"
	}
	if prior != nil {
		if prior.Table != "threads" || len(prior.Row) != len(row) {
			return remote.Change{}, ErrPrivateProjection
		}
		row = slices.Clone(prior.Row)
		for name, want := range map[string]string{"id": strconv.FormatInt(id, 10), "repo_id": strconv.FormatInt(repoID, 10), "number": strconv.Itoa(t.Number), "kind": t.Kind} {
			old := row[deliveryColumn("threads", name)]
			if old.Value == nil || *old.Value != want {
				return remote.Change{}, ErrPrivateProjection
			}
		}
		old := row[deliveryColumn("threads", "updated_at_gh")]
		if old.Value != nil && *old.Value != "" {
			a, e := time.Parse(time.RFC3339Nano, *old.Value)
			b, f := time.Parse(time.RFC3339Nano, t.UpdatedAtGitHub)
			if e != nil || f != nil || b.Before(a) {
				return remote.Change{}, errors.New("provider parent precedes published version")
			}
		}
	}
	fields := map[string]any{"id": id, "repo_id": repoID, "github_id": t.GitHubID, "number": t.Number, "kind": t.Kind, "state": t.State, "title": t.Title, "body": t.Body, "author_login": t.AuthorLogin, "author_type": t.AuthorType, "author_association": t.AuthorAssociation, "html_url": t.HTMLURL, "labels_json": t.LabelsJSON, "assignees_json": t.AssigneesJSON, "raw_json": t.RawJSON, "content_hash": t.ContentHash, "created_at_gh": t.CreatedAtGitHub, "updated_at_gh": t.UpdatedAtGitHub, "closed_at_gh": t.ClosedAtGitHub, "merged_at_gh": t.MergedAtGitHub, "last_pulled_at": at, "updated_at": at}
	if _, present := raw["draft"]; present || prior == nil {
		fields["is_draft"] = boolInteger(t.IsDraft)
	}
	if prior == nil {
		fields["first_pulled_at"] = at
		fields["observation_sequence"] = int64(0)
		fields["evidence_observation_sequence"] = int64(0)
		fields["evidence_source_updated_at"] = ""
	}
	return acquisitionUpsert(layout, row, fields)
}

func ProjectAcquiredComment(threadID, id int64, kind string, raw map[string]any) (remote.Change, error) {
	c := syncer.MapComment(threadID, kind, raw)
	fields := map[string]any{"id": id, "thread_id": threadID, "github_id": c.GitHubID, "comment_type": c.CommentType, "author_login": c.AuthorLogin, "author_type": c.AuthorType, "body": c.Body, "is_bot": boolInteger(c.IsBot), "raw_json": c.RawJSON, "created_at_gh": c.CreatedAtGitHub, "updated_at_gh": c.UpdatedAtGitHub, "deleted_at": c.DeletedAt, "deletion_reason": c.DeletionReason, "submitted_at_gh": c.Submitted, "publication_at_gh": c.Published}
	row := make([]remote.ChangeValue, len(strings.Fields(privateLayouts[2].columns)))
	for i := range row {
		row[i].Kind = "null"
	}
	return acquisitionUpsert(privateLayouts[2], row, fields)
}
func boolInteger(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
func acquisitionUpsert(layout privateLayout, row []remote.ChangeValue, fields map[string]any) (remote.Change, error) {
	for name, value := range fields {
		index := deliveryColumn(layout.name, name)
		switch v := value.(type) {
		case string:
			if v == "" && acquisitionNullableText(layout.name, name) {
				row[index] = remote.ChangeValue{Kind: "null"}
			} else {
				row[index] = remote.ChangeValue{Kind: "text", Value: &v}
			}
		case int64:
			s := strconv.FormatInt(v, 10)
			row[index] = remote.ChangeValue{Kind: "integer", Value: &s}
		case int:
			s := strconv.Itoa(v)
			row[index] = remote.ChangeValue{Kind: "integer", Value: &s}
		default:
			return remote.Change{}, ErrPrivateProjection
		}
	}
	change := remote.Change{Table: layout.name, Operation: "upsert", Key: row[:1], Row: row}
	codec, err := privateInventoryCodec()
	if err != nil {
		return change, err
	}
	_, err = codec.RowDigest(change.Table, change.Key, change.Row)
	return change, err
}

// Match the native UpsertThread/UpsertComment nullString parameters. Required
// native TEXT columns retain empty strings, including an absent provider URL.
func acquisitionNullableText(table, name string) bool {
	switch name {
	case "author_login", "author_type", "created_at_gh", "updated_at_gh":
		return true
	case "body", "author_association", "closed_at_gh", "merged_at_gh", "first_pulled_at", "last_pulled_at":
		return table == "threads"
	case "deleted_at", "deletion_reason", "submitted_at_gh", "publication_at_gh":
		return table == "comments"
	}
	return false
}

// AcquisitionRawID retains JSON-number precision from the provider decoder.
func AcquisitionRawID(value any) string {
	switch n := value.(type) {
	case json.Number:
		return n.String()
	case string:
		return n
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.FormatInt(n, 10)
	}
	return ""
}
