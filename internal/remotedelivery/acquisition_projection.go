package remotedelivery

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
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
func ProjectAcquiredThread(repoID, id int64, repository string, raw map[string]any, prior *remote.Change, at string, fullMerge map[string]any) (remote.Change, error) {
	t := syncer.MapIssueToThread(repoID, raw, at)
	t.ID = id
	layout := privateLayouts[1]
	cols := strings.Fields(layout.columns)
	row := make([]remote.ChangeValue, len(cols))
	for i := range row {
		row[i].Kind = "null"
	}
	if prior != nil {
		if prior.Table != "threads" || prior.Operation != "upsert" || len(prior.Row) != len(row) {
			return remote.Change{}, ErrPrivateProjection
		}
		row = slices.Clone(prior.Row)
		for name, want := range map[string]string{"id": strconv.FormatInt(id, 10), "repo_id": strconv.FormatInt(repoID, 10), "github_id": t.GitHubID, "number": strconv.Itoa(t.Number), "kind": t.Kind} {
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
	// The Issues endpoint can omit merge details. A prior positive merge
	// cannot be erased by a sparse response or by a provider test-merge SHA.
	var priorMergeTime string
	if prior != nil {
		if old := prior.Row[deliveryColumn("threads", "merged_at_gh")].Value; old != nil {
			priorMergeTime = *old
		}
	}
	pull, _ := raw["pull_request"].(map[string]any)
	if t.Kind == "pull_request" && (pull["merged_at"] != nil || priorMergeTime != "") {
		if pull["merged_at"] != nil {
			if _, err := time.Parse(time.RFC3339Nano, t.MergedAtGitHub); err != nil {
				return remote.Change{}, errors.New("invalid Issues pull merge time")
			}
		}
		if t.MergedAtGitHub == "" || (priorMergeTime != "" && priorMergeTime != t.MergedAtGitHub) {
			if fullMerge == nil {
				return remote.Change{}, ErrFullPullRequired
			}
			if fullMerge["merged_at"] != priorMergeTime {
				return remote.Change{}, errors.New("full pull differs from retained merge time")
			}
			t.MergedAtGitHub = priorMergeTime
		}
		merge, graph, err := retainedPullMerge(repository, raw, prior, t.MergedAtGitHub)
		if err != nil {
			return remote.Change{}, err
		}
		if fullMerge != nil {
			if err := checkMergeAgreement(merge, fullMerge); err != nil {
				return remote.Change{}, err
			}
			facts := maps.Clone(fullMerge)
			if _, observed := facts["merged_by"]; !observed {
				if actor, known := merge["merged_by"]; known {
					facts["merged_by"] = actor
				}
			}
			merge = facts
		}
		if merge["merge_commit_sha"] == nil {
			return remote.Change{}, ErrFullPullRequired
		}
		if err := checkMergeAgreement(merge, raw); err != nil {
			return remote.Change{}, err
		}
		enriched := maps.Clone(raw)
		maps.Copy(enriched, merge)
		if graph != nil {
			enriched["_gitcrawl_graphql_parent"] = graph
		}
		if err = github.CheckPullMergeAliases(enriched, merge); err != nil {
			return remote.Change{}, err
		}
		encoded, err := json.Marshal(enriched)
		if err != nil {
			return remote.Change{}, err
		}
		t.RawJSON = string(encoded)
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

// ErrFullPullRequired signals that Issues omitted facts only the full Pull
// response can establish. The runner fetches it before publishing this page.
var ErrFullPullRequired = errors.New("merged issue requires full pull evidence")

func retainedPullMerge(repository string, issue map[string]any, prior *remote.Change, stamp string) (map[string]any, map[string]any, error) {
	if prior == nil {
		return nil, nil, nil
	}
	oldStamp := prior.Row[deliveryColumn("threads", "merged_at_gh")].Value
	if oldStamp == nil || *oldStamp != stamp {
		return nil, nil, nil
	}
	raw := prior.Row[deliveryColumn("threads", "raw_json")].Value
	var old map[string]any
	if raw == nil {
		return nil, nil, ErrPrivateProjection
	}
	decoder := json.NewDecoder(strings.NewReader(*raw))
	decoder.UseNumber()
	if err := decoder.Decode(&old); err != nil {
		return nil, nil, err
	}
	return mergeFromRaw(repository, issue, old, stamp)
}

func mergeFromRaw(repository string, issue, old map[string]any, stamp string) (map[string]any, map[string]any, error) {
	node, _ := issue["node_id"].(string)
	if old["node_id"] == nil || old["id"] == nil || old["number"] == nil {
		return nil, nil, nil // Legacy row without enough identity proof; fetch the full Pull.
	}
	if node == "" || old["node_id"] != node || AcquisitionRawID(old["id"]) != AcquisitionRawID(issue["id"]) || AcquisitionRawID(old["number"]) != AcquisitionRawID(issue["number"]) {
		return nil, nil, errors.New("retained pull identity differs from issue")
	}
	var retained map[string]any
	if old["merged_at"] != nil || old["mergedAt"] != nil {
		var err error
		retained, err = github.PullMergeFields(old)
		if err != nil {
			return nil, nil, err
		}
		if retained["merged_at"] != stamp {
			return nil, nil, errors.New("retained pull merge time differs from issue")
		}
	}
	graph, _ := old["_gitcrawl_graphql_parent"].(map[string]any)
	if graph == nil {
		return retained, nil, github.CheckPullMergeAliases(old, retained)
	}
	fields, _ := graph["fields"].(map[string]any)
	// A previous unmerged snapshot cannot identify the later merge.
	if graph["schema"] != "gitcrawl-graphql-parent/v1" || fields["pullState"] != "MERGED" {
		return retained, nil, nil
	}
	graphRepo, _ := fields["repository"].(map[string]any)
	name, _ := graphRepo["nameWithOwner"].(string)
	if !strings.EqualFold(name, repository) || fields["__typename"] != "PullRequest" || fields["id"] != node || AcquisitionRawID(fields["number"]) != AcquisitionRawID(issue["number"]) || fields["mergedAt"] != stamp {
		return nil, nil, errors.New("retained GraphQL pull identity or merge time differs from issue")
	}
	facts, err := github.ParentMergeFields(fields)
	if err != nil {
		return nil, nil, err
	}
	if err = checkMergeAgreement(facts, retained); err != nil {
		return nil, nil, err
	}
	maps.Copy(facts, retained)
	if err = github.CheckPullMergeAliases(old, facts); err != nil {
		return nil, nil, err
	}
	return facts, graph, nil
}

// A known commit and merge time identify an immutable event. A GitHub actor
// may be renamed or deleted, so the latest authoritative actor wins.
func checkMergeAgreement(old, fresh map[string]any) error {
	for _, key := range []string{"merged_at", "merge_commit_sha"} {
		if value, observed := fresh[key]; old[key] != nil && observed && value != old[key] {
			return errors.New("conflicting pull merge evidence")
		}
	}
	return nil
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
