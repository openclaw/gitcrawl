package remotedelivery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/github"
)

// ProjectAcquiredParent updates only fields observed by the parent-only query.
// The backing issue ID comes from the frozen selection, never a PR database ID.
func ProjectAcquiredParent(repoID, id int64, backingID string, parent github.ParentSnapshot, prior *remote.Change, at string) (remote.Change, error) {
	if repoID <= 0 || id <= 0 || parent.Number <= 0 || (parent.Kind != "issue" && parent.Kind != "pull_request") {
		return remote.Change{}, ErrPrivateProjection
	}
	n, err := strconv.ParseInt(backingID, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != backingID || parent.NodeID == "" {
		return remote.Change{}, ErrPrivateProjection
	}
	if parent.Kind == "issue" && parent.BackingIssueID != backingID {
		return remote.Change{}, errors.New("issue backing identity mismatch")
	}
	raw := parent.ProviderFields
	title, titleOK := raw["title"].(string)
	body, bodyOK := raw["body"].(string)
	updated, _ := raw["updatedAt"].(string)
	created, _ := raw["createdAt"].(string)
	parsed, err := time.Parse(time.RFC3339Nano, updated)
	if !titleOK || !bodyOK || err != nil {
		return remote.Change{}, ErrPrivateProjection
	}
	if _, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return remote.Change{}, ErrPrivateProjection
	}
	if _, err = time.Parse(time.RFC3339Nano, at); err != nil {
		return remote.Change{}, ErrPrivateProjection
	}
	layout := privateLayouts[1]
	row := make([]remote.ChangeValue, len(strings.Fields(layout.columns)))
	for i := range row {
		row[i].Kind = "null"
	}
	retained := map[string]any{}
	labels, assignees := "null", "null"
	if prior != nil {
		if prior.Table != "threads" || prior.Operation != "upsert" || len(prior.Row) != len(row) {
			return remote.Change{}, ErrPrivateProjection
		}
		row = slices.Clone(prior.Row)
		for name, want := range map[string]string{"id": strconv.FormatInt(id, 10), "repo_id": strconv.FormatInt(repoID, 10), "github_id": backingID, "number": strconv.Itoa(parent.Number), "kind": parent.Kind} {
			v := row[deliveryColumn("threads", name)]
			if v.Value == nil || *v.Value != want {
				return remote.Change{}, errors.New("parent selection differs from retained identity")
			}
		}
		old := row[deliveryColumn("threads", "updated_at_gh")]
		if old.Value != nil && *old.Value != "" {
			t, e := time.Parse(time.RFC3339Nano, *old.Value)
			if e != nil || parsed.Before(t) {
				return remote.Change{}, errors.New("provider parent precedes published version")
			}
		}
		for name, target := range map[string]*string{"labels_json": &labels, "assignees_json": &assignees} {
			v := row[deliveryColumn("threads", name)]
			if v.Value == nil || !json.Valid([]byte(*v.Value)) {
				return remote.Change{}, ErrPrivateProjection
			}
			*target = *v.Value
		}
		v := row[deliveryColumn("threads", "raw_json")]
		if v.Value == nil {
			return remote.Change{}, ErrPrivateProjection
		}
		decoder := json.NewDecoder(strings.NewReader(*v.Value))
		decoder.UseNumber()
		if err := decoder.Decode(&retained); err != nil || retained == nil {
			return remote.Change{}, ErrPrivateProjection
		}
	}
	stateKey := "issueState"
	if parent.Kind == "pull_request" {
		stateKey = "pullState"
	}
	state, _ := raw[stateKey].(string)
	switch state {
	case "OPEN":
		state = "open"
	case "CLOSED":
		state = "closed"
	case "MERGED":
		if parent.Kind != "pull_request" {
			return remote.Change{}, ErrPrivateProjection
		}
		state = "closed"
	default:
		return remote.Change{}, ErrPrivateProjection
	}
	author, _ := raw["author"].(map[string]any)
	login, _ := author["login"].(string)
	actor, _ := author["__typename"].(string)
	if actor == "Bot" && login != "" && !strings.HasSuffix(strings.ToLower(login), "[bot]") {
		login += "[bot]"
	}
	association, _ := raw["authorAssociation"].(string)
	url, _ := raw["url"].(string)
	if url == "" {
		return remote.Change{}, ErrPrivateProjection
	}
	closed, _ := raw["closedAt"].(string)
	merged, _ := raw["mergedAt"].(string)
	for _, stamp := range []string{closed, merged} {
		if stamp != "" {
			if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
				return remote.Change{}, ErrPrivateProjection
			}
		}
	}
	// Preserve unrelated REST metadata. The namespaced observation identifies
	// which fields were refreshed; absent collections are not asserted empty.
	var user any
	if author != nil {
		fields := map[string]any{}
		// Numeric author IDs and other unqueried metadata belong to the old
		// identity only. Retain them when the returned node ID proves a match.
		if old, ok := retained["user"].(map[string]any); ok {
			node, _ := old["node_id"].(string)
			if node != "" && node == author["id"] {
				fields = old
			}
		}
		for key, value := range map[string]any{"login": login, "type": actor, "node_id": author["id"], "html_url": author["url"]} {
			fields[key] = value
		}
		user = fields
	}
	for key, value := range map[string]any{"id": json.Number(backingID), "node_id": parent.NodeID, "number": parent.Number, "title": title, "body": body, "state": state, "user": user, "author_association": association, "html_url": url, "created_at": created, "updated_at": updated, "closed_at": raw["closedAt"]} {
		retained[key] = value
	}
	if parent.Kind == "pull_request" {
		pull, _ := retained["pull_request"].(map[string]any)
		if pull == nil {
			pull = map[string]any{}
		}
		pull["merged_at"] = raw["mergedAt"]
		retained["pull_request"] = pull
		draft, ok := raw["isDraft"].(bool)
		if !ok {
			return remote.Change{}, ErrPrivateProjection
		}
		retained["draft"] = draft
	}
	retained["_gitcrawl_graphql_parent"] = map[string]any{"schema": "gitcrawl-graphql-parent/v1", "fields": raw}
	encoded, err := json.Marshal(retained)
	if err != nil {
		return remote.Change{}, err
	}
	hash := sha256.New()
	for _, s := range []string{title, body, labels} {
		hash.Write([]byte(s))
		hash.Write([]byte{0})
	}
	fields := map[string]any{"id": id, "repo_id": repoID, "github_id": backingID, "number": parent.Number, "kind": parent.Kind, "state": state, "title": title, "body": body, "author_login": login, "author_type": actor, "author_association": association, "html_url": url, "labels_json": labels, "assignees_json": assignees, "raw_json": string(encoded), "content_hash": hex.EncodeToString(hash.Sum(nil)), "created_at_gh": created, "updated_at_gh": updated, "closed_at_gh": closed, "merged_at_gh": merged, "last_pulled_at": at, "updated_at": at}
	if parent.Kind == "pull_request" {
		fields["is_draft"] = boolInteger(raw["isDraft"].(bool))
	} else if prior == nil {
		fields["is_draft"] = int64(0)
	}
	if prior == nil {
		fields["first_pulled_at"] = at
		fields["observation_sequence"] = int64(0)
		fields["evidence_observation_sequence"] = int64(0)
		fields["evidence_source_updated_at"] = ""
	}
	change, err := acquisitionUpsert(layout, row, fields)
	if err != nil {
		return change, err
	}
	// GraphQL body is non-null, including the genuinely empty string.
	change.Row[deliveryColumn("threads", "body")] = remote.ChangeValue{Kind: "text", Value: &body}
	return change, nil
}
