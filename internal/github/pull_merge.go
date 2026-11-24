package github

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// PullMergeFields normalizes observed REST or GraphQL merge fields for a
// confirmed merged pull. A null OID is unavailable, since archive readers
// interpret a top-level null OID as a proven absence.
func PullMergeFields(raw map[string]any) (map[string]any, error) {
	stamp, ok := raw["merged_at"].(string)
	if !ok {
		stamp = historyString(raw["mergedAt"])
	}
	if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
		return nil, errors.New("invalid pull merge time")
	}
	facts := map[string]any{"merged_at": stamp}
	oid, rest := raw["merge_commit_sha"]
	if commit, graph := raw["mergeCommit"]; graph && commit != nil {
		hash, ok := historyMap(commit)["oid"].(string)
		if !ok || (rest && oid != hash) {
			return nil, errors.New("invalid or conflicting pull merge commit")
		}
		oid = hash
	}
	if oid != nil {
		if !commitObjectID.MatchString(historyString(oid)) {
			return nil, errors.New("invalid pull merge commit")
		}
		facts["merge_commit_sha"] = oid
	}
	actor, present := raw["merged_by"]
	if !present {
		if graph, ok := raw["mergedBy"]; ok {
			present = true
			if graph != nil {
				if strings.TrimSpace(historyString(historyMap(graph)["login"])) == "" {
					return nil, errors.New("invalid graph pull merger")
				}
				actor = historyActorMap(historyMap(graph))
			}
		}
	}
	if present {
		if actor != nil && strings.TrimSpace(historyString(historyMap(actor)["login"])) == "" {
			return nil, errors.New("invalid pull merge actor")
		}
		facts["merged_by"] = actor
	}
	return facts, CheckPullMergeAliases(raw, facts)
}

// ParentMergeFields projects only a provider-confirmed merge. The original
// GraphQL selection is retained independently by the archive projection.
func ParentMergeFields(fields map[string]any) (map[string]any, error) {
	if fields["pullState"] != "MERGED" {
		if fields["merged"] == true || fields["mergedAt"] != nil {
			return nil, errors.New("conflicting pull merge state")
		}
		return nil, nil
	}
	if fields["merged"] != true {
		return nil, errors.New("unconfirmed pull merge state")
	}
	return PullMergeFields(fields)
}

// VerifiedPullMerge binds the full Pull response to the selected repository
// and the Issues response. Their numeric IDs belong to different namespaces.
func VerifiedPullMerge(repository, repositoryID string, number int, issue, pull map[string]any) (map[string]any, error) {
	base := historyMap(historyMap(pull["base"])["repo"])
	got, valid := historyInt(pull["number"])
	issueNumber, validIssue := historyInt(issue["number"])
	node := historyString(issue["node_id"])
	id, validID := historyInt(base["id"])
	mergedAt := historyString(pull["merged_at"])
	if !valid || got != number || !validIssue || issueNumber != number || !validID || strconv.Itoa(id) != repositoryID ||
		!strings.EqualFold(historyString(base["full_name"]), repository) || node == "" || historyString(pull["node_id"]) != node ||
		pull["merged"] != true || mergedAt == "" {
		return nil, errors.New("full pull merge differs from selected issue")
	}
	if nested := historyMap(issue["pull_request"])["merged_at"]; nested != nil && nested != mergedAt {
		return nil, errors.New("full pull merge time differs from issue")
	}
	facts, err := PullMergeFields(pull)
	if err != nil {
		return nil, err
	}
	_, observedOID := pull["merge_commit_sha"].(string)
	if !observedOID || facts["merge_commit_sha"] == nil {
		return nil, errors.New("incomplete full pull merge response")
	}
	return facts, nil
}

// CheckPullMergeAliases prevents an older top-level GraphQL alias from
// contradicting the REST aliases consumed by archive readers.
func CheckPullMergeAliases(raw, facts map[string]any) error {
	if stamp, present := raw["merged_at"]; present && stamp != facts["merged_at"] {
		return errors.New("conflicting pull merge time aliases")
	}
	if stamp, present := raw["mergedAt"]; present && stamp != facts["merged_at"] {
		return errors.New("conflicting pull merge time aliases")
	}
	if obj, present := raw["mergeCommit"]; present {
		if (obj == nil && facts["merge_commit_sha"] != nil) || (obj != nil && historyString(historyMap(obj)["oid"]) != facts["merge_commit_sha"]) {
			return errors.New("conflicting pull merge commit aliases")
		}
	}
	if actor, graph := raw["mergedBy"]; graph {
		canonical, observed := facts["merged_by"]
		if observed && ((actor == nil) != (canonical == nil) || (actor != nil && !strings.EqualFold(historyString(historyActorMap(historyMap(actor))["login"]), historyString(historyMap(canonical)["login"])))) {
			return errors.New("conflicting pull merger aliases")
		}
	}
	return nil
}
