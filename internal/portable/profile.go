package portable

import (
	"fmt"

	"github.com/openclaw/gitcrawl/internal/store"
)

// Profile defines a portable artifact's semantic policy independently of CLI
// parsing and publication concerns.
type Profile struct {
	Name          string
	Version       int
	ClearedTables []string
	Capabilities  []string
	Excluded      []string
	IndexProfile  string
	ColumnProfile string
}

var currentStateProfile = Profile{
	Name:    CurrentStateV1,
	Version: currentProfileVersion,
	ClearedTables: []string{
		"comment_revisions",
		"thread_key_summaries",
		"cluster_closures",
		"cluster_overrides",
		"cluster_aliases",
		"cluster_memberships",
		"cluster_groups",
	},
	Capabilities: []string{
		"body_excerpts",
		"comment_excerpts",
		"author_association",
		"current_comments",
		"thread_revisions",
		"thread_fingerprints",
		"pr_details",
		"pr_files",
		"pr_commits",
		"pr_checks",
		"pr_review_threads",
		"workflow_runs",
		"family_tombstones",
		"thread_child_observation_memberships",
		"raw_json_stripped",
	},
	Excluded: []string{
		"raw_json",
		"pull_request_file_patches",
		"documents",
		"fts",
		"vectors",
		"code_snapshots",
		"code_documents",
		"comment_revision_history",
		"thread_key_summaries",
		"cluster_governance",
		"cluster_lineage",
		"cluster_state",
		"run_history",
		"similarity_edges",
		"blobs",
		"sync_attempt_failures",
		"ordinary_indexes",
	},
	IndexProfile:  "constraints-only",
	ColumnProfile: store.PortableColumnProfileSanitizedCompatibility,
}

func ResolveProfile(name string) (Profile, error) {
	if name != CurrentStateV1 {
		return Profile{}, fmt.Errorf("unsupported portable export profile %q; supported profile: %s", name, CurrentStateV1)
	}
	return currentStateProfile, nil
}
