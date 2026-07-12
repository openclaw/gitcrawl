package store

import (
	"context"
	"fmt"
	"os"

	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type SchemaDiagnostics struct {
	Path              string                    `json:"path"`
	Exists            bool                      `json:"exists"`
	CurrentVersion    int                       `json:"current_version"`
	SupportedVersion  int                       `json:"supported_version"`
	State             string                    `json:"state"`
	Current           bool                      `json:"current"`
	PendingMigration  bool                      `json:"pending_migration"`
	Legacy            bool                      `json:"legacy"`
	Newer             bool                      `json:"newer"`
	PendingMigrations []string                  `json:"pending_migrations"`
	PRDetails         PRDetailSchemaDiagnostics `json:"pr_details"`
	NextSteps         []string                  `json:"next_steps,omitempty"`
	Error             string                    `json:"error,omitempty"`
}

type PRDetailSchemaDiagnostics struct {
	DetailsTable                bool   `json:"details_table"`
	FilesTable                  bool   `json:"files_table"`
	FilesPositionKey            bool   `json:"files_position_key"`
	DuplicatePathFilesSupported bool   `json:"duplicate_path_files_supported"`
	State                       string `json:"state"`
}

func InspectSchema(ctx context.Context, path string) SchemaDiagnostics {
	diag := SchemaDiagnostics{
		Path:              path,
		SupportedVersion:  schemaVersion,
		State:             "missing",
		PendingMigrations: []string{},
		PRDetails: PRDetailSchemaDiagnostics{
			State: "missing",
		},
	}
	if path == "" {
		diag.NextSteps = []string{"Check the configured db_path before running write commands."}
		return diag
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			diag.NextSteps = []string{"Run gitcrawl init or check the configured db_path."}
			return diag
		}
		diag.State = "error"
		diag.Error = err.Error()
		diag.NextSteps = []string{"Check the configured db_path before running write commands."}
		return diag
	}
	diag.Exists = true

	base, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		diag.State = "error"
		diag.Error = err.Error()
		diag.NextSteps = []string{"Check SQLite file health before running write commands."}
		return diag
	}
	defer base.Close()

	st := &Store{db: base.DB(), sqlc: storedb.New(base.DB()), path: path}
	current, err := st.schemaVersion(ctx)
	if err != nil {
		diag.State = "error"
		diag.Error = err.Error()
		diag.NextSteps = []string{"Check SQLite schema metadata before running write commands."}
		return diag
	}
	diag.CurrentVersion = current
	diag.PRDetails = inspectPRDetailSchema(ctx, st)
	diag.PendingMigrations = pendingCompatibilityMigrations(ctx, st, current, diag.PRDetails)
	if diag.PendingMigrations == nil {
		diag.PendingMigrations = []string{}
	}
	diag.Newer = current > schemaVersion
	diag.Legacy = current < schemaVersion || len(diag.PendingMigrations) > 0
	diag.PendingMigration = diag.Legacy && !diag.Newer
	diag.Current = current == schemaVersion && len(diag.PendingMigrations) == 0

	switch {
	case diag.Newer:
		diag.State = "newer"
	case diag.PendingMigration:
		diag.State = "pending_migration"
	case diag.Current:
		diag.State = "current"
	default:
		diag.State = "unknown"
	}
	diag.NextSteps = schemaNextSteps(diag)
	return diag
}

func inspectPRDetailSchema(ctx context.Context, st *Store) PRDetailSchemaDiagnostics {
	detailsTable := st.hasTable(ctx, "pull_request_details")
	filesTable := st.hasTable(ctx, "pull_request_files")
	filesPositionKey := false
	if filesTable {
		filesPositionKey = st.pullRequestFilesHavePositionKey(ctx)
	}
	state := "missing"
	switch {
	case detailsTable && filesTable && filesPositionKey:
		state = "supported"
	case detailsTable && filesTable:
		state = "legacy"
	case detailsTable || filesTable:
		state = "partial"
	}
	return PRDetailSchemaDiagnostics{
		DetailsTable:                detailsTable,
		FilesTable:                  filesTable,
		FilesPositionKey:            filesPositionKey,
		DuplicatePathFilesSupported: detailsTable && filesTable && filesPositionKey,
		State:                       state,
	}
}

func pendingCompatibilityMigrations(ctx context.Context, st *Store, current int, prDetails PRDetailSchemaDiagnostics) []string {
	var pending []string
	if current < schemaVersion {
		pending = append(pending, fmt.Sprintf("schema_version_%d_to_%d", current, schemaVersion))
	}
	if st.hasTable(ctx, "repositories") && !st.hasColumn(ctx, "repositories", "raw_json") {
		pending = append(pending, "repositories_raw_json_column")
	}
	if st.hasTable(ctx, "threads") {
		if !st.hasColumn(ctx, "threads", "body") {
			pending = append(pending, "threads_body_column")
		}
		if !st.hasColumn(ctx, "threads", "raw_json") {
			pending = append(pending, "threads_raw_json_column")
		}
		if !st.hasColumn(ctx, "threads", "author_association") {
			pending = append(pending, "threads_author_association_column")
		}
	}
	if st.hasTable(ctx, "thread_vectors") && !st.threadVectorsHaveCompositeKey(ctx) {
		pending = append(pending, "thread_vectors_composite_key")
	}
	if st.hasTable(ctx, "thread_revisions") {
		if st.threadRevisionsHaveUniqueContentHash(ctx) {
			pending = append(pending, "thread_revisions_transition_history")
		}
		if !st.hasColumn(ctx, "thread_revisions", "observation_sequence") {
			pending = append(pending, "thread_revisions_observation_sequence")
		}
	}
	if current > 0 && !st.hasTable(ctx, "thread_observation_sequence") {
		pending = append(pending, "thread_observation_sequence_table")
	}
	if current > 0 && current <= schemaVersion {
		if !prDetails.DetailsTable {
			pending = append(pending, "pull_request_details_table")
		}
		if !prDetails.FilesTable {
			pending = append(pending, "pull_request_files_table")
		}
	}
	if prDetails.FilesTable && !prDetails.FilesPositionKey {
		pending = append(pending, "pull_request_files_position_key")
	}
	return pending
}

func schemaNextSteps(diag SchemaDiagnostics) []string {
	switch diag.State {
	case "missing":
		return []string{"Run gitcrawl init or check the configured db_path."}
	case "newer":
		return []string{"Upgrade the active gitcrawl executable to a build that supports this database schema."}
	case "pending_migration":
		return []string{"Run a write-path command with the intended rebuilt gitcrawl executable to apply pending migrations."}
	case "error":
		return []string{"Check SQLite file health before running write commands."}
	default:
		return nil
	}
}
