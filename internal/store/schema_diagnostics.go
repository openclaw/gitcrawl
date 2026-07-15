package store

import (
	"context"
	"os"
	"strings"

	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type SchemaDiagnostics struct {
	Path                           string                    `json:"path"`
	Exists                         bool                      `json:"exists"`
	CurrentVersion                 int                       `json:"current_version"`
	SupportedVersion               int                       `json:"supported_version"`
	State                          string                    `json:"state"`
	Current                        bool                      `json:"current"`
	PendingMigration               bool                      `json:"pending_migration"`
	Legacy                         bool                      `json:"legacy"`
	Newer                          bool                      `json:"newer"`
	ChildReservations              bool                      `json:"child_observation_reservations"`
	ChildReservationsCurrent       bool                      `json:"child_observation_reservations_current"`
	WorkflowRunReservations        bool                      `json:"workflow_run_observation_reservations"`
	WorkflowRunReservationsCurrent bool                      `json:"workflow_run_observation_reservations_current"`
	PendingMigrations              []string                  `json:"pending_migrations"`
	PRDetails                      PRDetailSchemaDiagnostics `json:"pr_details"`
	NextSteps                      []string                  `json:"next_steps,omitempty"`
	Error                          string                    `json:"error,omitempty"`
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
	diag.ChildReservations = st.hasTable(ctx, "thread_child_observation_reservations")
	diag.ChildReservationsCurrent = diag.ChildReservations &&
		st.threadChildObservationReservationsHaveCurrentShape(ctx)
	diag.WorkflowRunReservations = st.hasTable(ctx, "workflow_run_observation_reservations")
	diag.WorkflowRunReservationsCurrent = diag.WorkflowRunReservations &&
		st.workflowRunObservationReservationsHaveCurrentShape(ctx)
	diag.PendingMigrations, err = inspectCompatibilityMigrations(
		ctx,
		st,
		current,
		diag.PRDetails,
	)
	if err != nil {
		diag.State = "error"
		diag.Error = err.Error()
		diag.NextSteps = []string{"Check SQLite schema and observation-order metadata before running write commands."}
		return diag
	}
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

func schemaNextSteps(diag SchemaDiagnostics) []string {
	switch diag.State {
	case "missing":
		return []string{"Run gitcrawl init or check the configured db_path."}
	case "newer":
		return []string{"Upgrade the active gitcrawl executable to a build that supports this database schema."}
	case "pending_migration":
		return []string{
			"Run a writable gitcrawl command once to repair: " +
				strings.Join(diag.PendingMigrations, ", ") + ".",
			"Re-run gitcrawl doctor --json and confirm db_schema.state is current.",
		}
	case "error":
		return []string{"Check SQLite file health before running write commands."}
	default:
		return nil
	}
}
