package store

import (
	"context"
	"fmt"
)

const (
	migrationThreadsCanonicalSchema            = "threads_canonical_schema"
	migrationThreadRevisionsCanonicalSchema    = "thread_revisions_canonical_schema"
	migrationThreadObservationSequenceValue    = "threads_observation_sequence_value"
	migrationThreadObservationSequenceTable    = "thread_observation_sequence_table"
	migrationThreadObservationSequenceShape    = "thread_observation_sequence_shape"
	migrationThreadObservationSequenceRow      = "thread_observation_sequence_row"
	migrationThreadObservationSequenceFloor    = "thread_observation_sequence_floor"
	migrationThreadEvidenceObservationSequence = "thread_evidence_observation_sequence_backfill"
	migrationThreadChildReservationsTable      = "thread_child_observation_reservations_table"
	migrationThreadChildReservationsShape      = "thread_child_observation_reservations_shape"
	migrationThreadChildReservationsBackfill   = "thread_child_observation_reservations_backfill"
	migrationWorkflowRunReservationsTable      = "workflow_run_observation_reservations_table"
	migrationWorkflowRunReservationsShape      = "workflow_run_observation_reservations_shape"
	migrationWorkflowRunReservationsBackfill   = "workflow_run_observation_reservations_backfill"
	migrationObservationSchemaConvergence      = "observation_schema_convergence"
	migrationFamilyTombstoneSchema             = "family_tombstone_schema"
)

func inspectCompatibilityMigrations(
	ctx context.Context,
	st *Store,
	current int,
	prDetails PRDetailSchemaDiagnostics,
) ([]string, error) {
	return inspectCompatibilityMigrationsMode(ctx, st, current, prDetails, true)
}

func inspectStructuralCompatibilityMigrations(
	ctx context.Context,
	st *Store,
	current int,
	prDetails PRDetailSchemaDiagnostics,
) ([]string, error) {
	return inspectCompatibilityMigrationsMode(ctx, st, current, prDetails, false)
}

func inspectCompatibilityMigrationsMode(
	ctx context.Context,
	st *Store,
	current int,
	prDetails PRDetailSchemaDiagnostics,
	includeSemantic bool,
) ([]string, error) {
	var pending []string
	add := func(value string) {
		for _, existing := range pending {
			if existing == value {
				return
			}
		}
		pending = append(pending, value)
	}
	if current < schemaVersion {
		add(fmt.Sprintf("schema_version_%d_to_%d", current, schemaVersion))
	}
	if st.hasTable(ctx, "repositories") && !st.hasColumn(ctx, "repositories", "raw_json") {
		add("repositories_raw_json_column")
	}
	threadsCanonical := false
	if st.hasTable(ctx, "threads") {
		if !st.hasColumn(ctx, "threads", "body") {
			add("threads_body_column")
		}
		if !st.hasColumn(ctx, "threads", "raw_json") {
			add("threads_raw_json_column")
		}
		if !st.hasColumn(ctx, "threads", "author_association") {
			add("threads_author_association_column")
		}
		if !st.hasColumn(ctx, "threads", "observation_sequence") {
			add("threads_observation_sequence")
		}
		if !st.hasColumn(ctx, "threads", "evidence_observation_sequence") {
			add("threads_evidence_observation_sequence")
		}
		threadsCanonical = st.threadsHaveCanonicalShape(ctx)
		if !threadsCanonical {
			add(migrationThreadsCanonicalSchema)
		}
	}
	if st.hasTable(ctx, "thread_vectors") && !st.threadVectorsHaveCompositeKey(ctx) {
		add("thread_vectors_composite_key")
	}
	revisionsCanonical := false
	if st.hasTable(ctx, "thread_revisions") {
		if st.threadRevisionsHaveUniqueContentHash(ctx) {
			add("thread_revisions_transition_history")
		}
		if !st.hasColumn(ctx, "thread_revisions", "observation_sequence") {
			add("thread_revisions_observation_sequence")
		}
		revisionsCanonical = st.threadRevisionsHaveCanonicalShape(ctx)
		if !revisionsCanonical {
			add(migrationThreadRevisionsCanonicalSchema)
		}
	}
	if current > 0 && !st.familyTombstoneSchemaHasCurrentShape(ctx) {
		add(migrationFamilyTombstoneSchema)
	}

	allocatorCurrent := false
	if current > 0 && !st.hasTable(ctx, "thread_observation_sequence") {
		add(migrationThreadObservationSequenceTable)
	} else if st.hasTable(ctx, "thread_observation_sequence") {
		allocatorCurrent = st.threadObservationSequenceHasCurrentShape(ctx)
		if !allocatorCurrent {
			add(migrationThreadObservationSequenceShape)
		}
	}

	childCurrent := false
	if current > 0 && !st.hasTable(ctx, "thread_child_observation_reservations") {
		add(migrationThreadChildReservationsTable)
	} else if st.hasTable(ctx, "thread_child_observation_reservations") {
		childCurrent = st.threadChildObservationReservationsHaveCurrentShape(ctx)
		if !childCurrent {
			add(migrationThreadChildReservationsShape)
		}
	}

	workflowCurrent := false
	if current > 0 && !st.hasTable(ctx, "workflow_run_observation_reservations") {
		add(migrationWorkflowRunReservationsTable)
	} else if st.hasTable(ctx, "workflow_run_observation_reservations") {
		workflowCurrent = st.workflowRunObservationReservationsHaveCurrentShape(ctx)
		if !workflowCurrent {
			add(migrationWorkflowRunReservationsShape)
		}
	}

	if current > 0 && current <= schemaVersion {
		if !prDetails.DetailsTable {
			add("pull_request_details_table")
		}
		if !prDetails.FilesTable {
			add("pull_request_files_table")
		}
	}
	if prDetails.FilesTable && !prDetails.FilesPositionKey {
		add("pull_request_files_position_key")
	}
	if current > 0 && !st.observationSchemaConvergenceHasCurrentShape(ctx) {
		add(migrationObservationSchemaConvergence)
	}
	if !includeSemantic {
		return pending, nil
	}

	if threadsCanonical {
		drift, err := st.threadObservationSequenceValuesNeedRepair(ctx)
		if err != nil {
			return nil, err
		}
		if drift {
			add(migrationThreadObservationSequenceValue)
		}
	}
	if threadsCanonical && revisionsCanonical {
		drift, err := st.threadEvidenceObservationSequenceNeedsRepair(ctx)
		if err != nil {
			return nil, err
		}
		if drift {
			add(migrationThreadEvidenceObservationSequence)
		}
	}
	if threadsCanonical && childCurrent {
		drift, err := st.threadChildObservationReservationsNeedRepair(ctx)
		if err != nil {
			return nil, err
		}
		if drift {
			add(migrationThreadChildReservationsBackfill)
		}
	}
	if threadsCanonical && workflowCurrent && prDetails.DetailsTable {
		drift, err := st.workflowRunObservationReservationsNeedRepair(ctx)
		if err != nil {
			return nil, err
		}
		if drift {
			add(migrationWorkflowRunReservationsBackfill)
		}
	}
	if threadsCanonical && revisionsCanonical && allocatorCurrent && childCurrent && workflowCurrent {
		missing, belowFloor, err := st.threadObservationSequenceNeedsRepair(ctx)
		if err != nil {
			return nil, err
		}
		if missing {
			add(migrationThreadObservationSequenceRow)
		}
		if belowFloor {
			add(migrationThreadObservationSequenceFloor)
		}
	}
	return pending, nil
}

func (s *Store) threadObservationSequenceValuesNeedRepair(ctx context.Context) (bool, error) {
	var drift bool
	if err := s.q().QueryRowContext(ctx, `
		select exists(
			select 1
			from threads
			where observation_sequence < -9223372036854775807
			limit 1
		)
	`).Scan(&drift); err != nil {
		return false, fmt.Errorf("inspect thread observation sequence values: %w", err)
	}
	return drift, nil
}

func (s *Store) threadEvidenceObservationSequenceNeedsRepair(ctx context.Context) (bool, error) {
	var drift bool
	evidenceTupleExists := threadEvidenceTupleExistsSQL("evidence_revision", "threads")
	if err := s.q().QueryRowContext(ctx, `
		select exists(
			select 1
			from threads
			where (evidence_observation_sequence = 0 or not `+evidenceTupleExists+`)
				and coalesce((
				select max(thread_revisions.observation_sequence)
				from thread_revisions
				where thread_revisions.thread_id = threads.id
					and thread_revisions.observation_sequence > 0
				), 0) > 0
			limit 1
		)
	`).Scan(&drift); err != nil {
		return false, fmt.Errorf("inspect thread evidence observation floor: %w", err)
	}
	return drift, nil
}

func (s *Store) threadChildObservationReservationsNeedRepair(ctx context.Context) (bool, error) {
	var drift bool
	effectiveEvidenceSequence := `case
		when evidence_observation_sequence > 0
			then evidence_observation_sequence
		else coalesce((
			select thread_revisions.observation_sequence
			from thread_revisions
			where thread_revisions.thread_id = threads.id
				and thread_revisions.observation_sequence > 0
			order by ` + threadEvidenceRevisionOrderSQL("thread_revisions", "threads") + `
			limit 1
		), 0)
	end`
	if err := s.q().QueryRowContext(ctx, `
		with effective_threads as (
			select threads.*,
				`+effectiveEvidenceSequence+` as effective_evidence_observation_sequence
			from threads
		),
		families(family, pull_request_only) as (
			values
				('comments', 0),
				('pull_request_details', 1),
				('pull_request_files', 1),
				('pull_request_commits', 1),
				('pull_request_checks', 1),
				('pull_request_review_threads', 1)
		)
		select exists(
			select 1
			from effective_threads
			join families
				on families.pull_request_only = 0
					or effective_threads.kind = 'pull_request'
			left join thread_child_observation_reservations
				on thread_child_observation_reservations.thread_id = effective_threads.id
					and thread_child_observation_reservations.family = families.family
			where effective_threads.effective_evidence_observation_sequence > 0
				and thread_child_observation_reservations.thread_id is null
			limit 1
		)
	`).Scan(&drift); err != nil {
		return false, fmt.Errorf("inspect child observation reservations: %w", err)
	}
	return drift, nil
}

func (s *Store) workflowRunObservationReservationsNeedRepair(ctx context.Context) (bool, error) {
	var drift bool
	effectiveEvidenceSequence := `case
		when evidence_observation_sequence > 0
			then evidence_observation_sequence
		else coalesce((
			select thread_revisions.observation_sequence
			from thread_revisions
			where thread_revisions.thread_id = threads.id
				and thread_revisions.observation_sequence > 0
			order by ` + threadEvidenceRevisionOrderSQL("thread_revisions", "threads") + `
			limit 1
		), 0)
	end`
	if err := s.q().QueryRowContext(ctx, `
		with effective_threads as (
			select threads.id,
				`+effectiveEvidenceSequence+` as evidence_observation_sequence
			from threads
		),
		expected(repo_id, head_sha, observation_sequence) as (
			select
				pull_request_details.repo_id,
				trim(pull_request_details.head_sha),
				max(effective_threads.evidence_observation_sequence)
			from pull_request_details
			join effective_threads
				on effective_threads.id = pull_request_details.thread_id
			where trim(coalesce(pull_request_details.head_sha, '')) <> ''
				and effective_threads.evidence_observation_sequence > 0
			group by pull_request_details.repo_id, trim(pull_request_details.head_sha)
		)
		select exists(
			select 1
			from expected
			left join workflow_run_observation_reservations
				on workflow_run_observation_reservations.repo_id = expected.repo_id
					and workflow_run_observation_reservations.head_sha = expected.head_sha
				where workflow_run_observation_reservations.repo_id is null
					or (
						trim(coalesce(
							workflow_run_observation_reservations.source_updated_at,
							''
						)) = ''
						and workflow_run_observation_reservations.observation_sequence <
							expected.observation_sequence
					)
				limit 1
			)
		`).Scan(&drift); err != nil {
		return false, fmt.Errorf("inspect workflow run observation reservations: %w", err)
	}
	return drift, nil
}

func (s *Store) threadObservationSequenceNeedsRepair(
	ctx context.Context,
) (missing bool, belowFloor bool, err error) {
	var (
		rows  int64
		value int64
		floor int64
	)
	if err := s.q().QueryRowContext(ctx, `
		select
			count(*),
			coalesce(max(value), 0)
		from thread_observation_sequence
		where id = 1
	`).Scan(&rows, &value); err != nil {
		return false, false, fmt.Errorf("inspect thread observation allocator row: %w", err)
	}
	if err := s.q().QueryRowContext(ctx, `
		select max(
			coalesce((
				select max(case
					when observation_sequence < -9223372036854775807
						then 9223372036854775807
					when observation_sequence < 0 then -observation_sequence
					else observation_sequence
				end)
				from threads
			), 0),
			coalesce((select max(evidence_observation_sequence) from threads), 0),
			coalesce((select max(observation_sequence) from thread_revisions), 0),
			coalesce((
				select max(observation_sequence)
				from thread_child_observation_reservations
			), 0),
			coalesce((
				select max(observation_sequence)
				from workflow_run_observation_reservations
			), 0)
		)
	`).Scan(&floor); err != nil {
		return false, false, fmt.Errorf("inspect thread observation allocator floor: %w", err)
	}
	return rows != 1, rows == 1 && value < floor, nil
}
