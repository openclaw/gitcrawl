package cli

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/gitcrawl/internal/store"
)

const (
	gitcrawlCloudSchemaName    = "gitcrawl-cloud-v2"
	gitcrawlCloudSchemaVersion = 8
	gitcrawlCloudSchemaHash    = "gitcrawl-cloud-v2"

	gitcrawlObservationOrderCapability = "gitcrawl.observation-order.v1"
)

type gitcrawlCloudDataset struct {
	Name        string
	Columns     []string
	Rows        [][]any
	MaxSourceAt string
}

type gitcrawlCloudSnapshot struct {
	ID                 string
	SourceSyncAt       string
	DatasetGeneratedAt string
	Capabilities       []string
	Datasets           []gitcrawlCloudDataset
	Hydration          crawlstore.EnrichmentCoverage
}

func buildGitcrawlCloudSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshotPath string,
	allowIncomplete bool,
	observationOrder bool,
) (gitcrawlCloudSnapshot, error) {
	snapshotID, err := cloudFileSHA256(snapshotPath)
	if err != nil {
		return gitcrawlCloudSnapshot{}, err
	}
	sourceSyncAt, err := gitcrawlCloudSourceSyncAt(ctx, db)
	if err != nil {
		return gitcrawlCloudSnapshot{}, err
	}
	capabilities, err := gitcrawlCloudCapabilities(ctx, db, observationOrder)
	if err != nil {
		return gitcrawlCloudSnapshot{}, err
	}
	datasets, err := loadGitcrawlCloudDatasets(ctx, db, slices.Contains(capabilities, gitcrawlObservationOrderCapability))
	if err != nil {
		return gitcrawlCloudSnapshot{}, err
	}
	if len(datasets) == 0 || len(datasets[0].Rows) == 0 {
		return gitcrawlCloudSnapshot{}, fmt.Errorf("cloud snapshot has no repositories")
	}
	hydration, err := gitcrawlCloudHydration(ctx, snapshotPath)
	if err != nil {
		return gitcrawlCloudSnapshot{}, err
	}
	if missing := incompleteGitcrawlCloudHydration(hydration); len(missing) > 0 && !allowIncomplete {
		return gitcrawlCloudSnapshot{}, fmt.Errorf(
			"cloud snapshot enrichment is incomplete (%s); hydrate the archive or pass --allow-incomplete",
			strings.Join(missing, ", "),
		)
	}
	return gitcrawlCloudSnapshot{
		ID:                 snapshotID,
		SourceSyncAt:       sourceSyncAt,
		DatasetGeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Capabilities:       capabilities,
		Datasets:           datasets,
		Hydration:          hydration,
	}, nil
}

func gitcrawlCloudManifest(archive string, snapshot gitcrawlCloudSnapshot) crawlremote.IngestManifest {
	return crawlremote.IngestManifest{
		App:           "gitcrawl",
		Archive:       archive,
		SchemaName:    gitcrawlCloudSchemaName,
		SchemaVersion: gitcrawlCloudSchemaVersion,
		SchemaHash:    gitcrawlCloudSchemaHash,
		Mode:          crawlremote.ModePublisher,
		Source:        "sqlite",
		SourceSyncAt:  snapshot.SourceSyncAt,
		SnapshotID:    snapshot.ID,
		SourceSHA256:  snapshot.ID,
		Capabilities:  snapshot.Capabilities,
	}
}

func loadGitcrawlCloudDatasets(ctx context.Context, db *sql.DB, observationOrder bool) ([]gitcrawlCloudDataset, error) {
	threadColumns := append([]string(nil), gitcrawlThreadColumns...)
	threadQuery, err := gitcrawlThreadExportSQL(ctx, db)
	if err != nil {
		return nil, err
	}
	threadSelect := strings.TrimSpace(strings.TrimSuffix(threadQuery, "order by repo_id, number"))
	revisionColumns := []string{
		"id", "thread_id", "source_updated_at", "content_hash", "title_hash",
		"body_hash", "labels_hash", "created_at",
	}
	revisionSelect := `
select id, thread_id, coalesce(source_updated_at, ''), content_hash, title_hash,
       body_hash, labels_hash, created_at
from thread_revisions`
	if observationOrder {
		threadColumns = append(threadColumns, "observation_sequence")
		threadSelect = strings.Replace(
			threadSelect,
			"from threads",
			", observation_sequence\nfrom threads",
			1,
		)
		revisionColumns = append(revisionColumns, "observation_sequence")
		revisionSelect = strings.Replace(
			revisionSelect,
			"from thread_revisions",
			", observation_sequence\nfrom thread_revisions",
			1,
		)
	}

	specs := []struct {
		name        string
		columns     []string
		query       string
		maxSourceAt string
	}{
		{
			name:        "repositories",
			columns:     gitcrawlRepositoryColumns,
			query:       gitcrawlRepositoryExportSQL,
			maxSourceAt: `select coalesce(max(updated_at), '') from repositories`,
		},
		{
			name:        "threads",
			columns:     threadColumns,
			query:       threadSelect + "\norder by repo_id, number",
			maxSourceAt: `select coalesce(max(coalesce(nullif(updated_at_gh, ''), updated_at)), '') from threads`,
		},
		{
			name:        "thread_revisions",
			columns:     revisionColumns,
			query:       revisionSelect + "\norder by id",
			maxSourceAt: `select coalesce(max(coalesce(nullif(source_updated_at, ''), created_at)), '') from thread_revisions`,
		},
		{
			name: "thread_fingerprints",
			columns: []string{
				"id", "thread_revision_id", "algorithm_version", "fingerprint_hash",
				"fingerprint_slug", "body_token_hash", "file_set_hash", "simhash64", "created_at",
			},
			query: `
select id, thread_revision_id, algorithm_version, fingerprint_hash,
       fingerprint_slug, body_token_hash, file_set_hash, simhash64, created_at
from thread_fingerprints
order by id`,
			maxSourceAt: `select coalesce(max(created_at), '') from thread_fingerprints`,
		},
		{
			name: "thread_key_summaries",
			columns: []string{
				"id", "thread_revision_id", "summary_kind", "prompt_version", "provider",
				"model", "input_hash", "output_hash", "key_text", "created_at",
			},
			query: `
select id, thread_revision_id, summary_kind, prompt_version, provider,
       model, input_hash, output_hash, key_text, created_at
from thread_key_summaries
order by id`,
			maxSourceAt: `select coalesce(max(created_at), '') from thread_key_summaries`,
		},
		{
			name: "cluster_groups",
			columns: []string{
				"id", "repo_id", "stable_key", "stable_slug", "status", "cluster_type",
				"representative_thread_id", "title", "member_count", "created_at", "updated_at", "closed_at",
			},
			query: `
select cluster.id, cluster.repo_id, cluster.stable_key, cluster.stable_slug,
       cluster.status, coalesce(cluster.cluster_type, ''),
       cluster.representative_thread_id, coalesce(cluster.title, ''),
       (select count(*) from cluster_memberships membership
        where membership.cluster_id = cluster.id and membership.state = 'active'),
       cluster.created_at, cluster.updated_at, coalesce(cluster.closed_at, '')
from cluster_groups cluster
order by cluster.id`,
			maxSourceAt: `select coalesce(max(updated_at), '') from cluster_groups`,
		},
		{
			name: "cluster_memberships",
			columns: []string{
				"cluster_id", "thread_id", "role", "state", "score_to_representative",
				"created_at", "updated_at", "removed_at",
			},
			query: `
select cluster_id, thread_id, role, state, score_to_representative,
       created_at, updated_at, coalesce(removed_at, '')
from cluster_memberships
order by cluster_id, thread_id`,
			maxSourceAt: `select coalesce(max(updated_at), '') from cluster_memberships`,
		},
		{
			name: "pull_request_details",
			columns: []string{
				"thread_id", "repo_id", "number", "base_sha", "head_sha", "head_ref",
				"head_repo_full_name", "mergeable_state", "additions", "deletions",
				"changed_files", "fetched_at", "updated_at",
			},
			query: `
select thread_id, repo_id, number, coalesce(base_sha, ''), coalesce(head_sha, ''),
       coalesce(head_ref, ''), coalesce(head_repo_full_name, ''),
       coalesce(mergeable_state, ''), additions, deletions, changed_files,
       fetched_at, updated_at
from pull_request_details
order by thread_id`,
			maxSourceAt: `select coalesce(max(fetched_at), '') from pull_request_details`,
		},
		{
			name: "pull_request_files",
			columns: []string{
				"thread_id", "position", "path", "status", "additions", "deletions",
				"changes", "previous_path", "fetched_at",
			},
			query: `
select thread_id, position, path, coalesce(status, ''), additions, deletions,
       changes, coalesce(previous_path, ''), fetched_at
from pull_request_files
order by thread_id, position`,
			maxSourceAt: `select coalesce(max(fetched_at), '') from pull_request_files`,
		},
	}

	datasets := make([]gitcrawlCloudDataset, 0, len(specs))
	for _, spec := range specs {
		rows, err := publishRows(ctx, db, spec.query, func(values []any) []any { return values })
		if err != nil {
			return nil, fmt.Errorf("export cloud dataset %s: %w", spec.name, err)
		}
		var maxSourceAt string
		if err := db.QueryRowContext(ctx, spec.maxSourceAt).Scan(&maxSourceAt); err != nil {
			return nil, fmt.Errorf("read cloud dataset %s freshness: %w", spec.name, err)
		}
		datasets = append(datasets, gitcrawlCloudDataset{
			Name:        spec.name,
			Columns:     spec.columns,
			Rows:        rows,
			MaxSourceAt: maxSourceAt,
		})
	}
	return datasets, nil
}

func gitcrawlCloudCapabilities(ctx context.Context, db *sql.DB, observationOrder bool) ([]string, error) {
	if !observationOrder {
		return nil, nil
	}
	threadSequence, err := sqliteColumnExists(ctx, db, "threads", "observation_sequence")
	if err != nil {
		return nil, err
	}
	revisionSequence, err := sqliteColumnExists(ctx, db, "thread_revisions", "observation_sequence")
	if err != nil {
		return nil, err
	}
	if !threadSequence || !revisionSequence {
		return nil, fmt.Errorf(
			"--observation-order requires observation_sequence on threads and thread_revisions",
		)
	}
	return []string{gitcrawlObservationOrderCapability}, nil
}

func gitcrawlCloudSourceSyncAt(ctx context.Context, db *sql.DB) (string, error) {
	var sourceSyncAt string
	err := db.QueryRowContext(ctx, `
select coalesce(max(value), '')
from (
  select coalesce(finished_at, started_at, '') as value
  from sync_runs
  where status in ('success', 'completed')
  union all
  select coalesce(nullif(updated_at_gh, ''), updated_at, '') from threads
  union all
  select coalesce(updated_at, '') from repositories
)`).Scan(&sourceSyncAt)
	if err != nil {
		return "", fmt.Errorf("read cloud snapshot source sync time: %w", err)
	}
	return sourceSyncAt, nil
}

func gitcrawlCloudHydration(ctx context.Context, snapshotPath string) (crawlstore.EnrichmentCoverage, error) {
	st, err := crawlstore.OpenReadOnly(ctx, snapshotPath)
	if err != nil {
		return crawlstore.EnrichmentCoverage{}, fmt.Errorf("open cloud snapshot for hydration coverage: %w", err)
	}
	defer st.Close()
	coverage, err := st.ArchiveCoverage(ctx, crawlstore.ArchiveCoverageOptions{})
	if err != nil {
		return crawlstore.EnrichmentCoverage{}, fmt.Errorf("read cloud snapshot hydration coverage: %w", err)
	}
	return coverage.Totals.Enrichment, nil
}

func incompleteGitcrawlCloudHydration(coverage crawlstore.EnrichmentCoverage) []string {
	metrics := []struct {
		name   string
		metric crawlstore.EnrichmentCoverageMetric
	}{
		{name: "revisions", metric: coverage.Revisions},
		{name: "fingerprints", metric: coverage.Fingerprints},
		{name: "summaries", metric: coverage.Summaries},
		{name: "clusters", metric: coverage.Clusters},
		{name: "pr_details", metric: coverage.PRDetails},
	}
	var missing []string
	for _, item := range metrics {
		if !item.metric.Supported || !item.metric.Complete {
			missing = append(missing, fmt.Sprintf(
				"%s=%d/%d fresh=%d",
				item.name,
				item.metric.Covered,
				item.metric.Eligible,
				item.metric.Fresh,
			))
		}
	}
	return missing
}

func gitcrawlCloudCoverageRows(snapshot gitcrawlCloudSnapshot, mutationToken string) [][]any {
	rows := make([][]any, 0, len(snapshot.Datasets))
	for _, dataset := range snapshot.Datasets {
		count := int64(len(dataset.Rows))
		rows = append(rows, []any{
			dataset.Name,
			count,
			count,
			count,
			dataset.MaxSourceAt,
			snapshot.DatasetGeneratedAt,
			true,
			mutationToken,
		})
	}
	return rows
}

func gitcrawlCloudDatasetCounts(snapshot gitcrawlCloudSnapshot) map[string]int64 {
	counts := make(map[string]int64, len(snapshot.Datasets))
	for _, dataset := range snapshot.Datasets {
		counts[dataset.Name] = int64(len(dataset.Rows))
	}
	return counts
}
