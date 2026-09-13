package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	portableexport "github.com/openclaw/gitcrawl/internal/portable"
	crawlstore "github.com/openclaw/gitcrawl/internal/store"
)

const (
	gitcrawlArchiveAdmissionPolicy     = "archive-v1"
	gitcrawlArchiveAdmissionCapability = "gitcrawl.archive-admission.v1"
	gitcrawlCloudAdmissionMetadataKey  = "cloud_admission_v1"
	gitcrawlCloudWarningsMaxElements   = 32
	gitcrawlCloudWarningsMaxBytes      = 8192
)

type gitcrawlCloudPublishOptions struct {
	AllowIncomplete  bool
	ObservationOrder bool
	AdmissionPolicy  string
}

type gitcrawlCloudIntegrity struct {
	SQLite             bool `json:"sqlite"`
	CompatibleSchema   bool `json:"compatible_schema"`
	RequiredData       bool `json:"required_data"`
	ReferentialClosure bool `json:"referential_closure"`
	FullBodies         bool `json:"full_bodies"`
	Privacy            bool `json:"privacy"`
}

type gitcrawlCloudAdmission struct {
	Policy     string                             `json:"policy"`
	Integrity  gitcrawlCloudIntegrity             `json:"integrity"`
	Source     crawlstore.ArchiveSourceAssessment `json:"source"`
	Enrichment crawlstore.EnrichmentCoverage      `json:"enrichment"`
	Warnings   []string                           `json:"warnings"`
	datasets   []gitcrawlCloudDataset
}

func assessGitcrawlCloudArchive(ctx context.Context, path string) (*gitcrawlCloudAdmission, error) {
	st, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("open frozen archive for admission: %w", err)
	}
	defer st.Close()
	if err := requireLosslessGitcrawlCloudSource(ctx, st.DB()); err != nil {
		return nil, err
	}
	coverage, err := st.ArchiveCoverage(ctx, crawlstore.ArchiveCoverageOptions{})
	if err != nil {
		return nil, err
	}
	source, err := st.ArchiveSourceObservations(ctx, coverage)
	if err != nil {
		return nil, err
	}
	admission := &gitcrawlCloudAdmission{
		Policy: gitcrawlArchiveAdmissionPolicy, Source: source, Enrichment: coverage.Totals.Enrichment,
	}
	admission.Warnings, err = gitcrawlArchiveWarnings(*admission)
	return admission, err
}

func requireLosslessGitcrawlCloudSource(ctx context.Context, db *sql.DB) error {
	if exists, err := sqliteTableExists(ctx, db, "portable_metadata"); err != nil {
		return err
	} else if !exists {
		return nil
	}
	rows, err := db.QueryContext(ctx, `select key, value from portable_metadata
		where key in ('profile', 'body_chars', 'capabilities', 'excluded')`)
	if err != nil {
		return fmt.Errorf("read archive source profile: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("scan archive source profile: %w", err)
		}
		// Current bodies can fit the excerpt limit even after history/review
		// bodies or patches were lost. Native profile declarations retain that
		// evidence; empty patch rows alone do not establish loss.
		lossy := false
		switch key {
		case "profile":
			lossy = value == portableexport.CurrentStateV1
		case "body_chars":
			limit, err := strconv.Atoi(value)
			lossy = err == nil && limit > 0
		case "capabilities":
			values := strings.Split(value, ",")
			lossy = slices.Contains(values, "body_excerpts") || slices.Contains(values, "comment_excerpts")
		case "excluded":
			values := strings.Split(value, ",")
			lossy = slices.Contains(values, "pull_request_file_patches") || slices.Contains(values, "comment_revision_history")
		}
		if lossy {
			return fmt.Errorf("archive source declares a lossy portable profile; use the full runtime archive")
		}
	}
	return rows.Err()
}

func gitcrawlArchiveWarnings(admission gitcrawlCloudAdmission) ([]string, error) {
	unknown := admission.Source.RemoteFreshness != crawlstore.ArchiveObservationComplete
	incomplete := false
	for _, repo := range admission.Source.Repositories {
		switch repo.Inventory.State {
		case crawlstore.ArchiveObservationUnsupported, crawlstore.ArchiveObservationUnknown:
			unknown = true
		case crawlstore.ArchiveObservationMissing, crawlstore.ArchiveObservationStale, crawlstore.ArchiveObservationPartial:
			incomplete = true
		}
		unknown = unknown || repo.WorkflowFreshness == crawlstore.ArchiveObservationUnknown ||
			repo.WorkflowFreshness == crawlstore.ArchiveObservationUnsupported || repo.FailedHydrations == nil
		incomplete = incomplete || (repo.FailedHydrations != nil && *repo.FailedHydrations > 0)
		for _, child := range repo.Children {
			unknown = unknown || child.State == crawlstore.ArchiveObservationUnsupported || child.Unknown > 0
			incomplete = incomplete || child.Missing > 0 || child.Stale > 0
		}
		for _, metric := range []crawlstore.EnrichmentCoverageMetric{repo.PRDetails, repo.PRFiles} {
			unknown = unknown || !metric.Supported
			incomplete = incomplete || (metric.Supported && !metric.Complete)
		}
	}
	warnings := []string{}
	if unknown {
		warnings = append(warnings, "gitcrawl.archive.source.unknown")
	}
	if incomplete {
		warnings = append(warnings, "gitcrawl.archive.source.incomplete")
	}
	for name, metric := range map[string]crawlstore.EnrichmentCoverageMetric{
		"revisions": admission.Enrichment.Revisions, "fingerprints": admission.Enrichment.Fingerprints,
		"summaries": admission.Enrichment.Summaries, "clusters": admission.Enrichment.Clusters,
		"pr_details": admission.Enrichment.PRDetails, "pr_files": admission.Enrichment.PRFiles,
	} {
		if !metric.Supported || !metric.Complete {
			warnings = append(warnings, "gitcrawl.archive.enrichment."+name+".incomplete")
		}
	}
	return canonicalGitcrawlCloudWarnings(warnings)
}

// Use the oldest repository's actual last observation, or unknown if any is
// missing. Export clocks and another repository's newer run are not evidence.
func (admission gitcrawlCloudAdmission) sourceSyncAt() string {
	var oldest time.Time
	for _, repo := range admission.Source.Repositories {
		if !repo.Inventory.Successful {
			return ""
		}
		at, err := time.Parse(time.RFC3339Nano, repo.Inventory.FinishedAt)
		if err != nil {
			return ""
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	if oldest.IsZero() {
		return ""
	}
	return oldest.UTC().Format(time.RFC3339Nano)
}

func validateGitcrawlArchiveIntegrity(ctx context.Context, db *sql.DB, admission *gitcrawlCloudAdmission) error {
	var result string
	if err := db.QueryRowContext(ctx, `pragma integrity_check(1)`).Scan(&result); err != nil {
		return fmt.Errorf("check archive SQLite integrity: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("archive SQLite integrity check failed")
	}
	admission.Integrity.SQLite = true
	if _, err := gitcrawlCloudCapabilities(ctx, db, true); err != nil {
		return err
	}
	for table, columns := range map[string][]string{
		"comments":                             {"id", "thread_id", "body"},
		"comment_revisions":                    {"id", "comment_id", "body"},
		"pull_request_files":                   {"thread_id", "patch"},
		"pull_request_review_threads":          {"thread_id", "review_thread_id", "first_comment_body"},
		"pull_request_review_thread_revisions": {"thread_id", "review_thread_id", "first_comment_body"},
	} {
		if ok, err := sqliteTableHasColumns(ctx, db, table, columns...); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("archive requires canonical %s schema", table)
		}
	}
	datasets, err := loadGitcrawlCloudDatasets(ctx, db, true, admission.Enrichment)
	if err != nil {
		return err
	}
	admission.Integrity.CompatibleSchema = true
	admission.datasets = datasets
	if len(datasets) == 0 || datasets[0].RowCount == 0 {
		return fmt.Errorf("cloud snapshot has no repositories")
	}
	admission.Integrity.RequiredData = true
	if err := crawlstore.ValidateArchiveReferences(ctx, db); err != nil {
		return err
	}
	// Keep additional declared constraints and semantic PR-kind/repository
	// ownership rules alongside the canonical schema relationships.
	queries := []string{
		`select exists(select 1 from pragma_foreign_key_check)`,
		`select exists(select 1 from pull_request_review_threads r left join threads t on t.id = r.thread_id where t.id is null or t.kind != 'pull_request')`,
		`select exists(select 1 from pull_request_review_thread_revisions r
			left join threads t on t.id = r.thread_id
			left join pull_request_review_threads p on p.thread_id = r.thread_id and p.review_thread_id = r.review_thread_id
			where t.id is null or t.kind != 'pull_request' or p.thread_id is null)`,
		`select exists(select 1 from cluster_groups g left join repositories r on r.id = g.repo_id
			left join threads t on t.id = g.representative_thread_id
			where r.id is null or (g.representative_thread_id is not null and (t.id is null or t.repo_id != g.repo_id)))`,
		`select exists(select 1 from cluster_memberships m left join cluster_groups g on g.id = m.cluster_id
			left join threads t on t.id = m.thread_id where g.id is null or t.id is null or t.repo_id != g.repo_id)`,
		`select exists(select 1 from pull_request_details d left join threads t on t.id = d.thread_id
			where t.id is null or t.repo_id != d.repo_id or t.number != d.number or t.kind != 'pull_request')`,
		`select exists(select 1 from pull_request_files f left join threads t on t.id = f.thread_id
			where t.id is null or t.kind != 'pull_request')`,
	}
	for _, relation := range []struct{ table, query string }{
		{"pull_request_commits", `select exists(select 1 from pull_request_commits c left join threads t on t.id = c.thread_id where t.id is null or t.kind != 'pull_request')`},
		{"pull_request_checks", `select exists(select 1 from pull_request_checks c left join threads t on t.id = c.thread_id where t.id is null or t.kind != 'pull_request')`},
	} {
		exists, err := sqliteTableExists(ctx, db, relation.table)
		if err != nil {
			return err
		}
		if exists {
			queries = append(queries, relation.query)
		}
	}
	for _, query := range queries {
		var invalid bool
		if err := db.QueryRowContext(ctx, query).Scan(&invalid); err != nil {
			return fmt.Errorf("check archive referential closure: %w", err)
		}
		if invalid {
			return fmt.Errorf("archive referential closure check failed")
		}
	}
	admission.Integrity.ReferentialClosure = true
	for _, table := range []string{"threads", "comments"} {
		ok, err := sqliteTableHasColumns(ctx, db, table, "body")
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("archive requires canonical %s bodies", table)
		}
		if metadata, err := sqliteColumnExists(ctx, db, table, "body_length"); err != nil {
			return err
		} else if metadata {
			var truncated bool
			if err := db.QueryRowContext(ctx, `select exists(select 1 from `+table+
				` where body_length > length(coalesce(body, '')))`).Scan(&truncated); err != nil {
				return err
			}
			if truncated {
				return fmt.Errorf("archive contains truncated %s bodies; use the full runtime archive", table)
			}
		}
	}
	admission.Integrity.FullBodies = true
	return nil
}

func writeGitcrawlCloudAdmission(ctx context.Context, db *sql.DB, admission *gitcrawlCloudAdmission) error {
	if admission == nil {
		if exists, err := sqliteTableExists(ctx, db, "portable_metadata"); err != nil {
			return err
		} else if exists {
			_, err := db.ExecContext(ctx, `delete from portable_metadata where key = ?`, gitcrawlCloudAdmissionMetadataKey)
			return err
		}
		return nil
	}
	encoded, err := json.Marshal(admission)
	if err != nil {
		return err
	}
	// This is the existing portable artifact metadata table, only in the copy.
	if _, err := db.ExecContext(ctx, `create table if not exists portable_metadata (
		key text primary key, value text not null
	)`); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `insert into portable_metadata(key, value) values(?, ?)
		on conflict(key) do update set value = excluded.value`, gitcrawlCloudAdmissionMetadataKey, string(encoded))
	return err
}

func (options gitcrawlCloudPublishOptions) validate() error {
	switch options.AdmissionPolicy {
	case "":
		return nil
	case gitcrawlArchiveAdmissionPolicy:
		if options.AllowIncomplete {
			return fmt.Errorf("--admission-policy cannot be combined with --allow-incomplete")
		}
		if !options.ObservationOrder {
			return fmt.Errorf("--admission-policy=archive-v1 requires --observation-order")
		}
		return nil
	default:
		return fmt.Errorf("unsupported admission policy; use archive-v1 or omit --admission-policy")
	}
}

func gitcrawlCloudWarningAllowed(value string) bool {
	switch value {
	case "gitcrawl.archive.source.incomplete",
		"gitcrawl.archive.source.unknown",
		"gitcrawl.archive.enrichment.revisions.incomplete",
		"gitcrawl.archive.enrichment.fingerprints.incomplete",
		"gitcrawl.archive.enrichment.summaries.incomplete",
		"gitcrawl.archive.enrichment.clusters.incomplete",
		"gitcrawl.archive.enrichment.pr_details.incomplete",
		"gitcrawl.archive.enrichment.pr_files.incomplete":
		return true
	default:
		return false
	}
}

func canonicalGitcrawlCloudWarnings(values []string) ([]string, error) {
	if len(values) > gitcrawlCloudWarningsMaxElements {
		return nil, fmt.Errorf("cloud warnings exceed %d elements", gitcrawlCloudWarningsMaxElements)
	}
	canonical := append([]string{}, values...)
	encoded, err := json.Marshal(canonical)
	if err != nil || len(encoded) > gitcrawlCloudWarningsMaxBytes {
		return nil, fmt.Errorf("cloud warnings exceed %d JSON bytes", gitcrawlCloudWarningsMaxBytes)
	}
	seen := make(map[string]struct{}, len(canonical))
	for _, value := range canonical {
		if !gitcrawlCloudWarningAllowed(value) {
			return nil, fmt.Errorf("cloud warnings contain an unsupported code")
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf("cloud warnings contain a duplicate code")
		}
		seen[value] = struct{}{}
	}
	// Warning order is immutable snapshot identity, not display order.
	slices.Sort(canonical)
	return canonical, nil
}

func gitcrawlCloudWarningsMatch(left, right []string) bool {
	leftCanonical, leftErr := canonicalGitcrawlCloudWarnings(left)
	rightCanonical, rightErr := canonicalGitcrawlCloudWarnings(right)
	return leftErr == nil && rightErr == nil && slices.Equal(leftCanonical, rightCanonical)
}
