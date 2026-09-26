package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

var ErrThreadExcluded = errors.New("thread is excluded by owner policy")

func (s *Store) ensureThreadExclusionsSchema(ctx context.Context) error {
	_, err := s.q().ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS thread_exclusions(
 repository TEXT NOT NULL,number INTEGER NOT NULL CHECK(number>0),kind TEXT NOT NULL,
 original_thread_id INTEGER NOT NULL,github_id TEXT NOT NULL,excluded_at TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(reason='owner_requested'),request_id TEXT NOT NULL,
 PRIMARY KEY(repository,number));
CREATE TABLE IF NOT EXISTS thread_excluded_nodes(
 node_id TEXT PRIMARY KEY,repository TEXT NOT NULL,number INTEGER NOT NULL,
 FOREIGN KEY(repository,number) REFERENCES thread_exclusions(repository,number));
`)
	if err != nil {
		return err
	}

	if s.hasTable(ctx, "analytics_review_state_coverage") && !s.hasColumn(ctx, "analytics_review_state_coverage", "owner_excluded_items") {
		_, err = s.q().ExecContext(ctx, "ALTER TABLE analytics_review_state_coverage ADD COLUMN owner_excluded_items INTEGER NOT NULL DEFAULT 0")
	}
	return err
}

func (s *Store) ensureThreadExclusionGuards(ctx context.Context) error {
	_, err := s.q().ExecContext(ctx, `
CREATE TRIGGER IF NOT EXISTS owner_excluded_thread_insert BEFORE INSERT ON threads
 WHEN EXISTS(SELECT 1 FROM thread_exclusions e JOIN repositories r ON lower(r.full_name)=e.repository WHERE r.id=NEW.repo_id AND e.number=NEW.number)
 BEGIN SELECT RAISE(ABORT,'owner_excluded_thread'); END;
CREATE TRIGGER IF NOT EXISTS owner_excluded_thread_update BEFORE UPDATE ON threads
 WHEN EXISTS(SELECT 1 FROM thread_exclusions e JOIN repositories r ON lower(r.full_name)=e.repository WHERE r.id=NEW.repo_id AND e.number=NEW.number)
 BEGIN SELECT RAISE(ABORT,'owner_excluded_thread'); END;
`)
	if err != nil {
		return err
	}
	for _, guard := range []struct{ table, name, condition, message string }{
		{"analytics_retries", "retry", "EXISTS(SELECT 1 FROM thread_exclusions WHERE repository=lower(NEW.repository) AND number=NEW.number)", "owner_excluded_thread"},
		{"actor_identity_evidence", "identity", "EXISTS(SELECT 1 FROM thread_excluded_nodes WHERE node_id=NEW.node_id)", "owner_excluded_node"},
		{"analytics_pending_nodes", "pending_node", "EXISTS(SELECT 1 FROM thread_excluded_nodes WHERE node_id=NEW.node_id)", "owner_excluded_node"},
	} {
		if !s.hasTable(ctx, guard.table) {
			continue
		}
		if _, err = s.q().ExecContext(ctx, "CREATE TRIGGER IF NOT EXISTS owner_excluded_"+guard.name+" BEFORE INSERT ON "+guard.table+" WHEN "+guard.condition+" BEGIN SELECT RAISE(ABORT,'"+guard.message+"'); END"); err != nil {
			return err
		}
	}
	return nil
}

// OpenThreadPurgeMirror opens an existing managed portable mirror without a full
// archive migration, body reconstruction, or publication. The CLI holds the
// portable owner lease and records writable-mirror ownership first.
func OpenThreadPurgeMirror(ctx context.Context, path string) (*Store, error) {
	probe, err := OpenReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	portable := probe.hasTable(ctx, "portable_metadata")
	// Older sparse formats can contain dangling blob FKs. Do not disable FK
	// validation or rebuild them as part of a targeted owner purge.
	portable = portable && (!probe.hasColumn(ctx, "comments", "raw_json_blob_id") || probe.hasTable(ctx, "blobs"))
	version, versionErr := probe.schemaVersion(ctx)
	probe.Close()
	if !portable || versionErr != nil || version != portableSchemaVersion {
		return nil, fmt.Errorf("owner purge requires a current portable mirror")
	}
	// Preserve the mirror's journal mode. A failed transaction must not turn a
	// pristine replica into local data merely by switching its SQLite header.
	encoded, err := immutableSQLiteURI(path)
	if err != nil {
		return nil, err
	}
	uri, err := url.Parse(encoded)
	if err != nil {
		return nil, err
	}
	query := uri.Query()
	query.Del("immutable")
	query.Set("mode", "rw")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "busy_timeout(5000)")
	uri.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, sqlc: storedb.New(db), path: path}, nil
}

func (s *Store) ThreadExcluded(ctx context.Context, repository string, number int) (bool, error) {
	var excluded bool
	err := s.q().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM thread_exclusions WHERE repository=lower(?) AND number=?)", repository, number).Scan(&excluded)
	return excluded, err
}
func (s *Store) NodeExcluded(ctx context.Context, id string) (bool, error) {
	var excluded bool
	err := s.q().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM thread_excluded_nodes WHERE node_id=?)", id).Scan(&excluded)
	return excluded, err
}
func (s *Store) FilterExcludedNumbers(ctx context.Context, repository string, numbers []int) ([]int, error) {
	kept := make([]int, 0, len(numbers))
	for _, n := range numbers {
		excluded, err := s.ThreadExcluded(ctx, repository, n)
		if err != nil {
			return nil, err
		}
		if !excluded {
			kept = append(kept, n)
		}
	}
	return kept, nil
}
func (s *Store) OwnerExcludedCount(ctx context.Context, repository string, reviewOnly bool) (int, error) {
	var n int
	err := s.q().QueryRowContext(ctx, "SELECT count(*) FROM thread_exclusions WHERE repository=lower(?) AND (?=0 OR kind='pull_request')", repository, boolInt(reviewOnly)).Scan(&n)
	return n, err
}

type PurgeTarget struct {
	Number          int      `json:"number"`
	ThreadID        int64    `json:"thread_id"`
	Kind            string   `json:"kind"`
	GitHubID        string   `json:"github_id"`
	Nodes           []string `json:"content_node_ids"`
	AlreadyExcluded bool     `json:"already_excluded"`
}
type ThreadPurgePlan struct {
	Repository string         `json:"repository"`
	RepoID     int64          `json:"repo_id"`
	Targets    []PurgeTarget  `json:"targets"`
	Counts     map[string]int `json:"counts"`
	BlobIDs    []int64        `json:"candidate_blob_ids"`
	RequestID  string         `json:"request_id,omitempty"`
	Applied    bool           `json:"applied"`
	Reason     string         `json:"reason"`
}

// Only source-owned relationships are traversed. Shared cluster descriptions
// need their own owner repair and are refused instead of discarding peer data.
func purgeRelations(ids string) map[string]string {
	rev := "select id from thread_revisions where thread_id in (" + ids + ")"
	snapshots := "select id from thread_code_snapshots where thread_revision_id in (" + rev + ")"
	out := map[string]string{}
	for _, table := range []string{"threads", "comments", "documents", "document_embeddings", "document_summaries", "pull_request_details", "pull_request_files", "pull_request_commits", "pull_request_checks", "pull_request_review_threads", "pull_request_review_thread_revisions", "pull_request_review_thread_syncs", "thread_revisions", "thread_vectors", "thread_child_observation_memberships", "thread_child_observation_reservations", "cluster_members", "cluster_memberships", "cluster_overrides"} {
		col := "thread_id"
		if table == "threads" {
			col = "id"
		}
		out[table] = col + " in (" + ids + ")"
	}
	out["comment_revisions"] = "comment_id in (select id from comments where thread_id in (" + ids + "))"
	for _, table := range []string{"thread_code_snapshots", "thread_fingerprints", "thread_key_summaries"} {
		out[table] = "thread_revision_id in (" + rev + ")"
	}
	for _, table := range []string{"thread_changed_files", "thread_hunk_signatures"} {
		out[table] = "snapshot_id in (" + snapshots + ")"
	}
	out["similarity_edges"] = "left_thread_id in (" + ids + ") or right_thread_id in (" + ids + ")"
	return out
}

var purgeBlobColumns = map[string][]string{"comments": {"raw_json_blob_id"}, "thread_revisions": {"raw_json_blob_id"}, "thread_changed_files": {"patch_blob_id"}, "thread_code_snapshots": {"raw_diff_blob_id"}}

func (s *Store) PlanThreadPurge(ctx context.Context, repository string, numbers []int) (ThreadPurgePlan, error) {
	p := ThreadPurgePlan{Repository: strings.ToLower(strings.TrimSpace(repository)), Reason: "owner_requested", Counts: map[string]int{}, Targets: []PurgeTarget{}, BlobIDs: []int64{}}
	if len(numbers) == 0 || len(numbers) > 100 {
		return p, fmt.Errorf("purge requires 1..100 explicit numbers")
	}
	var canonical string
	var matches int
	if err := s.q().QueryRowContext(ctx, "SELECT count(*),coalesce(min(full_name),'') FROM repositories WHERE lower(full_name)=?", p.Repository).Scan(&matches, &canonical); err != nil {
		return p, err
	}
	if matches != 1 {
		return p, fmt.Errorf("purge repository must match exactly one local identity")
	}
	repo, err := s.RepositoryByFullName(ctx, canonical)
	if err != nil {
		return p, err
	}
	p.RepoID = repo.ID
	seen := map[int]bool{}
	var ids []string
	for _, n := range numbers {
		if n <= 0 || seen[n] {
			return p, fmt.Errorf("purge numbers must be positive and unique")
		}
		seen[n] = true
		t := PurgeTarget{Number: n, Nodes: []string{}}
		var matches int
		if err := s.q().QueryRowContext(ctx, "SELECT count(*) FROM threads WHERE repo_id=? AND number=?", repo.ID, n).Scan(&matches); err != nil {
			return p, err
		}
		if matches > 1 {
			return p, fmt.Errorf("ambiguous native thread number #%d", n)
		}
		err := s.q().QueryRowContext(ctx, "SELECT id,kind,github_id FROM threads WHERE repo_id=? AND number=?", repo.ID, n).Scan(&t.ThreadID, &t.Kind, &t.GitHubID)
		if err == sql.ErrNoRows && s.hasTable(ctx, "thread_exclusions") {
			err = s.q().QueryRowContext(ctx, "SELECT original_thread_id,kind,github_id FROM thread_exclusions WHERE repository=? AND number=?", p.Repository, n).Scan(&t.ThreadID, &t.Kind, &t.GitHubID)
			t.AlreadyExcluded = err == nil
		}
		if err != nil {
			return p, fmt.Errorf("purge target #%d: %w", n, err)
		}
		if t.AlreadyExcluded {
			p.Targets = append(p.Targets, t)
			continue
		}
		ids = append(ids, fmt.Sprint(t.ThreadID))
		// historyProjection normalizes GraphQL id to node_id; top-level id
		// is the REST/fullDatabaseId and must not become a node exclusion.
		// Portable schemas may intentionally omit raw payload columns. Retain
		// only available content-node keys; the repository/number guard is primary.
		nodeQueries := []string{}
		for _, rel := range []struct{ table, predicate string }{
			{"threads", fmt.Sprintf("id=%d", t.ThreadID)},
			{"comments", fmt.Sprintf("thread_id=%d", t.ThreadID)},
			{"comment_revisions", fmt.Sprintf("comment_id IN(SELECT id FROM comments WHERE thread_id=%d)", t.ThreadID)},
			{"thread_revisions", fmt.Sprintf("thread_id=%d", t.ThreadID)},
		} {
			if s.hasColumn(ctx, rel.table, "raw_json") {
				nodeQueries = append(nodeQueries, "SELECT json_extract(CASE WHEN json_valid(raw_json) THEN raw_json ELSE '{}' END,'$.node_id') node_id FROM "+rel.table+" WHERE "+rel.predicate)
			}
		}
		if len(nodeQueries) > 0 {
			rows, e := s.q().QueryContext(ctx, "SELECT DISTINCT node_id FROM ("+strings.Join(nodeQueries, " UNION ALL ")+") WHERE node_id IS NOT NULL AND node_id<>'' ORDER BY node_id")
			if e != nil {
				return p, e
			}
			for rows.Next() {
				var node string
				if e = rows.Scan(&node); e != nil {
					rows.Close()
					return p, e
				}
				t.Nodes = append(t.Nodes, node)
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return p, e
			}
		}
		p.Targets = append(p.Targets, t)
	}
	sort.Slice(p.Targets, func(i, j int) bool { return p.Targets[i].Number < p.Targets[j].Number })
	if len(ids) == 0 {
		return p, nil
	}
	idSQL := strings.Join(ids, ",")
	relations := purgeRelations(idSQL)
	for table, predicate := range relations {
		if !s.hasTable(ctx, table) {
			continue
		}
		var n int
		if err = s.q().QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE "+predicate).Scan(&n); err != nil {
			return p, err
		}
		p.Counts[table] = n
	}
	for _, table := range []string{"clusters", "cluster_groups"} {
		if !s.hasTable(ctx, table) {
			continue
		}
		var n int
		if err = s.q().QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE representative_thread_id in ("+idSQL+")").Scan(&n); err != nil {
			return p, err
		}
		if n > 0 {
			return p, fmt.Errorf("purge requires a separate cluster representative repair")
		}
	}
	for _, table := range []string{"cluster_members", "cluster_memberships", "cluster_overrides"} {
		if p.Counts[table] > 0 {
			return p, fmt.Errorf("purge requires a separate shared cluster repair")
		}
	}
	// This deliberately bounded command does not resolve ownership inside
	// retained workflow payloads. Refuse the repository surface rather than
	// guessing from today's PR head or deleting a shared run.
	if s.hasTable(ctx, "github_workflow_runs") {
		var exists bool
		if err = s.q().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM github_workflow_runs WHERE repo_id=?)", p.RepoID).Scan(&exists); err != nil {
			return p, err
		}
		if exists {
			return p, fmt.Errorf("purge requires a separate repository workflow-snapshot repair")
		}
	}
	heads := []string{}
	if s.hasTable(ctx, "pull_request_details") {
		heads = append(heads, "SELECT head_sha FROM pull_request_details WHERE thread_id IN("+idSQL+")")
	}
	if s.hasTable(ctx, "thread_code_snapshots") {
		heads = append(heads, "SELECT head_sha FROM thread_code_snapshots WHERE "+relations["thread_code_snapshots"])
	}
	for _, table := range []string{"threads", "thread_revisions"} {
		if !s.hasColumn(ctx, table, "raw_json") {
			continue
		}
		for _, path := range []string{"$._graphql.headRefOid", "$.head.sha"} {
			heads = append(heads, "SELECT json_extract(CASE WHEN json_valid(raw_json) THEN raw_json ELSE '{}' END,'"+path+"') FROM "+table+" WHERE "+relations[table])
		}
	}
	if s.hasTable(ctx, "workflow_run_observation_reservations") && len(heads) > 0 {
		var n int
		if err = s.q().QueryRowContext(ctx, "SELECT count(*) FROM workflow_run_observation_reservations WHERE repo_id=? AND head_sha IN("+strings.Join(heads, " UNION ")+")", p.RepoID).Scan(&n); err != nil {
			return p, err
		}
		if n > 0 {
			return p, fmt.Errorf("purge requires a separate linked workflow-reservation repair")
		}
		p.Counts["workflow_run_observation_reservations"] = 0
	}
	// Blob-backed payloads need an identity/ownership-aware repair of their
	// own. Do not silently leave their nodes behind or read unknown blob prose.
	for table, cols := range purgeBlobColumns {
		for _, col := range cols {
			if !s.hasColumn(ctx, table, col) {
				continue
			}
			var exists bool
			if err = s.q().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+table+" WHERE ("+relations[table]+") AND "+col+" IS NOT NULL)").Scan(&exists); err != nil {
				return p, err
			}
			if exists {
				return p, fmt.Errorf("purge requires a separate blob-backed payload repair")
			}
		}
	}

	for _, t := range p.Targets {
		if t.AlreadyExcluded {
			continue
		}
		for _, table := range []string{"analytics_retries", "analytics_fetch_attempts", "sync_attempt_failures"} {
			if !s.hasTable(ctx, table) {
				continue
			}
			clause, args := "lower(repository)=? AND number=?", []any{p.Repository, t.Number}
			if table == "sync_attempt_failures" {
				clause, args = "repo_id=? AND number=?", []any{p.RepoID, t.Number}
			}
			var n int
			if err = s.q().QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE "+clause, args...).Scan(&n); err != nil {
				return p, err
			}
			p.Counts[table] += n
		}
		for _, node := range t.Nodes {
			for _, table := range []string{"actor_identity_evidence", "analytics_pending_nodes"} {
				if !s.hasTable(ctx, table) {
					continue
				}
				var n int
				if err = s.q().QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE node_id=?", node).Scan(&n); err != nil {
					return p, err
				}
				p.Counts[table] += n
			}
		}
	}
	// A durable downstream exclusion is keyed by native row ID. Refuse a tail
	// deletion that SQLite could later reuse; never create dummy rows or IDs.
	guardRelations := purgeRelations(idSQL)
	var numeric []string
	for _, t := range p.Targets {
		numeric = append(numeric, fmt.Sprint(t.Number))
	}
	guardRelations["analytics_fetch_attempts"] = fmt.Sprintf("lower(repository)='%s' AND number IN(%s)", strings.ReplaceAll(p.Repository, "'", "''"), strings.Join(numeric, ","))
	guardRelations["sync_attempt_failures"] = fmt.Sprintf("repo_id=%d AND number IN(%s)", p.RepoID, strings.Join(numeric, ","))
	for table, predicate := range guardRelations {
		var singleID bool
		if err := s.q().QueryRowContext(ctx, "SELECT count(*)=1 AND min(name)='id' AND min(upper(type))='INTEGER' FROM pragma_table_info(?) WHERE pk>0", table).Scan(&singleID); err != nil {
			return p, err
		}
		if singleID {
			if err := s.guardPurgeIDTail(ctx, table, predicate); err != nil {
				return p, err
			}
		}
	}
	return p, nil
}

func (s *Store) guardPurgeIDTail(ctx context.Context, table, predicate string) error {
	var selected, maximum sql.NullInt64
	if err := s.q().QueryRowContext(ctx, "SELECT max(id) FROM "+table+" WHERE "+predicate).Scan(&selected); err != nil {
		return err
	}
	if !selected.Valid {
		return nil
	}
	if err := s.q().QueryRowContext(ctx, "SELECT max(id) FROM "+table).Scan(&maximum); err != nil {
		return err
	}
	if selected.Int64 == maximum.Int64 {
		return fmt.Errorf("purge would permit native ID reuse in %s; a retained higher ID is required", table)
	}
	return nil
}

// PurgeThreads is owner-directed removal, not a provider deletion or successful
// fetch. Caller holds runner.lock; a single transaction fences all native writes.
func (s *Store) PurgeThreads(ctx context.Context, repository string, numbers []int, requestID string) (ThreadPurgePlan, error) {
	var result ThreadPurgePlan
	if strings.TrimSpace(requestID) == "" || len(requestID) > 128 {
		return result, fmt.Errorf("purge requires a bounded owner request id")
	}
	err := s.WithTx(ctx, func(tx *Store) error {
		if err := tx.ensureThreadExclusionsSchema(ctx); err != nil {
			return err
		}
		if err := tx.ensureThreadExclusionGuards(ctx); err != nil {
			return err
		}
		p, err := tx.PlanThreadPurge(ctx, repository, numbers)
		if err != nil {
			return err
		}
		var enabled int
		if err = tx.q().QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&enabled); err != nil {
			return err
		}
		if enabled != 1 {
			return fmt.Errorf("purge requires foreign keys")
		}
		at := time.Now().UTC().Format(time.RFC3339Nano)
		for _, t := range p.Targets {
			if t.AlreadyExcluded {
				continue
			}
			if _, err = tx.q().ExecContext(ctx, `INSERT INTO thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,request_id) VALUES(?,?,?,?,?,?,'owner_requested',?)`, p.Repository, t.Number, t.Kind, t.ThreadID, t.GitHubID, at, requestID); err != nil {
				return err
			}
			for _, node := range t.Nodes {
				if _, err = tx.q().ExecContext(ctx, "INSERT INTO thread_excluded_nodes(node_id,repository,number) VALUES(?,?,?)", node, p.Repository, t.Number); err != nil {
					return err
				}
				for _, table := range []string{"actor_identity_evidence", "analytics_pending_nodes"} {
					if !tx.hasTable(ctx, table) {
						continue
					}
					if _, err = tx.q().ExecContext(ctx, "DELETE FROM "+table+" WHERE node_id=?", node); err != nil {
						return err
					}
				}
			}
			for _, table := range []string{"analytics_retries", "analytics_fetch_attempts"} {
				if !tx.hasTable(ctx, table) {
					continue
				}
				if _, err = tx.q().ExecContext(ctx, "DELETE FROM "+table+" WHERE lower(repository)=? AND number=?", p.Repository, t.Number); err != nil {
					return err
				}
			}
			if tx.hasTable(ctx, "sync_attempt_failures") {
				if _, err = tx.q().ExecContext(ctx, "DELETE FROM sync_attempt_failures WHERE repo_id=? AND number=?", p.RepoID, t.Number); err != nil {
					return err
				}
			}
			if _, err = tx.q().ExecContext(ctx, "DELETE FROM threads WHERE id=?", t.ThreadID); err != nil {
				return err
			}
		}
		// Never turn removal of unavailable work into claimed provider completeness.
		if tx.hasTable(ctx, "analytics_review_state_coverage") {
			if _, err = tx.q().ExecContext(ctx, `UPDATE analytics_review_state_coverage SET pending_items=(SELECT count(*) FROM analytics_retries WHERE lower(repository)=? AND operation='review_state' AND resolved_at IS NULL),owner_excluded_items=(SELECT count(*) FROM thread_exclusions WHERE repository=? AND kind='pull_request'),complete=0 WHERE lower(repository)=?`, p.Repository, p.Repository, p.Repository); err != nil {
				return err
			}
		}
		p.RequestID = requestID
		result = p
		return nil
	})
	if err == nil {
		result.Applied = true
	}
	return result, err
}
