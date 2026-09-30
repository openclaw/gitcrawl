package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Diagnostic receipts and retry state are local operational evidence, not
// public conversation payloads. Resolved rows are retained, never reset/deleted.
func (s *Store) ensureAnalyticsIntegritySchema(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "pull_request_review_thread_syncs", "review_thread_ids_json", "text"); err != nil {
		return err
	}
	_, err := s.q().ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS analytics_fetch_attempts(
 id INTEGER PRIMARY KEY,repository TEXT NOT NULL,number INTEGER NOT NULL,operation TEXT NOT NULL,
 started_at TEXT NOT NULL,finished_at TEXT NOT NULL,status TEXT NOT NULL,
 error_class TEXT,error_text TEXT,evidence_json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS analytics_attempt_item ON analytics_fetch_attempts(repository,number,id);
CREATE INDEX IF NOT EXISTS analytics_attempt_repository ON analytics_fetch_attempts(repository,id);
CREATE TABLE IF NOT EXISTS analytics_retries(
 repository TEXT NOT NULL,number INTEGER NOT NULL,operation TEXT NOT NULL,
 first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL,next_attempt_at TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,last_attempt_id INTEGER,resolved_at TEXT,
 PRIMARY KEY(repository,number,operation));
CREATE INDEX IF NOT EXISTS analytics_retry_due ON analytics_retries(repository,resolved_at,next_attempt_at,number);
CREATE TABLE IF NOT EXISTS analytics_review_state_coverage(
 repository TEXT PRIMARY KEY,cursor INTEGER NOT NULL,ceiling INTEGER NOT NULL,
 scanned INTEGER NOT NULL,queued INTEGER NOT NULL,pending_items INTEGER NOT NULL,
 scan_complete INTEGER NOT NULL,complete INTEGER NOT NULL,observed_at TEXT NOT NULL);
`)
	return err
}

type AnalyticsAttempt struct {
	Repository string          `json:"repository"`
	Number     int             `json:"number"`
	Operation  string          `json:"operation"`
	StartedAt  string          `json:"started_at"`
	FinishedAt string          `json:"finished_at"`
	Status     string          `json:"status"`
	ErrorClass string          `json:"error_class,omitempty"`
	ErrorText  string          `json:"error_text,omitempty"`
	Evidence   json.RawMessage `json:"evidence"`
}

func (s *Store) RecordAnalyticsAttempt(ctx context.Context, a AnalyticsAttempt) error {
	if a.Repository == "" || a.Operation == "" || (a.Status != "success" && a.Status != "failed") || !json.Valid(a.Evidence) {
		return fmt.Errorf("invalid analytics attempt")
	}
	finished, err := time.Parse(time.RFC3339Nano, a.FinishedAt)
	if err != nil {
		return err
	}
	return s.WithTx(ctx, func(tx *Store) error {
		status, class, message := a.Status, a.ErrorClass, a.ErrorText
		knownReview := true
		if status == "success" && (a.Operation == "graphql_history" || a.Operation == "review_state") {
			if err := tx.q().QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM threads t JOIN repositories r ON r.id=t.repo_id WHERE r.full_name=? COLLATE NOCASE AND t.number=? AND (t.kind<>'pull_request' OR EXISTS(SELECT 1 FROM pull_request_review_thread_syncs x WHERE x.thread_id=t.id AND x.review_thread_ids_json IS NOT NULL)))`, a.Repository, a.Number).Scan(&knownReview); err != nil {
				return err
			}
			if a.Operation == "review_state" && !knownReview {
				status = "failed"
				class = "unapplied_review_state"
				message = "Fetch did not establish a review-state membership observation"
			}
		}
		r, err := tx.q().ExecContext(ctx, `INSERT INTO analytics_fetch_attempts(repository,number,operation,started_at,finished_at,status,error_class,error_text,evidence_json) VALUES(?,?,?,?,?,?,?,?,?)`, a.Repository, a.Number, a.Operation, a.StartedAt, a.FinishedAt, status, nullString(class), nullString(message), string(a.Evidence))
		if err != nil {
			return err
		}
		id, err := r.LastInsertId()
		if err != nil {
			return err
		}
		if status == "success" {
			_, err = tx.q().ExecContext(ctx, `UPDATE analytics_retries SET resolved_at=?,last_attempt_id=? WHERE repository=? AND number=? AND resolved_at IS NULL AND (operation=? OR (?='graphql_history' AND operation IN ('graphql_history','review_state'))) AND (operation<>'review_state' OR ?)`, a.FinishedAt, id, a.Repository, a.Number, a.Operation, a.Operation, knownReview)
			return err
		}
		if a.Operation != "review_state" {
			if _, err = tx.q().ExecContext(ctx, "UPDATE analytics_coverage SET complete=0 WHERE repository=?", a.Repository); err != nil {
				return err
			}
		}
		var attempts int
		if err = tx.q().QueryRowContext(ctx, `SELECT coalesce((SELECT attempts FROM analytics_retries WHERE repository=? AND number=? AND operation=?),0)`, a.Repository, a.Number, a.Operation).Scan(&attempts); err != nil {
			return err
		}
		delay := time.Duration(1<<min(attempts, 5)) * 30 * time.Second
		if a.Operation == "review_state" && providerUnavailableEvidence(a.Evidence) {
			delay = max(delay, 15*time.Minute)
		}
		next := finished.Add(delay).Format(time.RFC3339Nano)
		_, err = tx.q().ExecContext(ctx, `INSERT INTO analytics_retries(repository,number,operation,first_seen_at,last_seen_at,next_attempt_at,attempts,last_attempt_id) VALUES(?,?,?,?,?,?,1,?) ON CONFLICT(repository,number,operation) DO UPDATE SET last_seen_at=excluded.last_seen_at,next_attempt_at=excluded.next_attempt_at,attempts=analytics_retries.attempts+1,last_attempt_id=excluded.last_attempt_id,resolved_at=NULL`, a.Repository, a.Number, a.Operation, a.FinishedAt, a.FinishedAt, next, id)
		return err
	})
}

func providerUnavailableEvidence(evidence json.RawMessage) bool {
	var value struct {
		Errors struct {
			Items []struct {
				Type string `json:"type"`
			} `json:"items"`
		} `json:"graphql_errors"`
	}
	if json.Unmarshal(evidence, &value) != nil {
		return false
	}
	for _, item := range value.Errors.Items {
		if item.Type == "NOT_FOUND" {
			return true
		}
	}
	return false
}

// A bounded retry share prevents failed items waiting behind the entire seeded
// census, without allowing unavailable items to starve first-pass recovery.
func (s *Store) DueReviewStateWork(ctx context.Context, repository, at string, limit int) ([]int, error) {
	if limit <= 0 {
		return nil, nil
	}
	const eligible = `repository=? AND operation='review_state' AND resolved_at IS NULL AND number>0 AND next_attempt_at<=? AND NOT EXISTS(SELECT 1 FROM analytics_retries core WHERE core.repository=analytics_retries.repository AND core.number=analytics_retries.number AND core.operation='graphql_history' AND core.resolved_at IS NULL)`
	read := func(attempted bool, n int) ([]int, error) {
		predicate := "attempts=0"
		if attempted {
			predicate = "attempts>0"
		}
		rows, err := s.q().QueryContext(ctx, "SELECT number FROM analytics_retries WHERE "+eligible+" AND "+predicate+" ORDER BY next_attempt_at,number LIMIT ?", repository, at, n)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []int
		for rows.Next() {
			var number int
			if err = rows.Scan(&number); err != nil {
				return nil, err
			}
			out = append(out, number)
		}
		return out, rows.Err()
	}
	retries, err := read(true, max(1, limit/4))
	if err != nil {
		return nil, err
	}
	fresh, err := read(false, limit-len(retries))
	if err != nil {
		return nil, err
	}
	if len(fresh)+len(retries) < limit {
		retries, err = read(true, limit-len(fresh))
		if err != nil {
			return nil, err
		}
	}
	return append(retries, fresh...), nil
}

func (s *Store) ReviewStateParent(ctx context.Context, repository string, number int, providerRepositoryID, providerNodeID string) (Thread, error) {
	var t Thread
	var repoID, rawRepo string
	err := s.q().QueryRowContext(ctx, `SELECT t.id,t.repo_id,t.github_id,t.kind,t.raw_json,r.github_repo_id,r.raw_json FROM threads t JOIN repositories r ON r.id=t.repo_id WHERE r.full_name=? COLLATE NOCASE AND t.number=?`, repository, number).Scan(&t.ID, &t.RepoID, &t.GitHubID, &t.Kind, &t.RawJSON, &repoID, &rawRepo)
	if err != nil {
		return t, err
	}
	if t.Kind != "pull_request" || repoID != providerRepositoryID {
		return t, fmt.Errorf("review-state archived repository/PR identity mismatch")
	}
	var repo map[string]any
	if err = json.Unmarshal([]byte(rawRepo), &repo); err != nil {
		return t, err
	}
	if node, ok := repo["node_id"].(string); ok && node != "" && node != providerNodeID {
		return t, fmt.Errorf("review-state repository node mismatch")
	}
	return t, nil
}

func (s *Store) DueAnalyticsRetries(ctx context.Context, repository, at string, limit int, operations ...string) ([]int, error) {
	operation := ""
	if len(operations) > 0 {
		operation = operations[0]
	}
	rows, err := s.q().QueryContext(ctx, `SELECT r.number FROM analytics_retries r WHERE r.repository=? AND r.resolved_at IS NULL AND r.number>0 AND r.next_attempt_at<=? AND (?='' OR r.operation=?)
 AND (r.operation<>'review_state' OR NOT EXISTS(SELECT 1 FROM analytics_retries core WHERE core.repository=r.repository AND core.number=r.number AND core.operation='graphql_history' AND core.resolved_at IS NULL))
 GROUP BY r.number ORDER BY min(r.next_attempt_at),r.number LIMIT ?`, repository, at, operation, operation, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var n int
		if err = rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) AnalyticsOutstanding(ctx context.Context, repository string) (int, error) {
	var n int
	err := s.q().QueryRowContext(ctx, `SELECT count(*) FROM analytics_retries WHERE repository=? AND resolved_at IS NULL`, repository).Scan(&n)
	return n, err
}

func (s *Store) AnalyticsCoreOutstanding(ctx context.Context, repository string) (int, error) {
	var n int
	err := s.q().QueryRowContext(ctx, `SELECT count(*) FROM analytics_retries WHERE repository=? AND resolved_at IS NULL AND operation<>'review_state'`, repository).Scan(&n)
	return n, err
}

func (s *Store) AnalyticsItemQueued(ctx context.Context, repository string, number int, operations ...string) (bool, error) {
	operation := "graphql_history"
	if len(operations) > 0 {
		operation = operations[0]
	}
	var n int
	err := s.q().QueryRowContext(ctx, `SELECT count(*) FROM analytics_retries WHERE repository=? AND number=? AND operation=? AND resolved_at IS NULL`, repository, number, operation).Scan(&n)
	return n > 0, err
}

type ReviewStateRecovery struct {
	Cursor  int64 `json:"cursor"`
	Ceiling int64 `json:"ceiling"`
	Scanned int64 `json:"scanned"`
	Queued  int64 `json:"queued"`
	Done    bool  `json:"done"`
}

// SeedReviewStateRecovery walks at most limit primary-key rows per watch cycle.
// It queues only PRs with retained review threads (or unknown connection data),
// and never restarts historical discovery or invents missing resolution state.
func (s *Store) SeedReviewStateRecovery(ctx context.Context, repository string, limit int) (ReviewStateRecovery, error) {
	key := "review_state_recovery:" + repository
	value, err := s.AnalyticsState(ctx, key)
	if err != nil {
		return ReviewStateRecovery{}, err
	}
	var progress ReviewStateRecovery
	if value != "" {
		if err = json.Unmarshal([]byte(value), &progress); err != nil {
			return progress, err
		}
	} else {
		if err = s.q().QueryRowContext(ctx, "SELECT coalesce(max(id),0) FROM threads").Scan(&progress.Ceiling); err != nil {
			return progress, err
		}
	}
	if progress.Done {
		return progress, nil
	}
	rows, err := s.q().QueryContext(ctx, `SELECT t.id,t.number,t.kind,r.full_name,
 CASE WHEN json_valid(t.raw_json) THEN coalesce(
 json_extract(t.raw_json,'$._gitcrawl_source')='graphql' AND
 json_type(t.raw_json,'$._graphql.reviewThreads.totalCount')='integer' AND
 json_extract(t.raw_json,'$._graphql.reviewThreads.totalCount')=0 AND
 json_type(t.raw_json,'$._graphql.reviewThreads.nodes')='array' AND
 json_array_length(t.raw_json,'$._graphql.reviewThreads.nodes')=0 AND
 json_type(t.raw_json,'$._graphql.reviewThreads.pageInfo.hasNextPage')='false',0) ELSE 0 END,
 t.observation_sequence,coalesce(t.last_pulled_at,''),
 EXISTS(SELECT 1 FROM pull_request_review_thread_syncs x WHERE x.thread_id=t.id AND x.review_thread_ids_json IS NOT NULL)
 FROM threads t JOIN repositories r ON r.id=t.repo_id WHERE t.id>? AND t.id<=? ORDER BY t.id LIMIT ?`, progress.Cursor, progress.Ceiling, limit)
	if err != nil {
		return progress, err
	}
	type item struct {
		id         int64
		number     int
		kind, repo string
		empty      bool
		sequence   int64
		pulled     string
		known      bool
	}
	var items []item
	for rows.Next() {
		var v item
		if err = rows.Scan(&v.id, &v.number, &v.kind, &v.repo, &v.empty, &v.sequence, &v.pulled, &v.known); err != nil {
			rows.Close()
			return progress, err
		}
		items = append(items, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return progress, err
	}
	var committed ReviewStateRecovery
	err = s.WithTx(ctx, func(tx *Store) error {
		next := progress // A busy-transaction retry must not double-count progress.
		at := time.Now().UTC().Format(time.RFC3339Nano)
		for _, v := range items {
			next.Cursor = v.id
			next.Scanned++
			if !strings.EqualFold(v.repo, repository) || v.kind != "pull_request" || v.known {
				continue
			}
			if _, e := time.Parse(time.RFC3339Nano, v.pulled); v.empty && e == nil {
				// Materialize only an actual retained complete-empty observation.
				// CAS checks prevent an older scan from overwriting a live refresh.
				_, e = tx.q().ExecContext(ctx, `INSERT INTO pull_request_review_thread_syncs(thread_id,fetched_at,review_thread_ids_json)
 SELECT id,last_pulled_at,'[]' FROM threads WHERE id=? AND observation_sequence=? AND last_pulled_at=?
 ON CONFLICT(thread_id) DO UPDATE SET fetched_at=excluded.fetched_at,review_thread_ids_json=excluded.review_thread_ids_json
 WHERE pull_request_review_thread_syncs.review_thread_ids_json IS NULL AND pull_request_review_thread_syncs.fetched_at<=excluded.fetched_at`, v.id, v.sequence, v.pulled)
				if e != nil {
					return e
				}
				var known bool
				if e = tx.q().QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pull_request_review_thread_syncs WHERE thread_id=? AND review_thread_ids_json IS NOT NULL)`, v.id).Scan(&known); e != nil {
					return e
				}
				if known {
					continue
				}
			}
			r, e := tx.q().ExecContext(ctx, `INSERT OR IGNORE INTO analytics_retries(repository,number,operation,first_seen_at,last_seen_at,next_attempt_at) VALUES(?,?,'review_state',?,?,?)`, repository, v.number, at, at, at)
			if e != nil {
				return e
			}
			n, e := r.RowsAffected()
			if e != nil {
				return e
			}
			next.Queued += n
		}
		next.Done = len(items) < limit || next.Cursor >= next.Ceiling
		encoded, _ := json.Marshal(next)
		if e := tx.SetAnalyticsState(ctx, key, string(encoded)); e != nil {
			return e
		}
		committed = next
		return nil
	})
	if err == nil {
		progress = committed
	}
	return progress, err
}

// Core completeness concerns issue/PR/comment traversal. Review-state enrichment
// is deliberately independent so existing response/contributor KPIs remain usable.
func (s *Store) SetAnalyticsCoverageComplete(ctx context.Context, repository string, complete bool) error {
	_, err := s.q().ExecContext(ctx, "UPDATE analytics_coverage SET complete=? WHERE repository=?", boolInt(complete), repository)
	return err
}

func (s *Store) SaveReviewStateCoverage(ctx context.Context, repository string, progress ReviewStateRecovery) error {
	var pending int
	if err := s.q().QueryRowContext(ctx, `SELECT count(*) FROM analytics_retries WHERE repository=? AND operation='review_state' AND resolved_at IS NULL`, repository).Scan(&pending); err != nil {
		return err
	}
	_, err := s.q().ExecContext(ctx, `INSERT INTO analytics_review_state_coverage VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(repository) DO UPDATE SET cursor=excluded.cursor,ceiling=excluded.ceiling,scanned=excluded.scanned,queued=excluded.queued,pending_items=excluded.pending_items,scan_complete=excluded.scan_complete,complete=excluded.complete,observed_at=excluded.observed_at`, repository, progress.Cursor, progress.Ceiling, progress.Scanned, progress.Queued, pending, boolInt(progress.Done), boolInt(progress.Done && pending == 0), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) AnalyticsIntegrityStatus(ctx context.Context, repository string) (map[string]any, error) {
	out := map[string]any{"repository": repository, "checked_at": time.Now().UTC().Format(time.RFC3339Nano)}
	var through, observed string
	var issues, prs, complete int
	coverageErr := s.q().QueryRowContext(ctx, "SELECT through,issues,pull_requests,complete,observed_at FROM analytics_coverage WHERE repository=?", repository).Scan(&through, &issues, &prs, &complete, &observed)
	if coverageErr == sql.ErrNoRows {
		out["coverage"] = map[string]any{"state": "not_started", "through": nil, "issues": nil, "pull_requests": nil, "complete": false, "observed_at": nil}
	} else if coverageErr != nil {
		return nil, coverageErr
	} else {
		out["coverage"] = map[string]any{"state": "observed", "through": through, "issues": issues, "pull_requests": prs, "complete": complete == 1, "observed_at": observed}
	}
	n, err := s.AnalyticsOutstanding(ctx, repository)
	if err != nil {
		return nil, err
	}
	out["unresolved_retries"] = n
	progress, err := s.AnalyticsState(ctx, "review_state_recovery:"+repository)
	if err != nil {
		return nil, err
	}
	if progress != "" {
		out["review_state_recovery"] = json.RawMessage(progress)
		var recovery ReviewStateRecovery
		if err = json.Unmarshal([]byte(progress), &recovery); err != nil {
			return nil, err
		}
		out["review_state_scan_complete"] = recovery.Done
	} else {
		out["review_state_recovery"] = nil
		out["review_state_scan_complete"] = false
	}
	corePending, err := s.AnalyticsCoreOutstanding(ctx, repository)
	if err != nil {
		return nil, err
	}
	out["core_unresolved_retries"] = corePending
	out["coverage"].(map[string]any)["complete"] = complete == 1 && corePending == 0
	var reviewPending, reviewComplete int
	if err = s.q().QueryRowContext(ctx, `SELECT pending_items,complete FROM analytics_review_state_coverage WHERE repository=?`, repository).Scan(&reviewPending, &reviewComplete); err == nil {
		out["review_state_coverage"] = map[string]any{"pending_items": reviewPending, "complete": reviewComplete == 1}
	} else if err == sql.ErrNoRows {
		out["review_state_coverage"] = nil
	} else {
		return nil, err
	}
	rows, err := s.q().QueryContext(ctx, `SELECT id,number,operation,started_at,finished_at,status,coalesce(error_class,'') FROM analytics_fetch_attempts WHERE repository=? ORDER BY id DESC LIMIT 10`, repository)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, number int64
		var operation, start, finish, status, class string
		if err = rows.Scan(&id, &number, &operation, &start, &finish, &status, &class); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{"id": id, "number": number, "operation": operation, "started_at": start, "finished_at": finish, "status": status, "error_class": class})
	}
	out["latest_attempts"] = items
	return out, rows.Err()
}
