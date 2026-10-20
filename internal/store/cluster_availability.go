package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const ClusterAvailabilityPolicy = "gitcrawl.available-subset.v1:isolated-n0-NOT_FOUND;fresh-discovery;exact-vectors;partial-no-prune"

func ClusterAvailabilityPolicyDigest() string {
	h := sha256.Sum256([]byte(ClusterAvailabilityPolicy))
	return hex.EncodeToString(h[:])
}

type ClusterUnavailableBinding struct {
	Number          int    `json:"number"`
	ThreadID        int64  `json:"thread_id"`
	GitHubID        string `json:"github_id"`
	Kind            string `json:"kind"`
	InputHash       string `json:"input_hash"`
	SourceUpdatedAt string `json:"source_updated_at"`
	AttemptID       int64  `json:"attempt_id"`
	EvidenceSHA256  string `json:"evidence_sha256"`
}
type ClusterAvailability struct {
	AdmissionAt    string                      `json:"admission_at"`
	Scope          ClusterScope                `json:"scope"`
	SemanticDigest string                      `json:"semantic_digest"`
	Unavailable    []ClusterUnavailableBinding `json:"unavailable_bindings"`
	Blocked        map[int64]bool              `json:"-"`
}

// ReadClusterAvailability is a bounded admission read. A typed NOT_FOUND is
// evidence of unavailability, never a deletion, empty membership or permission.
func (s *Store) ReadClusterAvailability(ctx context.Context, repo string, now time.Time) (ClusterAvailability, error) {
	var out ClusterAvailability
	work := func(tx *Store) error {
		var err error
		out, err = tx.readClusterAvailability(ctx, repo, now)
		return err
	}
	var err error
	if s.queries != nil {
		err = work(s)
	} else {
		err = s.WithTx(ctx, work)
	}
	return out, err
}
func (s *Store) readClusterAvailability(ctx context.Context, repo string, now time.Time) (ClusterAvailability, error) {
	out := ClusterAvailability{AdmissionAt: now.UTC().Format(time.RFC3339Nano), Blocked: map[int64]bool{}, Unavailable: []ClusterUnavailableBinding{}}
	fail := func(why string) (ClusterAvailability, error) {
		return out, fmt.Errorf("cluster availability blocked: %s", why)
	}
	var baseline, node string
	var repoID int64
	var complete int
	var pub *int
	if err := s.q().QueryRowContext(ctx, `SELECT r.id,json_extract(r.raw_json,'$.node_id'),json_extract(r.raw_json,'$.private'),c.through,c.observed_at,c.complete,
 coalesce((SELECT value FROM analytics_collection_state WHERE name='core_baseline_verified:'||r.full_name),'')
 FROM repositories r JOIN analytics_coverage c ON c.repository=r.full_name WHERE r.full_name=?`, repo).Scan(&repoID, &node, &pub, &out.Scope.SourceAsOf.Through, &out.Scope.SourceAsOf.ObservedAt, &complete, &baseline); err != nil {
		return fail("missing repository/core receipt")
	}
	if baseline != "1" || node == "" || pub == nil || *pub != 0 || complete < 0 || complete > 1 {
		return fail("unverified baseline/repository visibility")
	}
	fresh := func(v string) bool {
		t, e := time.Parse(time.RFC3339Nano, v)
		return e == nil && !t.After(now) && now.Sub(t) <= 5*time.Minute
	}
	through, e := time.Parse(time.RFC3339Nano, out.Scope.SourceAsOf.Through)
	if e != nil {
		return fail("invalid frontier")
	}
	observed, e := time.Parse(time.RFC3339Nano, out.Scope.SourceAsOf.ObservedAt)
	if e != nil || through.After(observed) || !fresh(out.Scope.SourceAsOf.Through) || !fresh(out.Scope.SourceAsOf.ObservedAt) {
		return fail("stale/invalid progress")
	}
	// The item-zero attempt index bounds this read independently of all history.
	rows, e := s.q().QueryContext(ctx, `SELECT operation,status,finished_at FROM analytics_fetch_attempts WHERE repository=? AND number=0 ORDER BY id DESC LIMIT 64`, repo)
	if e != nil {
		return out, e
	}
	lanes := map[string]bool{}
	for rows.Next() {
		var op, status, at string
		if e = rows.Scan(&op, &status, &at); e != nil {
			rows.Close()
			return out, e
		}
		if op != "discover_issues" && op != "discover_pullRequests" {
			continue
		}
		if _, ok := lanes[op]; !ok {
			lanes[op] = status == "success" && fresh(at)
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	if !lanes["discover_issues"] || !lanes["discover_pullRequests"] {
		return fail("both latest discovery lanes must succeed recently")
	}
	for _, lane := range []string{"issues", "pullRequests"} {
		var at string
		if e = s.q().QueryRowContext(ctx, "SELECT value FROM analytics_collection_state WHERE name=?", "through:"+repo+":"+lane).Scan(&at); e != nil || !fresh(at) {
			return fail("missing/stale completed lane frontier")
		}
	}
	rows, e = s.q().QueryContext(ctx, `SELECT r.number,r.operation,r.last_seen_at,r.last_attempt_id,a.repository,a.number,a.operation,a.status,a.error_class,a.finished_at,a.evidence_json
 FROM analytics_retries r LEFT JOIN analytics_fetch_attempts a ON a.id=r.last_attempt_id
 WHERE r.repository=? AND r.resolved_at IS NULL AND r.operation<>'review_state' ORDER BY r.number,r.operation LIMIT 129`, repo)
	if e != nil {
		return out, e
	}
	type pending struct {
		n                                      int
		op, last                               string
		id                                     int64
		ar                                     string
		an                                     int
		aop, status, class, finished, evidence string
	}
	items := []pending{}
	for rows.Next() {
		var p pending
		if e = rows.Scan(&p.n, &p.op, &p.last, &p.id, &p.ar, &p.an, &p.aop, &p.status, &p.class, &p.finished, &p.evidence); e != nil {
			rows.Close()
			return fail("unbound retry receipt")
		}
		items = append(items, p)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	if len(items) > 128 {
		return fail("unavailable obligation bound exceeded")
	}
	seen := map[int]bool{}
	for _, p := range items {
		if p.n <= 0 || p.op != "graphql_history" || p.ar != repo || p.an != p.n || p.aop != p.op || p.status != "failed" || p.class != "partial_response" || p.finished != p.last || seen[p.n] {
			return fail("non-item or mismatched unresolved failure")
		}
		seen[p.n] = true
		failedAt, err := time.Parse(time.RFC3339Nano, p.finished)
		if err != nil || failedAt.After(now) {
			return fail("invalid failure observation clock")
		}
		if !isolatedNotFound(p.evidence, node) {
			return fail("unresolved failure is not isolated bound NOT_FOUND")
		}
		binding := ClusterUnavailableBinding{Number: p.n, AttemptID: p.id}
		h := sha256.Sum256([]byte(p.evidence))
		binding.EvidenceSHA256 = hex.EncodeToString(h[:])
		var pulled string
		e = s.q().QueryRowContext(ctx, `SELECT id,github_id,kind,content_hash,coalesce(updated_at_gh,''),coalesce(last_pulled_at,'') FROM threads WHERE repo_id=? AND number=?`, repoID, p.n).Scan(&binding.ThreadID, &binding.GitHubID, &binding.Kind, &binding.InputHash, &binding.SourceUpdatedAt, &pulled)
		if e != nil {
			if e != sql.ErrNoRows {
				return out, e
			}
			out.Scope.Population.UnmaterializedUnavailable++
		} else {
			accepted, err := time.Parse(time.RFC3339Nano, pulled)
			failed, err2 := time.Parse(time.RFC3339Nano, p.finished)
			if err != nil || err2 != nil || accepted.After(failed) || binding.GitHubID == "" || binding.Kind != "issue" && binding.Kind != "pull_request" {
				return fail("unavailable native identity/accepted clock unclear")
			}
			out.Blocked[binding.ThreadID] = true
		}
		out.Unavailable = append(out.Unavailable, binding)
	}
	if complete == 0 && len(items) == 0 {
		return fail("incomplete core without qualified unavailable obligations")
	}
	if complete == 1 && len(items) != 0 {
		return fail("core completeness disagrees with retries")
	}
	out.Scope.Format = ClusterScopeFormat
	out.Scope.Repository = repo
	out.Scope.PolicyVersion = 1
	out.Scope.PolicyDigest = ClusterAvailabilityPolicyDigest()
	out.Scope.Mode = "full-native"
	out.Scope.ProviderComplete = complete == 1
	out.Scope.FullNativeScopeComplete = complete == 1
	out.Scope.SelectedInputComplete = true
	out.Scope.Population.Unavailable = len(items)
	if len(items) > 0 {
		out.Scope.Mode = "available-subset"
	}
	// Retry clocks/attempt IDs remain audit evidence but do not churn graph inputs.
	semantic := []any{out.Scope.Mode, out.Scope.PolicyDigest}
	for _, b := range out.Unavailable {
		semantic = append(semantic, []any{b.Number, b.ThreadID, b.GitHubID, b.Kind, b.InputHash, b.SourceUpdatedAt})
	}
	raw, _ := json.Marshal(semantic)
	h := sha256.Sum256(raw)
	out.SemanticDigest = hex.EncodeToString(h[:])
	return out, nil
}
func isolatedNotFound(raw, node string) bool {
	var e struct {
		Version int `json:"version"`
		Errors  struct {
			Count     int  `json:"count"`
			Truncated bool `json:"truncated"`
			Items     []struct {
				Type      string   `json:"type"`
				Path      []string `json:"path"`
				Truncated bool     `json:"path_truncated"`
			} `json:"items"`
		} `json:"graphql_errors"`
		Structure struct {
			Repository map[string]json.RawMessage `json:"repository"`
		} `json:"structure"`
	}
	if json.Unmarshal([]byte(raw), &e) != nil || e.Version != 1 || e.Errors.Count != 1 || e.Errors.Truncated || len(e.Errors.Items) != 1 {
		return false
	}
	x := e.Errors.Items[0]
	if x.Type != "NOT_FOUND" || x.Truncated || len(x.Path) != 2 || x.Path[0] != "repository" || x.Path[1] != "n0" {
		return false
	}
	var id string
	if json.Unmarshal(e.Structure.Repository["id"], &id) != nil || id != node {
		return false
	}
	if strings.TrimSpace(string(e.Structure.Repository["n0"])) != "null" {
		return false
	}
	for key := range e.Structure.Repository {
		if strings.HasPrefix(key, "n") {
			if _, err := strconv.Atoi(key[1:]); err == nil && key != "n0" {
				return false
			}
		}
	}
	return true
}
