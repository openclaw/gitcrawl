package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// ClusterCandidateFormatV3 identifies an immutable, run-scoped computed
// candidate generation. Candidate IDs ("<generation>:<ordinal>") are only
// meaningful inside their generation; they never alias or merge durable
// cluster_groups identities, whose six history tables this file never writes.
const ClusterCandidateFormatV3 = "gitcrawl.cluster-candidates.v3"

var ErrNoClusterCandidateGeneration = errors.New("no computed cluster candidate generation")

// clusterCandidateGenerationTables lists the v3 tables, children first.
func clusterCandidateGenerationTables() []string {
	return []string{"cluster_candidate_selection", "cluster_candidate_overlaps", "cluster_candidate_members", "cluster_candidates", "cluster_candidate_generations"}
}

// ensureClusterCandidateGenerationSchema creates the v3 tables on first use by
// the native producer's transaction. Ordinary opens never pay for this DDL;
// readers, purge and portable shaping treat absent tables as no generations.
func (s *Store) ensureClusterCandidateGenerationSchema(ctx context.Context) error {
	_, err := s.q().ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS cluster_candidate_generations(
 id INTEGER PRIMARY KEY,
 repo_id INTEGER NOT NULL REFERENCES repositories(id),
 run_id INTEGER NOT NULL UNIQUE REFERENCES cluster_runs(id),
 format TEXT NOT NULL CHECK(format='gitcrawl.cluster-candidates.v3'),
 algorithm TEXT NOT NULL,
 input_sha256 TEXT NOT NULL,
 quarantine_plan_sha256 TEXT NOT NULL,
 partition_sha256 TEXT NOT NULL,
 candidate_count INTEGER NOT NULL CHECK(candidate_count>=0),
 member_count INTEGER NOT NULL CHECK(member_count>=0),
 overlap_count INTEGER NOT NULL CHECK(overlap_count>=0),
 durable_published_candidates INTEGER NOT NULL CHECK(durable_published_candidates>=0),
 durable_quarantined_candidates INTEGER NOT NULL CHECK(durable_quarantined_candidates>=0),
 created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS cluster_candidate_generations_repo ON cluster_candidate_generations(repo_id,id);
CREATE TABLE IF NOT EXISTS cluster_candidates(
 generation_id INTEGER NOT NULL REFERENCES cluster_candidate_generations(id),
 ordinal INTEGER NOT NULL CHECK(ordinal>0),
 computed_key TEXT NOT NULL,
 representative_thread_id INTEGER,
 member_count INTEGER NOT NULL CHECK(member_count>0),
 durable_status TEXT NOT NULL CHECK(durable_status IN ('published','identity_quarantined')),
 durable_cluster_id INTEGER,
 PRIMARY KEY(generation_id,ordinal),
 UNIQUE(generation_id,computed_key));
CREATE TABLE IF NOT EXISTS cluster_candidate_members(
 generation_id INTEGER NOT NULL,
 ordinal INTEGER NOT NULL,
 thread_id INTEGER NOT NULL,
 role TEXT NOT NULL,
 PRIMARY KEY(generation_id,thread_id),
 FOREIGN KEY(generation_id,ordinal) REFERENCES cluster_candidates(generation_id,ordinal));
CREATE INDEX IF NOT EXISTS cluster_candidate_members_candidate ON cluster_candidate_members(generation_id,ordinal);
CREATE TABLE IF NOT EXISTS cluster_candidate_overlaps(
 generation_id INTEGER NOT NULL,
 ordinal INTEGER NOT NULL,
 cluster_id INTEGER NOT NULL,
 shared_members INTEGER NOT NULL CHECK(shared_members>0),
 PRIMARY KEY(generation_id,ordinal,cluster_id),
 FOREIGN KEY(generation_id,ordinal) REFERENCES cluster_candidates(generation_id,ordinal));
CREATE TABLE IF NOT EXISTS cluster_candidate_selection(
 repo_id INTEGER PRIMARY KEY REFERENCES repositories(id),
 generation_id INTEGER NOT NULL REFERENCES cluster_candidate_generations(id),
 selected_at TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(reason IN ('accepted','rollback')));
`)
	if err != nil {
		return err
	}
	// Generations are immutable history; only the selection pointer moves.
	// The single exception is owner removal: once PurgeThreads has deleted a
	// thread, that thread's member rows may be deleted too.
	for _, table := range []string{"cluster_candidate_generations", "cluster_candidates", "cluster_candidate_members", "cluster_candidate_overlaps"} {
		for _, event := range []string{"UPDATE", "DELETE"} {
			condition := ""
			if table == "cluster_candidate_members" && event == "DELETE" {
				condition = " WHEN EXISTS(SELECT 1 FROM threads WHERE id=OLD.thread_id)"
			}
			if _, err = s.q().ExecContext(ctx, "CREATE TRIGGER IF NOT EXISTS "+table+"_immutable_"+event+" BEFORE "+event+" ON "+table+condition+
				" BEGIN SELECT RAISE(ABORT,'cluster candidate generations are immutable'); END"); err != nil {
				return err
			}
		}
	}
	return nil
}

// ClusterCandidateGenerationInput is the complete computed partition and its
// v2 quarantine plan for one accepted native run.
type ClusterCandidateGenerationInput struct {
	RepoID               int64
	RunID                int64
	Algorithm            string
	InputSHA256          string
	QuarantinePlanSHA256 string
	Plan                 ClusterQuarantinePlan
	// PublishedClusterIDs maps each published candidate key to the durable
	// cluster_groups ID it was written to in this run, including identities
	// newly created by this publication (plan targets record those as 0).
	PublishedClusterIDs map[string]int64
}

type ClusterCandidateGeneration struct {
	ID                           int64  `json:"generation_id"`
	Format                       string `json:"format"`
	RepoID                       int64  `json:"repo_id"`
	RunID                        int64  `json:"run_id"`
	Algorithm                    string `json:"algorithm"`
	InputSHA256                  string `json:"input_sha256"`
	QuarantinePlanSHA256         string `json:"quarantine_plan_sha256"`
	PartitionSHA256              string `json:"partition_sha256"`
	CandidateCount               int    `json:"candidate_count"`
	MemberCount                  int    `json:"member_count"`
	OverlapCount                 int    `json:"overlap_count"`
	DurablePublishedCandidates   int    `json:"durable_published_candidates"`
	DurableQuarantinedCandidates int    `json:"durable_quarantined_candidates"`
	CreatedAt                    string `json:"created_at"`
	Selected                     bool   `json:"selected"`
	SelectedAt                   string `json:"selected_at,omitempty"`
	SelectionReason              string `json:"selection_reason,omitempty"`
}

type ClusterCandidateOverlap struct {
	ClusterID     int64 `json:"durable_cluster_id"`
	SharedMembers int   `json:"shared_members"`
}

type ClusterCandidateMember struct {
	ThreadID int64  `json:"thread_id"`
	Role     string `json:"role"`
}

type ClusterCandidate struct {
	ID                     string                    `json:"computed_candidate_id"`
	Ordinal                int                       `json:"ordinal"`
	ComputedKey            string                    `json:"computed_key"`
	RepresentativeThreadID int64                     `json:"representative_thread_id,omitempty"`
	MemberCount            int                       `json:"member_count"`
	DurableStatus          string                    `json:"durable_status"`
	DurableClusterID       int64                     `json:"durable_cluster_id,omitempty"`
	Overlaps               []ClusterCandidateOverlap `json:"observed_overlaps"`
	Members                []ClusterCandidateMember  `json:"members,omitempty"`
}

func clusterCandidateID(generation int64, ordinal int) string {
	return strconv.FormatInt(generation, 10) + ":" + strconv.Itoa(ordinal)
}

// SaveClusterCandidateGeneration records the complete computed partition of
// an accepted native run and selects it, inside the caller's transaction.
// Overlaps are observed shared active members with prior durable groups; they
// are lineage evidence, never equivalence, aliases or merges.
func (s *Store) SaveClusterCandidateGeneration(ctx context.Context, in ClusterCandidateGenerationInput) (ClusterCandidateGeneration, error) {
	var out ClusterCandidateGeneration
	if s.queries == nil {
		return out, fmt.Errorf("cluster candidate generation requires the native run transaction")
	}
	if in.RepoID <= 0 || in.RunID <= 0 || in.Algorithm == "" || len(in.InputSHA256) != 64 || len(in.QuarantinePlanSHA256) != 64 {
		return out, fmt.Errorf("invalid cluster candidate generation binding")
	}
	if in.Plan.RepositoryID != in.RepoID {
		return out, fmt.Errorf("cluster candidate generation repository mismatch")
	}
	if err := s.ensureClusterCandidateGenerationSchema(ctx); err != nil {
		return out, err
	}
	var runRepo int64
	if err := s.q().QueryRowContext(ctx, "SELECT repo_id FROM cluster_runs WHERE id=?", in.RunID).Scan(&runRepo); err != nil {
		return out, fmt.Errorf("cluster candidate generation run: %w", err)
	}
	if runRepo != in.RepoID {
		return out, fmt.Errorf("cluster candidate generation run belongs to another repository")
	}
	candidates := append([]DurableClusterInput(nil), in.Plan.Candidates...)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].StableKey < candidates[j].StableKey })
	published := map[string]int64{}
	for _, target := range in.Plan.Targets {
		id := in.PublishedClusterIDs[target.CandidateKey]
		if id <= 0 || target.ClusterID != 0 && target.ClusterID != id {
			return out, fmt.Errorf("cluster candidate generation lacks the published durable identity for a target")
		}
		published[target.CandidateKey] = id
	}
	if len(in.PublishedClusterIDs) != len(published) {
		return out, fmt.Errorf("cluster candidate generation published identities do not match the plan targets")
	}
	activeOwner := map[int64][]int64{}
	for _, identity := range in.Plan.Identities {
		if !identity.Active {
			continue
		}
		for _, thread := range identity.ActiveMembers {
			activeOwner[thread] = append(activeOwner[thread], identity.ID)
		}
	}
	selected := map[int64]bool{}
	for _, id := range in.Plan.SelectedIDs {
		selected[id] = true
	}
	type partitionRow struct {
		Key     string  `json:"key"`
		Members []int64 `json:"members"`
	}
	partition := make([]partitionRow, 0, len(candidates))
	seen := map[int64]bool{}
	members, overlaps, durablePublished := 0, 0, 0
	type candidateRow struct {
		input    DurableClusterInput
		threads  []int64
		overlaps map[int64]int
		durable  int64
		status   string
	}
	rows := make([]candidateRow, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.StableKey == "" || len(candidate.Members) == 0 {
			return out, fmt.Errorf("cluster candidate generation requires keyed, non-empty candidates")
		}
		row := candidateRow{input: candidate, overlaps: map[int64]int{}, status: "identity_quarantined"}
		for _, member := range candidate.Members {
			if member.ThreadID <= 0 || seen[member.ThreadID] || !selected[member.ThreadID] {
				return out, fmt.Errorf("cluster candidate generation membership is not a partition of the selected cohort")
			}
			seen[member.ThreadID] = true
			row.threads = append(row.threads, member.ThreadID)
			for _, cluster := range activeOwner[member.ThreadID] {
				row.overlaps[cluster]++
			}
		}
		sort.Slice(row.threads, func(i, j int) bool { return row.threads[i] < row.threads[j] })
		if cluster, ok := published[candidate.StableKey]; ok {
			row.status, row.durable = "published", cluster
			durablePublished++
		}
		members += len(row.threads)
		overlaps += len(row.overlaps)
		partition = append(partition, partitionRow{Key: candidate.StableKey, Members: row.threads})
		rows = append(rows, row)
	}
	if len(seen) != len(selected) {
		return out, fmt.Errorf("cluster candidate generation does not cover the selected cohort")
	}
	// Owner-removed threads never enter a generation, even if a caller's input
	// is stale.
	if s.hasTable(ctx, "thread_exclusions") {
		for thread := range seen {
			var excluded bool
			if err := s.q().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM thread_exclusions WHERE original_thread_id=?)", thread).Scan(&excluded); err != nil {
				return out, err
			}
			if excluded {
				return out, fmt.Errorf("cluster candidate generation contains an owner-excluded thread")
			}
		}
	}
	encoded, err := json.Marshal(partition)
	if err != nil {
		return out, err
	}
	out = ClusterCandidateGeneration{Format: ClusterCandidateFormatV3, RepoID: in.RepoID, RunID: in.RunID, Algorithm: in.Algorithm,
		InputSHA256: in.InputSHA256, QuarantinePlanSHA256: in.QuarantinePlanSHA256, PartitionSHA256: clusterHash(encoded),
		CandidateCount: len(rows), MemberCount: members, OverlapCount: overlaps,
		DurablePublishedCandidates: durablePublished, DurableQuarantinedCandidates: len(rows) - durablePublished,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Selected: true, SelectionReason: "accepted"}
	result, err := s.q().ExecContext(ctx, `INSERT INTO cluster_candidate_generations(repo_id,run_id,format,algorithm,input_sha256,quarantine_plan_sha256,partition_sha256,candidate_count,member_count,overlap_count,durable_published_candidates,durable_quarantined_candidates,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		out.RepoID, out.RunID, out.Format, out.Algorithm, out.InputSHA256, out.QuarantinePlanSHA256, out.PartitionSHA256,
		out.CandidateCount, out.MemberCount, out.OverlapCount, out.DurablePublishedCandidates, out.DurableQuarantinedCandidates, out.CreatedAt)
	if err != nil {
		return out, err
	}
	if out.ID, err = result.LastInsertId(); err != nil {
		return out, err
	}
	for index, row := range rows {
		ordinal := index + 1
		var representative any
		if row.input.RepresentativeThreadID > 0 {
			index := sort.Search(len(row.threads), func(i int) bool { return row.threads[i] >= row.input.RepresentativeThreadID })
			if index == len(row.threads) || row.threads[index] != row.input.RepresentativeThreadID {
				return out, fmt.Errorf("cluster candidate representative is outside its candidate")
			}
			representative = row.input.RepresentativeThreadID
		}
		var durable any
		if row.durable > 0 {
			durable = row.durable
		}
		if _, err = s.q().ExecContext(ctx, "INSERT INTO cluster_candidates(generation_id,ordinal,computed_key,representative_thread_id,member_count,durable_status,durable_cluster_id) VALUES(?,?,?,?,?,?,?)",
			out.ID, ordinal, row.input.StableKey, representative, len(row.threads), row.status, durable); err != nil {
			return out, err
		}
		for _, member := range row.input.Members {
			role := member.Role
			if role == "" {
				role = "member"
			}
			if _, err = s.q().ExecContext(ctx, "INSERT INTO cluster_candidate_members(generation_id,ordinal,thread_id,role) VALUES(?,?,?,?)", out.ID, ordinal, member.ThreadID, role); err != nil {
				return out, err
			}
		}
		clusters := make([]int64, 0, len(row.overlaps))
		for cluster := range row.overlaps {
			clusters = append(clusters, cluster)
		}
		sort.Slice(clusters, func(i, j int) bool { return clusters[i] < clusters[j] })
		for _, cluster := range clusters {
			if _, err = s.q().ExecContext(ctx, "INSERT INTO cluster_candidate_overlaps(generation_id,ordinal,cluster_id,shared_members) VALUES(?,?,?,?)", out.ID, ordinal, cluster, row.overlaps[cluster]); err != nil {
				return out, err
			}
		}
	}
	if err = s.selectClusterCandidateGeneration(ctx, in.RepoID, out.ID, "accepted", out.CreatedAt); err != nil {
		return out, err
	}
	out.SelectedAt = out.CreatedAt
	return out, nil
}

func (s *Store) selectClusterCandidateGeneration(ctx context.Context, repoID, generationID int64, reason, at string) error {
	_, err := s.q().ExecContext(ctx, `INSERT INTO cluster_candidate_selection(repo_id,generation_id,selected_at,reason) VALUES(?,?,?,?)
ON CONFLICT(repo_id) DO UPDATE SET generation_id=excluded.generation_id,selected_at=excluded.selected_at,reason=excluded.reason`, repoID, generationID, at, reason)
	return err
}

// RollbackClusterCandidateGeneration atomically selects the newest generation
// older than the current selection. Generations themselves are unchanged; the
// next accepted native run selects its own newer generation again.
func (s *Store) RollbackClusterCandidateGeneration(ctx context.Context, repoID int64) (ClusterCandidateGeneration, error) {
	var out ClusterCandidateGeneration
	err := s.WithTx(ctx, func(tx *Store) error {
		if !tx.hasTable(ctx, "cluster_candidate_selection") {
			return ErrNoClusterCandidateGeneration
		}
		var current int64
		if err := tx.q().QueryRowContext(ctx, "SELECT generation_id FROM cluster_candidate_selection WHERE repo_id=?", repoID).Scan(&current); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNoClusterCandidateGeneration
			}
			return err
		}
		var previous int64
		if err := tx.q().QueryRowContext(ctx, "SELECT id FROM cluster_candidate_generations WHERE repo_id=? AND id<? ORDER BY id DESC LIMIT 1", repoID, current).Scan(&previous); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("no earlier computed cluster candidate generation to roll back to")
			}
			return err
		}
		if err := tx.selectClusterCandidateGeneration(ctx, repoID, previous, "rollback", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		var err error
		out, err = tx.clusterCandidateGeneration(ctx, repoID, previous)
		return err
	})
	return out, err
}

// SelectedClusterCandidateGeneration returns the selected generation.
func (s *Store) SelectedClusterCandidateGeneration(ctx context.Context, repoID int64) (ClusterCandidateGeneration, error) {
	if !s.hasTable(ctx, "cluster_candidate_selection") {
		return ClusterCandidateGeneration{}, ErrNoClusterCandidateGeneration
	}
	var id int64
	if err := s.q().QueryRowContext(ctx, "SELECT generation_id FROM cluster_candidate_selection WHERE repo_id=?", repoID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ClusterCandidateGeneration{}, ErrNoClusterCandidateGeneration
		}
		return ClusterCandidateGeneration{}, err
	}
	return s.clusterCandidateGeneration(ctx, repoID, id)
}

// ClusterCandidateGenerationByID returns one generation of the repository.
func (s *Store) ClusterCandidateGenerationByID(ctx context.Context, repoID, id int64) (ClusterCandidateGeneration, error) {
	if !s.hasTable(ctx, "cluster_candidate_generations") {
		return ClusterCandidateGeneration{}, ErrNoClusterCandidateGeneration
	}
	return s.clusterCandidateGeneration(ctx, repoID, id)
}

func (s *Store) clusterCandidateGeneration(ctx context.Context, repoID, id int64) (ClusterCandidateGeneration, error) {
	var g ClusterCandidateGeneration
	var selectedAt, reason sql.NullString
	err := s.q().QueryRowContext(ctx, `SELECT g.id,g.format,g.repo_id,g.run_id,g.algorithm,g.input_sha256,g.quarantine_plan_sha256,g.partition_sha256,g.candidate_count,g.member_count,g.overlap_count,g.durable_published_candidates,g.durable_quarantined_candidates,g.created_at,s.selected_at,s.reason
FROM cluster_candidate_generations g LEFT JOIN cluster_candidate_selection s ON s.repo_id=g.repo_id AND s.generation_id=g.id
WHERE g.repo_id=? AND g.id=?`, repoID, id).Scan(&g.ID, &g.Format, &g.RepoID, &g.RunID, &g.Algorithm, &g.InputSHA256, &g.QuarantinePlanSHA256, &g.PartitionSHA256,
		&g.CandidateCount, &g.MemberCount, &g.OverlapCount, &g.DurablePublishedCandidates, &g.DurableQuarantinedCandidates, &g.CreatedAt, &selectedAt, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return g, ErrNoClusterCandidateGeneration
	}
	if err != nil {
		return g, err
	}
	if g.Format != ClusterCandidateFormatV3 {
		return g, fmt.Errorf("unsupported cluster candidate generation format %q", g.Format)
	}
	g.Selected, g.SelectedAt, g.SelectionReason = selectedAt.Valid, selectedAt.String, reason.String
	return g, nil
}

// ListClusterCandidateGenerations returns the newest generations first.
func (s *Store) ListClusterCandidateGenerations(ctx context.Context, repoID int64, limit int) ([]ClusterCandidateGeneration, error) {
	if !s.hasTable(ctx, "cluster_candidate_generations") {
		return []ClusterCandidateGeneration{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.q().QueryContext(ctx, "SELECT id FROM cluster_candidate_generations WHERE repo_id=? ORDER BY id DESC LIMIT ?", repoID, limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]ClusterCandidateGeneration, 0, len(ids))
	for _, id := range ids {
		g, err := s.clusterCandidateGeneration(ctx, repoID, id)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

// ClusterCandidates returns a page of candidates of one generation, largest
// first, with observed overlaps and optionally members.
func (s *Store) ClusterCandidates(ctx context.Context, repoID, generationID int64, limit int, withMembers bool) ([]ClusterCandidate, error) {
	if _, err := s.clusterCandidateGeneration(ctx, repoID, generationID); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	// Member counts and representatives are read from current member rows,
	// so owner-removed threads are withheld everywhere.
	rows, err := s.q().QueryContext(ctx, `SELECT c.ordinal,c.computed_key,
 CASE WHEN EXISTS(SELECT 1 FROM cluster_candidate_members m WHERE m.generation_id=c.generation_id AND m.thread_id=c.representative_thread_id) THEN c.representative_thread_id ELSE 0 END,
 (SELECT count(*) FROM cluster_candidate_members m WHERE m.generation_id=c.generation_id AND m.ordinal=c.ordinal) AS members,
 c.durable_status,coalesce(c.durable_cluster_id,0)
FROM cluster_candidates c WHERE c.generation_id=? AND members>0 ORDER BY members DESC,c.ordinal LIMIT ?`, generationID, limit)
	if err != nil {
		return nil, err
	}
	var out []ClusterCandidate
	for rows.Next() {
		var c ClusterCandidate
		if err = rows.Scan(&c.Ordinal, &c.ComputedKey, &c.RepresentativeThreadID, &c.MemberCount, &c.DurableStatus, &c.DurableClusterID); err != nil {
			rows.Close()
			return nil, err
		}
		c.ID = clusterCandidateID(generationID, c.Ordinal)
		c.Overlaps = []ClusterCandidateOverlap{}
		out = append(out, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for i := range out {
		overlapRows, err := s.q().QueryContext(ctx, "SELECT cluster_id,shared_members FROM cluster_candidate_overlaps WHERE generation_id=? AND ordinal=? ORDER BY shared_members DESC,cluster_id", generationID, out[i].Ordinal)
		if err != nil {
			return nil, err
		}
		for overlapRows.Next() {
			var o ClusterCandidateOverlap
			if err = overlapRows.Scan(&o.ClusterID, &o.SharedMembers); err != nil {
				overlapRows.Close()
				return nil, err
			}
			out[i].Overlaps = append(out[i].Overlaps, o)
		}
		err = overlapRows.Err()
		overlapRows.Close()
		if err != nil {
			return nil, err
		}
		if !withMembers {
			continue
		}
		memberRows, err := s.q().QueryContext(ctx, "SELECT thread_id,role FROM cluster_candidate_members WHERE generation_id=? AND ordinal=? ORDER BY thread_id", generationID, out[i].Ordinal)
		if err != nil {
			return nil, err
		}
		for memberRows.Next() {
			var m ClusterCandidateMember
			if err = memberRows.Scan(&m.ThreadID, &m.Role); err != nil {
				memberRows.Close()
				return nil, err
			}
			out[i].Members = append(out[i].Members, m)
		}
		err = memberRows.Err()
		memberRows.Close()
		if err != nil {
			return nil, err
		}
	}
	if out == nil {
		out = []ClusterCandidate{}
	}
	return out, nil
}
