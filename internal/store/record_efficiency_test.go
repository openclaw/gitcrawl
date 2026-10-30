package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
)

// recordFixture opens an archive with the record-efficient layout and a
// version 1 change contract, which journals every UPDATE, so the contract
// proves whether a content row was written.
func recordFixture(t testing.TB) (*Store, int64) {
	t.Helper()
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if _, err := st.EnableRecordEfficiency(ctx, "2026-10-30T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if !st.RecordChecksEnabled() {
		t.Fatal("record checks should be enabled")
	}
	if _, err := st.EnableChangeContract(ctx, ChangeContractEnableOptions{}); err != nil {
		t.Fatal(err)
	}
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: `{"id":1,"_graphql":{"updatedAt":"2026-10-30T00:00:00Z","description":"d"}}`, UpdatedAt: "2026-10-30T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	return st, repoID
}

func tableWrites(t testing.TB, st *Store, table string) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(`select count(*) from change_contract_log l join change_contract_tables t using(table_id) where t.table_name = ?`, table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func prRaw(mergeStatus, updatedAt, title string) string {
	node := map[string]any{
		"id": "PR_1", "number": 7, "title": title, "body": "body", "updatedAt": updatedAt,
		"mergeable": map[string]string{"CLEAN": "MERGEABLE", "DIRTY": "CONFLICTING"}[mergeStatus], "mergeStateStatus": mergeStatus,
		"headRefOid": "abc", "baseRefOid": "def", "additions": 3, "deletions": 1, "changedFiles": 2,
		"commits": map[string]any{"totalCount": 1}, "comments": map[string]any{"totalCount": 0, "nodes": []any{}},
		"reviewThreads": map[string]any{"totalCount": 0, "nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false}},
	}
	encoded, _ := json.Marshal(map[string]any{"node_id": "PR_1", "title": title, "updated_at": updatedAt, "_gitcrawl_source": "graphql", "_graphql": node})
	return string(encoded)
}

func prThread(repoID int64, raw, title, updatedAtGH, now string) Thread {
	return Thread{
		RepoID: repoID, GitHubID: "PR_1", Number: 7, Kind: "pull_request", State: "open", Title: title, Body: "body",
		HTMLURL: "https://github.com/openclaw/gitcrawl/pull/7", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: raw, ContentHash: "hash-" + title, UpdatedAtGitHub: updatedAtGH,
		FirstPulledAt: now, LastPulledAt: now, UpdatedAt: now,
	}
}

type threadView struct {
	lastPulled, updatedAt, raw, title     string
	sequence, evidence                    int64
	complete                              int64
	contentSequence                       int64
	contentLastPulled, contentUpdatedAt   string
	evidenceSource, contentEvidenceSource string
}

func readThreadView(t testing.TB, st *Store, id int64) threadView {
	t.Helper()
	var v threadView
	if err := st.DB().QueryRow(`
		select v.last_pulled_at, v.updated_at, v.raw_json, v.title, v.observation_sequence,
			v.evidence_observation_sequence, v.evidence_complete, t.observation_sequence,
			t.last_pulled_at, t.updated_at, v.evidence_source_updated_at, t.evidence_source_updated_at
		from threads_current v join threads t on t.id = v.id where v.id = ?`, id).Scan(
		&v.lastPulled, &v.updatedAt, &v.raw, &v.title, &v.sequence, &v.evidence, &v.complete,
		&v.contentSequence, &v.contentLastPulled, &v.contentUpdatedAt, &v.evidenceSource, &v.contentEvidenceSource); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestRecordEfficiencyIsOptInAndLegacyLayoutUnchanged(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.RecordChecksEnabled() || st.hasTable(ctx, "thread_checks") || st.hasView(ctx, "threads_current") {
		t.Fatal("a new archive must keep the legacy layout until record checks are enabled")
	}
	if st.CurrentSource(ctx, "threads") != "threads" {
		t.Fatal("legacy archives read threads directly")
	}
}

func TestUnchangedThreadCheckWritesNoContentRow(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	raw := prRaw("CLEAN", "2026-10-30T01:00:00Z", "title")
	first, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "title", "2026-10-30T01:00:00Z", "2026-10-30T02:00:00Z"))
	if err != nil || !first.Applied {
		t.Fatalf("first observation: %+v %v", first, err)
	}
	before := tableWrites(t, st, "threads")
	checksBefore := tableWrites(t, st, "thread_checks")
	second, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "title", "2026-10-30T01:00:00Z", "2026-10-30T02:05:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.Applied || second.ID != first.ID || second.ObservationSequence <= first.ObservationSequence {
		t.Fatalf("an unchanged check is still an accepted observation: %+v", second)
	}
	if got := tableWrites(t, st, "threads") - before; got != 0 {
		t.Fatalf("unchanged check wrote %d thread content rows, want 0", got)
	}
	// One check-row update for the observation and one for its complete
	// evidence pointer, in the same transaction.
	if got := tableWrites(t, st, "thread_checks") - checksBefore; got < 1 || got > 2 {
		t.Fatalf("unchanged check wrote %d check-row updates, want 1-2", got)
	}
	v := readThreadView(t, st, first.ID)
	if v.lastPulled != "2026-10-30T02:05:00Z" || v.contentLastPulled != "2026-10-30T02:00:00Z" {
		t.Fatalf("check clock: view %q content %q", v.lastPulled, v.contentLastPulled)
	}
	if v.sequence != second.ObservationSequence || v.contentSequence != first.ObservationSequence || v.complete != 1 {
		t.Fatalf("observation order: view %d content %d complete %d", v.sequence, v.contentSequence, v.complete)
	}
	if v.updatedAt != "2026-10-30T02:05:00Z" || v.contentUpdatedAt != "2026-10-30T02:00:00Z" {
		t.Fatalf("updated_at: view %q content %q", v.updatedAt, v.contentUpdatedAt)
	}

	// A real content change writes exactly one content version.
	changed := prRaw("CLEAN", "2026-10-30T03:00:00Z", "new title")
	third, err := st.UpsertThreadObservation(ctx, prThread(repoID, changed, "new title", "2026-10-30T03:00:00Z", "2026-10-30T03:01:00Z"))
	if err != nil || !third.Applied {
		t.Fatalf("content change: %+v %v", third, err)
	}
	if got := tableWrites(t, st, "threads") - before; got != 1 {
		t.Fatalf("content change wrote %d thread rows, want 1", got)
	}
	v = readThreadView(t, st, first.ID)
	if v.title != "new title" || v.contentSequence != third.ObservationSequence || v.sequence != third.ObservationSequence {
		t.Fatalf("content change not applied: %+v", v)
	}
}

func TestMergeStateFlipIsAnEventNotAThreadVersion(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	first, err := st.UpsertThreadObservation(ctx, prThread(repoID, prRaw("CLEAN", at, "t"), "t", at, "2026-10-30T02:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	before := tableWrites(t, st, "threads")
	for i, status := range []string{"DIRTY", "DIRTY", "CLEAN", "UNKNOWN", "UNKNOWN"} {
		now := fmt.Sprintf("2026-10-30T02:%02d:00Z", i+1)
		if _, err := st.UpsertThreadObservation(ctx, prThread(repoID, prRaw(status, at, "t"), "t", at, now)); err != nil {
			t.Fatal(err)
		}
	}
	if got := tableWrites(t, st, "threads") - before; got != 0 {
		t.Fatalf("merge-state flips wrote %d thread rows, want 0", got)
	}
	rows, err := st.DB().Query(`select merge_state_status, mergeable_state, coalesce(mergeable, '') from pull_request_merge_state_events where thread_id = ? order by id`, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var status, state, mergeable string
		if err := rows.Scan(&status, &state, &mergeable); err != nil {
			t.Fatal(err)
		}
		got = append(got, status+"/"+state+"/"+mergeable)
	}
	want := []string{"CLEAN/clean/MERGEABLE", "DIRTY/dirty/CONFLICTING", "CLEAN/clean/MERGEABLE", "UNKNOWN/unknown/"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("merge-state events = %v, want %v", got, want)
	}
}

func TestEvidenceCompletenessFlagReplacesSequenceSign(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	raw := prRaw("CLEAN", at, "t")
	incomplete, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "t", at, "2026-10-30T02:00:00Z"), UpsertThreadOptions{IncompleteEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	v := readThreadView(t, st, incomplete.ID)
	if v.complete != 0 || v.sequence >= 0 || v.evidence != 0 {
		t.Fatalf("incomplete evidence: complete %d sequence %d evidence %d", v.complete, v.sequence, v.evidence)
	}
	var stored int64
	if err := st.DB().QueryRow(`select observation_sequence from thread_checks where thread_id = ?`, incomplete.ID).Scan(&stored); err != nil || stored <= 0 {
		t.Fatalf("thread_checks stores an absolute sequence: %d %v", stored, err)
	}
	complete, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "t", at, "2026-10-30T02:01:00Z"))
	if err != nil || !complete.EvidenceApplied {
		t.Fatalf("complete evidence: %+v %v", complete, err)
	}
	v = readThreadView(t, st, incomplete.ID)
	if v.complete != 1 || v.sequence != complete.ObservationSequence || v.evidence != complete.ObservationSequence || v.evidenceSource != at {
		t.Fatalf("complete evidence view: %+v", v)
	}
	if v.contentEvidenceSource != "" {
		t.Fatalf("evidence pointer must live in thread_checks, content row has %q", v.contentEvidenceSource)
	}
}

// TestRecordChecksMatchLegacyBookkeeping replays random observation streams
// against a legacy archive and a record-efficient archive and requires the
// same results and the same effective bookkeeping in threads_current.
func TestRecordChecksMatchLegacyBookkeeping(t *testing.T) {
	ctx := context.Background()
	for seed := int64(1); seed <= 25; seed++ {
		rng := rand.New(rand.NewSource(seed))
		legacy, err := Open(ctx, filepath.Join(t.TempDir(), "legacy.db"))
		if err != nil {
			t.Fatal(err)
		}
		split, _ := recordFixture(t)
		repo := Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: `{"id":1,"_graphql":{"updatedAt":"2026-10-30T00:00:00Z","description":"d"}}`, UpdatedAt: "2026-10-30T00:00:00Z"}
		legacyRepo, err := legacy.UpsertRepository(ctx, repo)
		if err != nil {
			t.Fatal(err)
		}
		sources := []string{"2026-10-30T01:00:00Z", "2026-10-30T01:00:00Z", "2026-10-30T02:00:00Z", "2026-10-30T03:00:00Z"}
		titles := []string{"a", "a", "b"}
		var explicit int64 = 1000
		for step := 0; step < 40; step++ {
			source := sources[rng.Intn(len(sources))]
			title := titles[rng.Intn(len(titles))]
			now := fmt.Sprintf("2026-10-30T04:%02d:%02dZ", step/60, step%60)
			options := UpsertThreadOptions{IncompleteEvidence: rng.Intn(2) == 0}
			if rng.Intn(4) == 0 {
				explicit += int64(rng.Intn(3)) - 1
				options.ObservationSequence = explicit
			}
			raw := prRaw("CLEAN", source, title)
			a, errA := legacy.UpsertThreadObservation(ctx, prThread(legacyRepo, raw, title, source, now), options)
			b, errB := split.UpsertThreadObservation(ctx, prThread(legacyRepo, raw, title, source, now), options)
			if (errA == nil) != (errB == nil) {
				t.Fatalf("seed %d step %d: errors differ: %v / %v", seed, step, errA, errB)
			}
			if errA != nil {
				continue
			}
			if a.Applied != b.Applied || a.EvidenceApplied != b.EvidenceApplied ||
				a.ObservationSequence != b.ObservationSequence || a.EvidenceObservationSequence != b.EvidenceObservationSequence {
				t.Fatalf("seed %d step %d: results differ: %+v / %+v", seed, step, a, b)
			}
			var la, lb [6]string
			q := `select title, coalesce(last_pulled_at,''), observation_sequence, evidence_observation_sequence, evidence_source_updated_at, coalesce(updated_at_gh,'') from %s where number = 7`
			if err := legacy.DB().QueryRow(fmt.Sprintf(q, "threads")).Scan(&la[0], &la[1], &la[2], &la[3], &la[4], &la[5]); err != nil {
				t.Fatal(err)
			}
			if err := split.DB().QueryRow(fmt.Sprintf(q, "threads_current")).Scan(&lb[0], &lb[1], &lb[2], &lb[3], &lb[4], &lb[5]); err != nil {
				t.Fatal(err)
			}
			if la != lb {
				t.Fatalf("seed %d step %d: bookkeeping differs:\nlegacy %v\nsplit  %v", seed, step, la, lb)
			}
		}
		legacy.Close()
	}
}

func TestRollbackContentWriteWinsOverOlderCheck(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	r, err := st.UpsertThreadObservation(ctx, prThread(repoID, prRaw("CLEAN", at, "t"), "t", at, "2026-10-30T02:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	// An older binary writes the legacy columns directly.
	if _, err := st.DB().Exec(`update threads set observation_sequence = ?, last_pulled_at = '2026-10-30T09:00:00Z', updated_at = '2026-10-30T09:00:00Z' where id = ?`, r.ObservationSequence+50, r.ID); err != nil {
		t.Fatal(err)
	}
	v := readThreadView(t, st, r.ID)
	if v.sequence != r.ObservationSequence+50 || v.lastPulled != "2026-10-30T09:00:00Z" {
		t.Fatalf("view must follow the newer content write: %+v", v)
	}
}

func TestPullRequestDetailWritesOnlyOnPROwnContentChange(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	thread, err := st.UpsertThreadObservation(ctx, prThread(repoID, prRaw("CLEAN", at, "t"), "t", at, "2026-10-30T02:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	detail := func(status, head, updated, fetched string) PullRequestDetail {
		var payload map[string]any
		_ = json.Unmarshal([]byte(prRaw(status, updated, "t")), &payload)
		payload["_graphql"].(map[string]any)["headRefOid"] = head
		payload["head"] = map[string]any{"sha": head}
		payload["mergeable_state"] = strings.ToLower(status)
		encoded, _ := json.Marshal(payload)
		return PullRequestDetail{ThreadID: thread.ID, RepoID: repoID, Number: 7, HeadSHA: head, BaseSHA: "def", MergeableState: strings.ToLower(status), Additions: 3, Deletions: 1, ChangedFiles: 2, RawJSON: string(encoded), FetchedAt: fetched, UpdatedAt: fetched}
	}
	families := PullRequestHydrationFamilies{Details: true}
	if err := st.UpsertPullRequestCacheFamilies(ctx, detail("CLEAN", "abc", at, "2026-10-30T02:00:00Z"), nil, nil, nil, nil, families); err != nil {
		t.Fatal(err)
	}
	before := tableWrites(t, st, "pull_request_details")
	events := tableWrites(t, st, "pull_request_merge_state_events")
	// Unchanged fetch, then merge-state flip and provider clock movement only.
	for i, d := range []PullRequestDetail{
		detail("CLEAN", "abc", at, "2026-10-30T02:01:00Z"),
		detail("DIRTY", "abc", "2026-10-30T05:00:00Z", "2026-10-30T02:02:00Z"),
	} {
		if err := st.UpsertPullRequestCacheFamilies(ctx, d, nil, nil, nil, nil, families); err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
	}
	if got := tableWrites(t, st, "pull_request_details") - before; got != 0 {
		t.Fatalf("unchanged fetches wrote %d detail rows, want 0", got)
	}
	if got := tableWrites(t, st, "pull_request_merge_state_events") - events; got != 1 {
		t.Fatalf("merge flip wrote %d events, want 1", got)
	}
	var fetched, mergeable, raw, contentFetched string
	if err := st.DB().QueryRow(`select v.fetched_at, v.mergeable_state, d.raw_json, d.fetched_at from pull_request_details_current v join pull_request_details d using(thread_id) where thread_id = ?`, thread.ID).Scan(&fetched, &mergeable, &raw, &contentFetched); err != nil {
		t.Fatal(err)
	}
	if fetched != "2026-10-30T02:02:00Z" || mergeable != "dirty" || contentFetched != "2026-10-30T02:00:00Z" {
		t.Fatalf("current detail: fetched %q mergeable %q content fetched %q", fetched, mergeable, contentFetched)
	}
	if strings.Contains(raw, `"reviewThreads"`) || strings.Contains(raw, `"body"`) || !strings.Contains(raw, GraphQLPullRequestEnvelopeRef) || !strings.Contains(raw, `"headRefOid":"abc"`) {
		t.Fatalf("detail envelope must keep only PR-own fields and reference the thread envelope: %s", raw)
	}
	// A head push is PR-own content: one version.
	if err := st.UpsertPullRequestCacheFamilies(ctx, detail("DIRTY", "fff", "2026-10-30T05:00:00Z", "2026-10-30T02:03:00Z"), nil, nil, nil, nil, families); err != nil {
		t.Fatal(err)
	}
	if got := tableWrites(t, st, "pull_request_details") - before; got != 1 {
		t.Fatalf("head change wrote %d detail rows, want 1", got)
	}
}

func TestReviewThreadsWriteOnlyOnContentChange(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	thread, err := st.UpsertThreadObservation(ctx, prThread(repoID, prRaw("CLEAN", at, "t"), "t", at, "2026-10-30T02:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	review := func(resolved bool, fetched string) []PullRequestReviewThread {
		return []PullRequestReviewThread{{ReviewThreadID: "RT_1", Path: "a.go", Line: 3, IsResolved: resolved, CommentsJSON: "[]", RawJSON: `{"id":"RT_1"}`, FetchedAt: fetched}}
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, thread.ID, "2026-10-30T02:00:00Z", review(false, "2026-10-30T02:00:00Z")); err != nil {
		t.Fatal(err)
	}
	before := tableWrites(t, st, "pull_request_review_threads")
	if err := st.UpsertPullRequestReviewThreads(ctx, thread.ID, "2026-10-30T02:10:00Z", review(false, "2026-10-30T02:10:00Z")); err != nil {
		t.Fatal(err)
	}
	if got := tableWrites(t, st, "pull_request_review_threads") - before; got != 0 {
		t.Fatalf("unchanged review-thread fetch wrote %d rows", got)
	}
	var fetched, content string
	if err := st.DB().QueryRow(`select fetched_at, content_fetched_at from pull_request_review_threads_current where thread_id = ?`, thread.ID).Scan(&fetched, &content); err != nil {
		t.Fatal(err)
	}
	if fetched != "2026-10-30T02:10:00Z" || content != "2026-10-30T02:00:00Z" {
		t.Fatalf("review thread fetch clock: %q / %q", fetched, content)
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, thread.ID, "2026-10-30T02:20:00Z", review(true, "2026-10-30T02:20:00Z")); err != nil {
		t.Fatal(err)
	}
	var revisions int
	_ = st.DB().QueryRow(`select count(*) from pull_request_review_thread_revisions where thread_id = ?`, thread.ID).Scan(&revisions)
	if got := tableWrites(t, st, "pull_request_review_threads") - before; got != 1 || revisions != 2 {
		t.Fatalf("resolution wrote %d rows and %d revisions, want 1 and 2", got, revisions)
	}
}

func TestRepositoryCheckDoesNotRewriteRepository(t *testing.T) {
	ctx := context.Background()
	st, _ := recordFixture(t)
	before := tableWrites(t, st, "repositories")
	checks := tableWrites(t, st, "repository_checks")
	repo := Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: `{"id":1,"_graphql":{"updatedAt":"2026-10-30T05:00:00Z","description":"d"}}`, UpdatedAt: "2026-10-30T05:00:00Z"}
	for i := 0; i < 5; i++ {
		if _, err := st.EnsureRepository(ctx, repo); err != nil {
			t.Fatal(err)
		}
	}
	if got := tableWrites(t, st, "repository_checks") - checks; got != 0 {
		t.Fatalf("per-item repository resolution wrote %d checks", got)
	}
	if _, err := st.UpsertRepository(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if got := tableWrites(t, st, "repositories") - before; got != 0 {
		t.Fatalf("provider activity rewrote the repository %d times", got)
	}
	var updated, provider string
	if err := st.DB().QueryRow(`select updated_at, provider_updated_at from repositories_current`).Scan(&updated, &provider); err != nil {
		t.Fatal(err)
	}
	if updated != "2026-10-30T05:00:00Z" || provider != "2026-10-30T05:00:00Z" {
		t.Fatalf("repository check: %q %q", updated, provider)
	}
	repo.RawJSON = `{"id":1,"_graphql":{"updatedAt":"2026-10-30T06:00:00Z","description":"changed"}}`
	if _, err := st.EnsureRepository(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if got := tableWrites(t, st, "repositories") - before; got != 1 {
		t.Fatalf("description change wrote %d repository rows, want 1", got)
	}
}

func TestAnalyticsCoverageChecksAndCountSeries(t *testing.T) {
	ctx := context.Background()
	st, _ := recordFixture(t)
	if err := st.SaveAnalyticsCoverage(ctx, "openclaw/gitcrawl", "2026-10-30T01:00:00Z", 10, 20); err != nil {
		t.Fatal(err)
	}
	before := tableWrites(t, st, "analytics_coverage")
	samples := tableWrites(t, st, "analytics_coverage_count_samples")
	if err := st.SaveAnalyticsCoverage(ctx, "openclaw/gitcrawl", "2026-10-30T01:02:00Z", 10, 20); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveAnalyticsCoverage(ctx, "openclaw/gitcrawl", "2026-10-30T01:04:00Z", 11, 20); err != nil {
		t.Fatal(err)
	}
	if got := tableWrites(t, st, "analytics_coverage") - before; got != 0 {
		t.Fatalf("coverage checks rewrote the content row %d times", got)
	}
	if got := tableWrites(t, st, "analytics_coverage_count_samples") - samples; got != 1 {
		t.Fatalf("one count change wrote %d samples, want 1", got)
	}
	var through string
	var issues int
	if err := st.DB().QueryRow(`select through, issues from analytics_coverage_current where repository = 'openclaw/gitcrawl'`).Scan(&through, &issues); err != nil {
		t.Fatal(err)
	}
	if through != "2026-10-30T01:04:00Z" || issues != 11 {
		t.Fatalf("current coverage: %q %d", through, issues)
	}
	status, err := st.AnalyticsIntegrityStatus(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatal(err)
	}
	if status["coverage"].(map[string]any)["through"] != "2026-10-30T01:04:00Z" {
		t.Fatalf("integrity status reads the current watermark: %v", status["coverage"])
	}
	if err := st.SetAnalyticsCoverageComplete(ctx, "openclaw/gitcrawl", false); err != nil {
		t.Fatal(err)
	}
	if err := st.SetAnalyticsCoverageComplete(ctx, "openclaw/gitcrawl", false); err != nil {
		t.Fatal(err)
	}
	if got := tableWrites(t, st, "analytics_coverage") - before; got != 1 {
		t.Fatalf("completeness change wrote %d rows, want 1", got)
	}
	progress := ReviewStateRecovery{Cursor: 5, Ceiling: 10, Scanned: 5}
	if err := st.SaveReviewStateCoverage(ctx, "openclaw/gitcrawl", progress); err != nil {
		t.Fatal(err)
	}
	reviewBefore := tableWrites(t, st, "analytics_review_state_coverage")
	if err := st.SaveReviewStateCoverage(ctx, "openclaw/gitcrawl", progress); err != nil {
		t.Fatal(err)
	}
	if got := tableWrites(t, st, "analytics_review_state_coverage") - reviewBefore; got != 0 {
		t.Fatalf("unchanged review-state progress rewrote %d rows", got)
	}
}

func TestChangeContractVersion2SkipsUnchangedUpdates(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, DefaultChangeContractRetention)
	start := changeContractSequence(t, s.DB())
	if _, err := s.DB().Exec(`update threads set title = title where id = 1`); err != nil {
		t.Fatal(err)
	}
	if changeContractSequence(t, s.DB()) == start {
		t.Fatal("version 1 journals every update")
	}
	repair, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{Version: ChangeContractVersionGuarded})
	if err != nil {
		t.Fatal(err)
	}
	if !repair.Upgraded || repair.Version != ChangeContractVersionGuarded || len(repair.Rearmed) == 0 {
		t.Fatalf("upgrade: %+v", repair)
	}
	status, err := s.ChangeContractStatus(ctx)
	if err != nil || status.ContractVersion != 2 || !status.Armed || status.Generation < 2 {
		t.Fatalf("status after upgrade: %+v %v", status, err)
	}
	start = changeContractSequence(t, s.DB())
	if _, err := s.DB().Exec(`update threads set title = title, updated_at = updated_at where id = 1`); err != nil {
		t.Fatal(err)
	}
	if got := changeContractSequence(t, s.DB()); got != start {
		t.Fatalf("version 2 journaled an unchanged update: %d -> %d", start, got)
	}
	if _, err := s.DB().Exec(`update threads set title = 'changed' where id = 1`); err != nil {
		t.Fatal(err)
	}
	entries := changeContractEntriesAfter(t, s.DB(), start)
	if len(entries) != 1 || entries[0].Table != "threads" || entries[0].Op != ChangeContractOpUpdate {
		t.Fatalf("version 2 must journal real updates: %+v", entries)
	}
	// Enabling again is idempotent.
	again, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{})
	if err != nil || len(again.Rearmed) != 0 || again.Upgraded {
		t.Fatalf("idempotent enable: %+v %v", again, err)
	}
	// A new column must be guarded: the writable-open repair re-arms.
	if _, err := s.DB().Exec(`alter table threads add column extra text`); err != nil {
		t.Fatal(err)
	}
	if repair, err := s.repairChangeContract(ctx); err != nil || len(repair.Rearmed) == 0 {
		t.Fatalf("repair after new column: %+v %v", repair, err)
	}
	start = changeContractSequence(t, s.DB())
	if _, err := s.DB().Exec(`update threads set extra = 'x' where id = 1`); err != nil {
		t.Fatal(err)
	}
	if changeContractSequence(t, s.DB()) == start {
		t.Fatal("a change to a new column must be journaled")
	}
	// Version 1 remains selectable.
	down, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{Version: ChangeContractVersion})
	if err != nil || down.Version != 1 || !down.Upgraded {
		t.Fatalf("downgrade: %+v %v", down, err)
	}
	if _, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{Version: 3}); err == nil {
		t.Fatal("unknown versions are refused")
	}
}

func TestBackfillSeedsChecksIdempotently(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "o", Name: "r", FullName: "o/r", RawJSON: `{"updated_at":"2026-01-01T00:00:00Z"}`, UpdatedAt: "2026-01-02T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	at := "2026-10-30T01:00:00Z"
	legacy, err := st.UpsertThreadObservation(ctx, prThread(repoID, prRaw("CLEAN", at, "t"), "t", at, "2026-10-30T02:00:00Z"), UpsertThreadOptions{IncompleteEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPullRequestCacheFamilies(ctx, PullRequestDetail{ThreadID: legacy.ID, RepoID: repoID, Number: 7, RawJSON: `{"mergeable":true,"mergeable_state":"clean"}`, FetchedAt: at, UpdatedAt: at}, nil, nil, nil, nil, PullRequestHydrationFamilies{Details: true}); err != nil {
		t.Fatal(err)
	}
	var contentBefore string
	_ = st.DB().QueryRow(`select group_concat(observation_sequence || last_pulled_at || updated_at) from threads`).Scan(&contentBefore)
	seed, err := st.EnableRecordEfficiency(ctx, "2026-10-30T03:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if seed.ThreadChecks != 1 || seed.DetailFetches != 1 || seed.MergeStateEvents != 1 || seed.RepositoryChecks != 1 {
		t.Fatalf("seed: %+v", seed)
	}
	again, err := st.BackfillRecordEfficiency(ctx)
	if err != nil || again != (RecordEfficiencyBackfill{}) {
		t.Fatalf("backfill must be idempotent: %+v %v", again, err)
	}
	var contentAfter string
	_ = st.DB().QueryRow(`select group_concat(observation_sequence || last_pulled_at || updated_at) from threads`).Scan(&contentAfter)
	if contentAfter != contentBefore {
		t.Fatal("backfill must not modify content rows")
	}
	v := readThreadView(t, st, legacy.ID)
	if v.sequence != -legacy.ObservationSequence || v.complete != 0 || v.lastPulled != "2026-10-30T02:00:00Z" {
		t.Fatalf("seeded view: %+v", v)
	}
	var mergeable, state string
	if err := st.DB().QueryRow(`select mergeable, mergeable_state from pull_request_merge_state_events where thread_id = ?`, legacy.ID).Scan(&mergeable, &state); err != nil || mergeable != "true" || state != "clean" {
		t.Fatalf("seeded merge state: %q %q %v", mergeable, state, err)
	}
	status, err := st.RecordEfficiencyStatus(ctx)
	if err != nil || !status.Enabled || status.ThreadsWithoutChecks != 0 || len(status.Views) != len(recordEfficiencyViews) {
		t.Fatalf("status: %+v %v", status, err)
	}
	// Reopening keeps the layout active.
	st.Close()
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.RecordChecksEnabled() {
		t.Fatal("record checks must persist across opens")
	}
	var schemaBefore, schemaAfter int64
	_ = reopened.DB().QueryRow(`pragma schema_version`).Scan(&schemaBefore)
	again2, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	again2.Close()
	_ = reopened.DB().QueryRow(`pragma schema_version`).Scan(&schemaAfter)
	if schemaBefore != schemaAfter {
		t.Fatal("a converged writable open must not change the schema")
	}
}

func TestDisableFoldsChecksIntoContentRows(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	raw := prRaw("CLEAN", at, "t")
	r, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "t", at, "2026-10-30T02:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	last, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "t", at, "2026-10-30T02:30:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DisableRecordEfficiency(ctx); err != nil {
		t.Fatal(err)
	}
	if st.RecordChecksEnabled() || st.hasView(ctx, "threads_current") || !st.hasTable(ctx, "thread_checks") {
		t.Fatal("disable removes views and marker and retains check tables")
	}
	var sequence int64
	var pulled string
	if err := st.DB().QueryRow(`select observation_sequence, last_pulled_at from threads where id = ?`, r.ID).Scan(&sequence, &pulled); err != nil {
		t.Fatal(err)
	}
	if sequence != last.ObservationSequence || pulled != "2026-10-30T02:30:00Z" {
		t.Fatalf("folded content row: %d %q", sequence, pulled)
	}
}

func TestPortablePruneFoldsAndDropsRecordChecks(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	raw := prRaw("CLEAN", at, "t")
	if _, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "t", at, "2026-10-30T02:00:00Z")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "t", at, "2026-10-30T02:30:00Z")); err != nil {
		t.Fatal(err)
	}
	if err := st.foldRecordEfficiencyChecks(ctx); err != nil {
		t.Fatal(err)
	}
	for _, table := range append([]string{"record_efficiency_state"}, recordEfficiencyTables...) {
		if st.hasTable(ctx, table) {
			t.Fatalf("%s must be dropped from derived copies", table)
		}
	}
	var pulled string
	if err := st.DB().QueryRow(`select last_pulled_at from threads where number = 7`).Scan(&pulled); err != nil || pulled != "2026-10-30T02:30:00Z" {
		t.Fatalf("folded last_pulled_at = %q %v", pulled, err)
	}
}

func TestNormalizationHelpers(t *testing.T) {
	if threadContentRaw(prRaw("CLEAN", "x", "t")) != threadContentRaw(prRaw("DIRTY", "x", "t")) {
		t.Fatal("merge state is not thread content")
	}
	if threadContentRaw(prRaw("CLEAN", "x", "t")) == threadContentRaw(prRaw("CLEAN", "y", "t")) {
		t.Fatal("provider updatedAt remains thread content")
	}
	if repositoryContentRaw(`{"stargazers_count":1,"description":"a"}`) != repositoryContentRaw(`{"stargazers_count":2,"description":"a"}`) {
		t.Fatal("repository counters are not content")
	}
	if repositoryContentRaw(`{"description":"a"}`) == repositoryContentRaw(`{"description":"b"}`) {
		t.Fatal("repository description is content")
	}
	if normalizeRawJSON("not json", []string{"a"}, nil) != "not json" {
		t.Fatal("invalid JSON compares verbatim")
	}
	if slimPullDetailRaw(`{"mergeable":true}`) != `{"mergeable":true}` {
		t.Fatal("REST payloads are retained verbatim")
	}
	if _, ok := mergeStateFromRaw(`{"title":"issue"}`); ok {
		t.Fatal("issues have no merge state")
	}
	if sqliteStoredCreateSQL("create view v as select 1") != "CREATE VIEW v as select 1" {
		t.Fatal("stored create SQL normalization")
	}
}

func TestExactCheckEvidenceSurvivesABAAndStaleWriter(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	a := " \n" + prRaw("CLEAN", at, "t") + "\n"
	b := prRaw("DIRTY", at, "t")
	for i, raw := range []string{a, b, a, a} {
		_, err := st.UpsertThreadObservation(ctx, prThread(repoID, raw, "t", at, fmt.Sprintf("2026-10-30T02:0%d:00Z", i)), UpsertThreadOptions{ObservationSequence: int64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
	}
	before := tableWrites(t, st, "threads")
	stale := prRaw("DIRTY", at, "stale")
	result, err := st.UpsertThreadObservation(ctx, prThread(repoID, stale, "stale", at, "2026-10-30T01:59:00Z"), UpsertThreadOptions{ObservationSequence: 1})
	if err != nil || result.Applied {
		t.Fatalf("stale writer: %+v %v", result, err)
	}
	if tableWrites(t, st, "threads") != before {
		t.Fatal("stale observation changed content")
	}
	rows, err := st.DB().Query(`select b.sha256 from record_check_observations o join record_evidence_blobs b on b.sha256=o.evidence_sha256 where o.source_table='threads' order by o.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []string{a, b, a, a, stale}
	var hashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, hash)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(hashes) != len(want) {
		t.Fatalf("lost observations: %d", len(hashes))
	}
	for i, hash := range hashes {
		raw, err := st.EvidenceRaw(ctx, hash)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != want[i] {
			t.Fatalf("evidence %d differs", i)
		}
	}
	var blobs int
	if err := st.DB().QueryRow(`select count(distinct evidence_sha256) from record_check_observations where source_table='threads'`).Scan(&blobs); err != nil {
		t.Fatal(err)
	}
	if blobs != 3 {
		t.Fatalf("identical checks did not share exact blobs: %d", blobs)
	}
}

func TestRepositoryCounterEvidenceAndUnknownPRFields(t *testing.T) {
	ctx := context.Background()
	st, _ := recordFixture(t)
	repo := Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", UpdatedAt: "2026-10-30T01:00:00Z", RawJSON: ` {"stargazers_count":1,"unknown":"keep"} `}
	id, err := st.UpsertRepository(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	before := tableWrites(t, st, "repositories")
	repo.RawJSON = ` {"stargazers_count":2,"unknown":"keep"} `
	if _, err = st.UpsertRepository(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if tableWrites(t, st, "repositories") != before {
		t.Fatal("counter changed content")
	}
	var n int
	if err = st.DB().QueryRow(`select count(*) from record_check_observations where source_table='repositories' and source_key=?`, fmt.Sprint(id)).Scan(&n); err != nil || n != 2 {
		t.Fatalf("counter observations lost: %d %v", n, err)
	}
	a := `{"_gitcrawl_source":"graphql","_graphql":{"unknown":1}}`
	b := `{"_gitcrawl_source":"graphql","_graphql":{"unknown":2}}`
	if pullDetailContentRaw(slimPullDetailRaw(a)) == pullDetailContentRaw(slimPullDetailRaw(b)) {
		t.Fatal("unknown PR field discarded")
	}
}

func TestRecordEfficiencyCurrentReadersAndSourceFirstEvidence(t *testing.T) {
	ctx := context.Background()
	st, repoID := recordFixture(t)
	at := "2026-10-30T01:00:00Z"
	now := "2026-10-30T02:00:00Z"
	thread := prThread(repoID, prRaw("CLEAN", at, "t"), "t", at, now)
	first, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 2})
	if err != nil {
		t.Fatal(err)
	}
	thread.LastPulledAt = "2026-10-30T02:05:00Z"
	thread.UpdatedAt = thread.LastPulledAt
	if _, err = st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 3}); err != nil {
		t.Fatal(err)
	}
	for _, numbers := range [][]int{nil, {7}} {
		rows, err := st.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repoID, IncludeClosed: true, Numbers: numbers})
		if err != nil || len(rows) != 1 || rows[0].LastPulledAt != thread.LastPulledAt {
			t.Fatalf("current threads: %+v %v", rows, err)
		}
	}
	d := PullRequestDetail{ThreadID: first.ID, RepoID: repoID, Number: 7, RawJSON: `{"mergeable_state":"clean"}`, MergeableState: "clean", FetchedAt: now, UpdatedAt: now}
	if err = st.UpsertPullRequestCache(ctx, d, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	d.RawJSON = `{"mergeable_state":"dirty"}`
	d.MergeableState = "dirty"
	d.FetchedAt = thread.LastPulledAt
	if err = st.UpsertPullRequestCache(ctx, d, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	direct, _, err := st.PullRequestDetailByThread(ctx, first.ID)
	if err != nil || direct.MergeableState != "dirty" || direct.FetchedAt != d.FetchedAt {
		t.Fatalf("current detail: %+v %v", direct, err)
	}
	cache, err := st.PullRequestCache(ctx, repoID, 7)
	if err != nil || cache.Detail.MergeableState != "dirty" {
		t.Fatalf("current cache: %+v %v", cache, err)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatal(err)
	}
	repo.UpdatedAt = "2026-10-30T03:00:00Z"
	if _, err = st.UpsertRepository(ctx, repo); err != nil {
		t.Fatal(err)
	}
	currentRepo, err := st.RepositoryByFullName(ctx, repo.FullName)
	if err != nil || currentRepo.UpdatedAt != repo.UpdatedAt {
		t.Fatalf("current repository: %+v %v", currentRepo, err)
	}
	repos, err := st.ListRepositories(ctx)
	if err != nil || len(repos) != 1 || repos[0].UpdatedAt != repo.UpdatedAt {
		t.Fatalf("current repositories: %+v %v", repos, err)
	}
	ro, err := OpenReadOnly(ctx, st.path)
	if err != nil {
		t.Fatal(err)
	}
	currentReadOnly, _, err := ro.PullRequestDetailByThread(ctx, first.ID)
	ro.Close()
	if err != nil || currentReadOnly.MergeableState != "dirty" {
		t.Fatalf("read-only overlay: %+v %v", currentReadOnly, err)
	}
	// Newer source time must beat a lower sequence for evidence, including after rollback folding.
	newer := "2026-10-30T01:01:00Z"
	if _, err = st.DB().Exec(`update thread_checks set evidence_source_updated_at=?,evidence_observation_sequence=1 where thread_id=?`, newer, first.ID); err != nil {
		t.Fatal(err)
	}
	v := readThreadView(t, st, first.ID)
	if v.evidenceSource != newer || v.evidence != 1 {
		t.Fatalf("source-first evidence: %+v", v)
	}
	if applied, err := st.reserveThreadCheckEvidence(ctx, first.ID, at, 4); err != nil || applied {
		t.Fatalf("stale source evidence accepted: %v %v", applied, err)
	}
	if _, err = st.PlanThreadPurge(ctx, "openclaw/gitcrawl", []int{7}); err == nil || !strings.Contains(err.Error(), "record-evidence ownership") {
		t.Fatalf("purge must refuse retained evidence: %v", err)
	}
}
