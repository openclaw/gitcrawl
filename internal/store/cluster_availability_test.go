package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func availabilityFixture(t *testing.T) (*Store, time.Time, int64) {
	t.Helper()
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Now().UTC()
	at := now.Add(-time.Second).Format(time.RFC3339Nano)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", GitHubRepoID: "42", RawJSON: `{"node_id":"R_FIXTURE","private":false}`, UpdatedAt: at})
	if e != nil {
		t.Fatal(e)
	}
	for key, value := range map[string]string{"core_baseline_verified:openclaw/openclaw": "1", "through:openclaw/openclaw:issues": at, "through:openclaw/openclaw:pullRequests": at} {
		if e = s.SetAnalyticsState(ctx, key, value); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = s.DB().Exec("INSERT INTO analytics_coverage(repository,through,issues,pull_requests,complete,observed_at) VALUES('openclaw/openclaw',?,1,0,1,?)", at, at); e != nil {
		t.Fatal(e)
	}
	for _, op := range []string{"discover_issues", "discover_pullRequests"} {
		if e = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "openclaw/openclaw", Operation: op, StartedAt: at, FinishedAt: at, Status: "success", Evidence: json.RawMessage(`{}`)}); e != nil {
			t.Fatal(e)
		}
	}
	accepted := now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	id, e := s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: "I_ONE", Number: 1, Kind: "issue", State: "open", Title: "fixture", Body: "body", HTMLURL: "https://example.invalid/1", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{}`, ContentHash: "h", LastPulledAt: accepted, UpdatedAt: accepted, UpdatedAtGitHub: accepted})
	if e != nil {
		t.Fatal(e)
	}
	return s, now, id
}
func unavailableAttempt(t *testing.T, s *Store, n int, at time.Time, evidence string) {
	t.Helper()
	clock := at.UTC().Format(time.RFC3339Nano)
	if e := s.RecordAnalyticsAttempt(context.Background(), AnalyticsAttempt{Repository: "openclaw/openclaw", Number: n, Operation: "graphql_history", StartedAt: clock, FinishedAt: clock, Status: "failed", ErrorClass: "partial_response", Evidence: json.RawMessage(evidence)}); e != nil {
		t.Fatal(e)
	}
}

const isolatedUnavailableFixture = `{"version":1,"graphql_errors":{"count":1,"truncated":false,"items":[{"type":"NOT_FOUND","path":["repository","n0"],"path_truncated":false}]},"structure":{"repository":{"id":"R_FIXTURE","n0":null}}}`

func TestClusterAvailabilityRecoveryAndRetryClockNoop(t *testing.T) {
	s, now, id := availabilityFixture(t)
	ctx := context.Background()
	full, e := s.ReadClusterAvailability(ctx, "openclaw/openclaw", now)
	if e != nil || full.Scope.Mode != "full-native" {
		t.Fatal(full, e)
	}
	unavailableAttempt(t, s, 1, now.Add(-time.Minute), isolatedUnavailableFixture)
	a, e := s.ReadClusterAvailability(ctx, "openclaw/openclaw", now)
	if e != nil || a.Scope.Mode != "available-subset" || !a.Blocked[id] || a.Scope.Population.Unavailable != 1 || a.Scope.ProviderComplete {
		t.Fatal(a, e)
	}
	unavailableAttempt(t, s, 1, now.Add(-time.Second), isolatedUnavailableFixture)
	b, e := s.ReadClusterAvailability(ctx, "openclaw/openclaw", now)
	if e != nil || b.SemanticDigest != a.SemanticDigest || b.Unavailable[0].AttemptID == a.Unavailable[0].AttemptID {
		t.Fatal("retry clocks changed semantic admission", e)
	}
	clock := now.Format(time.RFC3339Nano)
	if e = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "openclaw/openclaw", Number: 1, Operation: "graphql_history", StartedAt: clock, FinishedAt: clock, Status: "success", Evidence: json.RawMessage(`{}`)}); e != nil {
		t.Fatal(e)
	}
	if e = s.SetAnalyticsCoverageComplete(ctx, "openclaw/openclaw", true); e != nil {
		t.Fatal(e)
	}
	recovered, e := s.ReadClusterAvailability(ctx, "openclaw/openclaw", now)
	if e != nil || recovered.Scope.Mode != "full-native" || recovered.SemanticDigest != full.SemanticDigest {
		t.Fatal("full recovery", e)
	}
}

func TestClusterAvailabilityRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, evidence string
		sql            string
	}{
		{"wrong_repo", `{"version":1,"graphql_errors":{"count":1,"items":[{"type":"NOT_FOUND","path":["repository","n0"]}]},"structure":{"repository":{"id":"R_OTHER","n0":null}}}`, ""},
		{"auth", `{"version":1,"graphql_errors":{"count":1,"items":[{"type":"FORBIDDEN","path":["repository","n0"]}]},"structure":{"repository":{"id":"R_FIXTURE","n0":null}}}`, ""},
		{"ambiguous_alias", `{"version":1,"graphql_errors":{"count":1,"items":[{"type":"NOT_FOUND","path":["repository","n0"]}]},"structure":{"repository":{"id":"R_FIXTURE","n0":null,"n1":null}}}`, ""},
		{"private", isolatedUnavailableFixture, `UPDATE repositories SET raw_json='{"private":true,"node_id":"R_FIXTURE"}'`},
		{"stale_frontier", isolatedUnavailableFixture, `UPDATE analytics_coverage SET through='2020-01-01T00:00:00Z'`},
		{"missing_lane", isolatedUnavailableFixture, `DELETE FROM analytics_fetch_attempts WHERE operation='discover_issues'`},
		{"future_progress", isolatedUnavailableFixture, `UPDATE analytics_coverage SET observed_at='2999-01-01T00:00:00Z'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, now, _ := availabilityFixture(t)
			unavailableAttempt(t, s, 1, now.Add(-time.Minute), tc.evidence)
			if tc.sql != "" {
				if _, e := s.DB().Exec(tc.sql); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := s.ReadClusterAvailability(context.Background(), "openclaw/openclaw", now); e == nil {
				t.Fatal("unproven scope admitted")
			}
		})
	}
}
func TestClusterAvailabilityUnmaterializedAndGlobalFailure(t *testing.T) {
	s, now, _ := availabilityFixture(t)
	ctx := context.Background()
	unavailableAttempt(t, s, 99, now.Add(-time.Minute), isolatedUnavailableFixture)
	a, e := s.ReadClusterAvailability(ctx, "openclaw/openclaw", now)
	if e != nil || a.Scope.Population.UnmaterializedUnavailable != 1 || len(a.Blocked) != 0 {
		t.Fatal(a, e)
	}
	clock := now.Format(time.RFC3339Nano)
	if e = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "openclaw/openclaw", Operation: "discover_issues", StartedAt: clock, FinishedAt: clock, Status: "failed", ErrorClass: "partial_response", Evidence: json.RawMessage(isolatedUnavailableFixture)}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ReadClusterAvailability(ctx, "openclaw/openclaw", now); e == nil {
		t.Fatal("global failure quarantined as item")
	}
}
