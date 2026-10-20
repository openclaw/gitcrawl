package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestGraphQLOwnerCancellationKeepsEveryObligation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		owner        bool
		err          error
		wantAttempts int
	}{
		{"owner", true, context.Canceled, 0},
		{"child_without_parent", false, context.Canceled, 1},
		{"owner_plus_partial", true, errors.Join(context.Canceled, errors.New("partial response")), 1},
		{"owner_plus_deadline", true, errors.Join(context.Canceled, context.DeadlineExceeded), 1},
		{"provider_partial", true, &gh.HistoryFailure{Stage: "partial_response", Cause: errors.New("rejected")}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			at := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
			if err = st.SaveAnalyticsCoverage(ctx, "fixture/repo", at.Format(time.RFC3339Nano), 2, 0); err != nil {
				t.Fatal(err)
			}
			client := historyFixtureClient{err: tc.err}
			if tc.owner {
				client.beforeFetch = cancel
			}
			syncer := New(client, st)
			syncer.now = func() time.Time { return at }
			_, err = syncer.Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{11, 12}, State: "all", GraphQLHistory: true, IncludeComments: true, IncludePRMetadata: true})
			if err == nil {
				t.Fatal("failed read accepted")
			}
			var count, minAttempts, maxAttempts, complete int
			if err = st.DB().QueryRow("select count(*),min(attempts),max(attempts) from analytics_retries where resolved_at is null").Scan(&count, &minAttempts, &maxAttempts); err != nil {
				t.Fatal(err)
			}
			if count != 2 || minAttempts != tc.wantAttempts || maxAttempts != tc.wantAttempts {
				t.Fatalf("lost or misclassified obligations: count=%d attempts=%d..%d", count, minAttempts, maxAttempts)
			}
			var evidence string
			if err = st.DB().QueryRow("select evidence_json from analytics_fetch_attempts order by id limit 1").Scan(&evidence); err != nil {
				t.Fatal(err)
			}
			var binding struct {
				Parent bool `json:"parent_context_cancelled"`
			}
			if err = json.Unmarshal([]byte(evidence), &binding); err != nil {
				t.Fatal(err)
			}
			if binding.Parent != (tc.wantAttempts == 0) {
				t.Fatalf("incorrect parent binding: %s", evidence)
			}
			due, err := st.DueAnalyticsRetries(context.Background(), "fixture/repo", at.Add(31*time.Second).Format(time.RFC3339Nano), 8, "graphql_history")
			if err != nil || len(due) != 2 || due[0] != 11 || due[1] != 12 {
				t.Fatalf("resume lost required work: %v %v", due, err)
			}
			if err = st.DB().QueryRow("select complete from analytics_coverage").Scan(&complete); err != nil || complete != 0 {
				t.Fatalf("incomplete input claimed complete: %d %v", complete, err)
			}
		})
	}
}
