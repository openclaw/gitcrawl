package syncer

import (
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func mapPullChecks(threadID int64, rows []map[string]any, fetchedAt string) []store.PullRequestCheck {
	out := make([]store.PullRequestCheck, 0, len(rows))
	for _, row := range rows {
		name := stringValue(row["name"])
		if name == "" {
			continue
		}
		out = append(out, store.PullRequestCheck{
			ThreadID:     threadID,
			Name:         name,
			Status:       stringValue(row["status"]),
			Conclusion:   stringValue(row["conclusion"]),
			DetailsURL:   stringValue(row["details_url"]),
			WorkflowName: nestedString(row, "check_suite", "app", "name"),
			StartedAt:    stringValue(row["started_at"]),
			CompletedAt:  stringValue(row["completed_at"]),
			RawJSON:      mustJSON(row),
			FetchedAt:    fetchedAt,
		})
	}
	return out
}

func mapWorkflowRuns(repoID int64, rows []map[string]any, fetchedAt string) []store.WorkflowRun {
	out := make([]store.WorkflowRun, 0, len(rows))
	for _, row := range rows {
		runID := jsonID(row["id"])
		if runID == "" {
			continue
		}
		out = append(out, store.WorkflowRun{
			RepoID:       repoID,
			RunID:        runID,
			RunNumber:    intValue(row["run_number"]),
			HeadBranch:   stringValue(row["head_branch"]),
			HeadSHA:      stringValue(row["head_sha"]),
			Status:       stringValue(row["status"]),
			Conclusion:   stringValue(row["conclusion"]),
			WorkflowName: stringValue(row["name"]),
			Event:        stringValue(row["event"]),
			HTMLURL:      stringValue(row["html_url"]),
			CreatedAtGH:  stringValue(row["created_at"]),
			UpdatedAtGH:  stringValue(row["updated_at"]),
			RawJSON:      mustJSON(row),
			FetchedAt:    fetchedAt,
		})
	}
	return out
}

func workflowSnapshotSourceUpdatedAt(rows []map[string]any) string {
	sourceUpdatedAt := ""
	for _, row := range rows {
		sourceUpdatedAt = latestWorkflowTimestamp(
			sourceUpdatedAt,
			stringValue(row["updated_at"]),
			stringValue(row["created_at"]),
		)
	}
	return sourceUpdatedAt
}

func latestWorkflowTimestamp(values ...string) string {
	latestValue := ""
	var latestTime time.Time
	for _, value := range values {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			continue
		}
		if latestValue == "" || parsed.After(latestTime) {
			latestValue = value
			latestTime = parsed
		}
	}
	return latestValue
}

func nestedString(row map[string]any, path ...string) string {
	var current any = row
	for _, key := range path {
		typed, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = typed[key]
	}
	return stringValue(current)
}
