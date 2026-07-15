package syncer

import (
	"fmt"
	"strings"
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

func workflowSnapshotOrder(rows []map[string]any) (string, map[string]string, error) {
	sourceUpdatedAt := ""
	byRunID := make(map[string]string, len(rows))
	for _, row := range rows {
		runID := jsonID(row["id"])
		if runID == "" {
			continue
		}
		if _, exists := byRunID[runID]; exists {
			return "", nil, fmt.Errorf("workflow snapshot contains duplicate run %s", runID)
		}
		runSourceUpdatedAt, err := workflowRunTimestamp(
			stringValue(row["updated_at"]),
			stringValue(row["created_at"]),
		)
		if err != nil {
			return "", nil, fmt.Errorf("workflow run %s source: %w", runID, err)
		}
		byRunID[runID] = runSourceUpdatedAt
		sourceUpdatedAt, err = latestWorkflowTimestamp(
			sourceUpdatedAt,
			runSourceUpdatedAt,
		)
		if err != nil {
			return "", nil, err
		}
	}
	return sourceUpdatedAt, byRunID, nil
}

func latestWorkflowTimestamp(values ...string) (string, error) {
	latestValue := ""
	var latestTime time.Time
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return "", fmt.Errorf("invalid timestamp %q", value)
		}
		if latestValue == "" || parsed.After(latestTime) {
			latestValue = value
			latestTime = parsed
		}
	}
	return latestValue, nil
}

func workflowRunTimestamp(updatedAt, createdAt string) (string, error) {
	value, err := latestWorkflowTimestamp(updatedAt, createdAt)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", fmt.Errorf("missing created_at and updated_at")
	}
	return value, nil
}

func workflowTimestampBefore(incoming, current string) bool {
	if strings.TrimSpace(current) == "" {
		return false
	}
	incomingTime, incomingErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(incoming))
	currentTime, currentErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(current))
	return incomingErr == nil && currentErr == nil && incomingTime.Before(currentTime)
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
