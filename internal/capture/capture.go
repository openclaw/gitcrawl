// Package capture exports a stable, code-free view of GitHub conversations.
package capture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

// SchemaV1 identifies the first stable Gitcrawl conversation capture.
const SchemaV1 = "gitcrawl.capture.v1"

// Capture is one deterministic repository conversation snapshot.
type Capture struct {
	Schema          string            `json:"schema"`
	ProducerVersion string            `json:"producer_version"`
	Repository      CaptureRepository `json:"repository"`
	RateLimit       RateLimit         `json:"rate_limit"`
	SyncedAt        string            `json:"synced_at"`
	Threads         []Thread          `json:"threads"`
}

// RateLimit is one sanitized GitHub quota observation.
type RateLimit struct {
	Resource          string  `json:"resource"`
	Limit             int     `json:"limit"`
	Remaining         int     `json:"remaining"`
	ResetAt           *string `json:"reset_at"`
	ObservedAt        string  `json:"observed_at"`
	RetryAfterSeconds *int    `json:"retry_after_seconds"`
}

// CaptureRepository identifies the exact GitHub repository in a capture.
type CaptureRepository struct {
	ID       string `json:"id"`
	FullName string `json:"full_name"`
}

// Thread is one current issue or pull-request conversation.
type Thread struct {
	CanonicalID       string    `json:"canonical_id"`
	Number            int       `json:"number"`
	Kind              string    `json:"kind"`
	State             string    `json:"state"`
	Title             string    `json:"title"`
	Body              string    `json:"body"`
	AuthorLogin       string    `json:"author_login,omitempty"`
	AuthorType        string    `json:"author_type,omitempty"`
	AuthorAssociation string    `json:"author_association,omitempty"`
	URL               string    `json:"url"`
	Labels            []string  `json:"labels"`
	Assignees         []string  `json:"assignees"`
	Draft             bool      `json:"draft,omitempty"`
	CreatedAt         string    `json:"created_at,omitempty"`
	UpdatedAt         string    `json:"updated_at"`
	ClosedAt          string    `json:"closed_at,omitempty"`
	MergedAt          string    `json:"merged_at,omitempty"`
	Comments          []Comment `json:"comments"`
	ContentHash       string    `json:"content_hash"`
}

// Comment is one code-free issue comment, review, or review comment.
type Comment struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	AuthorLogin string `json:"author_login,omitempty"`
	AuthorType  string `json:"author_type,omitempty"`
	Body        string `json:"body"`
	Bot         bool   `json:"bot,omitempty"`
	ReviewState string `json:"review_state,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	DeletedAt   string `json:"deleted_at,omitempty"`
}

// Build reads one settled local repository without exposing archive internals.
// The optional since instant filters threads by their GitHub update time.
func Build(
	ctx context.Context,
	st *store.Store,
	fullName string,
	producerVersion string,
	rateLimit RateLimit,
	since string,
) (Capture, error) {
	if err := validateRateLimit(rateLimit); err != nil {
		return Capture{}, err
	}
	var result Capture
	err := st.WithTx(ctx, func(snapshot *store.Store) error {
		var err error
		result, err = buildSnapshot(
			ctx,
			snapshot,
			fullName,
			producerVersion,
			rateLimit,
			since,
		)
		return err
	})
	return result, err
}

// buildSnapshot performs every capture read through one SQLite transaction.
func buildSnapshot(
	ctx context.Context,
	st *store.Store,
	fullName string,
	producerVersion string,
	rateLimit RateLimit,
	since string,
) (Capture, error) {
	repository, err := st.RepositoryByFullName(ctx, fullName)
	if err != nil {
		return Capture{}, err
	}
	if strings.TrimSpace(repository.GitHubRepoID) == "" {
		return Capture{}, fmt.Errorf(
			"capture repository %s has no stable GitHub repository id",
			fullName,
		)
	}
	if _, err := strconv.ParseUint(repository.GitHubRepoID, 10, 64); err != nil {
		return Capture{}, fmt.Errorf(
			"capture repository %s has an invalid GitHub repository id",
			fullName,
		)
	}
	sinceTime, err := parseSince(since)
	if err != nil {
		return Capture{}, err
	}
	syncedAt, err := captureSyncAt(ctx, st, repository.ID, sinceTime)
	if err != nil {
		return Capture{}, fmt.Errorf("capture repository %s: %w", fullName, err)
	}
	storedThreads, err := st.ListThreads(ctx, repository.ID, true)
	if err != nil {
		return Capture{}, err
	}
	threads := make([]Thread, 0, len(storedThreads))
	for _, storedThread := range storedThreads {
		if !sinceTime.IsZero() {
			updatedAt, err := parseRequiredTime(
				storedThread.UpdatedAtGitHub,
				fmt.Sprintf("thread #%d updated_at", storedThread.Number),
			)
			if err != nil {
				return Capture{}, err
			}
			if updatedAt.Before(sinceTime) {
				continue
			}
		}
		thread, err := buildThread(ctx, st, storedThread)
		if err != nil {
			return Capture{}, err
		}
		threads = append(threads, thread)
	}
	sort.Slice(threads, func(left, right int) bool {
		if threads[left].Number != threads[right].Number {
			return threads[left].Number < threads[right].Number
		}
		return threads[left].Kind < threads[right].Kind
	})
	return Capture{
		Schema:          SchemaV1,
		ProducerVersion: strings.TrimSpace(producerVersion),
		Repository: CaptureRepository{
			ID:       repository.GitHubRepoID,
			FullName: repository.FullName,
		},
		RateLimit: rateLimit,
		SyncedAt:  syncedAt.UTC().Format(time.RFC3339Nano),
		Threads:   threads,
	}, nil
}

func validateRateLimit(rateLimit RateLimit) error {
	if strings.TrimSpace(rateLimit.Resource) == "" ||
		rateLimit.Limit < 0 ||
		rateLimit.Remaining < 0 ||
		rateLimit.Remaining > rateLimit.Limit {
		return fmt.Errorf("capture rate limit observation is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, rateLimit.ObservedAt); err != nil {
		return fmt.Errorf("capture rate limit observation time is invalid")
	}
	if rateLimit.ResetAt != nil {
		if _, err := time.Parse(time.RFC3339Nano, *rateLimit.ResetAt); err != nil {
			return fmt.Errorf("capture rate limit reset time is invalid")
		}
	}
	if rateLimit.RetryAfterSeconds != nil &&
		(*rateLimit.RetryAfterSeconds < 0 || *rateLimit.RetryAfterSeconds > 86400) {
		return fmt.Errorf("capture rate limit retry guidance is invalid")
	}
	return nil
}

// captureSyncAt selects the newest successful all-state list run whose bounds
// cover the requested capture range.
func captureSyncAt(
	ctx context.Context,
	st *store.Store,
	repositoryID int64,
	captureSince time.Time,
) (time.Time, error) {
	runs, err := st.SuccessfulListSyncRuns(ctx, repositoryID, "all")
	if err != nil {
		return time.Time{}, err
	}
	for _, run := range runs {
		var bounds struct {
			RequestedSince string `json:"requested_since"`
			Limit          int    `json:"limit"`
		}
		statsJSON := strings.TrimSpace(run.StatsJSON)
		if statsJSON != "" && json.Unmarshal([]byte(statsJSON), &bounds) != nil {
			continue
		}
		if bounds.Limit != 0 {
			continue
		}
		syncSince := time.Time{}
		if strings.TrimSpace(bounds.RequestedSince) != "" {
			syncSince, err = time.Parse(
				time.RFC3339Nano,
				strings.TrimSpace(bounds.RequestedSince),
			)
			if err != nil {
				continue
			}
		}
		if captureSince.IsZero() {
			if !syncSince.IsZero() {
				continue
			}
		} else if !syncSince.IsZero() && syncSince.After(captureSince) {
			continue
		}
		finishedAt, err := time.Parse(
			time.RFC3339Nano,
			strings.TrimSpace(run.FinishedAt),
		)
		if err == nil {
			return finishedAt, nil
		}
	}
	return time.Time{}, fmt.Errorf(
		"no successful unbounded all-state list sync covers the capture range",
	)
}

// buildThread normalizes one stored conversation and hashes its semantic JSON.
func buildThread(
	ctx context.Context,
	st *store.Store,
	stored store.Thread,
) (Thread, error) {
	if strings.TrimSpace(stored.GitHubID) == "" {
		return Thread{}, fmt.Errorf(
			"capture thread #%d has no stable GitHub id",
			stored.Number,
		)
	}
	if stored.Kind != "issue" && stored.Kind != "pull_request" {
		return Thread{}, fmt.Errorf(
			"capture thread #%d has unsupported kind %q",
			stored.Number,
			stored.Kind,
		)
	}
	updatedAt, err := normalizedRequiredTime(
		stored.UpdatedAtGitHub,
		fmt.Sprintf("thread #%d updated_at", stored.Number),
	)
	if err != nil {
		return Thread{}, err
	}
	commentsSourceUpdatedAt, commentsSequence, found, err := st.ThreadChildObservation(
		ctx,
		stored.ID,
		store.ThreadChildComments,
	)
	if err != nil {
		return Thread{}, err
	}
	if !found || !sameInstant(commentsSourceUpdatedAt, stored.UpdatedAtGitHub) {
		return Thread{}, fmt.Errorf(
			"capture thread #%d has no complete comment observation for its current source revision",
			stored.Number,
		)
	}
	observedCommentIDs, found, err := st.ThreadChildObservationMemberIDs(
		ctx,
		stored.ID,
		store.ThreadChildComments,
		commentsSequence,
	)
	if err != nil {
		return Thread{}, err
	}
	if !found {
		return Thread{}, fmt.Errorf(
			"capture thread #%d comment observation has no exact member set",
			stored.Number,
		)
	}
	createdAt, err := normalizedOptionalTime(
		stored.CreatedAtGitHub,
		fmt.Sprintf("thread #%d created_at", stored.Number),
	)
	if err != nil {
		return Thread{}, err
	}
	closedAt, err := normalizedOptionalTime(
		stored.ClosedAtGitHub,
		fmt.Sprintf("thread #%d closed_at", stored.Number),
	)
	if err != nil {
		return Thread{}, err
	}
	mergedAt, err := normalizedOptionalTime(
		stored.MergedAtGitHub,
		fmt.Sprintf("thread #%d merged_at", stored.Number),
	)
	if err != nil {
		return Thread{}, err
	}
	comments, err := st.ListComments(ctx, stored.ID)
	if err != nil {
		return Thread{}, err
	}
	observed := make(map[int64]struct{}, len(observedCommentIDs))
	for _, commentID := range observedCommentIDs {
		observed[commentID] = struct{}{}
	}
	capturedComments := make([]Comment, 0, len(observedCommentIDs))
	for _, storedComment := range comments {
		if _, included := observed[storedComment.ID]; !included {
			continue
		}
		comment, err := buildComment(stored.Number, storedComment)
		if err != nil {
			return Thread{}, err
		}
		capturedComments = append(capturedComments, comment)
	}
	sort.Slice(capturedComments, func(left, right int) bool {
		if capturedComments[left].CreatedAt != capturedComments[right].CreatedAt {
			leftCreated, rightCreated := capturedComments[left].CreatedAt, capturedComments[right].CreatedAt
			if leftCreated == "" || rightCreated == "" {
				return leftCreated == ""
			}
			// Validated RFC3339Nano strings are not ordered by fractional-second precision.
			leftTime, _ := time.Parse(time.RFC3339Nano, leftCreated)
			rightTime, _ := time.Parse(time.RFC3339Nano, rightCreated)
			return leftTime.Before(rightTime)
		}
		if capturedComments[left].Kind != capturedComments[right].Kind {
			return capturedComments[left].Kind < capturedComments[right].Kind
		}
		return capturedComments[left].ID < capturedComments[right].ID
	})
	thread := Thread{
		CanonicalID:       stored.GitHubID,
		Number:            stored.Number,
		Kind:              stored.Kind,
		State:             stored.State,
		Title:             stored.Title,
		Body:              stored.Body,
		AuthorLogin:       stored.AuthorLogin,
		AuthorType:        stored.AuthorType,
		AuthorAssociation: stored.AuthorAssociation,
		URL:               stored.HTMLURL,
		Labels:            normalizedNames(stored.LabelsJSON, "name"),
		Assignees:         normalizedNames(stored.AssigneesJSON, "login"),
		Draft:             stored.IsDraft,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
		ClosedAt:          closedAt,
		MergedAt:          mergedAt,
		Comments:          capturedComments,
	}
	semantic, err := json.Marshal(thread)
	if err != nil {
		return Thread{}, fmt.Errorf("marshal thread #%d capture: %w", stored.Number, err)
	}
	sum := sha256.Sum256(semantic)
	thread.ContentHash = hex.EncodeToString(sum[:])
	return thread, nil
}

// sameInstant compares source timestamps after strict RFC3339 parsing.
func sameInstant(left string, right string) bool {
	leftTime, leftErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(left))
	rightTime, rightErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(right))
	return leftErr == nil && rightErr == nil && leftTime.Equal(rightTime)
}

// buildComment admits only the normalized conversation families.
func buildComment(threadNumber int, stored store.Comment) (Comment, error) {
	switch stored.CommentType {
	case "issue_comment", "pull_review", "pull_review_comment":
	default:
		return Comment{}, fmt.Errorf(
			"capture thread #%d has unsupported comment kind %q",
			threadNumber,
			stored.CommentType,
		)
	}
	if strings.TrimSpace(stored.GitHubID) == "" {
		return Comment{}, fmt.Errorf(
			"capture thread #%d has a comment without a stable GitHub id",
			threadNumber,
		)
	}
	createdAt, err := normalizedOptionalTime(
		stored.CreatedAtGitHub,
		fmt.Sprintf("thread #%d comment %s created_at", threadNumber, stored.GitHubID),
	)
	if err != nil {
		return Comment{}, err
	}
	updatedAt, err := normalizedOptionalTime(
		stored.UpdatedAtGitHub,
		fmt.Sprintf("thread #%d comment %s updated_at", threadNumber, stored.GitHubID),
	)
	if err != nil {
		return Comment{}, err
	}
	deletedAt, err := normalizedOptionalTime(
		stored.DeletedAt,
		fmt.Sprintf("thread #%d comment %s deleted_at", threadNumber, stored.GitHubID),
	)
	if err != nil {
		return Comment{}, err
	}
	body := stored.Body
	if stored.DeletedAt != "" {
		body = ""
	}
	return Comment{
		ID:          stored.GitHubID,
		Kind:        stored.CommentType,
		AuthorLogin: stored.AuthorLogin,
		AuthorType:  stored.AuthorType,
		Body:        body,
		Bot:         stored.IsBot,
		ReviewState: stored.ReviewState,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		DeletedAt:   deletedAt,
	}, nil
}

// normalizedNames reads string or object arrays without retaining raw JSON.
func normalizedNames(raw string, objectKey string) []string {
	var values []any
	if json.Unmarshal([]byte(raw), &values) != nil {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		var name string
		switch typed := value.(type) {
		case string:
			name = typed
		case map[string]any:
			name, _ = typed[objectKey].(string)
		}
		name = strings.TrimSpace(name)
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func parseSince(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	return parseRequiredTime(value, "capture since")
}

func parseRequiredTime(value string, label string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339: %w", label, err)
	}
	return parsed, nil
}

func normalizedRequiredTime(value string, label string) (string, error) {
	parsed, err := parseRequiredTime(value, label)
	if err != nil {
		return "", err
	}
	return parsed.UTC().Format(time.RFC3339Nano), nil
}

func normalizedOptionalTime(value string, label string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return "", fmt.Errorf("%s must be RFC3339: %w", label, err)
	}
	return parsed.UTC().Format(time.RFC3339Nano), nil
}
