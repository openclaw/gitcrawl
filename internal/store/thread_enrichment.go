package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const ThreadFingerprintAlgorithmVersion = "thread-fingerprint-v2"

var (
	fingerprintTokenPattern = regexp.MustCompile(`[A-Za-z0-9]+`)
	fingerprintRefPattern   = regexp.MustCompile(`(?i)(?:https?://github\.com/[^/\s]+/[^/\s]+/(?:issues|pull)/|(?:issues|pull)/|#)([1-9][0-9]*)`)
)

type ThreadRevision struct {
	ID              int64  `json:"id"`
	ThreadID        int64  `json:"thread_id"`
	SourceUpdatedAt string `json:"source_updated_at,omitempty"`
	ContentHash     string `json:"content_hash"`
	TitleHash       string `json:"title_hash"`
	BodyHash        string `json:"body_hash"`
	LabelsHash      string `json:"labels_hash"`
	EvidenceJSON    string `json:"-"`
	CreatedAt       string `json:"created_at"`
}

type ThreadFingerprint struct {
	ID                int64  `json:"id"`
	ThreadRevisionID  int64  `json:"thread_revision_id"`
	AlgorithmVersion  string `json:"algorithm_version"`
	FingerprintHash   string `json:"fingerprint_hash"`
	FingerprintSlug   string `json:"fingerprint_slug"`
	TitleTokensJSON   string `json:"title_tokens_json"`
	BodyTokenHash     string `json:"body_token_hash"`
	LinkedRefsJSON    string `json:"linked_refs_json"`
	FileSetHash       string `json:"file_set_hash"`
	ModuleBucketsJSON string `json:"module_buckets_json"`
	SimHash64         string `json:"simhash64"`
	FeatureJSON       string `json:"feature_json"`
	CreatedAt         string `json:"created_at"`
}

type ThreadEvidence struct {
	Thread        Thread
	Comments      []Comment
	Detail        *PullRequestDetail
	Files         []PullRequestFile
	Commits       []PullRequestCommit
	Checks        []PullRequestCheck
	WorkflowRuns  []WorkflowRun
	ReviewThreads []PullRequestReviewThread
}

type ThreadEnrichmentResult struct {
	RevisionID          int64 `json:"revision_id"`
	RevisionCreated     bool  `json:"revision_created"`
	FingerprintUpserted bool  `json:"fingerprint_upserted"`
}

type canonicalThreadEvidence struct {
	Version           string                  `json:"version"`
	Kind              string                  `json:"kind"`
	State             string                  `json:"state"`
	Title             string                  `json:"title"`
	Body              string                  `json:"body"`
	Labels            []string                `json:"labels"`
	Assignees         []string                `json:"assignees"`
	AuthorLogin       string                  `json:"author_login"`
	AuthorType        string                  `json:"author_type"`
	AuthorAssociation string                  `json:"author_association"`
	IsDraft           bool                    `json:"is_draft"`
	BaseSHA           string                  `json:"base_sha,omitempty"`
	HeadSHA           string                  `json:"head_sha,omitempty"`
	MergeableState    string                  `json:"mergeable_state,omitempty"`
	Additions         int                     `json:"additions,omitempty"`
	Deletions         int                     `json:"deletions,omitempty"`
	ChangedFiles      int                     `json:"changed_files,omitempty"`
	Comments          []canonicalComment      `json:"comments,omitempty"`
	Files             []canonicalChangedFile  `json:"files,omitempty"`
	Commits           []canonicalCommit       `json:"commits,omitempty"`
	Checks            []canonicalCheck        `json:"checks,omitempty"`
	WorkflowRuns      []canonicalWorkflowRun  `json:"workflow_runs,omitempty"`
	ReviewThreads     []canonicalReviewThread `json:"review_threads,omitempty"`
}

type canonicalComment struct {
	GitHubID        string `json:"github_id"`
	CommentType     string `json:"comment_type"`
	AuthorLogin     string `json:"author_login,omitempty"`
	AuthorType      string `json:"author_type,omitempty"`
	Body            string `json:"body"`
	IsBot           bool   `json:"is_bot"`
	ReviewState     string `json:"review_state,omitempty"`
	CreatedAtGitHub string `json:"created_at_gh,omitempty"`
	UpdatedAtGitHub string `json:"updated_at_gh,omitempty"`
}

type canonicalChangedFile struct {
	Path         string `json:"path"`
	Status       string `json:"status,omitempty"`
	PreviousPath string `json:"previous_path,omitempty"`
	PatchHash    string `json:"patch_hash,omitempty"`
}

type canonicalCommit struct {
	SHA     string `json:"sha"`
	Subject string `json:"subject,omitempty"`
}

type canonicalCheck struct {
	Name         string `json:"name"`
	Status       string `json:"status,omitempty"`
	Conclusion   string `json:"conclusion,omitempty"`
	WorkflowName string `json:"workflow_name,omitempty"`
	DetailsURL   string `json:"details_url,omitempty"`
}

type canonicalWorkflowRun struct {
	RunID        string `json:"run_id"`
	RunNumber    int    `json:"run_number,omitempty"`
	HeadSHA      string `json:"head_sha,omitempty"`
	Status       string `json:"status,omitempty"`
	Conclusion   string `json:"conclusion,omitempty"`
	WorkflowName string `json:"workflow_name,omitempty"`
	Event        string `json:"event,omitempty"`
	HTMLURL      string `json:"html_url,omitempty"`
}

type canonicalReviewThread struct {
	ID                    string `json:"id"`
	Path                  string `json:"path,omitempty"`
	Line                  int    `json:"line,omitempty"`
	StartLine             int    `json:"start_line,omitempty"`
	IsResolved            bool   `json:"is_resolved"`
	IsOutdated            bool   `json:"is_outdated"`
	FirstAuthorLogin      string `json:"first_author_login,omitempty"`
	FirstCommentBody      string `json:"first_comment_body,omitempty"`
	FirstCommentUpdatedAt string `json:"first_comment_updated_at,omitempty"`
	CommentsJSON          string `json:"comments_json,omitempty"`
}

func (s *Store) UpsertThreadRevisionAndFingerprint(ctx context.Context, evidence ThreadEvidence, createdAt string) (ThreadEnrichmentResult, error) {
	if s.queries != nil {
		return s.upsertThreadRevisionAndFingerprint(ctx, evidence, createdAt)
	}
	var result ThreadEnrichmentResult
	err := s.WithTx(ctx, func(tx *Store) error {
		var err error
		result, err = tx.upsertThreadRevisionAndFingerprint(ctx, evidence, createdAt)
		return err
	})
	return result, err
}

func (s *Store) upsertThreadRevisionAndFingerprint(ctx context.Context, evidence ThreadEvidence, createdAt string) (ThreadEnrichmentResult, error) {
	if evidence.Thread.ID <= 0 {
		return ThreadEnrichmentResult{}, fmt.Errorf("thread id must be positive")
	}
	if strings.TrimSpace(createdAt) == "" {
		createdAt = time.Now().UTC().Format(timeLayout)
	}
	revision, fingerprint := buildThreadEnrichment(evidence, createdAt)
	evidenceBlobID, err := s.upsertThreadRevisionEvidenceBlob(ctx, revision)
	if err != nil {
		return ThreadEnrichmentResult{}, err
	}
	var latestID int64
	var latestHash, latestSourceUpdatedAt string
	err = s.q().QueryRowContext(ctx, `
		select id, content_hash, coalesce(source_updated_at, '')
		from thread_revisions
		where thread_id = ?
		order by gitcrawl_timestamp_key(coalesce(nullif(source_updated_at, ''), created_at)) desc, id desc
		limit 1
	`, revision.ThreadID).Scan(&latestID, &latestHash, &latestSourceUpdatedAt)
	if err != nil && err != sql.ErrNoRows {
		return ThreadEnrichmentResult{}, fmt.Errorf("read latest thread revision: %w", err)
	}
	var observedID int64
	observedErr := s.q().QueryRowContext(ctx, `
		select id
		from thread_revisions
		where thread_id = ?
			and content_hash = ?
			and coalesce(source_updated_at, '') = ?
		order by id desc
		limit 1
	`, revision.ThreadID, revision.ContentHash, revision.SourceUpdatedAt).Scan(&observedID)
	if observedErr != nil && observedErr != sql.ErrNoRows {
		return ThreadEnrichmentResult{}, fmt.Errorf("read matching thread revision observation: %w", observedErr)
	}

	created := false
	switch {
	case observedErr == nil &&
		(observedID == latestID || timestampBefore(revision.SourceUpdatedAt, latestSourceUpdatedAt)):
		revision.ID = observedID
		if _, err := s.q().ExecContext(ctx, `
			update thread_revisions
			set raw_json_blob_id = ?
			where id = ?
		`, evidenceBlobID, revision.ID); err != nil {
			return ThreadEnrichmentResult{}, fmt.Errorf("refresh matching thread revision observation: %w", err)
		}
	case err == nil && latestHash == revision.ContentHash:
		revision.ID = latestID
		refreshedSourceUpdatedAt := latestTimestamp(latestSourceUpdatedAt, revision.SourceUpdatedAt)
		if _, err := s.q().ExecContext(ctx, `
			update thread_revisions
			set source_updated_at = ?, raw_json_blob_id = ?
			where id = ?
		`, nullString(refreshedSourceUpdatedAt), evidenceBlobID, revision.ID); err != nil {
			return ThreadEnrichmentResult{}, fmt.Errorf("refresh thread revision evidence: %w", err)
		}
	default:
		created = true
		insert, err := s.q().ExecContext(ctx, `
			insert into thread_revisions(
				thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, raw_json_blob_id, created_at
			)
			values(?, ?, ?, ?, ?, ?, ?, ?)
		`, revision.ThreadID, nullString(revision.SourceUpdatedAt), revision.ContentHash, revision.TitleHash, revision.BodyHash, revision.LabelsHash, evidenceBlobID, revision.CreatedAt)
		if err != nil {
			return ThreadEnrichmentResult{}, fmt.Errorf("insert thread revision: %w", err)
		}
		revision.ID, err = insert.LastInsertId()
		if err != nil {
			return ThreadEnrichmentResult{}, fmt.Errorf("read inserted thread revision id: %w", err)
		}
	}
	fingerprint.ThreadRevisionID = revision.ID

	var existingHash string
	err = s.q().QueryRowContext(ctx, `
		select fingerprint_hash
		from thread_fingerprints
		where thread_revision_id = ? and algorithm_version = ?
	`, fingerprint.ThreadRevisionID, fingerprint.AlgorithmVersion).Scan(&existingHash)
	if err != nil && err != sql.ErrNoRows {
		return ThreadEnrichmentResult{}, fmt.Errorf("read thread fingerprint: %w", err)
	}
	upserted := err == sql.ErrNoRows || existingHash != fingerprint.FingerprintHash
	if _, err := s.q().ExecContext(ctx, `
		insert into thread_fingerprints(
			thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug,
			title_tokens_json, body_token_hash, linked_refs_json, file_set_hash,
			module_buckets_json, simhash64, feature_json, created_at
		)
		values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(thread_revision_id, algorithm_version) do update set
			fingerprint_hash = excluded.fingerprint_hash,
			fingerprint_slug = excluded.fingerprint_slug,
			title_tokens_json = excluded.title_tokens_json,
			body_token_hash = excluded.body_token_hash,
			linked_refs_json = excluded.linked_refs_json,
			file_set_hash = excluded.file_set_hash,
			module_buckets_json = excluded.module_buckets_json,
			simhash64 = excluded.simhash64,
			feature_json = excluded.feature_json
	`, fingerprint.ThreadRevisionID, fingerprint.AlgorithmVersion, fingerprint.FingerprintHash, fingerprint.FingerprintSlug,
		fingerprint.TitleTokensJSON, fingerprint.BodyTokenHash, fingerprint.LinkedRefsJSON, fingerprint.FileSetHash,
		fingerprint.ModuleBucketsJSON, fingerprint.SimHash64, fingerprint.FeatureJSON, fingerprint.CreatedAt); err != nil {
		return ThreadEnrichmentResult{}, fmt.Errorf("upsert thread fingerprint: %w", err)
	}
	return ThreadEnrichmentResult{
		RevisionID:          revision.ID,
		RevisionCreated:     created,
		FingerprintUpserted: upserted,
	}, nil
}

func (s *Store) upsertThreadRevisionEvidenceBlob(ctx context.Context, revision ThreadRevision) (int64, error) {
	var blobID int64
	if err := s.q().QueryRowContext(ctx, `
		insert into blobs(sha256, media_type, compression, size_bytes, storage_kind, inline_text, created_at)
		values(?, 'application/json', 'none', ?, 'inline', ?, ?)
		on conflict(sha256) do update set
			media_type = excluded.media_type,
			compression = excluded.compression,
			size_bytes = excluded.size_bytes,
			storage_kind = excluded.storage_kind,
			storage_path = null,
			inline_text = excluded.inline_text
		returning id
	`, revision.ContentHash, len([]byte(revision.EvidenceJSON)), revision.EvidenceJSON, revision.CreatedAt).Scan(&blobID); err != nil {
		return 0, fmt.Errorf("store thread revision evidence: %w", err)
	}
	return blobID, nil
}

func buildThreadEnrichment(evidence ThreadEvidence, createdAt string) (ThreadRevision, ThreadFingerprint) {
	thread := evidence.Thread
	labels := canonicalNameList(thread.LabelsJSON, "name")
	assignees := canonicalNameList(thread.AssigneesJSON, "login")
	comments := canonicalComments(evidence.Comments)
	files := canonicalChangedFiles(evidence.Files)
	commits := canonicalCommits(evidence.Commits)
	checks := canonicalChecks(evidence.Checks)
	workflowRuns := canonicalWorkflowRuns(evidence.WorkflowRuns)
	reviewThreads := canonicalReviewThreads(evidence.ReviewThreads)
	canonical := canonicalThreadEvidence{
		Version:           "thread-review-evidence-v2",
		Kind:              thread.Kind,
		State:             thread.State,
		Title:             thread.Title,
		Body:              thread.Body,
		Labels:            labels,
		Assignees:         assignees,
		AuthorLogin:       thread.AuthorLogin,
		AuthorType:        thread.AuthorType,
		AuthorAssociation: thread.AuthorAssociation,
		IsDraft:           thread.IsDraft,
		Comments:          comments,
		Files:             files,
		Commits:           commits,
		Checks:            checks,
		WorkflowRuns:      workflowRuns,
		ReviewThreads:     reviewThreads,
	}
	sourceUpdatedAt := firstNonEmptyString(thread.UpdatedAtGitHub, thread.UpdatedAt)
	if evidence.Detail != nil {
		canonical.BaseSHA = evidence.Detail.BaseSHA
		canonical.HeadSHA = evidence.Detail.HeadSHA
		canonical.MergeableState = evidence.Detail.MergeableState
		canonical.Additions = evidence.Detail.Additions
		canonical.Deletions = evidence.Detail.Deletions
		canonical.ChangedFiles = evidence.Detail.ChangedFiles
		sourceUpdatedAt = latestTimestamp(sourceUpdatedAt, evidence.Detail.UpdatedAt, evidence.Detail.FetchedAt)
	}
	for _, reviewThread := range evidence.ReviewThreads {
		sourceUpdatedAt = latestTimestamp(sourceUpdatedAt, reviewThread.FirstCommentUpdatedAt)
	}
	for _, comment := range evidence.Comments {
		sourceUpdatedAt = latestTimestamp(sourceUpdatedAt, comment.UpdatedAtGitHub, comment.CreatedAtGitHub)
	}
	for _, check := range evidence.Checks {
		sourceUpdatedAt = latestTimestamp(sourceUpdatedAt, check.CompletedAt, check.StartedAt, check.FetchedAt)
	}
	for _, run := range evidence.WorkflowRuns {
		sourceUpdatedAt = latestTimestamp(sourceUpdatedAt, run.UpdatedAtGH, run.CreatedAtGH, run.FetchedAt)
	}
	labelsJSON := mustStableJSON(labels)
	evidenceJSON := mustStableJSON(canonical)
	contentHash := StableHash(evidenceJSON)
	revision := ThreadRevision{
		ThreadID:        thread.ID,
		SourceUpdatedAt: sourceUpdatedAt,
		ContentHash:     contentHash,
		TitleHash:       StableHash(thread.Title),
		BodyHash:        StableHash(thread.Body),
		LabelsHash:      StableHash(labelsJSON),
		EvidenceJSON:    evidenceJSON,
		CreatedAt:       createdAt,
	}

	titleTokens := fingerprintTokens(thread.Title, 4)
	bodyTokens := fingerprintTokens(thread.Body, 3)
	linkedRefs := fingerprintReferences(thread.Title + "\n" + thread.Body + "\n" + commentText(comments) + "\n" + reviewText(reviewThreads) + "\n" + commitText(commits))
	filePaths := changedFilePathsForFingerprint(files)
	moduleBuckets := moduleBucketsForFingerprint(filePaths)
	bodyTokenHash := StableHash(mustStableJSON(bodyTokens))
	fileSetHash := StableHash(mustStableJSON(filePaths))
	simhash := simHash64(append(append(append([]string{}, titleTokens...), bodyTokens...), fingerprintTokens(strings.Join(filePaths, " "), 1)...))
	featureJSON := mustStableJSON(map[string]any{
		"title_token_count": len(titleTokens),
		"body_token_count":  len(bodyTokens),
		"linked_ref_count":  len(linkedRefs),
		"file_count":        len(filePaths),
		"module_count":      len(moduleBuckets),
	})
	fingerprintMaterial := mustStableJSON(map[string]any{
		"algorithm_version": ThreadFingerprintAlgorithmVersion,
		"title_hash":        revision.TitleHash,
		"labels_hash":       revision.LabelsHash,
		"title_tokens":      titleTokens,
		"body_token_hash":   bodyTokenHash,
		"linked_refs":       linkedRefs,
		"file_set_hash":     fileSetHash,
		"module_buckets":    moduleBuckets,
		"simhash64":         simhash,
	})
	fingerprintHash := StableHash(fingerprintMaterial)
	fingerprint := ThreadFingerprint{
		AlgorithmVersion:  ThreadFingerprintAlgorithmVersion,
		FingerprintHash:   fingerprintHash,
		FingerprintSlug:   HumanKeyStableSlug(HumanKeyFromHash(fingerprintHash)),
		TitleTokensJSON:   mustStableJSON(titleTokens),
		BodyTokenHash:     bodyTokenHash,
		LinkedRefsJSON:    mustStableJSON(linkedRefs),
		FileSetHash:       fileSetHash,
		ModuleBucketsJSON: mustStableJSON(moduleBuckets),
		SimHash64:         simhash,
		FeatureJSON:       featureJSON,
		CreatedAt:         createdAt,
	}
	return revision, fingerprint
}

func canonicalNameList(raw, field string) []string {
	var values []any
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return []string{}
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		name := ""
		switch typed := value.(type) {
		case string:
			name = typed
		case map[string]any:
			name, _ = typed[field].(string)
		}
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func canonicalComments(comments []Comment) []canonicalComment {
	out := make([]canonicalComment, 0, len(comments))
	for _, comment := range comments {
		githubID := strings.TrimSpace(comment.GitHubID)
		commentType := strings.TrimSpace(comment.CommentType)
		if githubID == "" || commentType == "" {
			continue
		}
		out = append(out, canonicalComment{
			GitHubID:        githubID,
			CommentType:     commentType,
			AuthorLogin:     strings.TrimSpace(comment.AuthorLogin),
			AuthorType:      strings.TrimSpace(comment.AuthorType),
			Body:            comment.Body,
			IsBot:           comment.IsBot,
			ReviewState:     strings.TrimSpace(comment.ReviewState),
			CreatedAtGitHub: comment.CreatedAtGitHub,
			UpdatedAtGitHub: comment.UpdatedAtGitHub,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].CommentType + "\x00" + out[i].GitHubID
		right := out[j].CommentType + "\x00" + out[j].GitHubID
		return left < right
	})
	return out
}

func canonicalChecks(checks []PullRequestCheck) []canonicalCheck {
	out := make([]canonicalCheck, 0, len(checks))
	for _, check := range checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		out = append(out, canonicalCheck{
			Name:         name,
			Status:       strings.TrimSpace(check.Status),
			Conclusion:   strings.TrimSpace(check.Conclusion),
			WorkflowName: strings.TrimSpace(check.WorkflowName),
			DetailsURL:   strings.TrimSpace(check.DetailsURL),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].Name + "\x00" + out[i].WorkflowName + "\x00" + out[i].DetailsURL
		right := out[j].Name + "\x00" + out[j].WorkflowName + "\x00" + out[j].DetailsURL
		return left < right
	})
	return out
}

func canonicalWorkflowRuns(runs []WorkflowRun) []canonicalWorkflowRun {
	out := make([]canonicalWorkflowRun, 0, len(runs))
	for _, run := range runs {
		runID := strings.TrimSpace(run.RunID)
		if runID == "" {
			continue
		}
		out = append(out, canonicalWorkflowRun{
			RunID:        runID,
			RunNumber:    run.RunNumber,
			HeadSHA:      strings.TrimSpace(run.HeadSHA),
			Status:       strings.TrimSpace(run.Status),
			Conclusion:   strings.TrimSpace(run.Conclusion),
			WorkflowName: strings.TrimSpace(run.WorkflowName),
			Event:        strings.TrimSpace(run.Event),
			HTMLURL:      strings.TrimSpace(run.HTMLURL),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RunNumber != out[j].RunNumber {
			return out[i].RunNumber < out[j].RunNumber
		}
		return out[i].RunID < out[j].RunID
	})
	return out
}

func canonicalChangedFiles(files []PullRequestFile) []canonicalChangedFile {
	out := make([]canonicalChangedFile, 0, len(files))
	for _, file := range files {
		path := strings.TrimSpace(file.Path)
		if path == "" {
			continue
		}
		item := canonicalChangedFile{
			Path:         path,
			Status:       strings.TrimSpace(file.Status),
			PreviousPath: strings.TrimSpace(file.PreviousPath),
		}
		if file.Patch != "" {
			item.PatchHash = StableHash(file.Patch)
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].Path + "\x00" + out[i].Status + "\x00" + out[i].PreviousPath + "\x00" + out[i].PatchHash
		right := out[j].Path + "\x00" + out[j].Status + "\x00" + out[j].PreviousPath + "\x00" + out[j].PatchHash
		return left < right
	})
	return out
}

func canonicalCommits(commits []PullRequestCommit) []canonicalCommit {
	out := make([]canonicalCommit, 0, len(commits))
	for _, commit := range commits {
		sha := strings.TrimSpace(commit.SHA)
		if sha == "" {
			continue
		}
		out = append(out, canonicalCommit{SHA: sha, Subject: commitSubject(commit.Message)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SHA == out[j].SHA {
			return out[i].Subject < out[j].Subject
		}
		return out[i].SHA < out[j].SHA
	})
	return out
}

func commitSubject(message string) string {
	message = strings.TrimSpace(message)
	if newline := strings.IndexByte(message, '\n'); newline >= 0 {
		message = strings.TrimSpace(message[:newline])
	}
	return message
}

func canonicalReviewThreads(threads []PullRequestReviewThread) []canonicalReviewThread {
	out := make([]canonicalReviewThread, 0, len(threads))
	for _, thread := range threads {
		id := strings.TrimSpace(thread.ReviewThreadID)
		if id == "" {
			continue
		}
		out = append(out, canonicalReviewThread{
			ID:                    id,
			Path:                  strings.TrimSpace(thread.Path),
			Line:                  thread.Line,
			StartLine:             thread.StartLine,
			IsResolved:            thread.IsResolved,
			IsOutdated:            thread.IsOutdated,
			FirstAuthorLogin:      strings.TrimSpace(thread.FirstAuthorLogin),
			FirstCommentBody:      thread.FirstCommentBody,
			FirstCommentUpdatedAt: thread.FirstCommentUpdatedAt,
			CommentsJSON:          canonicalJSON(thread.CommentsJSON),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func canonicalJSON(raw string) string {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return strings.TrimSpace(raw)
	}
	return mustStableJSON(value)
}

func fingerprintTokens(value string, minLength int) []string {
	seen := map[string]bool{}
	for _, match := range fingerprintTokenPattern.FindAllString(strings.ToLower(value), -1) {
		if len(match) < minLength {
			continue
		}
		seen[match] = true
	}
	out := make([]string, 0, len(seen))
	for token := range seen {
		out = append(out, token)
	}
	sort.Strings(out)
	return out
}

func fingerprintReferences(value string) []string {
	seen := map[int]bool{}
	for _, match := range fingerprintRefPattern.FindAllStringSubmatch(value, -1) {
		number, err := strconv.Atoi(match[1])
		if err == nil {
			seen[number] = true
		}
	}
	numbers := make([]int, 0, len(seen))
	for number := range seen {
		numbers = append(numbers, number)
	}
	sort.Ints(numbers)
	out := make([]string, 0, len(numbers))
	for _, number := range numbers {
		out = append(out, "#"+strconv.Itoa(number))
	}
	return out
}

func changedFilePathsForFingerprint(files []canonicalChangedFile) []string {
	seen := map[string]bool{}
	for _, file := range files {
		for _, path := range []string{file.Path, file.PreviousPath} {
			if path != "" {
				seen[path] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for path := range seen {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func moduleBucketsForFingerprint(paths []string) []string {
	seen := map[string]bool{}
	for _, path := range paths {
		path = strings.Trim(path, "/")
		if path == "" {
			continue
		}
		bucket := strings.SplitN(path, "/", 2)[0]
		seen[bucket] = true
	}
	out := make([]string, 0, len(seen))
	for bucket := range seen {
		out = append(out, bucket)
	}
	sort.Strings(out)
	return out
}

func simHash64(tokens []string) string {
	if len(tokens) == 0 {
		return "0000000000000000"
	}
	weights := make([]int, 64)
	for _, token := range tokens {
		sum := sha256.Sum256([]byte(token))
		value := uint64(0)
		for _, part := range sum[:8] {
			value = value<<8 | uint64(part)
		}
		for bit := 0; bit < 64; bit++ {
			if value&(uint64(1)<<bit) != 0 {
				weights[bit]++
			} else {
				weights[bit]--
			}
		}
	}
	var result uint64
	for bit, weight := range weights {
		if weight >= 0 {
			result |= uint64(1) << bit
		}
	}
	return fmt.Sprintf("%016x", result)
}

func reviewText(threads []canonicalReviewThread) string {
	parts := make([]string, 0, len(threads))
	for _, thread := range threads {
		parts = append(parts, thread.FirstCommentBody)
	}
	return strings.Join(parts, "\n")
}

func commentText(comments []canonicalComment) string {
	parts := make([]string, 0, len(comments))
	for _, comment := range comments {
		parts = append(parts, comment.Body)
	}
	return strings.Join(parts, "\n")
}

func commitText(commits []canonicalCommit) string {
	parts := make([]string, 0, len(commits))
	for _, commit := range commits {
		parts = append(parts, commit.Subject)
	}
	return strings.Join(parts, "\n")
}

func mustStableJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(data)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func latestTimestamp(values ...string) string {
	latestRaw := ""
	var latest time.Time
	fallback := ""
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			if value > fallback {
				fallback = value
			}
			continue
		}
		parsed = parsed.UTC()
		if latestRaw == "" || parsed.After(latest) {
			latest = parsed
			latestRaw = value
		}
	}
	if latestRaw != "" {
		return latestRaw
	}
	return fallback
}

func timestampBefore(left, right string) bool {
	leftTime, leftErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(left))
	rightTime, rightErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(right))
	return leftErr == nil && rightErr == nil && leftTime.Before(rightTime)
}
