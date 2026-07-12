package store

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	SummaryKindLLMKey       = "llm_key_summary"
	SummaryPromptVersionV1  = "key-summary-v1"
	MaxSummaryTextRunes     = 12_000
	MaxSummaryTextBytes     = 24_000
	summaryInputHashVersion = "summary-input:v2"
)

type SummaryTask struct {
	ThreadID      int64  `json:"thread_id"`
	RevisionID    int64  `json:"revision_id"`
	Number        int    `json:"number"`
	Kind          string `json:"kind"`
	Title         string `json:"title"`
	Text          string `json:"-"`
	RevisionHash  string `json:"revision_hash"`
	InputHash     string `json:"input_hash"`
	TextTruncated bool   `json:"text_truncated,omitempty"`
}

type SummaryTaskOptions struct {
	RepoID        int64
	Provider      string
	Model         string
	SummaryKind   string
	PromptVersion string
	Number        int
	Limit         int
	Force         bool
	IncludeClosed bool
}

type ThreadKeySummary struct {
	ThreadRevisionID int64
	SummaryKind      string
	PromptVersion    string
	Provider         string
	Model            string
	InputHash        string
	OutputHash       string
	KeyText          string
	CreatedAt        string
}

func (s *Store) ListSummaryTasks(ctx context.Context, options SummaryTaskOptions) ([]SummaryTask, error) {
	provider := strings.TrimSpace(options.Provider)
	model := strings.TrimSpace(options.Model)
	summaryKind := strings.TrimSpace(options.SummaryKind)
	promptVersion := strings.TrimSpace(options.PromptVersion)
	if provider == "" || model == "" || summaryKind == "" || promptVersion == "" {
		return nil, fmt.Errorf("summary provider, model, kind, and prompt version are required")
	}
	var number any
	if options.Number > 0 {
		number = options.Number
	}
	rows, err := s.q().QueryContext(ctx, `
		with latest_revisions as (
			select tr.*
			from thread_revisions tr
			join (
				select thread_id, max(id) as id
				from thread_revisions
				group by thread_id
			) latest on latest.id = tr.id
		)
		select t.id, lr.id, t.number, t.kind,
			b.inline_text,
			lr.content_hash,
			coalesce(s.input_hash, ''),
			t.state,
			t.title,
			coalesce(t.body, ''),
			t.labels_json,
			t.assignees_json,
			coalesce(t.author_login, ''),
			coalesce(t.author_type, ''),
			coalesce(t.author_association, ''),
			t.is_draft
		from threads t
		join latest_revisions lr on lr.thread_id = t.id
		join blobs b on b.id = lr.raw_json_blob_id
		left join thread_key_summaries s
			on s.thread_revision_id = lr.id
			and s.summary_kind = ?
			and s.prompt_version = ?
			and s.provider = ?
			and s.model = ?
		where t.repo_id = ?
			and (? != 0 or (t.state = 'open' and t.closed_at_local is null))
			and (? is null or t.number = ?)
			and b.sha256 = lr.content_hash
			and b.storage_kind = 'inline'
			and b.compression = 'none'
			and nullif(b.inline_text, '') is not null
			and julianday(coalesce(nullif(lr.source_updated_at, ''), lr.created_at)) >=
				julianday(coalesce(nullif(t.updated_at_gh, ''), t.updated_at))
		order by coalesce(t.updated_at_gh, t.updated_at) desc, t.number desc
	`, summaryKind, promptVersion, provider, model, options.RepoID, boolInt(options.IncludeClosed), number, number)
	if err != nil {
		return nil, fmt.Errorf("list summary tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]SummaryTask, 0)
	for rows.Next() {
		var task SummaryTask
		var existingInputHash string
		var current summaryTaskCurrentThread
		var isDraft int
		if err := rows.Scan(
			&task.ThreadID,
			&task.RevisionID,
			&task.Number,
			&task.Kind,
			&task.Text,
			&task.RevisionHash,
			&existingInputHash,
			&current.state,
			&current.title,
			&current.body,
			&current.labelsJSON,
			&current.assigneesJSON,
			&current.authorLogin,
			&current.authorType,
			&current.authorAssociation,
			&isDraft,
		); err != nil {
			return nil, fmt.Errorf("scan summary task: %w", err)
		}
		task.Text = strings.TrimSpace(task.Text)
		current.isDraft = isDraft != 0
		var canonical canonicalThreadEvidence
		if StableHash(task.Text) != task.RevisionHash ||
			json.Unmarshal([]byte(task.Text), &canonical) != nil ||
			!summaryEvidenceMatchesCurrentThread(canonical, task.Kind, current) {
			continue
		}
		task.Kind = canonical.Kind
		task.Title = canonical.Title
		if capStringByRunesAndBytes(task.Text, MaxSummaryTextRunes, MaxSummaryTextBytes) != task.Text {
			continue
		}
		task.InputHash = StableHash(fmt.Sprintf(
			"%s\nprovider=%s\nmodel=%s\nkind=%s\nprompt=%s\nrevision=%s\n%s",
			summaryInputHashVersion,
			provider,
			model,
			summaryKind,
			promptVersion,
			task.RevisionHash,
			task.Text,
		))
		if !options.Force && existingInputHash == task.InputHash {
			continue
		}
		tasks = append(tasks, task)
		if options.Limit > 0 && len(tasks) >= options.Limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate summary tasks: %w", err)
	}
	return tasks, nil
}

type summaryTaskCurrentThread struct {
	state             string
	title             string
	body              string
	labelsJSON        string
	assigneesJSON     string
	authorLogin       string
	authorType        string
	authorAssociation string
	isDraft           bool
}

func summaryEvidenceMatchesCurrentThread(canonical canonicalThreadEvidence, kind string, current summaryTaskCurrentThread) bool {
	return canonical.Version == "thread-review-evidence-v2" &&
		canonical.Kind == kind &&
		canonical.State == current.state &&
		canonical.Title == current.title &&
		canonical.Body == current.body &&
		slices.Equal(canonical.Labels, canonicalNameList(current.labelsJSON, "name")) &&
		slices.Equal(canonical.Assignees, canonicalNameList(current.assigneesJSON, "login")) &&
		canonical.AuthorLogin == current.authorLogin &&
		canonical.AuthorType == current.authorType &&
		canonical.AuthorAssociation == current.authorAssociation &&
		canonical.IsDraft == current.isDraft
}

func (s *Store) UpsertThreadKeySummary(ctx context.Context, summary ThreadKeySummary) error {
	if summary.ThreadRevisionID <= 0 {
		return fmt.Errorf("thread revision id must be positive")
	}
	if strings.TrimSpace(summary.SummaryKind) == "" ||
		strings.TrimSpace(summary.PromptVersion) == "" ||
		strings.TrimSpace(summary.Provider) == "" ||
		strings.TrimSpace(summary.Model) == "" ||
		strings.TrimSpace(summary.InputHash) == "" ||
		strings.TrimSpace(summary.OutputHash) == "" ||
		strings.TrimSpace(summary.KeyText) == "" {
		return fmt.Errorf("summary kind, prompt version, provider, model, hashes, and key text are required")
	}
	if strings.TrimSpace(summary.CreatedAt) == "" {
		summary.CreatedAt = time.Now().UTC().Format(timeLayout)
	}
	_, err := s.q().ExecContext(ctx, `
		insert into thread_key_summaries(
			thread_revision_id, summary_kind, prompt_version, provider, model,
			input_hash, output_hash, key_text, created_at
		)
		values(?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(thread_revision_id, summary_kind, prompt_version, provider, model) do update set
			input_hash = excluded.input_hash,
			output_hash = excluded.output_hash,
			key_text = excluded.key_text,
			created_at = excluded.created_at
	`, summary.ThreadRevisionID, summary.SummaryKind, summary.PromptVersion, summary.Provider, summary.Model,
		summary.InputHash, summary.OutputHash, strings.TrimSpace(summary.KeyText), summary.CreatedAt)
	if err != nil {
		return fmt.Errorf("upsert thread key summary: %w", err)
	}
	return nil
}
