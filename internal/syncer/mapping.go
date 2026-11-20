package syncer

import "github.com/openclaw/gitcrawl/internal/store"

// MapIssueToThread preserves the REST backing issue identity, full text and raw
// provider fields. Persistence owners retain existing local state on upsert.
func MapIssueToThread(repoID int64, row map[string]any, pulledAt string) store.Thread {
	kind := issueKind(row)
	labelsJSON := mustJSON(row["labels"])
	if labelsJSON == "null" {
		labelsJSON = "[]"
	}
	assigneesJSON := mustJSON(row["assignees"])
	if assigneesJSON == "null" {
		assigneesJSON = "[]"
	}
	title := stringValue(row["title"])
	body := stringValue(row["body"])
	return store.Thread{
		RepoID:            repoID,
		GitHubID:          jsonID(row["id"]),
		Number:            intValue(row["number"]),
		Kind:              kind,
		State:             stringValue(row["state"]),
		Title:             title,
		Body:              body,
		AuthorLogin:       loginFromUser(row["user"]),
		AuthorType:        typeFromUser(row["user"]),
		AuthorAssociation: stringValue(row["author_association"]),
		HTMLURL:           stringValue(row["html_url"]),
		LabelsJSON:        labelsJSON,
		AssigneesJSON:     assigneesJSON,
		RawJSON:           mustJSON(row),
		ContentHash:       contentHash(title, body, labelsJSON),
		IsDraft:           boolValue(row["draft"]),
		CreatedAtGitHub:   stringValue(row["created_at"]),
		UpdatedAtGitHub:   stringValue(row["updated_at"]),
		ClosedAtGitHub:    stringValue(row["closed_at"]),
		MergedAtGitHub:    stringValue(mapValue(row["pull_request"])["merged_at"]),
		FirstPulledAt:     pulledAt,
		LastPulledAt:      pulledAt,
		UpdatedAt:         pulledAt,
	}
}

// MapComment maps one published provider child. Pagination owns pending-review
// filtering; publication timestamps use the same pure owner as native storage.
func MapComment(threadID int64, kind string, row map[string]any) store.Comment {
	authorLogin := loginFromUser(row["user"])
	authorType := typeFromUser(row["user"])
	raw := mustJSON(row)
	return store.Comment{
		ThreadID:        threadID,
		GitHubID:        jsonID(row["id"]),
		CommentType:     kind,
		AuthorLogin:     authorLogin,
		AuthorType:      authorType,
		Body:            stringValue(row["body"]),
		IsBot:           isBot(authorLogin, authorType),
		ReviewState:     stringValue(row["state"]),
		RawJSON:         raw,
		Publication:     store.CommentPublication(kind, raw),
		CreatedAtGitHub: stringValue(row["created_at"]),
		UpdatedAtGitHub: stringValue(row["updated_at"]),
		DeletedAt:       stringValue(row["deleted_at"]),
		DeletionReason:  stringValue(row["deletion_reason"]),
	}
}
