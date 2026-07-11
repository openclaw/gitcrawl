package store

import (
	"database/sql"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

func stringValue(value sql.NullString) string {
	return value.String
}

func int64Bool(value int64) bool {
	return value != 0
}

func optionalString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func repositoryFromDB(repo storedb.Repository) Repository {
	return Repository{
		ID:           repo.ID,
		Owner:        repo.Owner,
		Name:         repo.Name,
		FullName:     repo.FullName,
		GitHubRepoID: stringValue(repo.GithubRepoID),
		RawJSON:      repo.RawJson,
		UpdatedAt:    repo.UpdatedAt,
	}
}

func threadFromCurrentSchemaDB(row storedb.ListThreadsCurrentSchemaRow) Thread {
	return Thread{
		ID:                row.ID,
		RepoID:            row.RepoID,
		GitHubID:          row.GithubID,
		Number:            int(row.Number),
		Kind:              row.Kind,
		State:             row.State,
		Title:             row.Title,
		Body:              stringValue(row.Body),
		AuthorLogin:       stringValue(row.AuthorLogin),
		AuthorType:        stringValue(row.AuthorType),
		AuthorAssociation: stringValue(row.AuthorAssociation),
		HTMLURL:           row.HtmlUrl,
		LabelsJSON:        row.LabelsJson,
		AssigneesJSON:     row.AssigneesJson,
		RawJSON:           row.RawJson,
		ContentHash:       row.ContentHash,
		IsDraft:           int64Bool(row.IsDraft),
		CreatedAtGitHub:   stringValue(row.CreatedAtGh),
		UpdatedAtGitHub:   stringValue(row.UpdatedAtGh),
		ClosedAtGitHub:    stringValue(row.ClosedAtGh),
		MergedAtGitHub:    stringValue(row.MergedAtGh),
		FirstPulledAt:     stringValue(row.FirstPulledAt),
		LastPulledAt:      stringValue(row.LastPulledAt),
		UpdatedAt:         row.UpdatedAt,
		ClosedAtLocal:     stringValue(row.ClosedAtLocal),
		CloseReasonLocal:  stringValue(row.CloseReasonLocal),
	}
}

func runRecordFromDB(kind string, run storedb.SyncRun) RunRecord {
	return RunRecord{
		ID:         run.ID,
		RepoID:     run.RepoID,
		Kind:       kind,
		Scope:      run.Scope,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		FinishedAt: stringValue(run.FinishedAt),
		StatsJSON:  stringValue(run.StatsJson),
		ErrorText:  stringValue(run.ErrorText),
	}
}

func summaryRunRecordFromDB(kind string, run storedb.SummaryRun) RunRecord {
	return RunRecord{
		ID:         run.ID,
		RepoID:     run.RepoID,
		Kind:       kind,
		Scope:      run.Scope,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		FinishedAt: stringValue(run.FinishedAt),
		StatsJSON:  stringValue(run.StatsJson),
		ErrorText:  stringValue(run.ErrorText),
	}
}

func embeddingRunRecordFromDB(kind string, run storedb.EmbeddingRun) RunRecord {
	return RunRecord{
		ID:         run.ID,
		RepoID:     run.RepoID,
		Kind:       kind,
		Scope:      run.Scope,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		FinishedAt: stringValue(run.FinishedAt),
		StatsJSON:  stringValue(run.StatsJson),
		ErrorText:  stringValue(run.ErrorText),
	}
}

func clusterRunRecordFromDB(kind string, run storedb.ClusterRun) RunRecord {
	return RunRecord{
		ID:         run.ID,
		RepoID:     run.RepoID,
		Kind:       kind,
		Scope:      run.Scope,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		FinishedAt: stringValue(run.FinishedAt),
		StatsJSON:  stringValue(run.StatsJson),
		ErrorText:  stringValue(run.ErrorText),
	}
}
