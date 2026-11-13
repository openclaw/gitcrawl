package store

import (
	"context"
	"database/sql"
	"strings"
	"unicode/utf8"
)

// CheckNativeClusterStageCustody pins the operator-selected current native
// repository. Exported bundles contain no physical identity, so this does not
// attest which file produced an older export or grant outbound authority.
// Only bounded identity predicates are returned, never native row contents.
func CheckNativeClusterStageCustody(ctx context.Context, path string, file NativeReportFile, repository string, repositoryID int64, githubID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrNativeClusterContext
	}
	if repositoryID <= 0 || repository == "" || len(repository) > 16384 || githubID == "" || len(githubID) > 16384 || !utf8.ValidString(repository) || !utf8.ValidString(githubID) || strings.ToLower(repository) != repository {
		return ErrNativeClusterContext
	}
	before, err := NativeReportFileIdentity(path)
	if err != nil || before != file {
		return ErrNativeClusterContext
	}
	source, err := OpenReadOnly(ctx, path)
	if err != nil {
		return err
	}
	defer source.Close()
	tx, err := source.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version, count int
	var portable, match bool
	if err = tx.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where name='portable_metadata')`).Scan(&portable); err != nil {
		return err
	}
	if version != schemaVersion || portable {
		return ErrNativeClusterContext
	}
	if err = tx.QueryRowContext(ctx, `select count(*),coalesce(min(id=? and length(cast(github_repo_id as blob))<=16384 and github_repo_id=?),0) from repositories where length(cast(full_name as blob))<=16384 and lower(full_name)=?`, repositoryID, githubID, repository).Scan(&count, &match); err != nil {
		return err
	}
	if count != 1 || !match {
		return ErrNativeClusterContext
	}
	after, err := NativeReportFileIdentity(path)
	if err != nil || after != before {
		return ErrNativeClusterContext
	}
	return ctx.Err()
}
