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
	return withNativeClusterCustody(ctx, path, file, repository, repositoryID, githubID, nil)
}

func withNativeClusterCustody(ctx context.Context, path string, file NativeReportFile, repository string, repositoryID int64, githubID string, visit func(*sql.Tx) error) error {
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
	if visit != nil {
		if err := visit(tx); err != nil {
			return err
		}
	}
	after, err := NativeReportFileIdentity(path)
	if err != nil || after != before {
		return ErrNativeClusterContext
	}
	return ctx.Err()
}

// NativeClusterDeliveryEvidence is compact identity from the saved snapshot;
// current native thread presence is not required and cannot replace old identity.
type NativeClusterDeliveryEvidence struct {
	Number int64
	NodeID *string
}

// CheckNativeClusterDelivery checks current local public/exclusion authority in
// the same read-only custody snapshot, with no journal or acquisition claim.
// MaxRows counts references; MaxBytes counts node text, not process memory.
func CheckNativeClusterDelivery(ctx context.Context, path string, file NativeReportFile, repository string, repositoryID int64, githubID string, limits NativeReportLimits, evidence []NativeClusterDeliveryEvidence) error {
	if limits.MaxRows <= 0 || limits.MaxBytes <= 0 || int64(len(evidence)) > limits.MaxRows {
		return ErrNativeReportLimit
	}
	var size int64
	missing := false
	for _, ref := range evidence {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ref.Number <= 0 {
			return ErrNativeClusterContext
		}
		if ref.NodeID == nil || *ref.NodeID == "" {
			missing = true
		}
		if ref.NodeID != nil {
			if !utf8.ValidString(*ref.NodeID) {
				return ErrNativeClusterContext
			}
			if int64(len(*ref.NodeID)) > limits.MaxBytes-size {
				return ErrNativeReportLimit
			}
			size += int64(len(*ref.NodeID))
		}
	}
	return withNativeClusterCustody(ctx, path, file, repository, repositoryID, githubID, func(tx *sql.Tx) error {
		var public, ledger bool
		if err := tx.QueryRowContext(ctx, `select coalesce(case when json_valid(raw_json) then json_type(raw_json,'$.private')='false' and (json_type(raw_json,'$.visibility') is null or json_extract(raw_json,'$.visibility')='public') else 0 end,0) from repositories where id=?`, repositoryID).Scan(&public); err != nil {
			return err
		}
		if !public {
			return ErrNativeClusterContext
		}
		if err := tx.QueryRowContext(ctx, `select exists(select 1 from thread_excluded_nodes)`).Scan(&ledger); err != nil {
			return err
		}
		if ledger && missing {
			return ErrNativeClusterContext
		}
		for start := 0; start < len(evidence); start += 500 {
			end := min(start+500, len(evidence))
			var batch NativeReportDeliveryEvidence
			for _, ref := range evidence[start:end] {
				batch.ThreadNumbers = append(batch.ThreadNumbers, ref.Number)
				if ref.NodeID != nil {
					batch.NodeIDs = append(batch.NodeIDs, *ref.NodeID)
				}
			}
			if err := checkNativeDeliveryEvidence(ctx, tx, repository, batch); err != nil {
				return err
			}
		}
		return nil
	})
}
