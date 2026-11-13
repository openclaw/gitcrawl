package portable

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const recentSelection = "open-unknown-or-recent-thread-comments-v1"

// RecentSubset describes selection from current captured state, not an as-of
// reconstruction or a complete provider acquisition window.
type RecentSubset struct {
	Since      string `json:"since"`
	Selection  string `json:"selection"`
	CapturedAt string `json:"capturedAt,omitempty"`
}

func ParseSince(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() {
		return time.Time{}, fmt.Errorf("since must be a nonzero RFC3339 timestamp")
	}
	return parsed, nil
}

func (s RecentSubset) metadata() map[string]string {
	// Capture time stays in the manifest so repeated captures retain semantic
	// identity when the selected content and scope have not changed.
	return map[string]string{
		"subset_since":                   s.Since,
		"subset_selection":               s.Selection,
		"subset_repository_completeness": "cleared",
	}
}

// ReadRecentSubset reads stable scope from a database, including writable local
// mirrors. Capture time belongs to the publisher manifest and is not inferred.
func ReadRecentSubset(ctx context.Context, db *sql.DB) (*RecentSubset, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type='table' and name='portable_metadata')`).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx, `select key,value from portable_metadata where key glob 'subset_*'`)
	if err != nil {
		return nil, err
	}
	metadata := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			rows.Close()
			return nil, err
		}
		metadata[key] = value
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	s := RecentSubset{Since: metadata["subset_since"], Selection: metadata["subset_selection"]}
	since, err := ParseSince(s.Since)
	if err != nil || since.UTC().Format(time.RFC3339Nano) != s.Since || s.Selection != recentSelection || len(metadata) != len(s.metadata()) {
		return nil, fmt.Errorf("invalid recent portable scope")
	}
	for key, want := range s.metadata() {
		if metadata[key] != want {
			return nil, fmt.Errorf("invalid recent portable metadata %s", key)
		}
	}
	return &s, nil
}

// ValidateRecentSubset also rejects a missing manifest/scope when the database
// declares a subset. Legacy complete databases without metadata remain valid.
func ValidateRecentSubset(ctx context.Context, db *sql.DB, expected *RecentSubset) error {
	actual, err := ReadRecentSubset(ctx, db)
	if err != nil {
		return err
	}
	if expected == nil && actual == nil {
		return nil
	}
	if expected == nil || actual == nil || expected.Since != actual.Since || expected.Selection != actual.Selection {
		return fmt.Errorf("portable manifest subset does not match database scope")
	}
	if _, err := ParseSince(expected.CapturedAt); err != nil {
		return fmt.Errorf("invalid recent portable capture time")
	}
	var count int
	if err := db.QueryRowContext(ctx, `select count(*) from repo_sync_state`).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("recent portable scope retains repository completeness checkpoints")
	}
	return nil
}

func rejectInheritedSubset(ctx context.Context, db *sql.DB) error {
	subset, err := ReadRecentSubset(ctx, db)
	if err != nil {
		return err
	}
	if subset != nil {
		return fmt.Errorf("re-export of a recent subset is unsupported; use the original source")
	}
	return nil
}
