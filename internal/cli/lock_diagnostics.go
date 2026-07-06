package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	crawlstore "github.com/openclaw/crawlkit/store"
)

type lockDiagnostic struct {
	DBPath                 string                 `json:"db_path"`
	DBExists               bool                   `json:"db_exists"`
	DBBytes                int64                  `json:"db_bytes"`
	WALPath                string                 `json:"wal_path"`
	WALExists              bool                   `json:"wal_exists"`
	WALBytes               int64                  `json:"wal_bytes"`
	SHMPath                string                 `json:"shm_path"`
	SHMExists              bool                   `json:"shm_exists"`
	SHMBytes               int64                  `json:"shm_bytes"`
	JournalPath            string                 `json:"journal_path"`
	JournalExists          bool                   `json:"journal_exists"`
	JournalBytes           int64                  `json:"journal_bytes"`
	ReadOnlyOpen           string                 `json:"read_only_open"`
	QuickCheck             string                 `json:"quick_check"`
	SafeReadOnlyInspection bool                   `json:"safe_read_only_inspection"`
	WriterActivity         string                 `json:"writer_activity"`
	AppearsIdle            bool                   `json:"appears_idle"`
	ProcessDetection       processDetectionReport `json:"process_detection"`
	ArchiveHealth          string                 `json:"archive_health"`
	FileStateErrors        []string               `json:"file_state_errors,omitempty"`
	Error                  string                 `json:"error,omitempty"`
}

type processDetectionReport struct {
	Method          string        `json:"method"`
	Platform        string        `json:"platform"`
	Available       bool          `json:"available"`
	Error           string        `json:"error,omitempty"`
	WriterProcesses []lockProcess `json:"writer_processes"`
}

type lockProcess struct {
	PID     int    `json:"pid"`
	Command string `json:"command,omitempty"`
	Access  string `json:"access"`
}

var detectDBProcesses = defaultDetectDBProcesses

func sqliteLockDiagnostic(ctx context.Context, dbPath string) lockDiagnostic {
	out := lockDiagnostic{
		DBPath:         dbPath,
		WALPath:        dbPath + "-wal",
		SHMPath:        dbPath + "-shm",
		JournalPath:    dbPath + "-journal",
		ReadOnlyOpen:   "missing",
		QuickCheck:     "missing",
		ArchiveHealth:  "missing",
		WriterActivity: "unknown",
		ProcessDetection: processDetectionReport{
			Method:          "not_run",
			Platform:        runtime.GOOS,
			WriterProcesses: []lockProcess{},
		},
	}
	if strings.TrimSpace(dbPath) == "" {
		out.Error = "database path is empty"
		out.ArchiveHealth = "error"
		out.ReadOnlyOpen = "error"
		out.QuickCheck = "skipped"
		return out
	}
	var err error
	out.DBExists, out.DBBytes, err = fileExistsAndSize(dbPath)
	if err != nil {
		out.Error = fmt.Sprintf("inspect database file: %v", err)
		out.ArchiveHealth = "error"
		out.ReadOnlyOpen = "error"
		out.QuickCheck = "skipped"
		return out
	}
	for _, file := range []struct {
		name   string
		path   string
		exists *bool
		bytes  *int64
	}{
		{name: "WAL", path: out.WALPath, exists: &out.WALExists, bytes: &out.WALBytes},
		{name: "SHM", path: out.SHMPath, exists: &out.SHMExists, bytes: &out.SHMBytes},
		{name: "rollback journal", path: out.JournalPath, exists: &out.JournalExists, bytes: &out.JournalBytes},
	} {
		*file.exists, *file.bytes, err = fileExistsAndSize(file.path)
		if err != nil {
			out.FileStateErrors = append(out.FileStateErrors, fmt.Sprintf("inspect %s: %v", file.name, err))
		}
	}
	if !out.DBExists {
		return out
	}
	out.ProcessDetection = detectDBProcesses(ctx, dbPath)
	switch {
	case !out.ProcessDetection.Available:
		out.WriterActivity = "unknown"
	case len(out.ProcessDetection.WriterProcesses) > 0:
		out.WriterActivity = "detected"
	default:
		out.WriterActivity = "none_detected"
	}
	st, err := crawlstore.OpenReadOnly(ctx, dbPath)
	if err != nil {
		out.ReadOnlyOpen = "error"
		out.QuickCheck = "skipped"
		out.ArchiveHealth = "error"
		out.Error = err.Error()
		return out
	}
	out.ReadOnlyOpen = "ok"
	if err := sqliteQuickCheck(ctx, st); err != nil {
		out.QuickCheck = "error"
		out.ArchiveHealth = "error"
		out.Error = err.Error()
		_ = st.Close()
		return out
	}
	out.QuickCheck = "ok"
	out.ArchiveHealth = "ok"
	out.SafeReadOnlyInspection = true
	out.AppearsIdle = out.WriterActivity == "none_detected"
	_ = st.Close()
	return out
}

func sqliteQuickCheck(ctx context.Context, st *crawlstore.Store) error {
	rows, err := st.DB().QueryContext(ctx, `pragma quick_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var problems []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return err
		}
		if strings.TrimSpace(line) != "ok" {
			problems = append(problems, line)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(problems) > 0 {
		return fmt.Errorf("sqlite quick_check failed: %s", strings.Join(problems, "; "))
	}
	return nil
}

func fileExistsAndSize(path string) (bool, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, err
	}
	return true, info.Size(), nil
}

func defaultDetectDBProcesses(ctx context.Context, dbPath string) processDetectionReport {
	report := processDetectionReport{Method: "lsof-writable-fd", Platform: runtime.GOOS, WriterProcesses: []lockProcess{}}
	if runtime.GOOS == "windows" {
		report.Method = "unsupported"
		report.Error = "process lock detection is not implemented on windows"
		return report
	}
	if _, err := exec.LookPath("lsof"); err != nil {
		report.Error = "lsof not found"
		return report
	}
	cmd := exec.CommandContext(ctx, "lsof", "-w", "-Fpcfa", "--", dbPath)
	data, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			report.Available = true
			return report
		}
		report.Error = err.Error()
		return report
	}
	report.Available = true
	report.WriterProcesses = parseLsofWriterOutput(string(data), os.Getpid())
	return report
}

func parseLsofWriterOutput(raw string, ignoredPID int) []lockProcess {
	out := make([]lockProcess, 0)
	current := lockProcess{}
	writable := false
	flush := func() {
		if current.PID != 0 && current.PID != ignoredPID && writable {
			current.Access = "read_write"
			out = append(out, current)
		}
		current = lockProcess{}
		writable = false
	}
	for _, line := range strings.Split(raw, "\n") {
		if line == "" {
			continue
		}
		switch line[0] {
		case 'p':
			flush()
			pid, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
			current.PID = pid
		case 'c':
			current.Command = strings.TrimSpace(line[1:])
		case 'a':
			access := strings.TrimSpace(line[1:])
			writable = writable || access == "u" || access == "w"
		}
	}
	flush()
	return out
}
