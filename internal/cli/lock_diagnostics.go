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

	"github.com/openclaw/gitcrawl/internal/store"
)

type lockDiagnostic struct {
	DBPath                 string                 `json:"db_path"`
	DBExists               bool                   `json:"db_exists"`
	DBBytes                int64                  `json:"db_bytes,omitempty"`
	WALPath                string                 `json:"wal_path"`
	WALExists              bool                   `json:"wal_exists"`
	WALBytes               int64                  `json:"wal_bytes,omitempty"`
	SHMPath                string                 `json:"shm_path"`
	SHMExists              bool                   `json:"shm_exists"`
	SHMBytes               int64                  `json:"shm_bytes,omitempty"`
	JournalPath            string                 `json:"journal_path"`
	JournalExists          bool                   `json:"journal_exists"`
	JournalBytes           int64                  `json:"journal_bytes,omitempty"`
	ReadOnlyOpen           string                 `json:"read_only_open"`
	QuickCheck             string                 `json:"quick_check"`
	SafeReadOnlyInspection bool                   `json:"safe_read_only_inspection"`
	ProcessDetection       processDetectionReport `json:"process_detection"`
	ArchiveHealth          string                 `json:"archive_health"`
	Error                  string                 `json:"error,omitempty"`
}

type processDetectionReport struct {
	Method    string        `json:"method"`
	Platform  string        `json:"platform"`
	Available bool          `json:"available"`
	Error     string        `json:"error,omitempty"`
	Processes []lockProcess `json:"processes"`
}

type lockProcess struct {
	PID     int    `json:"pid"`
	Command string `json:"command,omitempty"`
}

var detectDBProcesses = defaultDetectDBProcesses

func sqliteLockDiagnostic(ctx context.Context, dbPath string) lockDiagnostic {
	out := lockDiagnostic{
		DBPath:        dbPath,
		WALPath:       dbPath + "-wal",
		SHMPath:       dbPath + "-shm",
		JournalPath:   dbPath + "-journal",
		ReadOnlyOpen:  "missing",
		QuickCheck:    "missing",
		ArchiveHealth: "missing",
	}
	if strings.TrimSpace(dbPath) == "" {
		out.Error = "database path is empty"
		out.ArchiveHealth = "error"
		out.ReadOnlyOpen = "error"
		out.QuickCheck = "skipped"
		return out
	}
	out.DBExists, out.DBBytes, _ = fileExistsAndSize(dbPath)
	out.WALExists, out.WALBytes, _ = fileExistsAndSize(out.WALPath)
	out.SHMExists, out.SHMBytes, _ = fileExistsAndSize(out.SHMPath)
	out.JournalExists, out.JournalBytes, _ = fileExistsAndSize(out.JournalPath)
	out.ProcessDetection = detectDBProcesses(ctx, dbPath)
	if !out.DBExists {
		return out
	}
	st, err := store.OpenReadOnly(ctx, dbPath)
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
	_ = st.Close()
	return out
}

func sqliteQuickCheck(ctx context.Context, st *store.Store) error {
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
	report := processDetectionReport{Method: "lsof", Platform: runtime.GOOS, Processes: []lockProcess{}}
	if runtime.GOOS == "windows" {
		report.Method = "unsupported"
		report.Error = "process lock detection is not implemented on windows"
		return report
	}
	if _, err := exec.LookPath("lsof"); err != nil {
		report.Error = "lsof not found"
		return report
	}
	cmd := exec.CommandContext(ctx, "lsof", "-w", "-Fpc", "--", dbPath)
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
	report.Processes = parseLsofProcessOutput(string(data))
	return report
}

func parseLsofProcessOutput(raw string) []lockProcess {
	var out []lockProcess
	current := lockProcess{}
	flush := func() {
		if current.PID != 0 {
			out = append(out, current)
		}
		current = lockProcess{}
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
		}
	}
	flush()
	return out
}
