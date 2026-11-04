package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) writeJSONValue(value any, jqExpr string) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if strings.TrimSpace(jqExpr) == "" {
		_, err = fmt.Fprintf(a.Stdout, "%s\n", data)
		return err
	}
	jqPath, err := exec.LookPath("jq")
	if err != nil {
		return localGHUnsupported(fmt.Errorf("--jq requires jq executable"))
	}
	cmd := exec.Command(jqPath, jqExpr)
	cmd.Stdin = bytes.NewReader(data)
	cmd.Stdout = a.Stdout
	cmd.Stderr = a.Stderr
	return cmd.Run()
}

func (a *App) ghCommandCacheDir() (string, error) {
	cfg, err := config.Load(a.configPath)
	if err != nil {
		cfg = config.Default()
	}
	dir := filepath.Join(cfg.CacheDir, "octopool-migrated-gh")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

func writeAtomicFile(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func isDecimalString(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
