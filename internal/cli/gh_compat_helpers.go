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

func normalizeGHAPIRoute(args []string) string {
	path := ghAPIPathArg(args)
	path = strings.TrimPrefix(path, "https://api.github.com/")
	path = strings.TrimPrefix(path, "http://api.github.com/")
	path = strings.TrimPrefix(path, "/")
	if before, _, found := strings.Cut(path, "?"); found {
		path = before
	}
	if path == "" {
		return "api"
	}
	parts := strings.Split(path, "/")
	for index, part := range parts {
		if part == "" {
			continue
		}
		switch {
		case index > 0 && parts[index-1] == "commits" && isHexString(part) && len(part) >= 7:
			parts[index] = ":sha"
		case isDecimalString(part):
			parts[index] = ":id"
		case index >= 2 && parts[index-2] == "repos":
			parts[index-1] = ":owner"
			parts[index] = ":repo"
		}
	}
	return "api " + strings.Join(parts, "/")
}

func ghAPIPathArg(args []string) string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-X", "--method", "-H", "--header", "--hostname", "--jq", "-q", "--preview", "--template", "-t", "--input", "--cache", "-f", "-F", "--field", "--raw-field":
			index++
			continue
		case "--paginate":
			continue
		default:
			if strings.HasPrefix(arg, "--cache=") || strings.HasPrefix(arg, "-") {
				continue
			}
			return strings.TrimSpace(arg)
		}
	}
	return ""
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

func isHexString(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}
