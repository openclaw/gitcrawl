package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
)

func progressLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))
}

func (a *App) applyCommandJSON(enabled bool) {
	if enabled {
		a.format = FormatJSON
	}
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func resolveOutputFormat(value string, jsonOut bool) (OutputFormat, error) {
	if jsonOut {
		return FormatJSON, nil
	}
	switch OutputFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", FormatText:
		return FormatText, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatLog:
		return FormatLog, nil
	default:
		return "", fmt.Errorf("unsupported format %q: use text, json, or log", value)
	}
}

func (a *App) writeOutput(title string, payload any, allowLog bool) error {
	switch a.format {
	case FormatJSON:
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.Stdout, "%s\n", data)
		return err
	case FormatLog:
		if allowLog {
			_, err := fmt.Fprintf(a.Stdout, "%s=%v\n", title, payload)
			return err
		}
		fallthrough
	default:
		if versionPayload, ok := payload.(map[string]string); ok && title == "version" {
			_, err := fmt.Fprintln(a.Stdout, versionPayload["version"])
			return err
		}
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.Stdout, "%s\n%s\n", title, data)
		return err
	}
}

func (a *App) writeInitOutput(result initResult) error {
	switch a.format {
	case FormatJSON:
		return a.writeOutput("init", result, true)
	case FormatLog:
		_, err := fmt.Fprintf(a.Stdout, "init config_path=%s runtime_dir=%s db_path=%s portable_store=%s remote=%s archive=%s\n", result.ConfigPath, result.RuntimeDir, result.DBPath, result.PortableStore, result.RemoteEndpoint, result.RemoteArchive)
		return err
	default:
		lines := []string{
			"gitcrawl init",
			"config path: " + result.ConfigPath,
			"db path: " + result.DBPath,
			"cache dir: " + result.CacheDir,
			"vector dir: " + result.VectorDir,
			"log dir: " + result.LogDir,
		}
		if result.PortableStoreURL != "" {
			lines = append(lines,
				"",
				"Portable store",
				"  url: "+result.PortableStoreURL,
				"  checkout: "+result.PortableStoreDir,
				"  state: "+firstNonEmpty(result.PortableStore, "ready"),
			)
		}
		if result.RemoteEndpoint != "" {
			lines = append(lines,
				"",
				"Remote archive",
				"  endpoint: "+result.RemoteEndpoint,
				"  archive: "+result.RemoteArchive,
				"  mode: "+result.RemoteMode,
			)
		}
		_, err := fmt.Fprintln(a.Stdout, strings.Join(lines, "\n"))
		return err
	}
}
