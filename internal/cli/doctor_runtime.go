package cli

import (
	"os"
	"path/filepath"
	"runtime/debug"
)

func runtimeIdentityPayload() map[string]any {
	out := map[string]any{
		"version": version,
	}
	if executable, err := os.Executable(); err == nil {
		out["executable_path"] = executable
		if resolved, err := filepath.EvalSymlinks(executable); err == nil && resolved != executable {
			out["resolved_executable_path"] = resolved
		}
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		out["go_version"] = info.GoVersion
		if info.Main.Path != "" {
			out["module_path"] = info.Main.Path
		}
		if info.Main.Version != "" {
			out["module_version"] = info.Main.Version
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				out["build_revision"] = setting.Value
			case "vcs.time":
				out["build_time"] = setting.Value
			case "vcs.modified":
				out["build_modified"] = setting.Value == "true"
			}
		}
	}
	return out
}
