package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPortableGitEnvironmentIsolation(t *testing.T) {
	for _, key := range []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_CONFIG", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0", "GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM", "GIT_CONFIG_NOSYSTEM", "GIT_ASKPASS", "SSH_ASKPASS", "GIT_SSH_COMMAND"} {
		t.Setenv(key, "untrusted-input")
	}
	readEnv := func() map[string]string {
		result := map[string]string{}
		for _, entry := range portableGitEnv() {
			key, value, _ := strings.Cut(entry, "=")
			result[key] = value
			if value == "untrusted-input" {
				t.Fatalf("retained unsafe input: %s", key)
			}
		}
		return result
	}
	readEnv()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "true")
	env := readEnv()
	for key, value := range map[string]string{"GIT_CONFIG_GLOBAL": os.DevNull, "GIT_CONFIG_SYSTEM": os.DevNull, "GIT_CONFIG_NOSYSTEM": "true", "GIT_TERMINAL_PROMPT": "0", "GIT_ATTR_NOSYSTEM": "1", "GCM_INTERACTIVE": "never", "GIT_NO_REPLACE_OBJECTS": "1"} {
		if env[key] != value {
			t.Fatalf("lost isolation control %s", key)
		}
	}
}

func TestPortableRefreshRejectsArgumentsBeforeConfigOrGit(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.toml")
	writePortableSafetyFile(t, configPath, []byte("deliberately invalid config: must not be read"))
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"unknown-flag", []string{"--unknown-flag"}, "flag provided but not defined"},
		{"bad-duration", []string{"--timeout", "later"}, "invalid value"},
		{"positional", []string{"unexpected"}, "no positional arguments"},
		{"zero-timeout", []string{"--timeout", "0"}, "positive timeout and byte limits"},
		{"zero-reserve", []string{"--min-free-bytes", "0"}, "positive timeout and byte limits"},
		{"negative-growth", []string{"--max-growth-bytes=-1"}, "positive timeout and byte limits"},
		{"missing-remote", nil, "--expected-remote is required"},
		{"unsafe-path", []string{"--expected-remote", "https://example.invalid/store.git", "--portable-db", "../archive.db"}, "clean relative slash path"},
		{"relative-git", []string{"--expected-remote", "https://example.invalid/store.git", "--git", "relative-git"}, "absolute path"},
		{"unsafe-branch", []string{"--expected-remote", "https://example.invalid/store.git", "--git", os.Args[0], "--branch=-option"}, "invalid --branch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := New()
			app.configPath = configPath
			var stdout, stderr bytes.Buffer
			app.Stdout, app.Stderr = &stdout, &stderr
			before := portableTestSnapshot(t, root)
			err := app.runPortableRefresh(context.Background(), test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected argument refusal %q, got %v", test.want, err)
			}
			if stdout.Len() != 0 || stderr.Len() != 0 || !bytes.Equal(before, portableTestSnapshot(t, root)) {
				t.Fatal("invalid arguments reached refresh or changed config/runtime")
			}
		})
	}
}
