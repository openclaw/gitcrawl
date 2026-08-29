package cli

import (
	"os"
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
