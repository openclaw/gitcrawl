package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestCloseThreadRejectsCrossRepositoryReference(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	app := New()
	app.Stdout = &bytes.Buffer{}
	err := app.Run(ctx, []string{"--config", configPath, "close-thread", "openclaw/openclaw", "--number", "https://github.com/other/repo/issues/10"})
	if ExitCode(err) != 2 || !strings.Contains(err.Error(), "does not match") {
		t.Errorf("cross-repository close error = %v, want usage error for repository mismatch", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatal(err)
	}
	threads, err := st.ListThreadsFiltered(ctx, store.ThreadListOptions{RepoID: repo.ID, Numbers: []int{10}, IncludeClosed: true})
	if err != nil || len(threads) != 1 || threads[0].ClosedAtLocal != "" {
		t.Fatalf("wrong-repository reference changed local thread: %+v, %v", threads, err)
	}
}

func TestThreadReferencesPreserveRepositoryScope(t *testing.T) {
	for _, value := range []string{"10", "#10", "issues/10", "pull/10", "OPENCLAW/OPENCLAW#10", "https://github.com/OPENCLAW/openclaw/pull/10#discussion_r1"} {
		if number, err := parseOptionalThreadNumber(value, "openclaw/openclaw"); err != nil || number != 10 {
			t.Errorf("parse %q = %d, %v", value, number, err)
		}
	}
	for _, value := range []string{"other/repo#10", "https://github.com/other/repo/issues/10", "https://example.com/other/repo/issues/10", "other/repo/issues/10"} {
		if number, err := parseOptionalThreadNumber(value, "openclaw/openclaw"); err == nil {
			t.Errorf("parse %q = %d without rejecting mismatched or unsupported repository", value, number)
		}
	}
}

func TestTUIJumpRejectsCrossRepositoryReference(t *testing.T) {
	input := textinput.New()
	input.SetValue("https://github.com/other/repo/issues/10")
	model := clusterBrowserModel{
		searchInput: input, jumping: true,
		payload: clusterBrowserPayload{Repository: "openclaw/openclaw", Clusters: []store.ClusterSummary{{ID: 1, RepresentativeNumber: 10}}},
	}
	next, cmd := model.handleJumpKey(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd != nil || next.jumping || !strings.Contains(next.status, "does not match") || next.hasDetail {
		t.Fatalf("cross-repository jump: status=%q, hasDetail=%t, cmd=%v", next.status, next.hasDetail, cmd)
	}
}

func TestCommandsRejectCrossRepositoryReferencesBeforeOpeningRuntime(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "invalid.toml")
	if err := os.WriteFile(configPath, []byte("[invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"sync", "threads", "embed", "summarize", "neighbors", "close-thread", "reopen-thread", "exclude-cluster-member", "include-cluster-member", "set-cluster-canonical"} {
		t.Run(command, func(t *testing.T) {
			flag := "--number"
			if command == "sync" || command == "threads" {
				flag = "--numbers"
			}
			args := []string{"--config", configPath, command, "openclaw/openclaw", flag, "other/repo#10"}
			if strings.Contains(command, "cluster") {
				args = append(args, "--id", "1")
			}
			err := New().Run(context.Background(), args)
			if ExitCode(err) != 2 || !strings.Contains(err.Error(), "does not match") {
				t.Fatalf("error = %v, want repository mismatch before runtime access", err)
			}
		})
	}
}

func TestThreadNumberListsRejectEmptyMembers(t *testing.T) {
	for _, value := range []string{",", "10,", ",10", "10, ,12"} {
		t.Run(value, func(t *testing.T) {
			if numbers, err := parseOptionalThreadNumberList(value, "openclaw/openclaw"); err == nil {
				t.Fatalf("numbers = %v; malformed selection must not become an unrestricted sync", numbers)
			}
		})
	}
}

func TestExplicitEmptyThreadSelectionIsRejected(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "invalid.toml")
	if err := os.WriteFile(configPath, []byte("[invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"sync", "threads", "embed", "summarize"} {
		flag := "--numbers"
		if command == "embed" || command == "summarize" {
			flag = "--number"
		}
		for _, value := range []string{"", " ", ","} {
			err := New().Run(context.Background(), []string{"--config", configPath, command, "openclaw/openclaw", flag, value})
			if ExitCode(err) != 2 {
				t.Errorf("%s %s %q: error=%v, want usage error before runtime access", command, flag, value, err)
			}
		}
	}
}
