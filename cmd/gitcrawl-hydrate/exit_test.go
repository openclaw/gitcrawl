package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

// Run the real operator entry point in a child so os.Exit is observed without
// replacing run(), the HTTP client, or the error-classification boundary.
func TestHydrateExitProcess(t *testing.T) {
	if os.Getenv("GITCRAWL_HYDRATE_EXIT_FIXTURE") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"gitcrawl-hydrate"}, os.Args[i+1:]...)
			break
		}
	}
	flag.CommandLine = flag.NewFlagSet("gitcrawl-hydrate", flag.ExitOnError)
	main()
	os.Exit(0)
}

func TestHydrateExitStatus(t *testing.T) {
	for _, scenario := range []struct {
		name, phase string
		statuses    []int
		want        int
	}{
		{"REST reset exhausted", "REST", []int{0, 0, 0}, 75},
		{"GraphQL reset exhausted", "GraphQL", []int{0, 0, 0}, 75},
		{"REST incomplete bodies exhausted", "REST", []int{-1, -2, -1}, 75},
		{"GraphQL incomplete bodies exhausted", "GraphQL", []int{-2, -1, -2}, 75},
		{"REST storage then incomplete bodies", "REST", []int{424, -1, -2}, 1},
		{"GraphQL incomplete body then authentication", "GraphQL", []int{-1, 403}, 1},
		{"authentication", "GraphQL", []int{403}, 1},
		{"quota", "GraphQL", []int{429}, 1},
		{"storage then resets", "GraphQL", []int{424, 0, 0}, 1},
		{"reset then authentication", "GraphQL", []int{0, 403}, 1},
		{"inventory authentication with reset worker", "stream", []int{0, 0, 0}, 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Path  string              `json:"path"`
					Query map[string][]string `json:"query"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if scenario.phase == "REST" || request.Path == "/graphql" {
					i := int(calls.Add(1)) - 1
					if i >= len(scenario.statuses) {
						t.Error("exceeded retry budget")
						w.WriteHeader(500)
						return
					}
					status := scenario.statuses[i]
					if status < 0 {
						truncateHydrateBody(t, w, status == -2)
						return
					}
					if status == 0 {
						resetHydrateConnection(t, w)
						return
					}
					w.WriteHeader(status)
					if status == 424 {
						fmt.Fprint(w, `{"error":{"code":"fallback_local","details":{"reason":"relay_storage_unavailable"}}}`)
					}
					return
				}
				var body any
				headers := map[string]string{}
				switch request.Path {
				case "/repos/fixture/repo":
					body = map[string]any{"id": 123}
				case "/repos/fixture/repo/issues":
					if len(request.Query["after"]) > 0 {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					body = []any{map[string]any{"id": 7, "node_id": "I_7", "number": 7, "updated_at": "2026-01-01T00:00:00Z"}}
					headers["link"] = `<https://api.github.com/repositories/123/issues?state=all&sort=created&direction=asc&per_page=100&page=2&after=fixture-cursor>; rel="next"`
				case "/repos/fixture/repo/issues/7":
					body = map[string]any{"id": 7, "node_id": "I_7", "number": 7, "updated_at": "2026-01-01T00:00:00Z"}
				default:
					t.Errorf("unexpected path %s", request.Path)
					w.WriteHeader(400)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "headers": headers, "relay": map[string]any{"cache": "miss"}})
			}))
			defer server.Close()
			dir := t.TempDir()
			db := filepath.Join(dir, "native.sqlite")
			st, err := store.Open(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			numbers := filepath.Join(dir, "numbers")
			if err = os.WriteFile(numbers, []byte("7\n"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHydrateExitProcess$", "--", "--db", db, "--repo", "fixture/repo", "--numbers-file", numbers)
			if scenario.phase == "stream" {
				cmd.Args = append(cmd.Args, "--include-discovered", "--inventory-checkpoint", filepath.Join(dir, "inventory.sqlite"))
			}
			cmd.Env = append(os.Environ(), "GITCRAWL_HYDRATE_EXIT_FIXTURE=1", "OCTOPOOL_URL="+server.URL, "OCTOPOOL_POOL=fixture", "OCTOPOOL_TOKEN=fixture-caller")
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != scenario.want {
				t.Fatalf("exit=%v want=%d output=%s", err, scenario.want, output)
			}
			if int(calls.Load()) != len(scenario.statuses) {
				t.Fatalf("calls=%d want=%d output=%s", calls.Load(), len(scenario.statuses), output)
			}
			if strings.Contains(string(output), "fixture-caller") || strings.Contains(string(output), server.URL) {
				t.Fatalf("private transport diagnostics leaked: %s", output)
			}
		})
	}
}

func resetHydrateConnection(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		conn.Close()
		t.Error("not TCP")
		return
	}
	if err = tcp.SetLinger(0); err != nil {
		t.Error(err)
	}
	conn.Close()
}

func TestPreparationFailureKeepsEverySubstantiveError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { resetHydrateConnection(t, w) }))
	defer server.Close()
	client, err := github.NewOctopoolConversations(server.URL, "fixture", "fixture-caller", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	_, reset := client.Client.GetIssue(context.Background(), "fixture", "repo", 7, nil)
	if !github.IsExhaustedOctopoolRead(reset) {
		t.Fatalf("fixture did not exhaust native reset budget: %v", reset)
	}
	fatal := errors.New("fixture authentication failure")
	for _, test := range []struct {
		name     string
		failures []error
		canceled bool
		retry    bool
	}{
		{"own sibling cancellation", []error{reset, fmt.Errorf("sibling: %w", context.Canceled)}, false, true},
		{"mixed fatal", []error{reset, fatal}, false, false},
		{"reverse mixed fatal", []error{fatal, reset}, false, false},
		{"joined fatal and cancel", []error{reset, errors.Join(fatal, context.Canceled)}, false, false},
		{"raw reset not certified", []error{syscall.ECONNRESET}, false, false},
		{"external cancellation", []error{reset}, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.canceled {
				cancel()
			}
			got := preparationFailure(ctx, test.failures)
			if github.IsExhaustedOctopoolRead(got) != test.retry {
				t.Fatalf("retry=%t error=%v", !test.retry, got)
			}
			for _, failure := range test.failures {
				if errors.Is(failure, fatal) && !errors.Is(got, fatal) {
					t.Fatal("lost substantive failure")
				}
			}
			if test.canceled && !errors.Is(got, context.Canceled) {
				t.Fatal("lost external cancellation")
			}
		})
	}
}

func truncateHydrateBody(t *testing.T, w http.ResponseWriter, reset bool) {
	t.Helper()
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()
	if _, err = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n{\"private-fixture\":"); err != nil {
		t.Error(err)
		return
	}
	if err = buf.Flush(); err != nil {
		t.Error(err)
		return
	}
	if reset {
		time.Sleep(20 * time.Millisecond)
		if err = conn.(*net.TCPConn).SetLinger(0); err != nil {
			t.Error(err)
		}
	}
}
