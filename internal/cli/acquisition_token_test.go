package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
)

func TestAcquireAPIAppTokenFailsBeforeAPIOrPersonalAuth(t *testing.T) {
	for _, test := range []struct{ name, token, octopool string }{
		{"missing", "", ""},
		{"malformed", "not-a-credential", ""},
		{"conflicting", "not-a-credential", "relay-token"},
		{"expired", "expired", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := filepath.Join(t.TempDir(), "archive.sqlite")
			t.Setenv("GITCRAWL_DB", db)
			t.Setenv("PUB", "publisher")
			t.Setenv("READ", "reader")
			t.Setenv("OCTOPOOL_URL", "")
			t.Setenv("OCTOPOOL_POOL", "")
			t.Setenv("OCTOPOOL_TOKEN", test.octopool)
			token := test.token
			if token == "expired" {
				c := github.AcquisitionToken{Token: "synthetic-secret", AppID: 1, InstallationID: 2, RepositoryID: 3, Repository: "fixture/repo", ExpiresAt: time.Now().Add(time.Minute), Permissions: map[string]string{"metadata": "read", "issues": "read", "contents": "read", "pull_requests": "read"}}
				raw, _ := json.Marshal(c)
				token = string(raw)
			}
			t.Setenv("APP_CREDENTIAL", token)
			a := New()
			var output bytes.Buffer
			a.Stdout = &output
			a.Stderr = io.Discard
			a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal auth used"); return "", nil }
			err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", "http://127.0.0.1:1", "--source", "fixture", "--repo", "fixture/repo", "--publisher-token-env", "PUB", "--reader-token-env", "READ", "--github-app-token-env", "APP_CREDENTIAL", "--since", "2026-01-01T00:00:00Z"})
			if err == nil || strings.Contains(err.Error(), "synthetic-secret") || strings.Contains(err.Error(), "connection refused") || output.Len() != 0 {
				t.Fatal("invalid credential reached API or output", err)
			}
			if _, err := os.Stat(db); !os.IsNotExist(err) {
				t.Fatal("SQLite touched")
			}
		})
	}
}

func TestMintAcquisitionCommandNeverResolvesUserAuth(t *testing.T) {
	if releaseNotificationAllowed([]string{"mint-acquisition-token"}) {
		t.Fatal("mint would perform unrelated release lookup")
	}
	a := New()
	var output bytes.Buffer
	a.Stdout = &output
	a.Stderr = io.Discard
	a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal token lookup"); return "", nil }
	err := a.Run(context.Background(), []string{"mint-acquisition-token", "--app-id", "1", "--installation-id", "2", "--repository-id", "3", "--repo", "fixture/repo", "--private-key-file", "relative-key.pem"})
	if err == nil || !strings.Contains(err.Error(), "root") || output.Len() != 0 {
		t.Fatal("unsafe key route accepted", err)
	}
}

func TestExplicitEmptyAppTokenFlagCannotFallBackToOctopool(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++; w.WriteHeader(500) }))
	defer server.Close()
	t.Setenv("PUB", "publisher")
	t.Setenv("READ", "reader")
	t.Setenv("OCTOPOOL_TOKEN", "relay")
	t.Setenv("OCTOPOOL_URL", server.URL)
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", server.URL, "--source", "fixture", "--repo", "fixture/repo", "--publisher-token-env", "PUB", "--reader-token-env", "READ", "--github-app-token-env="})
	if err == nil || !strings.Contains(err.Error(), "github-app-token-env requires") || requests != 0 {
		t.Fatal("empty App auth fell back", err, requests)
	}
}

func TestMintAcquisitionCohortRejectsAmbiguousInputBeforeKeyHandling(t *testing.T) {
	for _, tc := range []struct {
		args []string
		in   string
		want string
	}{
		{[]string{"--repo", "fixture/repo", "--repository-id", "2"}, `[{"id":1,"name":"fixture/repo"}]`, "or cohort-stdin"},
		{nil, `[{"id":1,"name":"fixture/repo","scope":"invalid"}]`, "expected a nonempty acquisition cohort"},
		{nil, `[{"id":1,"name":"fixture/repo"}] {}`, "expected a nonempty acquisition cohort"},
		{nil, `[{"id":1,"name":"fixture/repo","id":2,"name":"fixture/other"}]`, "unique id and name fields"},
		{nil, `[{"id":1,"name":"fixture/repo","NAME":"fixture/other"}]`, "unique id and name fields"},
		{nil, strings.Repeat(" ", (128<<10)+1), "within its size bound"},
	} {
		a := New()
		a.Stdin = strings.NewReader(tc.in)
		var output bytes.Buffer
		a.Stdout = &output
		a.Stderr = io.Discard
		args := []string{"mint-acquisition-token", "--app-id", "1", "--installation-id", "2", "--private-key-file", "relative-key.pem", "--cohort-stdin"}
		err := a.Run(context.Background(), append(args, tc.args...))
		if err == nil || !strings.Contains(err.Error(), tc.want) || output.Len() != 0 {
			t.Fatalf("unsafe cohort input accepted: %v", err)
		}
	}
}
