package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/zalando/go-keyring"
)

func fixtureHumanAccessToken() string {
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"type":"app","sub":"fixture-human","email":"reader@example.test"}`)) + ".c2ln"
}

func TestRemoteAccessReaderLogin(t *testing.T) {
	keyring.MockInit()
	token := fixtureHumanAccessToken()
	t.Setenv("FIXTURE_ACCESS_TOKEN", token)
	if err := keyring.Set(config.DefaultRemoteTokenKeyringService, "gitcrawl:gitcrawl/fixture", "legacy-session"); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("CF-Access-Token") != token {
			t.Error("missing Access credential")
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.EscapedPath() {
		case "/api/v1/auth/access/session":
			if r.Method != http.MethodPost || r.Header.Get("Authorization") != "" {
				t.Error("unexpected session exchange")
			}
			_, _ = w.Write([]byte(`{"status":"complete","token":"worker-secret","owner":"fixture","org":"fixture","login":"reader@example.test"}`))
		case "/api/v1/whoami":
			if r.Header.Get("Authorization") != "Bearer worker-secret" {
				t.Error("missing Worker credential")
			}
			_, _ = w.Write([]byte(`{"owner":"fixture","org":"fixture","login":"reader@example.test","auth":"github","roles":["reader"]}`))
		case "/api/v1/apps/gitcrawl/archives/gitcrawl%2Ffixture/query":
			if r.Header.Get("Authorization") != "Bearer worker-secret" {
				t.Error("missing Worker credential on query")
			}
			var query map[string]any
			if err := json.NewDecoder(r.Body).Decode(&query); err != nil || query["name"] != "gitcrawl.threads.search" || query["limit"] != float64(1) {
				t.Error("unexpected bounded reader query")
			}
			_, _ = w.Write([]byte(`{"values":[{"thread_id":7,"number":12,"title":"fixture result","state":"open"}]}`))
		default:
			t.Errorf("unexpected route %s", r.URL.EscapedPath())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "reader.toml")
	cfg := config.Default()
	cfg.DBPath, cfg.CacheDir, cfg.LogDir, cfg.VectorDir = filepath.Join(dir, "unused.db"), filepath.Join(dir, "cache"), filepath.Join(dir, "logs"), filepath.Join(dir, "vectors")
	cfg.Remote.Mode, cfg.Remote.Endpoint, cfg.Remote.Archive = "cloud", server.URL+"/api", "gitcrawl/fixture"
	if err := config.Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	for _, args := range [][]string{
		{"remote", "login", "--access-token-env", "FIXTURE_ACCESS_TOKEN"},
		{"remote", "whoami"},
		{"search", "fixture/project", "--query", "fixture", "--limit", "1"},
	} {
		app := New()
		app.Stdout, app.Stderr = &output, &output
		if err := app.Run(context.Background(), append([]string{"--config", path, "--json"}, args...)); err != nil {
			t.Fatalf("reader flow failed at %s: %v", args[0], err)
		}
	}
	if requests != 4 || !strings.Contains(output.String(), "fixture result") {
		t.Fatalf("incomplete reader flow: requests=%d", requests)
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(stored, []byte("FIXTURE_ACCESS_TOKEN")) {
		t.Fatal("Access environment reference was not retained")
	}
	for _, token := range []string{token, "worker-secret"} {
		if strings.Contains(output.String(), token) || bytes.Contains(stored, []byte(token)) {
			t.Fatal("credential exposed in output or configuration")
		}
	}
	legacy, err := keyring.Get(config.DefaultRemoteTokenKeyringService, "gitcrawl:gitcrawl/fixture")
	if err != nil || legacy != "legacy-session" {
		t.Fatal("legacy reader session was changed")
	}
	if _, err := os.Stat(cfg.DBPath); !os.IsNotExist(err) {
		t.Fatal("reader flow created a local archive")
	}
}

func TestRemoteAccessGuardsAndSecretFreeErrors(t *testing.T) {
	token := fixtureHumanAccessToken()
	t.Setenv("FIXTURE_ACCESS_TOKEN", token)
	leakedRequests := 0
	leakReceiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leakedRequests++
		_, _ = w.Write([]byte(`{}`))
	}))
	defer leakReceiver.Close()
	for _, response := range []struct {
		name        string
		status      int
		contentType string
		location    string
	}{
		{"html", 200, "text/html", ""},
		{"denied", 403, "application/json", ""},
		{"same-origin-redirect", 302, "text/html", "/api/elsewhere"},
		{"foreign-redirect", 307, "text/html", leakReceiver.URL + "/steal"},
	} {
		t.Run(response.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Content-Type", response.contentType)
				w.Header().Set("Location", response.location)
				w.WriteHeader(response.status)
				_, _ = w.Write([]byte(token + " worker-secret"))
			}))
			defer server.Close()
			cfg := config.Default()
			cfg.Remote.Endpoint = server.URL + "/api"
			cfg.CloudflareAccess = config.CloudflareAccessConfig{Endpoint: cfg.Remote.Endpoint, TokenEnv: "FIXTURE_ACCESS_TOKEN"}
			client, err := remoteAccessHTTPClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for index, workerSession := range []bool{false, true} {
				req, _ := http.NewRequest(http.MethodPost, cfg.Remote.Endpoint+"/v1/auth/access/session", nil)
				if workerSession {
					req.Header.Set("Authorization", "Bearer worker-secret")
				}
				_, err = client.Do(req)
				if err == nil || requests != index+1 || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "worker-secret") {
					t.Fatal("challenge/redirect did not fail without credential exposure")
				}
			}
		})
	}
	if leakedRequests != 0 {
		t.Fatal("redirect reached a foreign receiver")
	}
	foreignCalls := 0
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { foreignCalls++ }))
	defer foreign.Close()
	cfg := config.Default()
	cfg.Remote.Endpoint = foreign.URL + "/api"
	cfg.CloudflareAccess = config.CloudflareAccessConfig{Endpoint: cfg.Remote.Endpoint, TokenEnv: "FIXTURE_ACCESS_TOKEN"}
	client, err := remoteAccessHTTPClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{foreign.URL + "/api-evil/v1/whoami", foreign.URL + "/api/%2e%2e/escape", "http://localhost:1/api/v1/whoami"} {
		req, _ := http.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("Authorization", "Bearer worker-secret")
		if _, err := client.Do(req); err == nil {
			t.Fatal("unbound request accepted")
		}
	}
	if foreignCalls != 0 {
		t.Fatal("unbound request reached the network")
	}
	cfg.Remote.Endpoint = foreign.URL + "/elsewhere"
	if _, err := remoteAccessHTTPClient(cfg); err == nil {
		t.Fatal("changed endpoint reused Access binding")
	}
}

func TestRemoteAccessRejectsWrongCredentialsAndRoles(t *testing.T) {
	for _, token := range []string{"", "github-pat", "worker.session", "bad\r\nheader", "e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"type":"app","sub":"","common_name":"service-client"}`)) + ".c2ln"} {
		t.Setenv("FIXTURE_ACCESS_TOKEN", token)
		if _, err := remoteAccessToken("FIXTURE_ACCESS_TOKEN"); err == nil || token != "" && strings.Contains(err.Error(), token) {
			t.Fatal("wrong credential kind accepted or disclosed")
		}
	}
	for _, roles := range [][]string{{"reader", "publisher"}, {"admin"}, {}} {
		token := fixtureHumanAccessToken()
		t.Setenv("FIXTURE_ACCESS_TOKEN", token)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "/session") {
				_, _ = w.Write([]byte(`{"status":"complete","token":"worker-secret","org":"fixture","login":"reader@example.test"}`))
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{"org": "fixture", "login": "reader@example.test", "roles": roles})
			}
		}))
		cfg := config.Default()
		cfg.Remote.Endpoint = server.URL
		cfg.CloudflareAccess = config.CloudflareAccessConfig{Endpoint: server.URL, TokenEnv: "FIXTURE_ACCESS_TOKEN"}
		client, err := remoteAccessHTTPClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := loginRemoteAccess(context.Background(), cfg, client); err == nil {
			t.Fatal("unexpected roles accepted")
		}
		server.Close()
	}
}

func TestRemoteAccessCancellationAndKeyringBinding(t *testing.T) {
	t.Setenv("FIXTURE_ACCESS_TOKEN", fixtureHumanAccessToken())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	cfg := config.Default()
	cfg.Remote.Endpoint = server.URL
	cfg.Remote.Archive = "gitcrawl/fixture"
	cfg.CloudflareAccess = config.CloudflareAccessConfig{Endpoint: server.URL, TokenEnv: "FIXTURE_ACCESS_TOKEN"}
	client, err := remoteAccessHTTPClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := loginRemoteAccess(ctx, cfg, client); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("login did not preserve caller deadline cancellation")
	}
	account := remoteAccessKeyringAccount(cfg, "reader.toml")
	cfg.CloudflareAccess.Endpoint += "/other"
	if remoteAccessKeyringAccount(cfg, "reader.toml") == account {
		t.Fatal("keyring account was not endpoint-bound")
	}
	cfg.CloudflareAccess.Endpoint = server.URL
	cfg.Remote.Archive += "-other"
	if remoteAccessKeyringAccount(cfg, "reader.toml") == account {
		t.Fatal("keyring account was not archive-bound")
	}
}

func TestRemoteAccessFailurePreservesConfigurationAndSessions(t *testing.T) {
	keyring.MockInit()
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-bound-session", true: "existing-bound-session"}[existing], func(t *testing.T) {
			dir := t.TempDir()
			cfg := config.Default()
			cfg.Remote.Endpoint, cfg.Remote.Archive = "https://archive.example.test/api", "gitcrawl/fixture"
			cfg.CloudflareAccess = config.CloudflareAccessConfig{Endpoint: cfg.Remote.Endpoint, TokenEnv: "FIXTURE_ACCESS_TOKEN"}
			path := filepath.Join(dir, "reader.toml")
			before := []byte("# original reader\nversion = 1\n")
			if err := os.WriteFile(path, before, 0o600); err != nil {
				t.Fatal(err)
			}
			account := remoteAccessKeyringAccount(cfg, path)
			if existing {
				if err := keyring.Set(config.DefaultRemoteTokenKeyringService, account, "prior-bound-session"); err != nil {
					t.Fatal(err)
				}
			}
			original := promoteRemoteAccessConfig
			promoteRemoteAccessConfig = func(string, string) error { return errors.New("fixture promotion failure") }
			t.Cleanup(func() { promoteRemoteAccessConfig = original })
			app := New()
			app.configPath = path
			if err := app.finishRemoteAccessLogin(cfg, crawlremote.LoginPollResult{Status: "complete", Token: "replacement-session"}); err == nil {
				t.Fatal("promotion failure accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("failed sign-in changed prior configuration")
			}
			stored, err := keyring.Get(config.DefaultRemoteTokenKeyringService, account)
			if existing && (err != nil || stored != "prior-bound-session") || !existing && !errors.Is(err, keyring.ErrNotFound) {
				t.Fatal("failed sign-in did not restore prior keyring state")
			}
		})
	}
}

func TestRemoteAccessReaderConfigsDoNotShareKeyringAccounts(t *testing.T) {
	keyring.MockInit()
	cfg := config.Default()
	cfg.Remote.Endpoint, cfg.Remote.Archive = "https://archive.example.test/api", "gitcrawl/fixture"
	cfg.CloudflareAccess = config.CloudflareAccessConfig{Endpoint: cfg.Remote.Endpoint, TokenEnv: "FIXTURE_ACCESS_TOKEN"}
	accounts := []string{}
	for _, name := range []string{"first", "second"} {
		app := New()
		app.Stdout = io.Discard
		app.configPath = filepath.Join(t.TempDir(), name+".toml")
		if err := app.finishRemoteAccessLogin(cfg, crawlremote.LoginPollResult{Status: "complete", Token: name + "-session"}); err != nil {
			t.Fatal(err)
		}
		loaded, err := config.Load(app.configPath)
		if err != nil {
			t.Fatal(err)
		}
		accounts = append(accounts, loaded.Remote.Auth.KeyringAccount)
	}
	if accounts[0] == accounts[1] {
		t.Fatal("isolated reader configs shared a keyring account")
	}
	for i, name := range []string{"first", "second"} {
		stored, err := keyring.Get(config.DefaultRemoteTokenKeyringService, accounts[i])
		if err != nil || stored != name+"-session" {
			t.Fatal("isolated reader overwrote another session")
		}
	}
}
