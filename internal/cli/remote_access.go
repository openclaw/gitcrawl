package cli

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/zalando/go-keyring"
)

// The gateway assertion and Worker bearer session are separate credentials.
// Every API request, including pre-login requests, stays within this endpoint.
type remoteAccessTransport struct {
	base     http.RoundTripper
	endpoint *url.URL
	tokenEnv string
}

func remoteAccessHTTPClient(cfg config.Config) (*http.Client, error) {
	endpoint, err := remoteAccessEndpoint(cfg.Remote.Endpoint)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(cfg.CloudflareAccess.TokenEnv)
	binding := strings.TrimSpace(cfg.CloudflareAccess.Endpoint)
	if name != "" || binding != "" {
		if !validAccessEnvName(name) || binding != strings.TrimRight(strings.TrimSpace(cfg.Remote.Endpoint), "/") {
			return nil, errors.New("Cloudflare Access settings must name an environment variable and match remote.endpoint; sign in again")
		}
		if _, err := remoteAccessToken(name); err != nil {
			return nil, err
		}
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &remoteAccessTransport{base: http.DefaultTransport, endpoint: endpoint, tokenEnv: name},
	}
	if name != "" {
		client.CheckRedirect = func(*http.Request, []*http.Request) error {
			return errors.New("remote API redirects are not allowed")
		}
	}
	return client, nil
}

func remoteAccessEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(raw), "/"))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("invalid remote endpoint for reader authentication")
	}
	loopback := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, fmt.Errorf("remote endpoint cannot use bearer auth over %s; HTTPS is required except for loopback HTTP", u.Scheme)
	}
	if u.Path != "" && path.Clean(u.Path) != u.Path {
		return nil, errors.New("remote endpoint must have a canonical API path")
	}
	return u, nil
}

func validAccessEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i, c := range name {
		if c != '_' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func remoteAccessToken(name string) (string, error) {
	token := os.Getenv(name)
	if token == "" {
		return "", fmt.Errorf("Cloudflare Access token environment variable %s is empty; renew the human Access login", name)
	}
	if len(token) > 16384 {
		return "", errors.New("invalid Cloudflare Access token value")
	}
	for _, c := range token {
		if c < 33 || c > 126 {
			return "", errors.New("invalid Cloudflare Access token value")
		}
	}
	// This rejects the wrong credential kind; only the edge/backend authenticates it.
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return "", errors.New("Cloudflare Access requires a human application JWT, not a Worker, GitHub or service credential")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	if err != nil || json.Unmarshal(payload, &claims) != nil || claims["type"] != "app" {
		return "", errors.New("invalid human Cloudflare Access token")
	}
	subject, ok := claims["sub"].(string)
	if _, service := claims["common_name"]; service || !ok || strings.TrimSpace(subject) == "" {
		return "", errors.New("Cloudflare Access sign-in requires a human identity")
	}
	return token, nil
}

func remoteAccessPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}

func (t *remoteAccessTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	u := request.URL
	basePath := t.endpoint.EscapedPath()
	if t.tokenEnv != "" && (u.User != nil || u.Scheme != t.endpoint.Scheme || !strings.EqualFold(u.Hostname(), t.endpoint.Hostname()) || remoteAccessPort(u) != remoteAccessPort(t.endpoint) ||
		(u.EscapedPath() != basePath && !strings.HasPrefix(u.EscapedPath(), basePath+"/")) || path.Clean(u.Path) != u.Path) {
		return nil, errors.New("remote reader request must remain within the configured API endpoint")
	}
	req := request.Clone(request.Context())
	if t.tokenEnv != "" {
		token, err := remoteAccessToken(t.tokenEnv)
		if err != nil {
			return nil, err
		}
		req.Header.Set("CF-Access-Token", token)
	}
	response, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	redirect := response.StatusCode >= 300 && response.StatusCode < 400
	location, _ := response.Location()
	accessRedirect := location != nil && strings.HasSuffix(strings.ToLower(location.Hostname()), ".cloudflareaccess.com")
	html := strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "text/html")
	if t.tokenEnv != "" && redirect || html && (!redirect || accessRedirect) {
		_ = response.Body.Close()
		return nil, errors.New("remote API returned a redirect or HTML sign-in page; check remote.endpoint and use remote login --access-token-env with a current human Cloudflare Access token")
	}
	if t.tokenEnv != "" && response.StatusCode >= 400 {
		_ = response.Body.Close()
		return nil, fmt.Errorf("remote Access request failed (HTTP %d); check the Access and Worker sessions", response.StatusCode)
	}
	return response, nil
}

func loginRemoteAccess(ctx context.Context, cfg config.Config, httpClient *http.Client) (crawlremote.LoginPollResult, error) {
	var result crawlremote.LoginPollResult
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.Remote.Endpoint, "/")+"/v1/auth/access/session", nil)
	if err != nil {
		return result, errors.New("invalid remote Access sign-in request")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "gitcrawl/"+version)
	response, err := httpClient.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return result, fmt.Errorf("remote Access sign-in failed (HTTP %d)", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(payload) > 1<<20 || json.Unmarshal(payload, &result) != nil || result.Status != "complete" || strings.TrimSpace(result.Token) == "" || result.Login == "" || result.Org == "" || strings.HasPrefix(result.Login, "access-service:") {
		return crawlremote.LoginPollResult{}, errors.New("invalid remote Access session response")
	}
	client, err := crawlremote.NewClientFromConfig(cfg.Remote, crawlremote.Options{HTTPClient: httpClient, TokenProvider: crawlremote.StaticToken(result.Token), UserAgent: "gitcrawl/" + version})
	if err != nil {
		return crawlremote.LoginPollResult{}, err
	}
	identity, err := client.Whoami(ctx)
	if err != nil {
		return crawlremote.LoginPollResult{}, err
	}
	if identity.Login != result.Login || identity.Org != result.Org || len(identity.Roles) != 1 || identity.Roles[0] != "reader" {
		return crawlremote.LoginPollResult{}, errors.New("remote Access sign-in must return the matching reader-only identity")
	}
	return result, nil
}

func remoteAccessKeyringAccount(cfg config.Config, configPath string) string {
	endpoint, _ := remoteAccessEndpoint(cfg.CloudflareAccess.Endpoint)
	canonical := cfg.CloudflareAccess.Endpoint
	if endpoint != nil {
		canonical = endpoint.Scheme + "://" + net.JoinHostPort(strings.ToLower(endpoint.Hostname()), remoteAccessPort(endpoint)) + endpoint.EscapedPath()
	}
	resolvedPath, _ := filepath.Abs(config.ResolvePath(configPath))
	binding, _ := json.Marshal([]string{canonical, strings.TrimSpace(cfg.Remote.Archive), resolvedPath})
	return fmt.Sprintf("gitcrawl:access-reader:%x", sha256.Sum256(binding))
}

var promoteRemoteAccessConfig = os.Rename

// Stage the config before touching the endpoint/archive-bound keyring entry.
// Rename failure restores the prior session and leaves the prior config intact.
func (a *App) finishRemoteAccessLogin(cfg config.Config, result crawlremote.LoginPollResult) error {
	configPath := config.ResolvePath(a.configPath)
	if info, err := os.Lstat(configPath); err == nil && !info.Mode().IsRegular() {
		return errors.New("reader sign-in requires a regular configuration file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("cannot inspect reader configuration")
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return errors.New("cannot create reader configuration directory")
	}
	file, err := os.CreateTemp(filepath.Dir(configPath), ".gitcrawl-reader-login-*")
	if err != nil {
		return errors.New("cannot stage reader configuration")
	}
	staged := file.Name()
	_ = file.Close()
	defer os.Remove(staged)
	cfg.Remote.Mode = crawlremote.ModeCloud
	cfg.Remote.Auth = crawlremote.AuthConfig{TokenSource: "keyring", KeyringService: config.DefaultRemoteTokenKeyringService, KeyringAccount: remoteAccessKeyringAccount(cfg, configPath)}
	if err := config.Save(staged, cfg); err != nil {
		return errors.New("cannot stage reader configuration")
	}
	service, account := cfg.Remote.Auth.KeyringService, cfg.Remote.Auth.KeyringAccount
	previous, previousErr := keyring.Get(service, account)
	if previousErr != nil && !errors.Is(previousErr, keyring.ErrNotFound) {
		return errors.New("cannot inspect the bound reader session in the keyring")
	}
	if _, err := config.StoreRemoteToken(cfg, result.Token); err != nil {
		return errors.New("cannot store the bound reader session in the keyring")
	}
	if err := promoteRemoteAccessConfig(staged, configPath); err != nil {
		var rollback error
		if previousErr == nil {
			rollback = keyring.Set(service, account, previous)
		} else {
			rollback = keyring.Delete(service, account)
		}
		if rollback != nil {
			return errors.New("reader configuration failed and the previous bound session could not be restored")
		}
		return errors.New("cannot promote reader configuration; previous session restored")
	}
	return a.writeRemoteLoginResult(cfg, "cloudflare-access", result)
}
