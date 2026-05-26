package cli

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) captureGHWebOrReal(ctx context.Context, args []string, controls ghShimControls) (string, string, int, error, bool) {
	if a.shouldUseGHWeb(ctx, args, controls) {
		if stdout, stderr, exitCode, err, ok := a.captureGHWeb(ctx, args); ok {
			return stdout, stderr, exitCode, err, true
		}
	}
	stdout, stderr, exitCode, err := a.captureRealGH(ctx, args)
	return stdout, stderr, exitCode, err, false
}

func (a *App) shouldUseGHWeb(ctx context.Context, args []string, controls ghShimControls) bool {
	if controls.Cached || controls.Live {
		return false
	}
	if controls.WebFallback {
		return true
	}
	if !ghWebCommandHostIsGitHub(args) || !ghWebArgsMayBeSupported(args) {
		return false
	}
	return a.sharedRateLimitBelowFraction(ctx, args, 0.5)
}

func ghWebArgsMayBeSupported(args []string) bool {
	switch {
	case len(args) > 0 && args[0] == "api":
		if _, ok := parseGHWebAPIContentsArgs(args[1:]); ok {
			return true
		}
		_, ok := parseGHWebAPIMediaArgs(args[1:])
		return ok
	case len(args) >= 2 && args[0] == "pr" && args[1] == "diff":
		return true
	default:
		return false
	}
}

func (a *App) sharedRateLimitBelowFraction(ctx context.Context, args []string, fraction float64) bool {
	state, ok := a.sharedRateLimitStateForArgs(ctx, args)
	if !ok || state.Limit <= 0 || state.Remaining < 0 {
		return false
	}
	if !state.UpdatedAt.IsZero() && time.Since(state.UpdatedAt) > ghRateLimitStateMaxAge() {
		return false
	}
	if !state.ResetAt.IsZero() && time.Now().After(state.ResetAt) {
		return false
	}
	return float64(state.Remaining)/float64(state.Limit) < fraction
}

func (a *App) sharedRateLimitStateForArgs(ctx context.Context, args []string) (ghSharedRateLimitState, bool) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return ghSharedRateLimitState{}, false
	}
	token := config.ResolveGitHubToken(cfg)
	host := ghRateLimitHostForArgs(args)
	if token.Value == "" {
		if !a.hasSharedRateLimitStateForHost(host) {
			return ghSharedRateLimitState{}, false
		}
		token = a.resolveGitHubToken(ctx, cfg)
		if token.Value == "" {
			return ghSharedRateLimitState{}, false
		}
	}
	return a.sharedRateLimitStateForTokenHost(token.Value, host)
}

func (a *App) captureGHWeb(ctx context.Context, args []string) (string, string, int, error, bool) {
	if !ghWebCommandHostIsGitHub(args) {
		return "", "", 0, nil, false
	}
	switch {
	case len(args) > 0 && args[0] == "api":
		return a.captureGHWebAPI(ctx, args[1:])
	case len(args) >= 2 && args[0] == "pr" && args[1] == "diff":
		return a.captureGHWebPRDiff(ctx, args[2:])
	default:
		return "", "", 0, nil, false
	}
}

func (a *App) captureGHWebAPI(ctx context.Context, args []string) (string, string, int, error, bool) {
	pathArg, ok := parseGHWebAPIContentsArgs(args)
	if ok {
		return a.captureGHWebAPIContents(ctx, pathArg)
	}
	media, ok := parseGHWebAPIMediaArgs(args)
	if ok {
		return a.captureGHWebAPIMedia(ctx, media)
	}
	return "", "", 0, nil, false
}

func (a *App) captureGHWebAPIContents(ctx context.Context, pathArg string) (string, string, int, error, bool) {
	contents, ok := parseGHWebContentsRoute(pathArg)
	if !ok {
		return "", "", 0, nil, false
	}
	rawURL := ghWebRawBaseURL() + "/" + escapePathSegments([]string{contents.Owner, contents.Repo, contents.Ref, contents.Path})
	body, status, err := fetchGHWeb(ctx, rawURL, "text/plain, */*")
	if err != nil {
		return "", "", status, nil, false
	}
	if status < 200 || status >= 300 {
		return "", "", 0, nil, false
	}
	payload := map[string]any{
		"type":         "file",
		"encoding":     "base64",
		"name":         path.Base(contents.Path),
		"path":         contents.Path,
		"sha":          gitBlobSHA(body),
		"size":         len(body),
		"content":      base64.StdEncoding.EncodeToString(body),
		"url":          fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s?ref=%s", contents.Owner, contents.Repo, escapePath(contents.Path), url.QueryEscape(contents.Ref)),
		"html_url":     ghWebBaseURL() + "/" + escapePathSegments([]string{contents.Owner, contents.Repo, "blob", contents.Ref, contents.Path}),
		"git_url":      fmt.Sprintf("https://api.github.com/repos/%s/%s/git/blobs/%s", contents.Owner, contents.Repo, gitBlobSHA(body)),
		"download_url": rawURL,
	}
	payload["_links"] = map[string]any{
		"self": payload["url"],
		"git":  payload["git_url"],
		"html": payload["html_url"],
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", "", 1, err, true
	}
	return string(data) + "\n", "", 0, nil, true
}

func (a *App) captureGHWebAPIMedia(ctx context.Context, media ghWebAPIMediaRequest) (string, string, int, error, bool) {
	var webURL string
	switch media.Kind {
	case "commit":
		webURL = fmt.Sprintf("%s/%s/%s/commit/%s.%s", ghWebBaseURL(), url.PathEscape(media.Owner), url.PathEscape(media.Repo), url.PathEscape(media.Ref), media.Format)
	case "compare":
		webURL = fmt.Sprintf("%s/%s/%s/compare/%s.%s", ghWebBaseURL(), url.PathEscape(media.Owner), url.PathEscape(media.Repo), escapeCompareRef(media.Ref), media.Format)
	default:
		return "", "", 0, nil, false
	}
	accept := "text/x-diff, text/plain, */*"
	if media.Format == "patch" {
		accept = "text/x-patch, text/plain, */*"
	}
	body, status, err := fetchGHWeb(ctx, webURL, accept)
	if err != nil {
		return "", "", status, nil, false
	}
	if status < 200 || status >= 300 {
		return "", "", 0, nil, false
	}
	return string(body), "", 0, nil, true
}

func parseGHWebAPIContentsArgs(args []string) (string, bool) {
	method := "GET"
	route := ""
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-X", "--method":
			if index+1 >= len(args) {
				return "", false
			}
			method = strings.ToUpper(strings.TrimSpace(args[index+1]))
			index++
		case "--cache":
			if index+1 >= len(args) {
				return "", false
			}
			index++
		case "--hostname":
			if index+1 >= len(args) {
				return "", false
			}
			index++
		case "-H", "--header", "--preview", "--jq", "-q", "--template", "-t", "--input",
			"-i", "--include", "--silent", "--slurp", "--paginate",
			"-f", "-F", "--field", "--raw-field":
			return "", false
		default:
			switch {
			case strings.HasPrefix(arg, "--method="):
				method = strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(arg, "--method=")))
			case strings.HasPrefix(arg, "--cache="), strings.HasPrefix(arg, "--hostname="):
			case strings.HasPrefix(arg, "--header="), strings.HasPrefix(arg, "--preview="),
				strings.HasPrefix(arg, "--jq="), strings.HasPrefix(arg, "--template="),
				strings.HasPrefix(arg, "--input="), strings.HasPrefix(arg, "-f="),
				strings.HasPrefix(arg, "-F="), strings.HasPrefix(arg, "--field="),
				strings.HasPrefix(arg, "--raw-field="):
				return "", false
			case strings.HasPrefix(arg, "-"):
				return "", false
			case route == "":
				route = arg
			default:
				return "", false
			}
		}
	}
	if method != "GET" || route == "" {
		return "", false
	}
	if _, ok := parseGHWebContentsRoute(route); !ok {
		return "", false
	}
	return route, true
}

type ghWebAPIMediaRequest struct {
	Kind   string
	Owner  string
	Repo   string
	Ref    string
	Format string
}

func parseGHWebAPIMediaArgs(args []string) (ghWebAPIMediaRequest, bool) {
	method := "GET"
	route := ""
	format := ""
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-X", "--method":
			if index+1 >= len(args) {
				return ghWebAPIMediaRequest{}, false
			}
			method = strings.ToUpper(strings.TrimSpace(args[index+1]))
			index++
		case "--cache":
			if index+1 >= len(args) {
				return ghWebAPIMediaRequest{}, false
			}
			index++
		case "--hostname":
			if index+1 >= len(args) {
				return ghWebAPIMediaRequest{}, false
			}
			index++
		case "-H", "--header":
			if index+1 >= len(args) {
				return ghWebAPIMediaRequest{}, false
			}
			var ok bool
			format, ok = mergeGHWebAPIMediaFormat(format, args[index+1])
			if !ok {
				return ghWebAPIMediaRequest{}, false
			}
			index++
		case "--preview", "--jq", "-q", "--template", "-t", "--input",
			"-i", "--include", "--silent", "--slurp", "--paginate",
			"-f", "-F", "--field", "--raw-field":
			return ghWebAPIMediaRequest{}, false
		default:
			switch {
			case strings.HasPrefix(arg, "--method="):
				method = strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(arg, "--method=")))
			case strings.HasPrefix(arg, "--cache="), strings.HasPrefix(arg, "--hostname="):
			case strings.HasPrefix(arg, "--header="):
				var ok bool
				format, ok = mergeGHWebAPIMediaFormat(format, strings.TrimPrefix(arg, "--header="))
				if !ok {
					return ghWebAPIMediaRequest{}, false
				}
			case strings.HasPrefix(arg, "--preview="), strings.HasPrefix(arg, "--jq="),
				strings.HasPrefix(arg, "--template="), strings.HasPrefix(arg, "--input="),
				strings.HasPrefix(arg, "-f="), strings.HasPrefix(arg, "-F="),
				strings.HasPrefix(arg, "--field="), strings.HasPrefix(arg, "--raw-field="):
				return ghWebAPIMediaRequest{}, false
			case strings.HasPrefix(arg, "-"):
				return ghWebAPIMediaRequest{}, false
			case route == "":
				route = arg
			default:
				return ghWebAPIMediaRequest{}, false
			}
		}
	}
	if method != "GET" || route == "" || format == "" {
		return ghWebAPIMediaRequest{}, false
	}
	request, ok := parseGHWebAPIMediaRoute(route)
	if !ok {
		return ghWebAPIMediaRequest{}, false
	}
	request.Format = format
	return request, true
}

func mergeGHWebAPIMediaFormat(existing, header string) (string, bool) {
	format, ok := ghWebAPIMediaFormat(header)
	if !ok {
		return "", false
	}
	if existing != "" && existing != format {
		return "", false
	}
	return format, true
}

func ghWebAPIMediaFormat(header string) (string, bool) {
	name, value, ok := strings.Cut(header, ":")
	if !ok || !strings.EqualFold(strings.TrimSpace(name), "Accept") {
		return "", false
	}
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimSpace(strings.Split(value, ";")[0])
	switch value {
	case "application/vnd.github.v3.diff", "application/vnd.github.diff":
		return "diff", true
	case "application/vnd.github.v3.patch", "application/vnd.github.patch":
		return "patch", true
	default:
		return "", false
	}
}

func parseGHWebAPIMediaRoute(route string) (ghWebAPIMediaRequest, bool) {
	route = strings.TrimPrefix(strings.TrimSpace(route), "https://api.github.com/")
	route = strings.TrimPrefix(route, "http://api.github.com/")
	route = strings.TrimPrefix(route, "/")
	if before, _, found := strings.Cut(route, "?"); found {
		route = before
	}
	parts := strings.Split(route, "/")
	if len(parts) != 5 || parts[0] != "repos" || parts[1] == "" || parts[2] == "" {
		return ghWebAPIMediaRequest{}, false
	}
	ref, err := url.PathUnescape(parts[4])
	if err != nil || strings.TrimSpace(ref) == "" {
		return ghWebAPIMediaRequest{}, false
	}
	switch parts[3] {
	case "commits":
		if !isHexString(ref) || len(ref) < 7 {
			return ghWebAPIMediaRequest{}, false
		}
		return ghWebAPIMediaRequest{Kind: "commit", Owner: parts[1], Repo: parts[2], Ref: ref}, true
	case "compare":
		if !strings.Contains(ref, "...") {
			return ghWebAPIMediaRequest{}, false
		}
		return ghWebAPIMediaRequest{Kind: "compare", Owner: parts[1], Repo: parts[2], Ref: ref}, true
	default:
		return ghWebAPIMediaRequest{}, false
	}
}

func gitBlobSHA(body []byte) string {
	hash := sha1.New()
	_, _ = fmt.Fprintf(hash, "blob %d\x00", len(body))
	_, _ = hash.Write(body)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func (a *App) captureGHWebPRDiff(ctx context.Context, args []string) (string, string, int, error, bool) {
	repo, number, patch, ok := a.parseGHWebPRDiffArgs(ctx, args)
	if !ok {
		return "", "", 0, nil, false
	}
	owner, repoName, err := parseOwnerRepo(repo)
	if err != nil {
		return "", "", 0, nil, false
	}
	suffix := ".diff"
	accept := "text/x-diff, text/plain, */*"
	if patch {
		suffix = ".patch"
		accept = "text/x-patch, text/plain, */*"
	}
	webURL := fmt.Sprintf("%s/%s/%s/pull/%d%s", ghWebBaseURL(), url.PathEscape(owner), url.PathEscape(repoName), number, suffix)
	body, status, err := fetchGHWeb(ctx, webURL, accept)
	if err != nil {
		return "", "", status, nil, false
	}
	if status < 200 || status >= 300 {
		return "", "", 0, nil, false
	}
	return string(body), "", 0, nil, true
}

func (a *App) parseGHWebPRDiffArgs(ctx context.Context, args []string) (string, int, bool, bool) {
	repo := ""
	number := 0
	patch := false
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-R", "--repo":
			if index+1 >= len(args) {
				return "", 0, false, false
			}
			repo = strings.TrimSpace(args[index+1])
			index++
		case "--patch":
			patch = true
		case "--color":
			if index+1 >= len(args) {
				return "", 0, false, false
			}
			if !ghWebPRDiffColorSupported(args[index+1]) {
				return "", 0, false, false
			}
			index++
		default:
			switch {
			case strings.HasPrefix(arg, "--repo="):
				repo = strings.TrimSpace(strings.TrimPrefix(arg, "--repo="))
			case strings.HasPrefix(arg, "--color="):
				if !ghWebPRDiffColorSupported(strings.TrimPrefix(arg, "--color=")) {
					return "", 0, false, false
				}
			case strings.HasPrefix(arg, "-"):
				return "", 0, false, false
			case number == 0:
				if ref, ok := parseThreadReference(arg); ok && ref.FullName() != "" && repo == "" {
					repo = ref.FullName()
				}
				parsed, err := parseThreadNumber(arg)
				if err != nil {
					return "", 0, false, false
				}
				number = parsed
			default:
				return "", 0, false, false
			}
		}
	}
	if repo == "" {
		resolved, err := a.resolveGHWebRepo(ctx)
		if err != nil {
			return "", 0, false, false
		}
		repo = resolved
	}
	return repo, number, patch, repo != "" && number > 0
}

func (a *App) resolveGHWebRepo(ctx context.Context) (string, error) {
	if envRepo := strings.TrimSpace(os.Getenv("GH_REPO")); envRepo != "" {
		return envRepo, nil
	}
	cmd := exec.CommandContext(ctx, "git", "remote", "get-url", "origin")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("repository is required outside a git checkout; pass -R owner/repo")
	}
	repo := githubRepoFromRemote(strings.TrimSpace(string(out)))
	if repo == "" {
		return "", fmt.Errorf("origin remote is not github.com; pass -R owner/repo")
	}
	return repo, nil
}

func ghWebPRDiffColorSupported(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "" || value == "never" || value == "auto"
}

type ghWebContentsRoute struct {
	Owner string
	Repo  string
	Path  string
	Ref   string
}

func parseGHWebContentsRoute(route string) (ghWebContentsRoute, bool) {
	route = strings.TrimPrefix(strings.TrimSpace(route), "https://api.github.com/")
	route = strings.TrimPrefix(route, "http://api.github.com/")
	route = strings.TrimPrefix(route, "/")
	rawQuery := ""
	if before, after, found := strings.Cut(route, "?"); found {
		route = before
		rawQuery = after
	}
	parts := strings.Split(route, "/")
	if len(parts) < 5 || parts[0] != "repos" || parts[3] != "contents" {
		return ghWebContentsRoute{}, false
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ghWebContentsRoute{}, false
	}
	ref := strings.TrimSpace(values.Get("ref"))
	if ref == "" {
		return ghWebContentsRoute{}, false
	}
	filePath, err := url.PathUnescape(strings.Join(parts[4:], "/"))
	if err != nil || strings.TrimSpace(filePath) == "" {
		return ghWebContentsRoute{}, false
	}
	return ghWebContentsRoute{Owner: parts[1], Repo: parts[2], Path: filePath, Ref: ref}, true
}

func fetchGHWeb(ctx context.Context, targetURL, accept string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", "gitcrawl")
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 || ghWebRedirectIsLogin(req.URL) {
				return http.ErrUseLastResponse
			}
			if len(via) > 0 && strings.EqualFold(req.URL.Host, via[0].URL.Host) {
				return nil
			}
			if ghWebRedirectHostAllowed(req.URL) {
				return nil
			}
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	const maxWebBodyBytes = 64 * 1024 * 1024
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxWebBodyBytes+1))
	if readErr != nil {
		return nil, resp.StatusCode, readErr
	}
	if len(body) > maxWebBodyBytes {
		return nil, resp.StatusCode, fmt.Errorf("web response exceeds %d bytes", maxWebBodyBytes)
	}
	return body, resp.StatusCode, nil
}

func ghWebCommandHostIsGitHub(args []string) bool {
	host := strings.TrimSpace(os.Getenv("GH_HOST"))
	explicitHost := host != ""
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "--hostname":
			if index+1 < len(args) {
				host = strings.TrimSpace(args[index+1])
				explicitHost = true
				index++
			}
		default:
			if strings.HasPrefix(arg, "--hostname=") {
				host = strings.TrimSpace(strings.TrimPrefix(arg, "--hostname="))
				explicitHost = true
			}
		}
	}
	if explicitHost {
		return strings.EqualFold(host, "github.com")
	}
	if baseURL := githubBaseURL(); strings.TrimSpace(baseURL) != "" {
		return ghRateLimitHostForAPIBaseURL(baseURL) == "github.com"
	}
	return true
}

func ghWebBaseURL() string {
	if raw := strings.TrimRight(strings.TrimSpace(os.Getenv("GITCRAWL_GH_WEB_BASE_URL")), "/"); raw != "" {
		return raw
	}
	return "https://github.com"
}

func ghWebRawBaseURL() string {
	if raw := strings.TrimRight(strings.TrimSpace(os.Getenv("GITCRAWL_GH_RAW_BASE_URL")), "/"); raw != "" {
		return raw
	}
	return "https://raw.githubusercontent.com"
}

func ghWebRedirectIsLogin(target *url.URL) bool {
	return strings.Trim(target.EscapedPath(), "/") == "login"
}

func ghWebRedirectHostAllowed(target *url.URL) bool {
	switch strings.ToLower(target.Hostname()) {
	case "github.com", "raw.githubusercontent.com", "patch-diff.githubusercontent.com":
		return true
	default:
		return false
	}
}

func escapePathSegments(parts []string) string {
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		for _, segment := range strings.Split(part, "/") {
			if segment != "" {
				escaped = append(escaped, url.PathEscape(segment))
			}
		}
	}
	return strings.Join(escaped, "/")
}

func escapePath(value string) string {
	return escapePathSegments([]string{value})
}

func escapeCompareRef(value string) string {
	base, head, found := strings.Cut(value, "...")
	if !found {
		return url.PathEscape(value)
	}
	return url.PathEscape(base) + "..." + url.PathEscape(head)
}
