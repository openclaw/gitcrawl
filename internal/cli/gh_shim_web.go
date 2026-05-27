package cli

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
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
	case len(args) >= 2 && args[0] == "run" && args[1] == "view":
		return true
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
	case len(args) >= 2 && args[0] == "run" && args[1] == "view":
		return a.captureGHWebRunView(ctx, args[2:])
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

func (a *App) captureGHWebRunView(ctx context.Context, args []string) (string, string, int, error, bool) {
	request, ok := a.parseGHWebRunViewArgs(ctx, args)
	if !ok {
		return "", "", 0, nil, false
	}
	needsJobs := ghWebRunViewNeedsJobs(request.Fields)
	run, status, err := fetchGHWebRunSnapshot(ctx, request.Owner, request.Repo, request.RunID, needsJobs, needsJobs)
	if err != nil || status < 200 || status >= 300 {
		return "", "", status, nil, false
	}
	payload, ok := run.ghRunViewPayload(request.Fields)
	if !ok {
		return "", "", 0, nil, false
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", "", 1, err, true
	}
	output := string(data) + "\n"
	if strings.TrimSpace(request.JQExpr) != "" {
		projectedOut, projectedErr, err := runGHAPIProjection(output, request.JQExpr)
		if err != nil {
			return "", "", 0, nil, false
		}
		return projectedOut, projectedErr, 0, nil, true
	}
	return output, "", 0, nil, true
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

type ghWebRunViewRequest struct {
	Owner  string
	Repo   string
	RunID  string
	Fields []string
	JQExpr string
}

func (a *App) parseGHWebRunViewArgs(ctx context.Context, args []string) (ghWebRunViewRequest, bool) {
	repoValue := ""
	runID := ""
	fieldsRaw := ""
	jqExpr := ""
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-R", "--repo":
			if index+1 >= len(args) {
				return ghWebRunViewRequest{}, false
			}
			repoValue = strings.TrimSpace(args[index+1])
			index++
		case "--json":
			if index+1 >= len(args) {
				return ghWebRunViewRequest{}, false
			}
			fieldsRaw = strings.TrimSpace(args[index+1])
			index++
		case "--jq", "-q":
			if index+1 >= len(args) {
				return ghWebRunViewRequest{}, false
			}
			jqExpr = args[index+1]
			index++
		case "--log", "--log-failed", "--verbose", "--exit-status", "--job", "--web":
			return ghWebRunViewRequest{}, false
		default:
			switch {
			case strings.HasPrefix(arg, "--repo="):
				repoValue = strings.TrimSpace(strings.TrimPrefix(arg, "--repo="))
			case strings.HasPrefix(arg, "--json="):
				fieldsRaw = strings.TrimSpace(strings.TrimPrefix(arg, "--json="))
			case strings.HasPrefix(arg, "--jq="):
				jqExpr = strings.TrimPrefix(arg, "--jq=")
			case strings.HasPrefix(arg, "-"):
				return ghWebRunViewRequest{}, false
			case runID == "":
				runID = strings.TrimSpace(arg)
			default:
				return ghWebRunViewRequest{}, false
			}
		}
	}
	if fieldsRaw == "" || runID == "" || !isDigits(runID) {
		return ghWebRunViewRequest{}, false
	}
	if repoValue == "" {
		resolved, err := a.resolveGHWebRepo(ctx)
		if err != nil {
			return ghWebRunViewRequest{}, false
		}
		repoValue = resolved
	}
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return ghWebRunViewRequest{}, false
	}
	fields := parseJSONFields(fieldsRaw)
	if !ghWebRunViewFieldsSupported(fields) {
		return ghWebRunViewRequest{}, false
	}
	return ghWebRunViewRequest{Owner: owner, Repo: repoName, RunID: runID, Fields: fields, JQExpr: jqExpr}, true
}

func ghWebRunViewFieldsSupported(fields []string) bool {
	for _, field := range fields {
		switch field {
		case "databaseId", "number", "workflowName", "name", "displayTitle", "status",
			"conclusion", "url", "event", "headBranch", "headSha", "createdAt", "jobs":
		default:
			return false
		}
	}
	return len(fields) > 0
}

func ghWebRunViewNeedsJobs(fields []string) bool {
	for _, field := range fields {
		if field == "jobs" {
			return true
		}
	}
	return false
}

type ghWebAPIRunRequest struct {
	Owner string
	Repo  string
	RunID string
}

type ghWebAPIJobsRequest struct {
	Owner string
	Repo  string
	RunID string
}

func (a *App) captureGHWebAPIProjection(ctx context.Context, args []string, jqExpr string, controls ghShimControls) (string, string, error, bool) {
	if len(args) == 0 || args[0] != "api" {
		return "", "", nil, false
	}
	if controls.Cached || controls.Live || !ghWebCommandHostIsGitHub(args) {
		return "", "", nil, false
	}
	if !controls.WebFallback && !a.sharedRateLimitBelowFraction(ctx, args, 0.5) {
		return "", "", nil, false
	}
	if request, ok := parseGHWebAPIRunArgs(args[1:]); ok {
		if !ghWebJQFieldsSupported(jqExpr, map[string]bool{
			"id": true, "name": true, "display_title": true,
			"run_number": true, "event": true, "status": true, "conclusion": true,
			"head_branch": true, "head_sha": true, "html_url": true, "url": true,
			"jobs_url": true, "created_at": true,
		}) {
			return "", "", nil, false
		}
		run, status, err := fetchGHWebRunSnapshot(ctx, request.Owner, request.Repo, request.RunID, false, false)
		if err != nil || status < 200 || status >= 300 {
			return "", "", nil, false
		}
		data, err := json.Marshal(run.ghAPIRunPayload())
		if err != nil {
			return "", "", err, true
		}
		out, errOut, err := runGHAPIProjection(string(data)+"\n", jqExpr)
		return out, errOut, err, true
	}
	if request, ok := parseGHWebAPIJobsArgs(args[1:]); ok {
		if !ghWebJQJobsProjectionSupported(jqExpr) {
			return "", "", nil, false
		}
		if !ghWebJQFieldsSupported(jqExpr, map[string]bool{
			"total_count": true, "jobs": true, "id": true, "run_id": true, "name": true,
			"status": true, "conclusion": true, "html_url": true, "url": true,
			"started_at": true, "completed_at": true, "steps": true, "number": true,
		}) {
			return "", "", nil, false
		}
		run, status, err := fetchGHWebRunSnapshot(ctx, request.Owner, request.Repo, request.RunID, true, ghWebJQNeedsJobDetails(jqExpr))
		if err != nil || status < 200 || status >= 300 {
			return "", "", nil, false
		}
		payload := map[string]any{
			"total_count": len(run.Jobs),
			"jobs":        run.ghAPIJobsPagePayload(30),
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return "", "", err, true
		}
		out, errOut, err := runGHAPIProjection(string(data)+"\n", jqExpr)
		return out, errOut, err, true
	}
	return "", "", nil, false
}

func ghWebJQFieldsSupported(jqExpr string, fields map[string]bool) bool {
	if ghWebJQUsesUnsafeIntrospection(jqExpr) || ghWebJQUsesIdentityDot(jqExpr) {
		return false
	}
	matches := regexp.MustCompile(`\.([A-Za-z_][A-Za-z0-9_]*)`).FindAllStringSubmatch(jqExpr, -1)
	seen := false
	for _, match := range matches {
		seen = true
		if !fields[match[1]] {
			return false
		}
	}
	for _, field := range ghWebJQObjectShorthandFields(jqExpr) {
		seen = true
		if !fields[field] {
			return false
		}
	}
	indexMatches := regexp.MustCompile(`\[\s*["']([A-Za-z_][A-Za-z0-9_]*)["']\s*\]`).FindAllStringSubmatch(jqExpr, -1)
	for _, match := range indexMatches {
		seen = true
		if !fields[match[1]] {
			return false
		}
	}
	return seen
}

func ghWebJQJobsProjectionSupported(jqExpr string) bool {
	if regexp.MustCompile(`\[\s*["']jobs["']\s*\]`).MatchString(jqExpr) {
		return false
	}
	for _, field := range ghWebJQObjectShorthandFields(jqExpr) {
		if field == "jobs" {
			return false
		}
	}
	if !strings.Contains(jqExpr, ".jobs") {
		return true
	}
	if ghWebJQEmitsWholeJobs(jqExpr) {
		return false
	}
	compact := regexp.MustCompile(`\s+`).ReplaceAllString(jqExpr, " ")
	return strings.Contains(compact, ".jobs[] | [") ||
		strings.Contains(compact, ".jobs[] | {") ||
		regexp.MustCompile(`\.jobs\s*\|\s*length\b`).MatchString(jqExpr)
}

func ghWebJQNeedsJobDetails(jqExpr string) bool {
	if regexp.MustCompile(`\.(steps|started_at|completed_at)\b|\[\s*["'](steps|started_at|completed_at)["']\s*\]`).MatchString(jqExpr) {
		return true
	}
	for _, field := range ghWebJQObjectShorthandFields(jqExpr) {
		switch field {
		case "steps", "started_at", "completed_at":
			return true
		}
	}
	return false
}

func ghWebJQEmitsWholeJobs(jqExpr string) bool {
	inString := false
	escaped := false
	for index := 0; index < len(jqExpr); index++ {
		ch := jqExpr[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		if ch == '"' {
			inString = true
			continue
		}
		if !strings.HasPrefix(jqExpr[index:], ".jobs") {
			continue
		}
		nextIndex := index + len(".jobs")
		if nextIndex < len(jqExpr) && ((jqExpr[nextIndex] >= 'A' && jqExpr[nextIndex] <= 'Z') || (jqExpr[nextIndex] >= 'a' && jqExpr[nextIndex] <= 'z') || (jqExpr[nextIndex] >= '0' && jqExpr[nextIndex] <= '9') || jqExpr[nextIndex] == '_') {
			continue
		}
		for nextIndex < len(jqExpr) && (jqExpr[nextIndex] == ' ' || jqExpr[nextIndex] == '\t' || jqExpr[nextIndex] == '\n' || jqExpr[nextIndex] == '\r') {
			nextIndex++
		}
		iteratesJobs := false
		if nextIndex+1 < len(jqExpr) && jqExpr[nextIndex] == '[' && jqExpr[nextIndex+1] == ']' {
			iteratesJobs = true
			nextIndex += 2
			if nextIndex < len(jqExpr) && jqExpr[nextIndex] == '?' {
				nextIndex++
			}
			for nextIndex < len(jqExpr) && (jqExpr[nextIndex] == ' ' || jqExpr[nextIndex] == '\t' || jqExpr[nextIndex] == '\n' || jqExpr[nextIndex] == '\r') {
				nextIndex++
			}
		} else if nextIndex < len(jqExpr) && jqExpr[nextIndex] == '[' {
			return true
		}
		if nextIndex >= len(jqExpr) {
			return true
		}
		if strings.ContainsRune(",}])", rune(jqExpr[nextIndex])) {
			return true
		}
		if iteratesJobs && jqExpr[nextIndex] == '|' {
			afterPipe := nextIndex + 1
			for afterPipe < len(jqExpr) && (jqExpr[afterPipe] == ' ' || jqExpr[afterPipe] == '\t' || jqExpr[afterPipe] == '\n' || jqExpr[afterPipe] == '\r') {
				afterPipe++
			}
			if strings.HasPrefix(jqExpr[afterPipe:], "length") || strings.HasPrefix(jqExpr[afterPipe:], "select") {
				return true
			}
		}
	}
	return false
}

func ghWebJQUsesUnsafeIntrospection(jqExpr string) bool {
	return regexp.MustCompile(`\b(has|keys|keys_unsorted|to_entries|with_entries|paths|path|getpath|delpaths|type|map|select|del)\b`).MatchString(jqExpr)
}

func ghWebJQUsesIdentityDot(jqExpr string) bool {
	inString := false
	escaped := false
	for index := 0; index < len(jqExpr); index++ {
		ch := jqExpr[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		if ch == '"' {
			inString = true
			continue
		}
		if ch != '.' {
			continue
		}
		nextIndex := index + 1
		for nextIndex < len(jqExpr) && (jqExpr[nextIndex] == ' ' || jqExpr[nextIndex] == '\t' || jqExpr[nextIndex] == '\n' || jqExpr[nextIndex] == '\r') {
			nextIndex++
		}
		if nextIndex >= len(jqExpr) {
			return true
		}
		next := jqExpr[nextIndex]
		if (next >= 'A' && next <= 'Z') || (next >= 'a' && next <= 'z') || next == '_' {
			continue
		}
		if next == '[' {
			valueIndex := nextIndex + 1
			for valueIndex < len(jqExpr) && (jqExpr[valueIndex] == ' ' || jqExpr[valueIndex] == '\t' || jqExpr[valueIndex] == '\n' || jqExpr[valueIndex] == '\r') {
				valueIndex++
			}
			if valueIndex < len(jqExpr) && (jqExpr[valueIndex] == '"' || jqExpr[valueIndex] == '\'') {
				continue
			}
		}
		return true
	}
	return false
}

func ghWebJQObjectShorthandFields(jqExpr string) []string {
	var fields []string
	ident := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	for start := 0; start < len(jqExpr); start++ {
		if jqExpr[start] != '{' {
			continue
		}
		end := ghWebJQMatchingBrace(jqExpr, start)
		if end <= start {
			continue
		}
		for _, part := range ghWebJQSplitTopLevel(jqExpr[start+1:end], ',') {
			part = strings.TrimSpace(part)
			if part == "" || ghWebJQContainsTopLevel(part, ':') || !ident.MatchString(part) {
				continue
			}
			fields = append(fields, part)
		}
	}
	return fields
}

func ghWebJQMatchingBrace(value string, start int) int {
	depth := 0
	inString := false
	escaped := false
	for index := start; index < len(value); index++ {
		ch := value[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func ghWebJQSplitTopLevel(value string, sep byte) []string {
	var parts []string
	start := 0
	depth := 0
	inString := false
	escaped := false
	for index := 0; index < len(value); index++ {
		ch := value[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{', '[', '(':
			depth++
		case '}', ']', ')':
			if depth > 0 {
				depth--
			}
		case sep:
			if depth == 0 {
				parts = append(parts, value[start:index])
				start = index + 1
			}
		}
	}
	return append(parts, value[start:])
}

func ghWebJQContainsTopLevel(value string, target byte) bool {
	return len(ghWebJQSplitTopLevel(value, target)) > 1
}

func parseGHWebAPIRunArgs(args []string) (ghWebAPIRunRequest, bool) {
	route, ok := parseGHWebAPIRawGETRoute(args)
	if !ok {
		return ghWebAPIRunRequest{}, false
	}
	owner, repoName, runID, ok := parseGHWebAPIRunRoute(route)
	if !ok {
		return ghWebAPIRunRequest{}, false
	}
	return ghWebAPIRunRequest{Owner: owner, Repo: repoName, RunID: runID}, true
}

func parseGHWebAPIJobsArgs(args []string) (ghWebAPIJobsRequest, bool) {
	route, ok := parseGHWebAPIRawGETRoute(args)
	if !ok {
		return ghWebAPIJobsRequest{}, false
	}
	owner, repoName, runID, ok := parseGHWebAPIJobsRoute(route)
	if !ok {
		return ghWebAPIJobsRequest{}, false
	}
	return ghWebAPIJobsRequest{Owner: owner, Repo: repoName, RunID: runID}, true
}

func parseGHWebAPIRawGETRoute(args []string) (string, bool) {
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
		case "--cache", "--hostname":
			if index+1 >= len(args) {
				return "", false
			}
			index++
		case "-H", "--header":
			if index+1 >= len(args) || !ghWebAPIJSONHeaderSupported(args[index+1]) {
				return "", false
			}
			index++
		case "--preview", "--jq", "-q", "--template", "-t", "--input",
			"-i", "--include", "--silent", "--slurp", "--paginate",
			"-f", "-F", "--field", "--raw-field":
			return "", false
		default:
			switch {
			case strings.HasPrefix(arg, "--method="):
				method = strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(arg, "--method=")))
			case strings.HasPrefix(arg, "--cache="), strings.HasPrefix(arg, "--hostname="):
			case strings.HasPrefix(arg, "--header="):
				if !ghWebAPIJSONHeaderSupported(strings.TrimPrefix(arg, "--header=")) {
					return "", false
				}
			case strings.HasPrefix(arg, "--preview="), strings.HasPrefix(arg, "--jq="),
				strings.HasPrefix(arg, "--template="), strings.HasPrefix(arg, "--input="),
				strings.HasPrefix(arg, "-f="), strings.HasPrefix(arg, "-F="),
				strings.HasPrefix(arg, "--field="), strings.HasPrefix(arg, "--raw-field="):
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
	return route, method == "GET" && route != ""
}

func ghWebAPIJSONHeaderSupported(header string) bool {
	name, value, ok := strings.Cut(header, ":")
	if !ok || !strings.EqualFold(strings.TrimSpace(name), "Accept") {
		return false
	}
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "" || strings.Contains(value, "application/json") || strings.Contains(value, "application/vnd.github+json")
}

func parseGHWebAPIRunRoute(route string) (string, string, string, bool) {
	if strings.Contains(route, "?") {
		return "", "", "", false
	}
	parts := ghWebAPIRouteParts(route)
	if len(parts) != 6 || parts[0] != "repos" || parts[3] != "actions" || parts[4] != "runs" || !isDigits(parts[5]) {
		return "", "", "", false
	}
	return parts[1], parts[2], parts[5], true
}

func parseGHWebAPIJobsRoute(route string) (string, string, string, bool) {
	if strings.Contains(route, "?") {
		return "", "", "", false
	}
	parts := ghWebAPIRouteParts(route)
	if len(parts) != 7 || parts[0] != "repos" || parts[3] != "actions" || parts[4] != "runs" || parts[6] != "jobs" || !isDigits(parts[5]) {
		return "", "", "", false
	}
	return parts[1], parts[2], parts[5], true
}

func ghWebAPIRouteParts(route string) []string {
	route = strings.TrimPrefix(strings.TrimSpace(route), "https://api.github.com/")
	route = strings.TrimPrefix(route, "http://api.github.com/")
	route = strings.TrimPrefix(route, "/")
	if before, _, found := strings.Cut(route, "?"); found {
		route = before
	}
	return strings.Split(route, "/")
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

type ghWebRunSnapshot struct {
	Owner        string
	Repo         string
	RunID        string
	WorkflowName string
	DisplayTitle string
	Number       int
	Status       string
	Conclusion   string
	Event        string
	HeadBranch   string
	HeadSHA      string
	CreatedAt    string
	URL          string
	Jobs         []ghWebRunJob
}

type ghWebRunJob struct {
	ID          int64
	Name        string
	Status      string
	Conclusion  string
	URL         string
	StartedAt   string
	CompletedAt string
	Steps       []ghWebRunStep
}

type ghWebRunStep struct {
	Name        string
	Number      int
	Status      string
	Conclusion  string
	StartedAt   string
	CompletedAt string
}

func fetchGHWebRunSnapshot(ctx context.Context, owner, repoName, runID string, withJobs, withJobDetails bool) (ghWebRunSnapshot, int, error) {
	webURL := fmt.Sprintf("%s/%s/%s/actions/runs/%s", ghWebBaseURL(), url.PathEscape(owner), url.PathEscape(repoName), url.PathEscape(runID))
	body, status, err := fetchGHWeb(ctx, webURL, "text/html, */*")
	if err != nil || status < 200 || status >= 300 {
		return ghWebRunSnapshot{}, status, err
	}
	htmlBody := string(body)
	run := ghWebRunSnapshot{
		Owner: owner,
		Repo:  repoName,
		RunID: runID,
		URL:   webURL,
		Jobs:  nil,
	}
	if withJobs {
		jobs, ok := parseGHWebRunJobs(htmlBody)
		if !ok {
			return ghWebRunSnapshot{}, status, fmt.Errorf("could not parse GitHub run jobs")
		}
		total, ok := fetchGHWebRunJobTotal(ctx, htmlBody)
		if !ok {
			return ghWebRunSnapshot{}, status, fmt.Errorf("could not verify GitHub run job count")
		}
		if total != len(jobs) {
			jobs = mergeGHWebRunJobs(jobs, parseGHWebRunJobRefs(htmlBody, runID))
			if total != len(jobs) {
				return ghWebRunSnapshot{}, status, fmt.Errorf("GitHub run jobs are collapsed")
			}
		}
		for index := range jobs {
			needsJobPage := withJobDetails || jobs[index].Name == "" || jobs[index].Status == ""
			if !needsJobPage {
				continue
			}
			requireSteps := withJobDetails && jobs[index].Status == "completed"
			details, ok := fetchGHWebJobDetails(ctx, jobs[index].URL, requireSteps)
			if !ok {
				return ghWebRunSnapshot{}, status, fmt.Errorf("could not parse GitHub job page")
			}
			if withJobDetails && details.Status == "completed" && len(details.Steps) == 0 {
				return ghWebRunSnapshot{}, status, fmt.Errorf("could not parse GitHub job steps")
			}
			if jobs[index].Name == "" {
				jobs[index].Name = details.Name
			}
			if jobs[index].Status == "" {
				jobs[index].Status = details.Status
				jobs[index].Conclusion = details.Conclusion
			}
			if !withJobDetails {
				continue
			}
			jobs[index].StartedAt = details.StartedAt
			jobs[index].CompletedAt = details.CompletedAt
			jobs[index].Steps = details.Steps
		}
		for _, job := range jobs {
			if job.Name == "" || job.Status == "" {
				return ghWebRunSnapshot{}, status, fmt.Errorf("could not parse GitHub run job")
			}
		}
		run.Jobs = jobs
	}
	run.WorkflowName = firstNonEmpty(
		cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<span class="PageHeader-parentLink-label">\s*(.*?)</span>`)),
		cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<title>(.*?)\s+·\s+[^<]+</title>`)),
	)
	run.DisplayTitle = firstNonEmpty(
		cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<h1[^>]*class="[^"]*PageHeader-title[^"]*"[^>]*>.*?<span class="markdown-title"[^>]*>(.*?)</span>`)),
		run.WorkflowName,
	)
	if number := firstRegexSubmatch(htmlBody, `<span class="color-fg-muted" style="font-weight: 400">#([0-9]+)</span>`); number != "" {
		if parsed, err := strconv.Atoi(number); err == nil {
			run.Number = parsed
		}
	}
	run.Event = cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<div class="text-small color-fg-muted">on:\s*([^<]+)</div>`))
	run.HeadSHA = firstRegexSubmatch(htmlBody, `/commit/([0-9a-f]{40})`)
	run.HeadBranch = cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<a[^>]+class="[^"]*branch-name[^"]*"[^>]+title="([^"]+)"`))
	run.CreatedAt = firstRegexSubmatch(htmlBody, `<relative-time[^>]+datetime="([^"]+)"`)
	run.Status, run.Conclusion = parseGHWebRunState(htmlBody)
	if run.WorkflowName == "" || run.Status == "" {
		return ghWebRunSnapshot{}, status, fmt.Errorf("could not parse GitHub run page")
	}
	return run, status, nil
}

func (run ghWebRunSnapshot) ghRunViewPayload(fields []string) (map[string]any, bool) {
	row := make(map[string]any, len(fields))
	for _, field := range fields {
		switch field {
		case "databaseId":
			row[field] = ghWebNumericID(run.RunID)
		case "number":
			row[field] = run.Number
		case "workflowName", "name":
			row[field] = run.WorkflowName
		case "displayTitle":
			row[field] = run.DisplayTitle
		case "status":
			row[field] = run.Status
		case "conclusion":
			row[field] = run.Conclusion
		case "url":
			row[field] = run.URL
		case "event":
			row[field] = run.Event
		case "headBranch":
			row[field] = run.HeadBranch
		case "headSha":
			row[field] = run.HeadSHA
		case "createdAt":
			row[field] = run.CreatedAt
		case "jobs":
			row[field] = run.ghRunViewJobsPayload()
		default:
			return nil, false
		}
	}
	return row, true
}

func (run ghWebRunSnapshot) ghRunViewJobsPayload() []map[string]any {
	jobs := make([]map[string]any, 0, len(run.Jobs))
	for _, job := range run.Jobs {
		jobs = append(jobs, map[string]any{
			"databaseId":  job.ID,
			"name":        job.Name,
			"status":      job.Status,
			"conclusion":  job.Conclusion,
			"url":         job.URL,
			"startedAt":   job.StartedAt,
			"completedAt": job.CompletedAt,
			"steps":       ghWebRunStepsPayload(job.Steps, false),
		})
	}
	return jobs
}

func (run ghWebRunSnapshot) ghAPIRunPayload() map[string]any {
	payload := map[string]any{
		"id":            ghWebNumericID(run.RunID),
		"name":          run.WorkflowName,
		"display_title": run.DisplayTitle,
		"run_number":    run.Number,
		"event":         run.Event,
		"status":        run.Status,
		"conclusion":    ghWebNullableString(run.Conclusion),
		"head_branch":   run.HeadBranch,
		"head_sha":      run.HeadSHA,
		"html_url":      run.URL,
		"url":           fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%s", run.Owner, run.Repo, run.RunID),
		"jobs_url":      fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%s/jobs", run.Owner, run.Repo, run.RunID),
		"created_at":    run.CreatedAt,
	}
	return payload
}

func (run ghWebRunSnapshot) ghAPIJobsPayload() []map[string]any {
	return run.ghAPIJobsPagePayload(0)
}

func (run ghWebRunSnapshot) ghAPIJobsPagePayload(limit int) []map[string]any {
	jobRows := run.Jobs
	if limit > 0 && len(jobRows) > limit {
		jobRows = jobRows[:limit]
	}
	jobs := make([]map[string]any, 0, len(jobRows))
	for _, job := range jobRows {
		jobs = append(jobs, map[string]any{
			"id":           job.ID,
			"run_id":       ghWebNumericID(run.RunID),
			"name":         job.Name,
			"status":       job.Status,
			"conclusion":   ghWebNullableString(job.Conclusion),
			"html_url":     job.URL,
			"url":          fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/jobs/%d", run.Owner, run.Repo, job.ID),
			"started_at":   ghWebNullableString(job.StartedAt),
			"completed_at": ghWebNullableString(job.CompletedAt),
			"steps":        ghWebRunStepsPayload(job.Steps, true),
		})
	}
	return jobs
}

func ghWebRunStepsPayload(steps []ghWebRunStep, snakeCase bool) []map[string]any {
	payload := make([]map[string]any, 0, len(steps))
	for _, step := range steps {
		if snakeCase {
			payload = append(payload, map[string]any{
				"name":         step.Name,
				"number":       step.Number,
				"status":       step.Status,
				"conclusion":   ghWebNullableString(step.Conclusion),
				"started_at":   ghWebNullableString(step.StartedAt),
				"completed_at": ghWebNullableString(step.CompletedAt),
			})
			continue
		}
		payload = append(payload, map[string]any{
			"name":        step.Name,
			"number":      step.Number,
			"status":      step.Status,
			"conclusion":  step.Conclusion,
			"startedAt":   step.StartedAt,
			"completedAt": step.CompletedAt,
		})
	}
	return payload
}

func parseGHWebRunJobs(htmlBody string) ([]ghWebRunJob, bool) {
	matches := regexp.MustCompile(`(?s)<streaming-graph-job\b.*?</streaming-graph-job>`).FindAllString(htmlBody, -1)
	jobs := make([]ghWebRunJob, 0, len(matches))
	for _, block := range matches {
		idRaw := firstRegexSubmatch(block, `/job/([0-9]+)`)
		if idRaw == "" {
			continue
		}
		id, err := strconv.ParseInt(idRaw, 10, 64)
		if err != nil {
			continue
		}
		name := cleanGHWebText(firstRegexSubmatch(block, `(?s)<tool-tip[^>]*>(.*?)</tool-tip>`))
		if name == "" {
			name = cleanGHWebText(firstRegexSubmatch(block, `(?s)data-target="streaming-graph-job.name"[^>]*>(.*?)</span>`))
		}
		status, conclusion, ok := parseGHWebJobState(block)
		if !ok {
			return nil, false
		}
		href := firstRegexSubmatch(block, `href="([^"]*/job/[0-9]+)"`)
		if name == "" || href == "" {
			continue
		}
		jobs = append(jobs, ghWebRunJob{
			ID:         id,
			Name:       name,
			Status:     status,
			Conclusion: conclusion,
			URL:        ghWebBaseURL() + html.UnescapeString(href),
		})
	}
	return jobs, true
}

func parseGHWebRunJobRefs(htmlBody, runID string) []ghWebRunJob {
	pattern := fmt.Sprintf(`(?s)<a\b[^>]*href="([^"]*/actions/runs/%s/job/([0-9]+)(?:#[^"]*)?)"[^>]*>(.*?)</a>`, regexp.QuoteMeta(runID))
	matches := regexp.MustCompile(pattern).FindAllStringSubmatch(htmlBody, -1)
	jobs := make([]ghWebRunJob, 0, len(matches))
	seen := make(map[int64]bool)
	for _, match := range matches {
		id, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil || seen[id] {
			continue
		}
		seen[id] = true
		href := html.UnescapeString(match[1])
		if before, _, found := strings.Cut(href, "#"); found {
			href = before
		}
		jobs = append(jobs, ghWebRunJob{
			ID:  id,
			URL: ghWebBaseURL() + href,
		})
	}
	return jobs
}

func mergeGHWebRunJobs(primary, extra []ghWebRunJob) []ghWebRunJob {
	merged := make([]ghWebRunJob, 0, len(primary)+len(extra))
	byID := make(map[int64]int)
	for _, job := range primary {
		byID[job.ID] = len(merged)
		merged = append(merged, job)
	}
	for _, job := range extra {
		if index, ok := byID[job.ID]; ok {
			if merged[index].Name == "" {
				merged[index].Name = job.Name
			}
			if merged[index].URL == "" {
				merged[index].URL = job.URL
			}
			continue
		}
		byID[job.ID] = len(merged)
		merged = append(merged, job)
	}
	return merged
}

type ghWebJobDetails struct {
	Name        string
	Status      string
	Conclusion  string
	StartedAt   string
	CompletedAt string
	Steps       []ghWebRunStep
}

func fetchGHWebJobDetails(ctx context.Context, jobURL string, requireSteps bool) (ghWebJobDetails, bool) {
	body, httpStatus, err := fetchGHWeb(ctx, jobURL, "text/html, */*")
	if err != nil || httpStatus < 200 || httpStatus >= 300 {
		return ghWebJobDetails{}, false
	}
	htmlBody := string(body)
	stepBlocks := regexp.MustCompile(`(?s)<check-step\b.*?</check-step>`).FindAllString(htmlBody, -1)
	steps := make([]ghWebRunStep, 0, len(stepBlocks))
	for _, block := range stepBlocks {
		step := ghWebRunStep{
			Name:        cleanGHWebText(firstRegexSubmatch(block, `data-name="([^"]*)"`)),
			StartedAt:   firstRegexSubmatch(block, `data-started-at="([^"]*)"`),
			CompletedAt: firstRegexSubmatch(block, `data-completed-at="([^"]*)"`),
			Conclusion:  firstRegexSubmatch(block, `data-conclusion="([^"]*)"`),
		}
		if number := firstRegexSubmatch(block, `data-number="([0-9]+)"`); number != "" {
			if parsed, err := strconv.Atoi(number); err == nil {
				step.Number = parsed
			}
		}
		if step.Conclusion != "" || step.CompletedAt != "" {
			step.Status = "completed"
		} else {
			step.Status = "in_progress"
		}
		steps = append(steps, step)
	}
	if requireSteps && len(steps) == 0 {
		return ghWebJobDetails{}, false
	}
	jobStatus, conclusion, ok := parseGHWebJobPageState(htmlBody)
	if !ok {
		jobStatus, conclusion = ghWebJobStateFromSteps(steps)
	}
	details := ghWebJobDetails{
		Name:       firstNonEmpty(cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<span class="two-line-wrapping">(.*?)</span>`)), cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<h1[^>]*class="[^"]*PageHeader-title[^"]*"[^>]*>(.*?)</h1>`))),
		Status:     jobStatus,
		Conclusion: conclusion,
		Steps:      steps,
	}
	if len(steps) > 0 {
		details.StartedAt = steps[0].StartedAt
		details.CompletedAt = steps[len(steps)-1].CompletedAt
	}
	return details, true
}

func ghWebJobStateFromSteps(steps []ghWebRunStep) (string, string) {
	if len(steps) == 0 {
		return "", ""
	}
	allCompleted := true
	conclusion := ""
	sawSkipped := false
	sawNeutral := false
	for _, step := range steps {
		if step.Status != "completed" {
			allCompleted = false
		}
		switch step.Conclusion {
		case "failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale":
			return "completed", step.Conclusion
		case "skipped", "neutral":
			if step.Conclusion == "skipped" {
				sawSkipped = true
			} else {
				sawNeutral = true
			}
		case "success":
			if conclusion == "" {
				conclusion = "success"
			}
		}
	}
	if !allCompleted {
		return "in_progress", ""
	}
	if conclusion == "" {
		switch {
		case sawSkipped:
			conclusion = "skipped"
		case sawNeutral:
			conclusion = "neutral"
		}
	}
	return "completed", conclusion
}

func fetchGHWebRunJobTotal(ctx context.Context, htmlBody string) (int, bool) {
	fetchPath := html.UnescapeString(firstRegexSubmatch(htmlBody, `"jobGroupsFetchUrl":"([^"]+)"`))
	if fetchPath == "" {
		return 0, false
	}
	body, status, err := fetchGHWebXHR(ctx, ghWebBaseURL()+fetchPath, "application/json, */*")
	if err != nil || status < 200 || status >= 300 {
		return 0, false
	}
	var payload struct {
		TotalCount *int `json:"totalCount"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.TotalCount == nil || *payload.TotalCount < 0 {
		return 0, false
	}
	return *payload.TotalCount, true
}

func parseGHWebRunState(htmlBody string) (string, string) {
	if status := cleanGHWebText(firstRegexSubmatch(htmlBody, `(?s)<span class="h4 color-fg-default">([^<]+)</span>`)); status != "" {
		return ghWebStatusConclusionFromLabel(status)
	}
	conclusion := parseGHWebConclusion(htmlBody)
	if conclusion != "" {
		return "completed", conclusion
	}
	return "", ""
}

func parseGHWebJobState(htmlBody string) (string, string, bool) {
	conclusion := parseGHWebConclusion(htmlBody)
	if strings.Contains(htmlBody, `data-concluded="true"`) {
		if conclusion == "" {
			return "", "", false
		}
		return "completed", conclusion, true
	}
	label := strings.ToLower(html.UnescapeString(firstRegexSubmatch(htmlBody, `aria-label="([^"]+)"`)))
	switch {
	case strings.Contains(label, "waiting"):
		return "waiting", "", true
	case strings.Contains(label, "queued"):
		return "queued", "", true
	case strings.Contains(label, "pending"):
		return "pending", "", true
	case strings.Contains(label, "requested"):
		return "requested", "", true
	case strings.Contains(label, "in progress"), strings.Contains(label, "running"):
		return "in_progress", "", true
	default:
		return "", "", false
	}
}

func parseGHWebJobPageState(htmlBody string) (string, string, bool) {
	label := firstRegexSubmatch(htmlBody, `(?s)<span[^>]*class="[^"]*PageHeader-leadingVisual[^"]*"[^>]*>.*?aria-label="([^"]+)"`)
	if label == "" {
		label = firstRegexSubmatch(htmlBody, `(?s)<div[^>]*class="[^"]*PageHeader-titleBar[^"]*"[^>]*>.*?aria-label="([^"]+)"`)
	}
	if conclusion := ghWebConclusionFromLabel(label); conclusion != "" {
		return "completed", conclusion, true
	}
	label = strings.ToLower(html.UnescapeString(label))
	switch {
	case strings.Contains(label, "waiting"):
		return "waiting", "", true
	case strings.Contains(label, "queued"):
		return "queued", "", true
	case strings.Contains(label, "pending"):
		return "pending", "", true
	case strings.Contains(label, "requested"):
		return "requested", "", true
	case strings.Contains(label, "in progress"), strings.Contains(label, "running"):
		return "in_progress", "", true
	default:
		return "", "", false
	}
}

func parseGHWebConclusion(htmlBody string) string {
	return ghWebConclusionFromLabel(firstRegexSubmatch(htmlBody, `aria-label="([^"]+)"`))
}

func ghWebConclusionFromLabel(label string) string {
	label = strings.ToLower(html.UnescapeString(label))
	switch {
	case strings.Contains(label, "completed successfully") || strings.Contains(label, "success"):
		return "success"
	case strings.Contains(label, "startup failure"):
		return "startup_failure"
	case strings.Contains(label, "failed") || strings.Contains(label, "failure"):
		return "failure"
	case strings.Contains(label, "cancel"):
		return "cancelled"
	case strings.Contains(label, "skipped"):
		return "skipped"
	case strings.Contains(label, "timed out"):
		return "timed_out"
	case strings.Contains(label, "action required"):
		return "action_required"
	case strings.Contains(label, "neutral"):
		return "neutral"
	case strings.Contains(label, "stale"):
		return "stale"
	default:
		return ""
	}
}

func ghWebStatusConclusionFromLabel(label string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(label)) {
	case "success":
		return "completed", "success"
	case "failure", "failed":
		return "completed", "failure"
	case "cancelled", "canceled":
		return "completed", "cancelled"
	case "skipped":
		return "completed", "skipped"
	case "timed out":
		return "completed", "timed_out"
	case "action required":
		return "completed", "action_required"
	case "neutral":
		return "completed", "neutral"
	case "startup failure":
		return "completed", "startup_failure"
	case "stale":
		return "completed", "stale"
	case "waiting":
		return "waiting", ""
	case "queued":
		return "queued", ""
	case "pending":
		return "pending", ""
	case "requested":
		return "requested", ""
	case "in progress", "running":
		return "in_progress", ""
	default:
		return "", ""
	}
}

func cleanGHWebText(value string) string {
	value = regexp.MustCompile(`(?s)<[^>]+>`).ReplaceAllString(value, " ")
	value = html.UnescapeString(value)
	return strings.Join(strings.Fields(value), " ")
}

func firstRegexSubmatch(value, pattern string) string {
	match := regexp.MustCompile(pattern).FindStringSubmatch(value)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func ghWebNumericID(value string) any {
	if id, err := strconv.ParseInt(value, 10, 64); err == nil {
		return id
	}
	return value
}

func ghWebNullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func fetchGHWeb(ctx context.Context, targetURL, accept string) ([]byte, int, error) {
	return fetchGHWebWithHeaders(ctx, targetURL, accept, nil)
}

func fetchGHWebXHR(ctx context.Context, targetURL, accept string) ([]byte, int, error) {
	return fetchGHWebWithHeaders(ctx, targetURL, accept, map[string]string{"X-Requested-With": "XMLHttpRequest"})
}

func fetchGHWebWithHeaders(ctx context.Context, targetURL, accept string, extraHeaders map[string]string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", "gitcrawl")
	for name, value := range extraHeaders {
		req.Header.Set(name, value)
	}
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
