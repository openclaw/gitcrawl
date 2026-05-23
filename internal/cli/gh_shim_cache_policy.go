package cli

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

func cacheableGHRead(args []string) bool {
	if len(args) == 0 || hasAnyGHFlag(args, "--web", "--browser", "--interactive") {
		return false
	}
	switch args[0] {
	case "api":
		return ghAPIReadOnly(args[1:])
	case "cache":
		return len(args) >= 2 && args[1] == "list"
	case "gist":
		return len(args) >= 2 && (args[1] == "list" || args[1] == "view")
	case "label":
		return len(args) >= 2 && args[1] == "list"
	case "org":
		return len(args) >= 2 && args[1] == "list"
	case "project":
		return len(args) >= 2 && (args[1] == "list" || args[1] == "view" || args[1] == "field-list" || args[1] == "item-list")
	case "run":
		return len(args) >= 2 && (args[1] == "list" || args[1] == "view")
	case "pr":
		return len(args) >= 2 && (args[1] == "diff" || args[1] == "checks" || args[1] == "list" || args[1] == "status" || args[1] == "view")
	case "issue":
		return len(args) >= 2 && (args[1] == "list" || args[1] == "status" || args[1] == "view")
	case "release":
		return len(args) >= 2 && (args[1] == "list" || args[1] == "view")
	case "repo":
		return len(args) >= 2 && (args[1] == "view" || args[1] == "list")
	case "ruleset":
		return len(args) >= 2 && (args[1] == "check" || args[1] == "list" || args[1] == "view")
	case "search":
		return len(args) >= 2 && (args[1] == "code" || args[1] == "commits" || args[1] == "issues" || args[1] == "prs" || args[1] == "repos")
	case "secret":
		return len(args) >= 2 && args[1] == "list"
	case "variable":
		return len(args) >= 2 && (args[1] == "get" || args[1] == "list")
	case "workflow":
		return len(args) >= 2 && (args[1] == "list" || args[1] == "view")
	default:
		return false
	}
}

func ghCommandName(args []string) string {
	if len(args) == 0 {
		return ""
	}
	if args[0] == "api" {
		return "api"
	}
	if len(args) == 1 {
		return args[0]
	}
	return args[0] + " " + args[1]
}

func ghAPIReadOnly(args []string) bool {
	method := "GET"
	path := ghAPIPathArg(args)
	if path == "graphql" {
		return ghGraphQLReadOnly(args)
	}
	hasFields := false
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "--input":
			return false
		case "--cache":
			index++
		case "-F", "-f", "--field", "--raw-field":
			if index+1 >= len(args) {
				return false
			}
			hasFields = true
			index++
		case "--method", "-X":
			if index+1 >= len(args) {
				return false
			}
			method = strings.ToUpper(args[index+1])
			index++
		default:
			if strings.HasPrefix(arg, "--method=") {
				method = strings.ToUpper(strings.TrimPrefix(arg, "--method="))
			}
			for _, prefix := range []string{"-f=", "-F=", "--field=", "--raw-field="} {
				if strings.HasPrefix(arg, prefix) {
					hasFields = true
				}
			}
		}
	}
	// REST field flags usually make gh send a request body, so keep them
	// mutating unless this is a GET-only Search endpoint sanitized above.
	if hasFields && !ghAPISearchEndpoint(path) {
		return false
	}
	return method == "GET"
}

func sanitizeGHShimArgs(args []string) []string {
	if len(args) == 0 || args[0] != "api" {
		return args
	}
	apiArgs := args[1:]
	if !ghAPISearchEndpoint(ghAPIPathArg(apiArgs)) || ghAPIHasExplicitMethod(apiArgs) || !ghAPIHasFieldArgs(apiArgs) {
		return args
	}
	// Raw gh treats `gh api <path> -f name=value` as a POST unless the method
	// is explicit. GitHub Search REST endpoints are GET-only, so agent-shaped
	// field calls need this before they fall through to real gh or the cache.
	out := make([]string, 0, len(args)+2)
	out = append(out, "api", "--method", "GET")
	out = append(out, args[1:]...)
	return out
}

func ghAPISearchEndpoint(path string) bool {
	// Keep this route-only so normal mutating `gh api repos/... -f ...` calls
	// are not accidentally reclassified as safe reads.
	path = strings.TrimPrefix(path, "https://api.github.com/")
	path = strings.TrimPrefix(path, "http://api.github.com/")
	path = strings.TrimPrefix(path, "/")
	if before, _, found := strings.Cut(path, "?"); found {
		path = before
	}
	return path == "search" || strings.HasPrefix(path, "search/")
}

func ghAPIHasExplicitMethod(args []string) bool {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "--method", "-X":
			return true
		default:
			if strings.HasPrefix(arg, "--method=") {
				return true
			}
		}
	}
	return false
}

func ghAPIHasFieldArgs(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-f", "-F", "--field", "--raw-field":
			return true
		default:
			for _, prefix := range []string{"-f=", "-F=", "--field=", "--raw-field="} {
				if strings.HasPrefix(arg, prefix) {
					return true
				}
			}
		}
	}
	return false
}

func ghGraphQLReadOnly(args []string) bool {
	method := "POST"
	query := ""
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "--input":
			return false
		case "--method", "-X":
			if index+1 >= len(args) {
				return false
			}
			method = strings.ToUpper(args[index+1])
			index++
		case "-f", "-F", "--field", "--raw-field":
			if index+1 >= len(args) {
				return false
			}
			name, value, ok := strings.Cut(args[index+1], "=")
			if ok && strings.HasPrefix(strings.TrimSpace(value), "@") {
				return false
			}
			if ok && name == "query" {
				query = value
			}
			index++
		default:
			for _, prefix := range []string{"-f=", "-F=", "--field=", "--raw-field="} {
				if strings.HasPrefix(arg, prefix) {
					name, value, ok := strings.Cut(strings.TrimPrefix(arg, prefix), "=")
					if ok && strings.HasPrefix(strings.TrimSpace(value), "@") {
						return false
					}
					if ok && name == "query" {
						query = value
					}
				}
			}
			if strings.HasPrefix(arg, "--method=") {
				method = strings.ToUpper(strings.TrimPrefix(arg, "--method="))
			}
		}
	}
	if method != "GET" && method != "POST" {
		return false
	}
	query = strings.TrimSpace(query)
	if query == "" || strings.HasPrefix(query, "@") {
		return false
	}
	lower := strings.ToLower(query)
	return strings.HasPrefix(lower, "query") || strings.HasPrefix(lower, "{")
}

func (a *App) ghCommandCacheTTL(ctx context.Context, args []string) time.Duration {
	return ghCommandCacheTTLBase(args, a.ghCommandStableIdentity(ctx, args) != "")
}

func ghCommandCacheTTL(args []string) time.Duration {
	return ghCommandCacheTTLBase(args, false)
}

func ghCommandCacheTTLBase(args []string, stablePRDiff bool) time.Duration {
	if raw := strings.TrimSpace(os.Getenv("GITCRAWL_GH_CACHE_TTL")); raw != "" {
		if duration, err := time.ParseDuration(raw); err == nil && duration > 0 {
			return duration
		}
	}
	if len(args) >= 2 {
		if args[0] == "pr" && args[1] == "diff" {
			if stablePRDiff {
				return 7 * 24 * time.Hour
			}
			return 5 * time.Minute
		}
		if args[0] == "api" {
			return ghAPICacheTTL(args[1:])
		}
		switch args[0] {
		case "run":
			return ghRunCacheTTL(args[1:])
		case "workflow":
			return 15 * time.Minute
		case "search":
			return 15 * time.Minute
		case "release":
			return time.Hour
		case "repo", "ruleset":
			return time.Hour
		case "secret", "variable", "label", "org", "project", "gist", "cache":
			return 10 * time.Minute
		case "issue", "pr":
			return 15 * time.Minute
		}
	}
	return 5 * time.Minute
}

func ghRunCacheTTL(args []string) time.Duration {
	if len(args) == 0 {
		return 30 * time.Second
	}
	switch args[0] {
	case "view":
		if hasAnyGHFlag(args[1:], "--log", "--log-failed") {
			return 12 * time.Hour
		}
		if hasAnyGHFlag(args[1:], "--job") {
			return 1 * time.Minute
		}
		return 30 * time.Second
	case "list":
		return 30 * time.Second
	default:
		return 30 * time.Second
	}
}

func ghAPICacheTTL(args []string) time.Duration {
	if ttl, ok := ghAPICacheFlagTTL(args); ok {
		return ttl
	}
	route := normalizeGHAPIRoute(args)
	switch {
	case ghAPISearchEndpoint(ghAPIPathArg(args)) && ghAPISearchQueryIsStable(args):
		return 7 * 24 * time.Hour
	case ghAPISearchEndpoint(ghAPIPathArg(args)):
		return 15 * time.Minute
	case route == "api graphql":
		return 6 * time.Hour
	case strings.HasPrefix(route, "api users/"):
		return 7 * 24 * time.Hour
	case strings.Contains(route, "/contents"):
		if ghAPIContentRefIsStable(args) {
			return 7 * 24 * time.Hour
		}
		return 30 * time.Minute
	case strings.Contains(route, "/pages/builds/latest"):
		return 2 * time.Minute
	case strings.Contains(route, "/pages/health"):
		return 15 * time.Minute
	case strings.Contains(route, "/pages"):
		return 30 * time.Minute
	case strings.Contains(route, "/actions/runs/:id/logs"):
		return 12 * time.Hour
	case strings.Contains(route, "/actions/jobs/:id/logs"):
		return 12 * time.Hour
	case strings.Contains(route, "/actions/runs/:id/jobs"):
		return 1 * time.Minute
	case strings.Contains(route, "/actions/jobs/:id"):
		return 1 * time.Minute
	case strings.Contains(route, "/pending_deployments"):
		return 30 * time.Second
	case strings.Contains(route, "/actions/runs/:id"):
		return 30 * time.Second
	case strings.Contains(route, "/actions/workflows/"):
		return 15 * time.Minute
	case strings.Contains(route, "/actions/runs"):
		return 30 * time.Second
	case strings.Contains(route, "/releases"):
		return 1 * time.Hour
	case strings.Contains(route, "/issues/:id/comments") || strings.Contains(route, "/pulls/:id/comments") || strings.Contains(route, "/pulls/:id/reviews"):
		return time.Hour
	case strings.Contains(route, "/issues/:id") || strings.Contains(route, "/pulls/:id"):
		return 30 * time.Minute
	case route == "api repos/:owner/:repo":
		return time.Hour
	case strings.Contains(route, "/pulls") || strings.Contains(route, "/issues"):
		return 15 * time.Minute
	case strings.Contains(route, "/branches") || strings.Contains(route, "/commits"):
		return 10 * time.Minute
	default:
		return 5 * time.Minute
	}
}

func ghAPIContentRefIsStable(args []string) bool {
	path := ghAPIPathArg(args)
	_, rawQuery, found := strings.Cut(path, "?")
	if !found {
		return false
	}
	for _, part := range strings.Split(rawQuery, "&") {
		name, value, ok := strings.Cut(part, "=")
		if !ok || name != "ref" {
			continue
		}
		value = strings.TrimSpace(value)
		if decoded, err := url.QueryUnescape(value); err == nil {
			value = strings.TrimSpace(decoded)
		}
		if len(value) == 40 && isHexString(value) {
			return true
		}
		if ghAPIContentRefIsStableReleaseTag(value) {
			return true
		}
	}
	return false
}

func ghAPICacheFlagTTL(args []string) (time.Duration, bool) {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--cache" {
			if index+1 >= len(args) {
				return 0, false
			}
			ttl, err := time.ParseDuration(args[index+1])
			return ttl, err == nil && ttl > 0
		}
		if strings.HasPrefix(arg, "--cache=") {
			ttl, err := time.ParseDuration(strings.TrimPrefix(arg, "--cache="))
			return ttl, err == nil && ttl > 0
		}
	}
	return 0, false
}

var ghAPIDatedSearchRangeRE = regexp.MustCompile(`(?:^|\s)(?:created|closed|merged):(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})(?:\s|$)`)

func ghAPISearchQueryIsStable(args []string) bool {
	query := ghAPIFieldValue(args, "q")
	if query == "" {
		return false
	}
	matches := ghAPIDatedSearchRangeRE.FindStringSubmatch(strings.ReplaceAll(query, "+", " "))
	if len(matches) != 3 {
		return false
	}
	end, err := time.Parse("2006-01-02", matches[2])
	if err != nil {
		return false
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	return end.Before(today)
}

func ghAPIFieldValue(args []string, name string) string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-f", "-F", "--field", "--raw-field":
			if index+1 >= len(args) {
				return ""
			}
			if key, value, ok := strings.Cut(args[index+1], "="); ok && key == name {
				return value
			}
			index++
		default:
			for _, prefix := range []string{"-f=", "-F=", "--field=", "--raw-field="} {
				if strings.HasPrefix(arg, prefix) {
					field := strings.TrimPrefix(arg, prefix)
					if key, value, ok := strings.Cut(field, "="); ok && key == name {
						return value
					}
				}
			}
		}
	}
	return ""
}

func ghAPIContentRefIsStableReleaseTag(value string) bool {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "refs/heads/") {
		return false
	}
	value = strings.TrimPrefix(value, "refs/tags/")
	if strings.HasPrefix(value, "refs/") {
		return false
	}
	if strings.HasPrefix(value, "v") {
		value = strings.TrimPrefix(value, "v")
	}
	core := value
	if before, _, found := strings.Cut(core, "+"); found {
		core = before
	}
	if before, _, found := strings.Cut(core, "-"); found {
		core = before
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if !isDecimalString(part) {
			return false
		}
	}
	return true
}

func isGHPRDiff(args []string) bool {
	return len(args) >= 2 && args[0] == "pr" && args[1] == "diff"
}

func parseGHPRDiffIdentityArgs(args []string) (string, int, bool) {
	if !isGHPRDiff(args) {
		return "", 0, false
	}
	var repo string
	var number int
	for index := 2; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-R", "--repo":
			if index+1 >= len(args) {
				return "", 0, false
			}
			repo = strings.TrimSpace(args[index+1])
			index++
		default:
			if strings.HasPrefix(arg, "--repo=") {
				repo = strings.TrimSpace(strings.TrimPrefix(arg, "--repo="))
				continue
			}
			if strings.HasPrefix(arg, "-") || number != 0 {
				continue
			}
			if ref, ok := parseThreadReference(arg); ok && ref.FullName() != "" && repo == "" {
				repo = ref.FullName()
			}
			parsed, err := parseThreadNumber(arg)
			if err != nil {
				return "", 0, false
			}
			number = parsed
		}
	}
	if repo == "" {
		if envRepo := strings.TrimSpace(os.Getenv("GH_REPO")); envRepo != "" {
			repo = envRepo
		}
	}
	return repo, number, repo != "" && number > 0
}

func ghPRHeadSHAFromRawJSON(raw string) string {
	var payload struct {
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Head.SHA)
}

func normalizeGHAPIRoute(args []string) string {
	path := ghAPIPathArg(args)
	path = strings.TrimPrefix(path, "https://api.github.com/")
	path = strings.TrimPrefix(path, "http://api.github.com/")
	path = strings.TrimPrefix(path, "/")
	if before, _, found := strings.Cut(path, "?"); found {
		path = before
	}
	if path == "" {
		return "api"
	}
	parts := strings.Split(path, "/")
	for index, part := range parts {
		if part == "" {
			continue
		}
		if index >= 4 && len(parts) > 3 && parts[3] == "contents" {
			parts = append(parts[:4], ":path")
			break
		}
		if index >= 5 && len(parts) > 4 && parts[3] == "git" && parts[4] == "ref" {
			parts = append(parts[:5], ":ref")
			break
		}
		switch {
		case isDecimalString(part):
			parts[index] = ":id"
		case index >= 2 && parts[index-2] == "repos":
			// Preserve owner/repo placeholders without leaking every repo into the route cardinality.
			parts[index-1] = ":owner"
			parts[index] = ":repo"
		}
	}
	return "api " + strings.Join(parts, "/")
}

func ghAPIPathArg(args []string) string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-X", "--method":
			index++
			continue
		case "--paginate":
			continue
		case "-H", "--header", "--hostname", "--jq", "-q", "--preview", "--template", "-t", "--input", "--cache":
			if index+1 < len(args) && !strings.Contains(arg, "=") {
				index++
			}
			continue
		case "-f", "-F", "--field", "--raw-field":
			index++
			continue
		default:
			if strings.HasPrefix(arg, "--cache=") {
				continue
			}
			if strings.HasPrefix(arg, "-") {
				continue
			}
			return strings.TrimSpace(arg)
		}
	}
	return ""
}

func isDecimalString(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isHexString(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

func mutatingGHCommand(args []string) bool {
	if len(args) < 2 {
		return false
	}
	switch args[0] {
	case "cache":
		return args[1] == "delete"
	case "gist":
		switch args[1] {
		case "create", "delete", "edit":
			return true
		}
	case "issue":
		switch args[1] {
		case "close", "comment", "create", "delete", "edit", "lock", "pin", "reopen", "transfer", "unlock", "unpin":
			return true
		}
	case "label":
		switch args[1] {
		case "clone", "create", "delete", "edit":
			return true
		}
	case "pr":
		switch args[1] {
		case "checkout":
			return false
		case "close", "comment", "create", "edit", "lock", "merge", "ready", "reopen", "review", "unlock":
			return true
		}
	case "project":
		switch args[1] {
		case "close", "copy", "create", "delete", "edit", "field-create", "field-delete", "item-add", "item-archive", "item-create", "item-delete", "item-edit", "link", "mark-template", "unlink":
			return true
		}
	case "release":
		switch args[1] {
		case "create", "delete", "delete-asset", "edit", "upload":
			return true
		}
	case "repo":
		switch args[1] {
		case "archive", "create", "delete", "edit", "fork", "rename", "sync":
			return true
		}
	case "ruleset":
		return args[1] == "delete"
	case "run":
		switch args[1] {
		case "cancel", "delete", "rerun":
			return true
		}
	case "secret":
		switch args[1] {
		case "delete", "remove", "set":
			return true
		}
	case "variable":
		switch args[1] {
		case "delete", "remove", "set":
			return true
		}
	case "workflow":
		switch args[1] {
		case "disable", "enable", "run":
			return true
		}
	case "api":
		return !ghAPIReadOnly(args[1:])
	}
	return false
}

func hasAnyGHFlag(args []string, flags ...string) bool {
	for _, arg := range args {
		for _, flag := range flags {
			if arg == flag || strings.HasPrefix(arg, flag+"=") {
				return true
			}
		}
	}
	return false
}

func ghBoolFlagEnabled(args []string, flag string) bool {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == flag {
			return true
		}
		if value, ok := strings.CutPrefix(arg, flag+"="); ok {
			switch strings.ToLower(strings.TrimSpace(value)) {
			case "", "1", "t", "true", "yes", "on":
				return true
			default:
				return false
			}
		}
	}
	return false
}
