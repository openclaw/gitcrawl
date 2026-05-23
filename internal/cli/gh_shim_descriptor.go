package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func canonicalGHCommandArgs(args []string) []string {
	if len(args) == 0 {
		return nil
	}
	valueFlags := map[string]string{
		"-R": "--repo", "--repo": "--repo",
		"-L": "--limit", "--limit": "--limit",
		"-q": "--jq", "--jq": "--jq",
		"-t": "--template", "--template": "--template",
		"-X": "--method", "--method": "--method",
		"--cache": "--cache",
		"-H":      "--header", "--header": "--header",
		"-f": "--field", "-F": "--field", "--field": "--field", "--raw-field": "--raw-field",
		"--hostname": "--hostname", "--preview": "--preview",
		"--state": "--state", "--author": "--author", "--assignee": "--assignee", "--label": "--label",
		"--branch": "--branch", "--commit": "--commit", "--workflow": "--workflow", "--job": "--job",
		"--json": "--json", "--search": "--search",
	}
	valueFlagNames := make(map[string]struct{}, len(valueFlags))
	for _, name := range valueFlags {
		valueFlagNames[name] = struct{}{}
	}
	var positionals []string
	var flags []string
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if canonical, ok := valueFlags[arg]; ok {
			if canonical == "--cache" {
				if index+1 < len(args) {
					index++
				}
				continue
			}
			if index+1 < len(args) {
				flags = append(flags, canonical+"="+canonicalGHFlagValue(canonical, args[index+1]))
				index++
			} else {
				flags = append(flags, canonical)
			}
			continue
		}
		if strings.HasPrefix(arg, "--") {
			name, value, hasValue := strings.Cut(arg, "=")
			if name == "--cache" && hasValue {
				_ = value
				continue
			}
			if _, ok := valueFlagNames[name]; ok && hasValue {
				flags = append(flags, name+"="+canonicalGHFlagValue(name, value))
				continue
			}
			flags = append(flags, arg)
			continue
		}
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			continue
		}
		positionals = append(positionals, arg)
	}
	sort.Strings(flags)
	out := make([]string, 0, len(positionals)+len(flags))
	out = append(out, positionals...)
	out = append(out, flags...)
	return out
}

func canonicalGHFlagValue(name, value string) string {
	value = strings.TrimSpace(value)
	switch name {
	case "--json":
		if value == "" {
			return value
		}
		fields := strings.Split(value, ",")
		for index := range fields {
			fields[index] = strings.TrimSpace(fields[index])
		}
		sort.Strings(fields)
		return strings.Join(fields, ",")
	case "--method":
		return strings.ToUpper(value)
	default:
		return value
	}
}

func (a *App) clearGHCommandCacheForMutation(ctx context.Context, args []string) error {
	tags := a.ghMutationInvalidationTags(ctx, args)
	if len(tags) == 0 {
		return a.clearGHCommandCache()
	}
	return a.clearGHCommandCacheMatching(tags)
}

func (a *App) clearGHCommandCacheMatching(tags []string) error {
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return err
	}
	tagSet := stringSet(tags)
	if _, ok := tagSet["global"]; ok {
		return a.clearGHCommandCache()
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".lock") {
			_ = os.Remove(filepath.Join(dir, name))
			continue
		}
		if !entry.Type().IsRegular() || !isGHCommandCacheEntryFile(name) {
			continue
		}
		path := filepath.Join(dir, name)
		cached, ok := readGHCommandCacheEntry(path)
		if !ok || ghCacheTagsMatch(cached.Tags, tagSet) {
			_ = os.Remove(path)
		}
	}
	return nil
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	return set
}

func ghCacheTagsMatch(entryTags []string, mutationTags map[string]struct{}) bool {
	hasSpecificMutationTag := false
	for tag := range mutationTags {
		if tag != "global" && !strings.HasPrefix(tag, "repo:") {
			hasSpecificMutationTag = true
			break
		}
	}
	for _, tag := range entryTags {
		if _, ok := mutationTags[tag]; !ok {
			continue
		}
		if hasSpecificMutationTag && strings.HasPrefix(tag, "repo:") {
			continue
		}
		return true
	}
	return false
}

func (a *App) ghCommandCacheTags(ctx context.Context, args []string) []string {
	return uniqueStrings(a.ghCommandTags(ctx, args, false))
}

func (a *App) ghMutationInvalidationTags(ctx context.Context, args []string) []string {
	return uniqueStrings(a.ghCommandTags(ctx, args, true))
}

func (a *App) ghCommandTags(ctx context.Context, args []string, mutation bool) []string {
	var tags []string
	repo := ghCommandRepo(args)
	if repo == "" {
		repo = strings.TrimSpace(os.Getenv("GH_REPO"))
	}
	if repo != "" {
		tags = append(tags, "repo:"+repo)
	}
	if len(args) < 2 {
		if mutation {
			tags = append(tags, "global")
		}
		return tags
	}
	switch args[0] {
	case "issue", "pr":
		kind := args[0]
		if kind == "issue" {
			tags = append(tags, "issues")
		} else {
			tags = append(tags, "pulls")
		}
		if number := firstGHNumberArg(args[2:]); number != "" {
			tags = append(tags, kind+":"+number)
		}
	case "run":
		tags = append(tags, "actions")
		if args[1] == "view" || mutation {
			if id := firstGHNumberArg(args[2:]); id != "" {
				tags = append(tags, "run:"+id)
			}
		}
	case "workflow":
		tags = append(tags, "actions")
	case "release":
		tags = append(tags, "releases")
		if id := firstGHPositionalArg(args[2:]); id != "" {
			tags = append(tags, "release:"+id)
		}
	case "api":
		tags = append(tags, ghAPITags(args[1:])...)
	}
	if mutation && repo != "" {
		switch args[0] {
		case "issue":
			tags = append(tags, "issues")
		case "pr":
			tags = append(tags, "pulls")
		case "run", "workflow":
			tags = append(tags, "actions")
		case "release":
			tags = append(tags, "releases")
		}
	}
	if mutation && len(tags) == 0 {
		tags = append(tags, "global")
	}
	return tags
}

func ghCommandRepo(args []string) string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-R", "--repo":
			if index+1 < len(args) {
				return strings.TrimSpace(args[index+1])
			}
		default:
			if strings.HasPrefix(arg, "--repo=") {
				return strings.TrimSpace(strings.TrimPrefix(arg, "--repo="))
			}
		}
	}
	if len(args) >= 3 && args[0] == "repo" && args[1] == "view" {
		if repo := firstGHPositionalArg(args[2:]); strings.Contains(repo, "/") {
			return repo
		}
	}
	if len(args) > 0 && args[0] == "api" {
		return ghAPIRepo(args[1:])
	}
	return ""
}

func ghAPIRepo(args []string) string {
	path := ghAPIPathArg(args)
	path = strings.TrimPrefix(path, "https://api.github.com/")
	path = strings.TrimPrefix(path, "http://api.github.com/")
	path = strings.TrimPrefix(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) >= 3 && parts[0] == "repos" && parts[1] != "" && parts[2] != "" {
		return parts[1] + "/" + parts[2]
	}
	return ""
}

func ghAPITags(args []string) []string {
	path := strings.TrimPrefix(ghAPIPathArg(args), "https://api.github.com/")
	path = strings.TrimPrefix(path, "http://api.github.com/")
	path = strings.TrimPrefix(path, "/")
	if before, _, found := strings.Cut(path, "?"); found {
		path = before
	}
	parts := strings.Split(path, "/")
	if len(parts) < 4 || parts[0] != "repos" {
		return nil
	}
	var tags []string
	if repo := ghAPIRepo(args); repo != "" {
		tags = append(tags, "repo:"+repo)
	}
	switch parts[3] {
	case "actions":
		tags = append(tags, "actions")
		if len(parts) >= 6 && parts[4] == "runs" && isDecimalString(parts[5]) {
			tags = append(tags, "run:"+parts[5])
		}
	case "issues":
		tags = append(tags, "issues")
		if len(parts) >= 5 && isDecimalString(parts[4]) {
			tags = append(tags, "issue:"+parts[4])
		}
	case "pulls":
		tags = append(tags, "pulls")
		if len(parts) >= 5 && isDecimalString(parts[4]) {
			tags = append(tags, "pr:"+parts[4])
		}
	case "releases":
		tags = append(tags, "releases")
	}
	return tags
}

func firstGHNumberArg(args []string) string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if strings.HasPrefix(arg, "-") {
			if !strings.Contains(arg, "=") && index+1 < len(args) {
				index++
			}
			continue
		}
		if ref, ok := parseThreadReference(arg); ok && ref.Number > 0 {
			return strconv.Itoa(ref.Number)
		}
	}
	return ""
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func ghCompletedRunCacheTTL(entry ghCommandCacheEntry) time.Duration {
	if len(entry.Args) == 0 {
		return 0
	}
	if entry.Args[0] == "run" {
		if len(entry.Args) >= 2 && entry.Args[1] == "view" && ghJSONStatusCompleted(entry.Stdout) {
			return 12 * time.Hour
		}
		if len(entry.Args) >= 2 && entry.Args[1] == "list" && ghJSONCollectionCompleted(entry.Stdout) {
			return 30 * time.Minute
		}
	}
	if entry.Args[0] == "api" {
		route := normalizeGHAPIRoute(entry.Args[1:])
		if strings.Contains(route, "/actions/runs/:id/jobs") && ghJSONJobsCompleted(entry.Stdout) {
			return 12 * time.Hour
		}
		if strings.Contains(route, "/actions/jobs/:id") && ghJSONStatusCompleted(entry.Stdout) {
			return 12 * time.Hour
		}
		if strings.Contains(route, "/actions/runs/:id") && ghJSONStatusCompleted(entry.Stdout) {
			return 12 * time.Hour
		}
		if strings.Contains(route, "/actions/runs") && ghJSONCollectionCompleted(entry.Stdout) {
			return 30 * time.Minute
		}
	}
	return 0
}

func ghClosedThreadCacheTTL(entry ghCommandCacheEntry) time.Duration {
	if len(entry.Args) < 2 {
		return 0
	}
	command := ghCommandName(entry.Args)
	if command != "issue view" && command != "pr view" && entry.Args[0] != "api" {
		return 0
	}
	if entry.Args[0] == "api" {
		route := normalizeGHAPIRoute(entry.Args[1:])
		if !strings.Contains(route, "/issues/:id") && !strings.Contains(route, "/pulls/:id") {
			return 0
		}
		if strings.Contains(route, "/comments") || strings.Contains(route, "/reviews") {
			return 0
		}
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(entry.Stdout), &payload); err != nil {
		return 0
	}
	state := strings.TrimSpace(fmt.Sprint(payload["state"]))
	if state == "" {
		return 0
	}
	if !strings.EqualFold(state, "open") {
		return 24 * time.Hour
	}
	return 0
}

func ghJSONJobsCompleted(raw string) bool {
	var payload struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err == nil {
		return len(payload.Jobs) > 0 && allGHStatusMapsCompleted(payload.Jobs)
	}
	return ghJSONCollectionCompleted(raw)
}

func ghJSONStatusCompleted(raw string) bool {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return false
	}
	return ghStatusMapCompleted(payload)
}

func ghJSONCollectionCompleted(raw string) bool {
	var rows []map[string]any
	if err := json.Unmarshal([]byte(raw), &rows); err == nil {
		return len(rows) > 0 && allGHStatusMapsCompleted(rows)
	}
	var payload struct {
		WorkflowRuns []map[string]any `json:"workflow_runs"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err == nil {
		return len(payload.WorkflowRuns) > 0 && allGHStatusMapsCompleted(payload.WorkflowRuns)
	}
	return false
}

func allGHStatusMapsCompleted(rows []map[string]any) bool {
	for _, row := range rows {
		if !ghStatusMapCompleted(row) {
			return false
		}
	}
	return true
}

func ghStatusMapCompleted(row map[string]any) bool {
	status, _ := row["status"].(string)
	conclusion, _ := row["conclusion"].(string)
	return strings.EqualFold(status, "completed") || strings.TrimSpace(conclusion) != ""
}
