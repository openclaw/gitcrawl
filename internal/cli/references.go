package cli

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

func parseOwnerRepo(value string) (string, string, error) {
	if ref, ok := parseThreadReference(value); ok && ref.Owner != "" && ref.Repo != "" {
		return ref.Owner, ref.Repo, nil
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("expected owner/repo, got %q", value)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

type threadReference struct {
	Owner  string
	Repo   string
	Number int
}

func (ref threadReference) FullName() string {
	if ref.Owner == "" || ref.Repo == "" {
		return ""
	}
	return ref.Owner + "/" + ref.Repo
}

func parseThreadReference(value string) (threadReference, bool) {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "<>()[]{}\"'`")
	value = strings.TrimRight(value, ".,;")
	if value == "" {
		return threadReference{}, false
	}
	if number, ok := parsePositiveIntLiteral(value); ok {
		return threadReference{Number: number}, true
	}
	if strings.HasPrefix(value, "#") {
		if number, ok := parsePositiveIntLiteral(strings.TrimPrefix(value, "#")); ok {
			return threadReference{Number: number}, true
		}
	}
	if match := githubThreadURLPattern.FindStringSubmatch(value); match != nil {
		if number, ok := parsePositiveIntLiteral(match[3]); ok {
			return threadReference{Owner: match[1], Repo: match[2], Number: number}, true
		}
	}
	if match := ownerRepoThreadPattern.FindStringSubmatch(value); match != nil {
		if number, ok := parsePositiveIntLiteral(match[3]); ok {
			return threadReference{Owner: match[1], Repo: match[2], Number: number}, true
		}
	}
	if match := pathThreadPattern.FindStringSubmatch(value); match != nil {
		if number, ok := parsePositiveIntLiteral(match[1]); ok {
			return threadReference{Number: number}, true
		}
	}
	return threadReference{}, false
}

func parsePositiveIntLiteral(value string) (int, bool) {
	if !isDecimalString(value) {
		return 0, false
	}
	number, err := strconv.Atoi(value)
	return number, err == nil && number > 0
}

func parseOptionalPositiveInt(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("expected positive integer, got %q", value)
	}
	return parsed, nil
}

func parseOptionalNonNegativeInt(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("expected non-negative integer, got %q", value)
	}
	return parsed, nil
}

func parseRequiredPositiveInt(name, value string) (int, error) {
	parsed, err := parseOptionalPositiveInt(value)
	if err != nil {
		return 0, err
	}
	if parsed == 0 {
		return 0, fmt.Errorf("missing --%s", name)
	}
	return parsed, nil
}

func parseOptionalThreadNumber(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	ref, ok := parseThreadReference(value)
	if !ok || ref.Number <= 0 {
		return 0, fmt.Errorf("expected positive issue or pull request number, got %q", value)
	}
	return ref.Number, nil
}

func parseRequiredThreadNumber(name, value string) (int, error) {
	parsed, err := parseOptionalThreadNumber(value)
	if err != nil {
		return 0, err
	}
	if parsed == 0 {
		return 0, fmt.Errorf("missing --%s", name)
	}
	return parsed, nil
}

func parseClusterMemberCommandIDs(command, clusterIDRaw, numberRaw string) (int, int, error) {
	clusterID, err := parseOptionalPositiveInt(clusterIDRaw)
	if err != nil {
		return 0, 0, err
	}
	if clusterID == 0 {
		return 0, 0, fmt.Errorf("%s requires --id", command)
	}
	number, err := parseOptionalThreadNumber(numberRaw)
	if err != nil {
		return 0, 0, err
	}
	if number == 0 {
		return 0, 0, fmt.Errorf("%s requires --number", command)
	}
	return clusterID, number, nil
}

func parseOptionalFloat(value string) (float64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("expected number, got %q", value)
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("expected finite number, got %q", value)
	}
	return parsed, nil
}

func stateIncludesClosed(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "all", "closed":
		return true
	default:
		return false
	}
}

func parseOptionalPositiveIntList(value string) ([]int, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := parseOptionalPositiveInt(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func parseOptionalThreadNumberList(value string) ([]int, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := parseOptionalThreadNumber(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

var githubThreadURLPattern = regexp.MustCompile(`(?i)^https?://github\.com/([\w.-]+)/([\w.-]+)/(?:issues|pull)/(\d+)(?:[/?#].*)?$`)
var ownerRepoThreadPattern = regexp.MustCompile(`(?i)^([\w.-]+)/([\w.-]+)#(\d+)$`)
var pathThreadPattern = regexp.MustCompile(`(?i)(?:^|/)(?:issues|pull)/(\d+)(?:[/?#].*)?$`)
