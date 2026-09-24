package github

// The history API batches complete conversations through GraphQL. The maps
// consumed by syncer are explicit projections; _graphql retains the unmodified
// provider object and _gitcrawl_source identifies its transport. No REST fallback.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type HistoryItem struct {
	Thread, Pull                      map[string]any
	Comments, Reviews, ReviewComments []map[string]any
}
type HistoryBatch struct {
	Repository map[string]any
	Items      []HistoryItem
}

const historyActor = `author { login __typename url }`
const historyComment = `id __typename fullDatabaseId body ` + historyActor + ` authorAssociation createdAt updatedAt publishedAt url isMinimized minimizedReason`
const historyInline = historyComment + ` path diffHunk line startLine originalLine originalStartLine position originalPosition state subjectType outdated commit { oid } originalCommit { oid } replyTo { id fullDatabaseId } pullRequestReview { id fullDatabaseId }`

var historyReview = historyComment + ` state submittedAt commit { oid } ` + historyConnection("comments", historyInline, "")
var historyReviewThread = `id __typename ` + historyConnection("comments", historyInline, "")
var historyCommon = `id __typename fullDatabaseId number title body ` + historyActor + ` authorAssociation createdAt updatedAt closedAt url state locked activeLockReason repository { nameWithOwner } milestone { number title state dueOn createdAt updatedAt url } ` + historyConnection("labels", `id name color description`, "") + " " + historyConnection("assignees", `id login __typename url`, "") + " " + historyConnection("comments", historyComment, "")
var historyIssue = historyCommon + ` stateReason`
var historyPull = historyCommon + ` isDraft merged mergedAt mergedBy { login __typename url } mergeCommit { oid } mergeable mergeStateStatus maintainerCanModify additions deletions changedFiles headRefName headRefOid baseRefName baseRefOid headRepository { nameWithOwner } baseRepository { nameWithOwner } commits { totalCount } ` + historyConnection("reviews", historyReview, "") + " " + historyConnection("reviewThreads", historyReviewThread, "")

func historyConnection(name, fields, after string) string {
	return name + `(first:20` + after + `) { totalCount pageInfo { hasNextPage endCursor } nodes { ` + fields + ` } }`
}

type historySession struct {
	client     *Client
	reporter   Reporter
	calls      int
	remaining  int
	retrySleep func(context.Context, time.Duration) error
}

func (h *historySession) request(ctx context.Context, query string, variables map[string]any, estimate int) (map[string]any, error) {
	for attempt := 0; ; attempt++ {
		data, err := h.requestOnce(ctx, query, variables, estimate)
		if err == nil {
			return data, nil
		}
		if attempt >= 2 || ctx.Err() != nil || !transientHistoryError(err) {
			return nil, err
		}
		// An unanswered attempt may have consumed points. Do not let a retry
		// refund that spending, even if the next provider receipt is higher.
		h.remaining -= estimate
		wait := time.Second << attempt
		var response *RequestError
		if errors.As(err, &response) {
			if providerWait, ok := retryAfterWait(response.Headers.Get("Retry-After")); ok {
				wait = max(wait, providerWait)
			}
		}
		h.reporter.Printf("[github] transient retry wait=%s", wait)
		sleep := h.retrySleep
		if sleep == nil {
			sleep = sleepHistoryRetry
		}
		if err := sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
}

func transientHistoryError(err error) bool {
	var response *RequestError
	if errors.As(err, &response) {
		switch response.Status {
		case 500, 502, 503, 504:
			return true
		}
		return false
	}
	var transport net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		(errors.As(err, &transport) && (transport.Timeout() || transport.Temporary()))
}

func sleepHistoryRetry(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (h *historySession) requestOnce(ctx context.Context, query string, variables map[string]any, estimate int) (map[string]any, error) {
	if h.calls >= 1000 {
		return nil, fmt.Errorf("GraphQL history pagination budget exceeded")
	}
	if h.remaining < 500+estimate {
		return nil, fmt.Errorf("GraphQL history quota reserve reached")
	}
	h.calls++
	// An unanswered request is charged conservatively by external supervisors.
	h.reporter.Printf("[github] graphql budget %d %d", h.calls, estimate)
	var data map[string]any
	started := time.Now()
	err := h.client.doGraphQL(ctx, query, variables, h.reporter, &data)
	h.reporter.Printf("[github] graphql timing %d %d", h.calls, time.Since(started).Milliseconds())
	if err != nil {
		return nil, err
	}
	rate := historyMap(data["rateLimit"])
	cost, ok := historyInt(rate["cost"])
	if !ok || cost < 0 {
		return nil, fmt.Errorf("GraphQL history missing cost")
	}
	remaining, ok := historyInt(rate["remaining"])
	if !ok || remaining < 0 {
		return nil, fmt.Errorf("GraphQL history missing remaining quota")
	}
	reset, err := time.Parse(time.RFC3339, historyString(rate["resetAt"]))
	if err != nil {
		return nil, fmt.Errorf("GraphQL history invalid reset")
	}
	h.remaining = min(h.remaining-cost, remaining)
	h.reporter.Printf("[github] graphql cost %d %d remaining %d reset %d", h.calls, cost, remaining, reset.Unix())
	return data, nil
}

// FetchGraphQLHistory supports exact selections and the conversation/metadata
// profile only. Full code/check hydration intentionally remains a separate mode.
func (c *Client) FetchGraphQLHistory(ctx context.Context, owner, repo string, numbers []int, reporter Reporter) (HistoryBatch, error) {
	var result HistoryBatch
	if len(numbers) == 0 || len(numbers) > 100 {
		return result, fmt.Errorf("GraphQL history requires 1..100 numbers")
	}
	h := historySession{client: c, reporter: reporter, remaining: 20000}
	if _, err := h.request(ctx, `query { rateLimit { cost remaining limit used resetAt } }`, nil, 1); err != nil {
		return result, err
	}
	for offset := 0; offset < len(numbers); offset += 25 {
		selected := numbers[offset:min(offset+25, len(numbers))]
		var fields strings.Builder
		for i, n := range selected {
			if n <= 0 {
				return result, fmt.Errorf("invalid history number")
			}
			fmt.Fprintf(&fields, "n%d: issueOrPullRequest(number:%d) { ... on Issue { %s } ... on PullRequest { %s } }\n", i, n, historyIssue, historyPull)
		}
		query := `query($owner:String!,$repo:String!){ repository(owner:$owner,name:$repo){ id databaseId nameWithOwner url description isPrivate createdAt updatedAt defaultBranchRef{name} ` + fields.String() + ` } rateLimit{cost remaining limit used resetAt}}`
		data, err := h.request(ctx, query, map[string]any{"owner": owner, "repo": repo}, 16)
		if err != nil {
			return result, err
		}
		r := historyMap(data["repository"])
		if !strings.EqualFold(historyString(r["nameWithOwner"]), owner+"/"+repo) {
			return result, fmt.Errorf("GraphQL history repository identity mismatch")
		}
		if result.Repository == nil {
			raw := map[string]any{}
			for k, v := range r {
				if !strings.HasPrefix(k, "n") || k == "nameWithOwner" {
					raw[k] = v
				}
			}
			result.Repository = map[string]any{"id": r["databaseId"], "node_id": r["id"], "full_name": r["nameWithOwner"], "html_url": r["url"], "private": r["isPrivate"], "description": r["description"], "_gitcrawl_source": "graphql", "_graphql": raw}
		}
		for i, n := range selected {
			node := historyMap(r[fmt.Sprintf("n%d", i)])
			got, _ := historyInt(node["number"])
			if got != n || !strings.EqualFold(historyString(historyMap(node["repository"])["nameWithOwner"]), owner+"/"+repo) || historyString(node["id"]) == "" {
				return result, fmt.Errorf("GraphQL history item #%d unavailable or moved", n)
			}
			if err := h.hydrate(ctx, node); err != nil {
				return result, fmt.Errorf("GraphQL history #%d: %w", n, err)
			}
			item, err := historyItem(node)
			if err != nil {
				return result, err
			}
			result.Items = append(result.Items, item)
		}
	}
	return result, nil
}

func historyFields(typ, key string) (string, error) {
	switch {
	case (typ == "Issue" || typ == "PullRequest") && key == "comments":
		return historyComment, nil
	case (typ == "Issue" || typ == "PullRequest") && key == "labels":
		return `id name color description`, nil
	case (typ == "Issue" || typ == "PullRequest") && key == "assignees":
		return `id login __typename url`, nil
	case typ == "PullRequest" && key == "reviews":
		return historyReview, nil
	case typ == "PullRequest" && key == "reviewThreads":
		return historyReviewThread, nil
	case (typ == "PullRequestReview" || typ == "PullRequestReviewThread") && key == "comments":
		return historyInline, nil
	}
	return "", fmt.Errorf("unsupported history connection %s.%s", typ, key)
}

func (h *historySession) hydrate(ctx context.Context, node map[string]any) error {
	typ := historyString(node["__typename"])
	var required []string
	switch typ {
	case "Issue":
		required = []string{"labels", "assignees", "comments"}
	case "PullRequest":
		required = []string{"labels", "assignees", "comments", "reviews", "reviewThreads"}
	case "PullRequestReview", "PullRequestReviewThread":
		required = []string{"comments"}
	}
	for _, key := range required {
		if historyMap(node[key]) == nil {
			return fmt.Errorf("missing history %s", key)
		}
	}
	if typ == "Issue" || typ == "PullRequest" || typ == "PullRequestReview" || typ == "IssueComment" || typ == "PullRequestReviewComment" {
		if node["fullDatabaseId"] == nil || historyString(node["id"]) == "" {
			return fmt.Errorf("missing provider identity")
		}
	}
	for _, key := range []string{"labels", "assignees", "comments", "reviews", "reviewThreads"} {
		connection, exists := node[key]
		if !exists {
			continue
		}
		conn := historyMap(connection)
		if conn == nil {
			return fmt.Errorf("missing %s connection", key)
		}
		fields, err := historyFields(typ, key)
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		for {
			page := historyMap(conn["pageInfo"])
			next, ok := page["hasNextPage"].(bool)
			if !ok {
				return fmt.Errorf("missing %s pageInfo", key)
			}
			if !next {
				break
			}
			cursor := historyString(page["endCursor"])
			if cursor == "" || seen[cursor] {
				return fmt.Errorf("nonadvancing %s cursor", key)
			}
			seen[cursor] = true
			q := `query($id:ID!,$after:String!){node(id:$id){id ... on ` + typ + `{` + historyConnection(key, fields, `,after:$after`) + `}} rateLimit{cost remaining limit used resetAt}}`
			data, err := h.request(ctx, q, map[string]any{"id": node["id"], "after": cursor}, 2)
			if err != nil {
				return err
			}
			parent := historyMap(data["node"])
			if parent["id"] != node["id"] {
				return fmt.Errorf("history pagination identity mismatch")
			}
			nxt := historyMap(parent[key])
			a, ok := conn["nodes"].([]any)
			if !ok {
				return fmt.Errorf("missing history nodes")
			}
			b, ok := nxt["nodes"].([]any)
			if !ok || len(b) == 0 {
				return fmt.Errorf("empty history continuation")
			}
			conn["nodes"] = append(a, b...)
			conn["pageInfo"] = nxt["pageInfo"]
		}
		children, ok := conn["nodes"].([]any)
		if !ok {
			return fmt.Errorf("missing history nodes")
		}
		if total, ok := historyInt(conn["totalCount"]); !ok || total != len(children) {
			return fmt.Errorf("incomplete history %s count", key)
		}
		ids := map[string]bool{}
		for _, child := range children {
			m := historyMap(child)
			id := historyString(m["id"])
			if id == "" || ids[id] {
				return fmt.Errorf("missing or duplicate history child identity")
			}
			ids[id] = true
			if err := h.hydrate(ctx, m); err != nil {
				return err
			}
		}
	}
	return nil
}

func historyItem(node map[string]any) (HistoryItem, error) {
	var item HistoryItem
	typ := historyString(node["__typename"])
	if typ != "Issue" && typ != "PullRequest" {
		return item, fmt.Errorf("invalid history item type")
	}
	row := historyProjection(node)
	// GraphQL does not expose a PR's REST issue-database ID. New thread IDs
	// therefore use the opaque node ID. Syncer preserves any existing legacy ID.
	row["id"] = node["id"]
	row["number"] = node["number"]
	row["title"] = node["title"]
	row["state"] = strings.ToLower(historyString(node["state"]))
	if row["state"] == "merged" {
		row["state"] = "closed"
	}
	row["closed_at"] = node["closedAt"]
	row["locked"] = node["locked"]
	row["active_lock_reason"] = node["activeLockReason"]
	row["labels"] = historyNodes(node, "labels")
	assignees := []map[string]any{}
	for _, a := range historyNodes(node, "assignees") {
		assignees = append(assignees, historyActorMap(a))
	}
	row["assignees"] = assignees
	row["comments"] = historyMap(node["comments"])["totalCount"]
	for _, v := range historyNodes(node, "comments") {
		item.Comments = append(item.Comments, historyProjection(v))
	}
	if typ == "PullRequest" {
		row["draft"] = node["isDraft"]
		row["pull_request"] = map[string]any{"merged_at": node["mergedAt"], "html_url": node["url"]}
		pull := historyProjection(node)
		pull["number"] = node["number"]
		pull["title"] = node["title"]
		pull["state"] = row["state"]
		pull["draft"] = node["isDraft"]
		pull["merged"] = node["merged"]
		pull["merged_at"] = node["mergedAt"]
		pull["closed_at"] = node["closedAt"]
		pull["merged_by"] = historyActorMap(historyMap(node["mergedBy"]))
		pull["merge_commit_sha"] = historyMap(node["mergeCommit"])["oid"]
		pull["mergeable_state"] = strings.ToLower(historyString(node["mergeStateStatus"]))
		pull["additions"] = node["additions"]
		pull["deletions"] = node["deletions"]
		pull["changed_files"] = node["changedFiles"]
		for _, side := range []string{"head", "base"} {
			pull[side] = map[string]any{"sha": node[side+"RefOid"], "ref": node[side+"RefName"], "repo": map[string]any{"full_name": historyMap(node[side+"Repository"])["nameWithOwner"]}}
		}
		item.Pull = pull
		inlineByID := map[string]map[string]any{}
		for _, v := range historyNodes(node, "reviews") {
			r := historyProjection(v)
			r["state"] = v["state"]
			r["submitted_at"] = v["submittedAt"]
			r["commit_id"] = historyMap(v["commit"])["oid"]
			item.Reviews = append(item.Reviews, r)
			for _, comment := range historyNodes(v, "comments") {
				if _, exists := inlineByID[historyString(comment["id"])]; exists {
					continue
				}
				p := historyInlineProjection(comment)
				p["pull_request_review_id"] = v["fullDatabaseId"]
				inlineByID[historyString(comment["id"])] = p
				item.ReviewComments = append(item.ReviewComments, p)
			}
		}
		// A comment's review association is nullable. Threads independently
		// supply standalone comments; review bodies and their metadata stay above.
		for _, thread := range historyNodes(node, "reviewThreads") {
			for _, comment := range historyNodes(thread, "comments") {
				id := historyString(comment["id"])
				if _, exists := inlineByID[id]; exists {
					continue
				}
				p := historyInlineProjection(comment)
				inlineByID[id] = p
				item.ReviewComments = append(item.ReviewComments, p)
			}
		}
	}
	item.Thread = row
	return item, nil
}

func historyInlineProjection(comment map[string]any) map[string]any {
	p := historyProjection(comment)
	for dest, src := range map[string]string{"path": "path", "diff_hunk": "diffHunk", "line": "line", "start_line": "startLine", "original_line": "originalLine", "original_start_line": "originalStartLine", "position": "position", "original_position": "originalPosition", "subject_type": "subjectType"} {
		p[dest] = comment[src]
	}
	p["in_reply_to_id"] = historyMap(comment["replyTo"])["fullDatabaseId"]
	p["pull_request_review_id"] = historyMap(comment["pullRequestReview"])["fullDatabaseId"]
	p["commit_id"] = historyMap(comment["commit"])["oid"]
	p["original_commit_id"] = historyMap(comment["originalCommit"])["oid"]
	return p
}

func historyProjection(node map[string]any) map[string]any {
	return map[string]any{"id": node["fullDatabaseId"], "node_id": node["id"], "body": node["body"], "user": historyActorMap(historyMap(node["author"])), "author_association": node["authorAssociation"], "created_at": node["createdAt"], "updated_at": node["updatedAt"], "html_url": node["url"], "_gitcrawl_source": "graphql", "_graphql": node}
}
func historyActorMap(a map[string]any) map[string]any {
	if a == nil {
		return nil
	}
	login := historyString(a["login"])
	typ := historyString(a["__typename"])
	if typ == "Bot" && !strings.HasSuffix(login, "[bot]") {
		login += "[bot]"
	}
	return map[string]any{"login": login, "type": typ, "node_id": a["id"], "html_url": a["url"]}
}
func historyNodes(node map[string]any, key string) []map[string]any {
	var out []map[string]any
	values, _ := historyMap(node[key])["nodes"].([]any)
	for _, v := range values {
		out = append(out, historyMap(v))
	}
	return out
}
func historyMap(v any) map[string]any { m, _ := v.(map[string]any); return m }
func historyString(v any) string      { s, _ := v.(string); return s }
func historyInt(v any) (int, bool) {
	switch n := v.(type) {
	case json.Number:
		i, e := strconv.Atoi(string(n))
		return i, e == nil
	case int:
		return n, true
	case float64:
		return int(n), n == float64(int(n))
	}
	return 0, false
}
