package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

// OctopoolConversations is a bounded acquisition adapter for native sync. REST
// owns thread identity and metadata; GraphQL only supplies complete child sets.
// It deliberately does not implement the broader GraphQL history profile.
type OctopoolConversations struct {
	*Client
	relay         *octopoolTransport
	repository    string
	metadata      map[string]any
	issues        map[int]map[string]any
	conversations map[int]HistoryItem
}

func NewOctopoolConversations(endpoint, pool, token, repository string) (*OctopoolConversations, error) {
	client, err := NewOctopool(endpoint, pool, token, repository)
	if err != nil {
		return nil, err
	}
	return &OctopoolConversations{Client: client, relay: client.httpClient.Transport.(*octopoolTransport), repository: repository, issues: map[int]map[string]any{}, conversations: map[int]HistoryItem{}}, nil
}

// Inventory performs one fresh, paginated REST traversal. In particular, a PR's
// backing issue ID must never be replaced by its GraphQL PullRequest database ID.
func (c *OctopoolConversations) Inventory(ctx context.Context, reporter Reporter) error {
	owner, repo, ok := strings.Cut(c.repository, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return errors.New("expected owner/repository")
	}
	metadata, err := c.Client.GetRepo(ctx, owner, repo, reporter)
	if err != nil {
		return err
	}
	rows, err := c.Client.ListRepositoryIssues(ctx, owner, repo, ListIssuesOptions{State: "all"}, reporter)
	if err != nil {
		return err
	}
	issues := make(map[int]map[string]any, len(rows))
	for _, row := range rows {
		number, ok := historyInt(row["number"])
		if !ok || number <= 0 {
			return errors.New("REST inventory has invalid issue number")
		}
		if _, exists := issues[number]; exists {
			return fmt.Errorf("duplicate REST inventory number %d", number)
		}
		issues[number] = row
	}
	c.metadata, c.issues = metadata, issues
	return nil
}

func (c *OctopoolConversations) GetRepo(ctx context.Context, owner, repo string, reporter Reporter) (map[string]any, error) {
	if owner+"/"+repo != c.repository || c.metadata == nil {
		return nil, errors.New("conversation inventory unavailable for repository")
	}
	return c.metadata, nil
}
func (c *OctopoolConversations) GetIssue(ctx context.Context, owner, repo string, number int, reporter Reporter) (map[string]any, error) {
	if owner+"/"+repo != c.repository || c.issues[number] == nil {
		return nil, fmt.Errorf("number %d absent from REST inventory", number)
	}
	if _, ok := c.conversations[number]; !ok {
		return nil, fmt.Errorf("number %d not fully hydrated", number)
	}
	return c.issues[number], nil
}
func (c *OctopoolConversations) children(owner, repo string, number int) (HistoryItem, error) {
	item, ok := c.conversations[number]
	if owner+"/"+repo != c.repository || !ok {
		return item, fmt.Errorf("number %d not fully hydrated", number)
	}
	return item, nil
}
func (c *OctopoolConversations) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	item, err := c.children(owner, repo, number)
	return item.Comments, err
}
func (c *OctopoolConversations) ListPullReviews(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	item, err := c.children(owner, repo, number)
	return item.Reviews, err
}
func (c *OctopoolConversations) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter Reporter) ([]map[string]any, error) {
	item, err := c.children(owner, repo, number)
	return item.ReviewComments, err
}

const conversationComments = `comments(first:10) { totalCount pageInfo { hasNextPage } nodes { ` + historyComment + ` } }`
const conversationInline = `comments(first:10) { totalCount pageInfo { hasNextPage } nodes { ` + historyInline + ` } }`
const conversationFields = `__typename id number repository { nameWithOwner } ` + conversationComments + ` ... on PullRequest { reviews(first:10) { totalCount pageInfo { hasNextPage } nodes { ` + historyComment + ` state submittedAt commit { oid } ` + conversationInline + ` } } reviewThreads(first:10) { totalCount pageInfo { hasNextPage } nodes { id ` + conversationInline + ` } } }`

// Prepare replaces the bounded child cache only after the entire batch succeeds.
// Large conversations use the existing fully paginated same-pool REST reader.
func (c *OctopoolConversations) Prepare(ctx context.Context, numbers []int, reporter Reporter) error {
	if len(numbers) == 0 || len(numbers) > 25 {
		return errors.New("conversation batch must contain 1..25 numbers")
	}
	var query strings.Builder
	query.WriteString(`query($owner:String!,$repo:String!) { repository(owner:$owner,name:$repo) {`)
	seen := map[int]bool{}
	for i, n := range numbers {
		if n <= 0 || c.issues[n] == nil || seen[n] {
			return fmt.Errorf("invalid, duplicate or uninventoried number %d", n)
		}
		seen[n] = true
		fmt.Fprintf(&query, " n%d: issueOrPullRequest(number:%d) { ...ConversationIssue ...ConversationPull }", i, n)
	}
	query.WriteString(" } } fragment ConversationIssue on Issue { __typename id number repository { nameWithOwner } " + conversationComments + " } fragment ConversationPull on PullRequest { " + conversationFields + " }")
	owner, repo, _ := strings.Cut(c.repository, "/")
	data, err := c.requestConversations(ctx, query.String(), map[string]any{"owner": owner, "repo": repo}, reporter)
	if err != nil {
		return err
	}
	repository := historyMap(data["repository"])
	prepared := make(map[int]HistoryItem, len(numbers))
	for i, n := range numbers {
		node := historyMap(repository[fmt.Sprintf("n%d", i)])
		typ := historyString(node["__typename"])
		number, ok := historyInt(node["number"])
		_, isPull := c.issues[n]["pull_request"]
		if !ok || number != n || historyString(node["id"]) != historyString(c.issues[n]["node_id"]) || historyString(historyMap(node["repository"])["nameWithOwner"]) != c.repository || (typ != "Issue" && typ != "PullRequest") || (typ == "PullRequest") != isPull {
			return fmt.Errorf("GraphQL identity mismatch or missing node for #%d", n)
		}
		complete := completeConversationConnection(node, "comments")
		if isPull {
			for _, family := range []string{"reviews", "reviewThreads"} {
				complete = completeConversationConnection(node, family) && complete
				for _, parent := range historyNodes(node, family) {
					complete = completeConversationConnection(parent, "comments") && complete
				}
			}
		}
		var item HistoryItem
		if !complete {
			item.Comments, err = c.Client.ListIssueComments(ctx, owner, repo, n, reporter)
			if err == nil && isPull {
				item.Reviews, err = c.Client.ListPullReviews(ctx, owner, repo, n, reporter)
			}
			if err == nil && isPull {
				item.ReviewComments, err = c.Client.ListPullReviewComments(ctx, owner, repo, n, reporter)
			}
		} else {
			item, err = projectConversation(node)
		}
		if err != nil {
			return fmt.Errorf("hydrate #%d: %w", n, err)
		}
		prepared[n] = item
	}
	c.conversations = prepared
	return nil
}

func completeConversationConnection(node map[string]any, key string) bool {
	connection := historyMap(node[key])
	info := historyMap(connection["pageInfo"])
	more, ok := info["hasNextPage"].(bool)
	count, valid := historyInt(connection["totalCount"])
	nodes, hasNodes := connection["nodes"].([]any)
	if !ok || more || !valid || !hasNodes || count != len(nodes) {
		return false
	}
	seen := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		id := historyString(historyMap(node)["id"])
		if id == "" || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}
func projectConversation(node map[string]any) (HistoryItem, error) {
	node = withoutPendingHistoryDrafts(node)
	item := HistoryItem{}
	for _, comment := range historyNodes(node, "comments") {
		if err := validateConversationComment(comment); err != nil {
			return item, err
		}
		item.Comments = append(item.Comments, historyProjection(comment))
	}
	inline := map[string]map[string]any{}
	addInline := func(comment map[string]any) error {
		id := historyString(comment["id"])
		if err := validateConversationComment(comment); err != nil {
			return err
		}
		if previous, ok := inline[id]; ok {
			if !reflect.DeepEqual(previous, comment) {
				return errors.New("conflicting duplicate inline comment")
			}
			return nil
		}
		inline[id] = comment
		item.ReviewComments = append(item.ReviewComments, historyInlineProjection(comment))
		return nil
	}
	for _, review := range historyNodes(node, "reviews") {
		if err := validateConversationComment(review); err != nil {
			return item, err
		}
		p := historyProjection(review)
		p["state"] = review["state"]
		p["submitted_at"] = review["submittedAt"]
		p["commit_id"] = historyMap(review["commit"])["oid"]
		item.Reviews = append(item.Reviews, p)
		for _, comment := range historyNodes(review, "comments") {
			if err := addInline(comment); err != nil {
				return item, err
			}
		}
	}
	for _, thread := range historyNodes(node, "reviewThreads") {
		for _, comment := range historyNodes(thread, "comments") {
			if err := addInline(comment); err != nil {
				return item, err
			}
		}
	}
	return item, nil
}

func validateConversationComment(node map[string]any) error {
	if historyString(node["id"]) == "" {
		return errors.New("comment missing node identity")
	}
	var id string
	switch value := node["fullDatabaseId"].(type) {
	case json.Number:
		id = string(value)
	case string:
		id = value
	default:
		return errors.New("comment missing exact database identity")
	}
	value, err := strconv.ParseInt(id, 10, 64)
	if err != nil || value <= 0 || strconv.FormatInt(value, 10) != id {
		return errors.New("comment has invalid database identity")
	}
	if _, ok := node["body"].(string); !ok {
		return errors.New("comment missing full body")
	}
	return nil
}

func (c *OctopoolConversations) requestConversations(ctx context.Context, query string, variables map[string]any, reporter Reporter) (map[string]any, error) {
	if len(query) > 16<<10 {
		return nil, errors.New("GraphQL query exceeds relay limit; reduce batch size")
	}
	payload, err := json.Marshal(map[string]any{"pool": c.relay.pool, "method": "POST", "path": "/graphql", "graphql": map[string]any{"query": query, "variables": variables}, "headers": map[string]string{"cache-control": "max-age=0"}})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.relay.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.relay.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.relay.client.Do(request)
	if err != nil {
		return nil, errors.New("Octopool GraphQL request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Octopool GraphQL relay status %d; no direct fallback", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (3<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 3<<20 {
		return nil, errors.New("Octopool GraphQL response too large; reduce batch size")
	}
	var envelope struct {
		Status       int               `json:"status"`
		Headers      map[string]string `json:"headers"`
		Body         json.RawMessage   `json:"body"`
		BodyEncoding string            `json:"body_encoding"`
		Relay        struct {
			RouteKind string `json:"route_kind"`
			Cache     string `json:"cache"`
			StaleOK   bool   `json:"stale_ok"`
		} `json:"relay"`
		Identity struct {
			Kind string `json:"kind"`
		} `json:"identity"`
	}
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return nil, errors.New("invalid Octopool GraphQL envelope")
	}
	if envelope.Status != 200 || envelope.Relay.RouteKind != "graphql_read" || envelope.Identity.Kind != "github_app" || envelope.Relay.StaleOK || envelope.Relay.Cache == "stale" {
		return nil, errors.New("Octopool GraphQL receipt is not a fresh GitHub App read")
	}
	if reporter != nil {
		headers := http.Header{}
		for key, value := range envelope.Headers {
			headers.Set(key, value)
		}
		reporter.Printf("[github] GraphQL quota remaining=%s used=%s limit=%s reset=%s", headers.Get("X-RateLimit-Remaining"), headers.Get("X-RateLimit-Used"), headers.Get("X-RateLimit-Limit"), headers.Get("X-RateLimit-Reset"))
	}
	var result struct {
		Data   map[string]any    `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	body, err := decodeOctopoolBody(envelope.Body, envelope.BodyEncoding)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err = decoder.Decode(&result); err != nil {
		return nil, errors.New("invalid GraphQL response")
	}
	if len(result.Errors) > 0 || result.Data == nil {
		return nil, errors.New("GraphQL returned errors or missing data; batch remains incomplete")
	}
	return result.Data, nil
}
