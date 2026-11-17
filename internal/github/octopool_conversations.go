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
	"slices"
	"strconv"
	"strings"
	"time"
)

// MaxConversationBatch keeps fragment expansion below the relay selection limit.
const MaxConversationBatch = 20

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
	return c.inventory(ctx, nil, 1, reporter)
}

// InventoryWithWorkers retains the existing caller contract. Inventory reads
// follow one authentic cursor chain; concurrency applies only to child batches.
func (c *OctopoolConversations) InventoryWithWorkers(ctx context.Context, workers int, reporter Reporter) error {
	return c.inventory(ctx, nil, workers, reporter)
}

// InventoryNumbers bounds a small repair or pilot to one conversation batch.
// Large runs still use one shared repository inventory through Inventory.
func (c *OctopoolConversations) InventoryNumbers(ctx context.Context, numbers []int, reporter Reporter) error {
	if len(numbers) == 0 || len(numbers) > 25 {
		return errors.New("targeted inventory requires 1..25 numbers")
	}
	seen := map[int]bool{}
	for _, number := range numbers {
		if number <= 0 || seen[number] {
			return errors.New("targeted inventory requires unique positive numbers")
		}
		seen[number] = true
	}
	return c.inventory(ctx, numbers, 1, reporter)
}

func (c *OctopoolConversations) inventory(ctx context.Context, numbers []int, workers int, reporter Reporter) error {
	owner, repo, ok := strings.Cut(c.repository, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return errors.New("expected owner/repository")
	}
	metadata, err := c.Client.GetRepo(ctx, owner, repo, reporter)
	if err != nil {
		return err
	}
	var rows []map[string]any
	if len(numbers) == 0 {
		rows, err = c.Client.listRepositoryInventory(ctx, owner, repo, fmt.Sprint(metadata["id"]), workers, reporter)
		if err != nil {
			return err
		}
	} else {
		for _, number := range numbers {
			row, err := c.Client.GetIssue(ctx, owner, repo, number, reporter)
			if err != nil {
				return err
			}
			got, ok := historyInt(row["number"])
			if !ok || got != number {
				return fmt.Errorf("targeted REST inventory number mismatch for #%d", number)
			}
			rows = append(rows, row)
		}
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
const conversationFields = `__typename id number updatedAt repository { nameWithOwner } ` + conversationComments + ` ... on PullRequest { reviews(first:10) { totalCount pageInfo { hasNextPage } nodes { ` + historyComment + ` state submittedAt commit { oid } ` + conversationInline + ` } } reviewThreads(first:10) { totalCount pageInfo { hasNextPage } nodes { id ` + conversationInline + ` } } }`

// Prepare replaces the bounded child cache only after the entire batch succeeds.
// Large conversations use the existing fully paginated same-pool REST reader.
var errConversationParentRefreshed = errors.New("conversation parent refreshed")

func (c *OctopoolConversations) Prepare(ctx context.Context, numbers []int, reporter Reporter) error {
	for attempt := 0; attempt < 3; attempt++ {
		err := c.prepareOnce(ctx, numbers, reporter)
		if !errors.Is(err, errConversationParentRefreshed) {
			return err
		}
	}
	return errors.New("REST conversation parent kept changing across three acquisition attempts")
}

// PrepareBatch keeps each acquisition worker's refreshed parent and child maps
// private. Shared inventory rows and the pooled HTTP client are read-only.
func (c *OctopoolConversations) PrepareBatch(ctx context.Context, numbers []int, reporter Reporter) (*OctopoolConversations, error) {
	batch := &OctopoolConversations{Client: c.Client, relay: c.relay, repository: c.repository, metadata: c.metadata, issues: make(map[int]map[string]any, len(numbers)), conversations: map[int]HistoryItem{}}
	for _, number := range numbers {
		batch.issues[number] = c.issues[number]
	}
	if err := batch.Prepare(ctx, numbers, reporter); err != nil {
		return nil, err
	}
	return batch, nil
}

func (c *OctopoolConversations) PrepareRows(ctx context.Context, rows []map[string]any, reporter Reporter) (*OctopoolConversations, error) {
	batch := &OctopoolConversations{Client: c.Client, relay: c.relay, repository: c.repository, metadata: c.metadata, issues: map[int]map[string]any{}, conversations: map[int]HistoryItem{}}
	var numbers []int
	for _, row := range rows {
		number, ok := historyInt(row["number"])
		if !ok || batch.issues[number] != nil {
			return nil, errors.New("invalid or duplicate streamed number")
		}
		numbers = append(numbers, number)
		batch.issues[number] = row
	}
	if err := batch.Prepare(ctx, numbers, reporter); err != nil {
		return nil, err
	}
	return batch, nil
}

func (c *OctopoolConversations) IssueNumbers() []int {
	numbers := make([]int, 0, len(c.issues))
	for number := range c.issues {
		numbers = append(numbers, number)
	}
	slices.Sort(numbers)
	return numbers
}

func (c *OctopoolConversations) prepareOnce(ctx context.Context, numbers []int, reporter Reporter) error {
	if len(numbers) == 0 || len(numbers) > MaxConversationBatch {
		return fmt.Errorf("conversation batch must contain 1..%d numbers", MaxConversationBatch)
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
	query.WriteString(" } } fragment ConversationIssue on Issue { __typename id number updatedAt repository { nameWithOwner } " + conversationComments + " } fragment ConversationPull on PullRequest { " + conversationFields + " }")
	owner, repo, _ := strings.Cut(c.repository, "/")
	data, err := c.requestConversations(ctx, query.String(), map[string]any{"owner": owner, "repo": repo}, reporter)
	if err != nil {
		return err
	}
	repository := historyMap(data["repository"])
	refreshed := false
	for i, n := range numbers {
		node := historyMap(repository[fmt.Sprintf("n%d", i)])
		if err := c.validateConversationIdentity(n, node); err != nil {
			return err
		}
		restTime, err := time.Parse(time.RFC3339Nano, historyString(c.issues[n]["updated_at"]))
		if err != nil {
			return fmt.Errorf("invalid REST update timestamp for #%d", n)
		}
		graphTime, err := time.Parse(time.RFC3339Nano, historyString(node["updatedAt"]))
		if err != nil {
			return fmt.Errorf("invalid GraphQL update timestamp for #%d", n)
		}
		if !restTime.Equal(graphTime) {
			old := c.issues[n]
			fresh, err := c.Client.GetIssue(ctx, owner, repo, n, reporter)
			if err != nil {
				return err
			}
			got, ok := historyInt(fresh["number"])
			_, oldPull := old["pull_request"]
			_, freshPull := fresh["pull_request"]
			if !ok || got != n || fmt.Sprint(old["id"]) != fmt.Sprint(fresh["id"]) || old["node_id"] != fresh["node_id"] || oldPull != freshPull || !strings.EqualFold(historyString(fresh["repository_url"]), "https://api.github.com/repos/"+c.repository) {
				return fmt.Errorf("REST parent identity changed for #%d", n)
			}
			freshTime, err := time.Parse(time.RFC3339Nano, historyString(fresh["updated_at"]))
			if err != nil {
				return fmt.Errorf("invalid refreshed REST update timestamp for #%d", n)
			}
			c.issues[n] = fresh
			// A PR's GraphQL timestamp can differ from its backing issue's
			// timestamp. Retry only when REST confirms the parent changed.
			refreshed = refreshed || !freshTime.Equal(restTime)
		}
	}
	if refreshed {
		return errConversationParentRefreshed
	}
	prepared := make(map[int]HistoryItem, len(numbers))
	for i, n := range numbers {
		node := historyMap(repository[fmt.Sprintf("n%d", i)])
		_, isPull := c.issues[n]["pull_request"]
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

func (c *OctopoolConversations) validateConversationIdentity(n int, node map[string]any) error {
	typ := historyString(node["__typename"])
	number, ok := historyInt(node["number"])
	_, isPull := c.issues[n]["pull_request"]
	if !ok || number != n || historyString(node["id"]) != historyString(c.issues[n]["node_id"]) || historyString(historyMap(node["repository"])["nameWithOwner"]) != c.repository || (typ != "Issue" && typ != "PullRequest") || (typ == "PullRequest") != isPull {
		return fmt.Errorf("GraphQL identity mismatch or missing node for #%d", n)
	}
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
	response, err := c.relay.read(request)
	if err != nil {
		return nil, &conversationTransportError{cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Octopool GraphQL relay status %d: %s", response.StatusCode, octopoolRejectionBody(response))
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
