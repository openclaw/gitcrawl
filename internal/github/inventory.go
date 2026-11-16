package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// listRepositoryInventory keeps the ordinary incremental reader unchanged.
// Creation order avoids moving old rows when issue metadata changes. This is
// still a traversal, not a snapshot: catch-up must start at the original scan.
func (c *Client) listRepositoryInventory(ctx context.Context, owner, repo, repositoryID string, workers int, reporter Reporter) ([]map[string]any, error) {
	if workers < 1 || workers > 8 {
		return nil, errors.New("inventory workers must be 1..8")
	}
	path := "/repos/" + pathEscape(owner) + "/" + pathEscape(repo) + "/issues"
	values := url.Values{"state": {"all"}, "sort": {"created"}, "direction": {"asc"}, "per_page": {"100"}}
	fetch := func(ctx context.Context, page int) ([]map[string]any, string, error) {
		query := values.Clone()
		query.Set("page", strconv.Itoa(page))
		response, err := c.do(ctx, http.MethodGet, path+"?"+query.Encode(), nil, reporter)
		if err != nil {
			return nil, "", err
		}
		defer response.Body.Close()
		var rows []map[string]any
		if err = decodeJSON(response.Body, &rows); err != nil {
			return nil, "", fmt.Errorf("decode inventory page %d: %w", page, err)
		}
		if rows == nil {
			return nil, "", fmt.Errorf("inventory page %d is not an array", page)
		}
		if len(rows) > 100 {
			return nil, "", fmt.Errorf("inventory page %d exceeded page size", page)
		}
		return rows, response.Header.Get("Link"), nil
	}
	var rows []map[string]any
	seen := map[int]bool{}
	seenIDs := map[string]bool{}
	appendPage := func(page int, items []map[string]any, links string) (bool, error) {
		_, next, err := c.inventoryLinks(links, path, repositoryID, values)
		if err != nil {
			return false, err
		}
		if next != 0 && next != page+1 {
			return false, errors.New("inventory pagination skipped a page")
		}
		if len(items) == 0 && next != 0 {
			return false, errors.New("empty inventory page has a next link")
		}
		for _, row := range items {
			number, ok := historyInt(row["number"])
			if !ok || number <= 0 || row["id"] == nil {
				return false, errors.New("inventory row has invalid issue identity")
			}
			if seen[number] {
				return false, fmt.Errorf("inventory repeated issue #%d across pages", number)
			}
			id := fmt.Sprint(row["id"])
			numericID, idErr := strconv.ParseInt(id, 10, 64)
			if idErr != nil || numericID <= 0 || strconv.FormatInt(numericID, 10) != id || seenIDs[id] {
				return false, errors.New("inventory has invalid or duplicate canonical ID")
			}
			seenIDs[id] = true
			seen[number] = true
		}
		rows = append(rows, items...)
		reporter.Printf("[github] inventory page %d fetched count=%d accumulated=%d", page, len(items), len(rows))
		return len(items) < 100 && next == 0, nil
	}
	first, links, err := fetch(ctx, 1)
	if err != nil {
		return nil, err
	}
	tail, err := appendPage(1, first, links)
	if err != nil {
		return nil, err
	}
	if tail {
		return rows, nil
	}
	for start := 2; ; start += workers {
		type pageResult struct {
			rows  []map[string]any
			links string
			err   error
		}
		results := make([]pageResult, workers)
		window, cancel := context.WithCancel(ctx)
		var pending sync.WaitGroup
		for index := range workers {
			page := start + index
			pending.Go(func() {
				results[index].rows, results[index].links, results[index].err = fetch(window, page)
				if results[index].err != nil {
					cancel()
				}
			})
		}
		pending.Wait()
		cancel()
		for _, result := range results {
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				return nil, result.err
			}
		}
		for index, result := range results {
			if result.err != nil {
				return nil, result.err
			}
			tail, err = appendPage(start+index, result.rows, result.links)
			if err != nil {
				return nil, err
			}
			if tail {
				for _, later := range results[index+1:] {
					if len(later.rows) > 0 {
						return nil, errors.New("inventory returned records after a terminal short page")
					}
					_, next, linkErr := c.inventoryLinks(later.links, path, repositoryID, values)
					if linkErr != nil {
						return nil, linkErr
					}
					if next != 0 {
						return nil, errors.New("empty inventory tail has next page")
					}
				}
				return rows, nil
			}
		}
	}
}

// Numbered offset requests are the provider's documented API, qualified against
// its cursor-next response. We validate returned continuation links but never
// construct or reuse their opaque cursors. Numeric paths require GetRepo's ID.
func (c *Client) inventoryLinks(header, path, repositoryID string, values url.Values) (last, next int, err error) {
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return 0, 0, err
	}
	seen := map[string]bool{}
	for _, part := range strings.Split(header, ",") {
		fields := strings.Split(part, ";")
		if len(fields) < 2 {
			continue
		}
		relation := ""
		for _, field := range fields[1:] {
			field = strings.TrimSpace(field)
			if field == `rel="last"` {
				relation = "last"
			}
			if field == `rel="next"` {
				relation = "next"
			}
		}
		if relation == "" {
			continue
		}
		if seen[relation] {
			return 0, 0, errors.New("duplicate inventory pagination relation")
		}
		seen[relation] = true
		target, parseErr := url.Parse(strings.Trim(strings.TrimSpace(fields[0]), "<>"))
		if parseErr != nil || target.Scheme != base.Scheme || !strings.EqualFold(target.Host, base.Host) || target.User != nil || target.Fragment != "" {
			return 0, 0, errors.New("inventory pagination changed origin")
		}
		expectedPath := strings.TrimRight(base.EscapedPath(), "/") + path
		numericPath := strings.TrimRight(base.EscapedPath(), "/") + "/repositories/" + repositoryID + "/issues"
		if target.EscapedPath() != expectedPath && (repositoryID == "" || target.EscapedPath() != numericPath) {
			return 0, 0, errors.New("inventory pagination changed repository")
		}
		query, parseErr := url.ParseQuery(target.RawQuery)
		if parseErr != nil || (len(query) != len(values)+1 && len(query) != len(values)+2) {
			return 0, 0, errors.New("inventory pagination changed query")
		}
		if after, ok := query["after"]; ok && (len(after) != 1 || after[0] == "") {
			return 0, 0, errors.New("invalid inventory continuation cursor")
		}
		for key := range query {
			if _, ok := values[key]; !ok && key != "page" && key != "after" {
				return 0, 0, errors.New("inventory pagination changed query")
			}
		}
		for key, want := range values {
			got := query[key]
			if len(got) != 1 || got[0] != want[0] {
				return 0, 0, errors.New("inventory pagination changed query")
			}
		}
		pageValues := query["page"]
		if len(pageValues) != 1 {
			return 0, 0, errors.New("inventory pagination lacks one page number")
		}
		page, parseErr := strconv.Atoi(pageValues[0])
		if parseErr != nil || page < 1 || strconv.Itoa(page) != pageValues[0] {
			return 0, 0, errors.New("invalid inventory page number")
		}
		if relation == "last" {
			last = page
		} else {
			next = page
		}
	}
	return last, next, nil
}
