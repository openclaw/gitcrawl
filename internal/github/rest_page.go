package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const MaxRESTPageBytes = 8 << 20

// RESTPageRequest describes one repository-scoped page. Cursor is empty for the
// first page, then exactly the preceding response's NextCursor. It is suitable
// for an external checkpoint owner; this reader never opens a local store.
type RESTPageRequest struct {
	Owner, Repo        string
	Family             string // issues, issue_comment, pull_review or pull_review_comment
	Number             int
	State, Since, Sort string // issues only; sort is updated (default) or created
	PerPage            int
	Cursor             string
}

type RESTPage struct {
	Rows       []map[string]any
	NextCursor string
}

// ReadRESTPage reads one complete page, preserving full provider values. It does
// not resize pages or restart a traversal: either would invalidate an external
// owner's committed cursor. A caller may choose a smaller initial PerPage.
func (c *Client) ReadRESTPage(ctx context.Context, request RESTPageRequest, reporter Reporter) (RESTPage, error) {
	path, query, err := request.resource()
	if err != nil {
		return RESTPage{}, err
	}
	current := path + "?" + query.Encode()
	if request.Cursor != "" {
		current, err = c.restPageCursor(request.Cursor, path, query)
		if err != nil {
			return RESTPage{}, err
		}
	}
	response, err := c.do(ctx, http.MethodGet, current, nil, reporter)
	if err != nil {
		return RESTPage{}, err
	}
	defer response.Body.Close()
	rows, err := decodeRESTPage(response.Body)
	if err != nil {
		return RESTPage{}, err
	}
	next, err := c.restPageNext(response.Header.Get("Link"), path, query)
	if err != nil {
		return RESTPage{}, err
	}
	if next == current {
		return RESTPage{}, errors.New("github page repeated its cursor")
	}
	if request.Family == "pull_review" {
		rows = withoutPendingReviews(rows)
	}
	return RESTPage{Rows: rows, NextCursor: next}, nil
}

func (r RESTPageRequest) resource() (string, url.Values, error) {
	if r.Owner == "" || r.Repo == "" || strings.ContainsAny(r.Owner+r.Repo, "/\\?#") || r.PerPage < 1 || r.PerPage > 100 {
		return "", nil, errors.New("invalid repository page request")
	}
	path := "/repos/" + pathEscape(r.Owner) + "/" + pathEscape(r.Repo)
	query := url.Values{"per_page": {strconv.Itoa(r.PerPage)}}
	if r.Family == "issues" {
		if r.Number != 0 {
			return "", nil, errors.New("inventory page cannot select a thread")
		}
		state, sort := r.State, r.Sort
		if state == "" {
			state = "all"
		}
		if sort == "" {
			sort = "updated"
		}
		if state != "all" && state != "open" && state != "closed" || sort != "updated" && sort != "created" {
			return "", nil, errors.New("invalid inventory page ordering")
		}
		query.Set("state", state)
		query.Set("sort", sort)
		if sort == "created" {
			query.Set("direction", "asc")
		} else {
			query.Set("direction", "desc")
		}
		if r.Since != "" {
			query.Set("since", r.Since)
		}
		return path + "/issues", query, nil
	}
	if r.Number <= 0 || r.State != "" || r.Since != "" || r.Sort != "" {
		return "", nil, errors.New("invalid child page request")
	}
	switch r.Family {
	case "issue_comment":
		path += fmt.Sprintf("/issues/%d/comments", r.Number)
	case "pull_review":
		path += fmt.Sprintf("/pulls/%d/reviews", r.Number)
	case "pull_review_comment":
		path += fmt.Sprintf("/pulls/%d/comments", r.Number)
	default:
		return "", nil, errors.New("unknown repository page family")
	}
	return path, query, nil
}

func decodeRESTPage(body io.Reader) ([]map[string]any, error) {
	raw, err := io.ReadAll(io.LimitReader(body, MaxRESTPageBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxRESTPageBytes {
		return nil, errors.New("github page exceeds 8 MiB")
	}
	var rows []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&rows); err != nil {
		return nil, fmt.Errorf("decode github page: %w", err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("github page has trailing input")
	}
	if rows == nil || len(rows) > 100 {
		return nil, errors.New("invalid github page array")
	}
	return rows, nil
}

func (c *Client) restPageCursor(cursor, path string, fixed url.Values) (string, error) {
	invalid := func() (string, error) { return "", errors.New("github page cursor changed resource or query") }
	if len(cursor) > 4096 {
		return invalid()
	}
	target, err := url.Parse(cursor)
	if err != nil || target.User != nil || target.Fragment != "" || target.Opaque != "" {
		return invalid()
	}
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return invalid()
	}
	if target.IsAbs() || target.Host != "" {
		if target.Scheme != base.Scheme || !strings.EqualFold(target.Host, base.Host) {
			return invalid()
		}
	}
	resolved := requestPathFromLink(target, c.baseURL)
	rel, err := url.Parse(resolved)
	if err != nil {
		return invalid()
	}
	resource := rel.EscapedPath()
	// Octopool has already bound this numeric repository identity in GetRepo.
	if relay, ok := c.httpClient.Transport.(*octopoolTransport); ok {
		relay.repositoryMu.RLock()
		id := relay.repositoryID
		relay.repositoryMu.RUnlock()
		prefix := "/repositories/" + id
		if id != "" && strings.HasPrefix(resource, prefix+"/") {
			resource = relay.repositoryPath + strings.TrimPrefix(resource, prefix)
		}
	}
	if app, ok := c.httpClient.Transport.(*appReadTransport); ok {
		resource = app.repositoryResource(resource)
	}
	if resource != path {
		return invalid()
	}
	actual, err := url.ParseQuery(rel.RawQuery)
	if err != nil {
		return invalid()
	}
	for key, values := range actual {
		if len(values) != 1 {
			return invalid()
		}
		if _, ok := fixed[key]; !ok && key != "page" && key != "after" {
			return invalid()
		}
	}
	for key, values := range fixed {
		if actual.Get(key) != values[0] {
			return invalid()
		}
	}
	if p := actual.Get("page"); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || strconv.Itoa(n) != p {
			return invalid()
		}
	}
	if actual.Get("page") == "" && actual.Get("after") == "" {
		return invalid()
	}
	return resolved, nil
}

func (c *Client) restPageNext(header, path string, query url.Values) (string, error) {
	next := ""
	for _, part := range strings.Split(header, ",") {
		fields := strings.Split(part, ";")
		isNext := false
		for _, field := range fields[1:] {
			if strings.TrimSpace(field) == `rel="next"` {
				isNext = true
			}
		}
		if !isNext {
			continue
		}
		if next != "" {
			return "", errors.New("duplicate github next cursor")
		}
		var err error
		next, err = c.restPageCursor(strings.Trim(strings.TrimSpace(fields[0]), "<>"), path, query)
		if err != nil {
			return "", err
		}
	}
	return next, nil
}
