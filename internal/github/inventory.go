package github

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// InventoryScope binds recovery to the same native archive and requested work.
// NativeIdentity comes from the existing native file-identity owner, not a path.
type InventoryScope struct {
	Checkpoint        string
	NativeIdentity    string
	SelectionSHA256   string
	IncludeDiscovered bool
	StartedAt         time.Time
}
type InventoryPage struct {
	Index     int
	Rows      []map[string]any
	Completed []int
}
type InventoryStream struct {
	client     *OctopoolConversations
	checkpoint *inventoryCheckpoint
	path       string
	query      url.Values
}

func (c *OctopoolConversations) OpenInventory(ctx context.Context, scope InventoryScope, reporter Reporter) (*InventoryStream, error) {
	if scope.NativeIdentity == "" || scope.SelectionSHA256 == "" || scope.StartedAt.IsZero() {
		return nil, errors.New("inventory checkpoint requires archive, selection and start identity")
	}
	owner, repo, ok := strings.Cut(c.repository, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return nil, errors.New("expected owner/repository")
	}
	metadata, err := c.Client.GetRepo(ctx, owner, repo, reporter)
	if err != nil {
		return nil, err
	}
	query := url.Values{"state": {"all"}, "sort": {"created"}, "direction": {"asc"}, "per_page": {"100"}}
	path := "/repos/" + pathEscape(owner) + "/" + pathEscape(repo) + "/issues"
	descriptor, err := json.Marshal(struct {
		Profile, Endpoint, Pool, CallerSHA256, Repository, RepositoryID, Query, NativeIdentity, SelectionSHA256 string
		IncludeDiscovered                                                                                       bool
	}{
		"cursor-inventory-v1", c.relay.endpoint, c.relay.pool, fmt.Sprintf("%x", sha256.Sum256([]byte("gitcrawl-inventory-caller-v1\x00"+c.relay.token))), c.repository, fmt.Sprint(metadata["id"]), query.Encode(), scope.NativeIdentity, scope.SelectionSHA256, scope.IncludeDiscovered,
	})
	if err != nil {
		return nil, err
	}
	checkpoint, err := openInventoryCheckpoint(ctx, scope.Checkpoint, string(descriptor), path+"?"+query.Encode(), scope.StartedAt)
	if err != nil {
		return nil, err
	}
	c.metadata = metadata
	return &InventoryStream{client: c, checkpoint: checkpoint, path: path, query: query}, nil
}
func (s *InventoryStream) Close() error         { return s.checkpoint.Close() }
func (s *InventoryStream) StartedAt() time.Time { return s.checkpoint.started }

// Complete follows a successful native transaction; a crash before this commit
// only repeats that bounded batch. It never precedes native persistence.
func (s *InventoryStream) Complete(ctx context.Context, page int, numbers []int) error {
	return s.checkpoint.complete(ctx, page, numbers)
}

// Walk first replays saved full pages, then follows only provider-issued cursors.
// A page and its next cursor commit together before it is sent to a consumer.
func (s *InventoryStream) Walk(ctx context.Context, reporter Reporter, yield func(InventoryPage) error) error {
	next, finished, saved, err := s.checkpoint.state(ctx)
	if err != nil {
		return err
	}
	for index := 1; index <= saved; index++ {
		raw, completed, err := s.checkpoint.page(ctx, index)
		if err != nil {
			return err
		}
		rows, err := decodeInventoryPage(raw)
		if err != nil {
			return err
		}
		reporter.Printf("[github] inventory page %d replayed count=%d completed=%d", index, len(rows), len(completed))
		if err = yield(InventoryPage{index, rows, completed}); err != nil {
			return err
		}
	}
	for index := saved + 1; !finished; index++ {
		response, err := s.client.Client.do(ctx, http.MethodGet, next, nil, reporter)
		if err != nil {
			return err
		}
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
		response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if len(raw) > 8<<20 {
			return errors.New("inventory page exceeds 8 MiB")
		}
		rows, err := decodeInventoryPage(raw)
		if err != nil {
			return err
		}
		following, err := s.client.Client.inventoryNext(response.Header.Get("Link"), s.path, fmt.Sprint(s.client.metadata["id"]), s.query)
		if err != nil {
			return err
		}
		if len(rows) == 0 && following != "" {
			return errors.New("empty inventory page has a next cursor")
		}
		if err = s.checkpoint.savePage(ctx, index, next, following, raw); err != nil {
			return err
		}
		reporter.Printf("[github] inventory page %d saved count=%d", index, len(rows))
		if err = yield(InventoryPage{Index: index, Rows: rows}); err != nil {
			return err
		}
		next = following
		finished = following == ""
	}
	return nil
}
func decodeInventoryPage(raw []byte) ([]map[string]any, error) {
	var rows []map[string]any
	if err := decodeJSON(bytes.NewReader(raw), &rows); err != nil {
		return nil, err
	}
	if rows == nil || len(rows) > 100 {
		return nil, errors.New("invalid inventory page array")
	}
	for _, row := range rows {
		number, ok := historyInt(row["number"])
		id := fmt.Sprint(row["id"])
		numeric, err := strconv.ParseInt(id, 10, 64)
		if !ok || number <= 0 || err != nil || numeric <= 0 || strconv.FormatInt(numeric, 10) != id || historyString(row["node_id"]) == "" {
			return nil, errors.New("inventory row has invalid canonical identity")
		}
	}
	return rows, nil
}

// inventoryNext validates provider scope before retaining its opaque cursor.
// Numeric paths remain native reads: the existing transport normalizes them to
// the selected repository. No offset page or cursor is constructed here.
func (c *Client) inventoryNext(header, path, repositoryID string, query url.Values) (string, error) {
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return "", err
	}
	next := ""
	for _, part := range strings.Split(header, ",") {
		fields := strings.Split(part, ";")
		if len(fields) < 2 {
			continue
		}
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
			return "", errors.New("duplicate next cursor")
		}
		target, err := url.Parse(strings.Trim(strings.TrimSpace(fields[0]), "<>"))
		if err != nil {
			return "", err
		}
		if target.Scheme != base.Scheme || !strings.EqualFold(target.Host, base.Host) || target.User != nil || target.Fragment != "" {
			return "", errors.New("inventory cursor changed origin")
		}
		prefix := strings.TrimRight(base.EscapedPath(), "/")
		if target.EscapedPath() != prefix+path && target.EscapedPath() != prefix+"/repositories/"+repositoryID+"/issues" {
			return "", errors.New("inventory cursor changed repository")
		}
		actual, err := url.ParseQuery(target.RawQuery)
		if err != nil {
			return "", err
		}
		for key, want := range query {
			got := actual[key]
			if len(got) != 1 || got[0] != want[0] {
				return "", errors.New("inventory cursor changed query")
			}
		}
		for key, values := range actual {
			if len(values) != 1 {
				return "", errors.New("duplicate inventory query value")
			}
			if _, ok := query[key]; !ok && key != "page" && key != "after" {
				return "", errors.New("unexpected inventory cursor query")
			}
		}
		if actual.Get("after") == "" {
			return "", errors.New("inventory next link lacks authentic cursor")
		}
		if page := actual.Get("page"); page != "" {
			n, err := strconv.Atoi(page)
			if err != nil || n <= 0 || strconv.Itoa(n) != page {
				return "", errors.New("invalid cursor page label")
			}
		}
		next = target.EscapedPath()
		if target.RawQuery != "" {
			next += "?" + target.RawQuery
		}
	}
	return next, nil
}

// The library inventory reader is deliberately serial: large GitHub datasets
// require authentic cursors. The operator command uses the durable stream.
func (c *Client) listRepositoryInventory(ctx context.Context, owner, repo, repositoryID string, workers int, reporter Reporter) ([]map[string]any, error) {
	if workers < 1 || workers > 8 {
		return nil, errors.New("inventory workers must be 1..8")
	}
	path := "/repos/" + pathEscape(owner) + "/" + pathEscape(repo) + "/issues"
	query := url.Values{"state": {"all"}, "sort": {"created"}, "direction": {"asc"}, "per_page": {"100"}}
	next := path + "?" + query.Encode()
	seen := map[string]bool{}
	var out []map[string]any
	for next != "" {
		if seen[next] {
			return nil, errors.New("inventory cursor repeated")
		}
		seen[next] = true
		response, err := c.do(ctx, http.MethodGet, next, nil, reporter)
		if err != nil {
			return nil, err
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
		response.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(raw) > 8<<20 {
			return nil, errors.New("inventory page exceeds 8 MiB")
		}
		rows, err := decodeInventoryPage(raw)
		if err != nil {
			return nil, err
		}
		next, err = c.inventoryNext(response.Header.Get("Link"), path, repositoryID, query)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 && next != "" {
			return nil, errors.New("empty inventory page has a next cursor")
		}
		out = append(out, rows...)
	}
	return out, nil
}
