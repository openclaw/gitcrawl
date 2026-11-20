// Package acquireapi owns the collector's PostgreSQL acquisition transport and
// step machine. PostgreSQL owns checkpoints, publication and native identities.
package acquireapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
)

const MaxPageBytes = 4 << 20

type RunRequest struct {
	ID               string          `json:"id"`
	ExpectedRevision int64           `json:"expectedRevision"`
	Horizon          time.Time       `json:"horizon"`
	Scope            json.RawMessage `json:"scope"`
}
type State struct {
	Revision         int64           `json:"revision"`
	Sequence         int64           `json:"sequence"`
	Checkpoint       json.RawMessage `json:"checkpoint"`
	CompletedThrough *time.Time      `json:"completedThrough"`
	ActiveRun        *RunRequest     `json:"activeRun,omitempty"`
}
type ChildObservation struct {
	ThreadID   string `json:"threadId"`
	Action     string `json:"action"`
	Family     string `json:"family,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
	NextCursor string `json:"nextCursor,omitempty"`
	Terminal   bool   `json:"terminal,omitempty"`
}
type PageRequest struct {
	RunID            string            `json:"runId"`
	ID               string            `json:"id"`
	ExpectedRevision int64             `json:"expectedRevision"`
	Checkpoint       json.RawMessage   `json:"checkpoint"`
	Changes          []remote.Change   `json:"changes"`
	Final            bool              `json:"final"`
	ChildObservation *ChildObservation `json:"childObservation,omitempty"`
}
type Receipt struct {
	RunID    string `json:"runId"`
	PageID   string `json:"pageId"`
	SHA256   string `json:"sha256"`
	Revision int64  `json:"revision"`
	Sequence int64  `json:"sequence"`
	Final    bool   `json:"final"`
	Replayed bool   `json:"replayed"`
}
type ThreadIdentity struct {
	GitHubID string `json:"githubId"`
	Kind     string `json:"kind"`
	Number   string `json:"number"`
}
type CommentIdentity struct {
	GitHubID     string `json:"githubId"`
	Type         string `json:"type"`
	ThreadKind   string `json:"threadKind"`
	ThreadNumber string `json:"threadNumber"`
}
type ResolveRequest struct {
	Threads  []ThreadIdentity  `json:"threads"`
	Comments []CommentIdentity `json:"comments"`
}
type ResolvedIDs struct {
	RepositoryID string   `json:"repositoryId"`
	Threads      []string `json:"threads"`
	Comments     []string `json:"comments"`
}

// Client separates publisher and reader tokens and never follows redirects.
type Client struct {
	endpoint, source, token string
	http                    *http.Client
	reader                  *remote.Client
}

func NewClient(endpoint, source, publisherToken, readerToken string, transport http.RoundTripper) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"))) {
		return nil, errors.New("acquisition requires a fixed HTTPS endpoint or loopback HTTP")
	}
	if source == "" || len(source) > 128 || strings.ContainsAny(source, "/\\?#\x00") || publisherToken == "" || readerToken == "" || publisherToken == readerToken || strings.ContainsAny(publisherToken+readerToken, " \t\r\n") {
		return nil, errors.New("explicit source and publisher/reader credentials required")
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	h := &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	reader, err := remote.NewClient(remote.Options{Endpoint: u.String(), HTTPClient: h, TokenProvider: remote.StaticToken(readerToken)})
	if err != nil {
		return nil, err
	}
	return &Client{u.String(), source, publisherToken, h, reader}, nil
}
func (c *Client) request(ctx context.Context, kind string, body []byte, out any) error {
	method := http.MethodPost
	limit := 128 << 10
	if kind == "state" {
		method = http.MethodGet
		limit = 1 << 20
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+"/v1/apps/gitcrawl/archives/"+url.PathEscape(c.source)+"/acquisition/"+kind, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, int64(limit+1)))
	if err != nil {
		return err
	}
	if len(raw) > limit {
		return errors.New("acquisition response exceeds bound")
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("acquisition %s failed (HTTP %d)", kind, response.StatusCode)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if err = d.Decode(out); err != nil {
		return errors.New("invalid acquisition response")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing acquisition response")
	}
	return nil
}
func (c *Client) State(ctx context.Context) (State, error) {
	var out State
	err := c.request(ctx, "state", nil, &out)
	return out, err
}
func (c *Client) Begin(ctx context.Context, r RunRequest) error {
	b, err := json.Marshal(r)
	if err != nil || len(b) > 128<<10 {
		return errors.New("run request exceeds bound")
	}
	var out struct {
		RunID  string `json:"runId"`
		Status string `json:"status"`
	}
	if err = c.request(ctx, "runs", b, &out); err != nil {
		return err
	}
	if out.RunID != r.ID || out.Status != "accepted" {
		return errors.New("run receipt mismatch")
	}
	return nil
}
func (c *Client) Resolve(ctx context.Context, r ResolveRequest) (ResolvedIDs, error) {
	var out ResolvedIDs
	b, err := json.Marshal(r)
	if err != nil || len(b) > 128<<10 || len(r.Threads)+len(r.Comments) > 500 {
		return out, errors.New("identity request exceeds bound")
	}
	err = c.request(ctx, "resolve-ids", b, &out)
	if err == nil && (len(out.Threads) != len(r.Threads) || len(out.Comments) != len(r.Comments)) {
		err = errors.New("identity response mismatch")
	}
	return out, err
}

// Only an ambiguous transport failure gets one retry, using the exact frozen
// page bytes. HTTP rejection is terminal; process restart recovers through State.
func (c *Client) Commit(ctx context.Context, r PageRequest) (Receipt, error) {
	var out Receipt
	if len(r.Changes) > 500 {
		return out, errors.New("page exceeds 500 changes")
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > MaxPageBytes {
		return out, errors.New("page exceeds 4 MiB")
	}
	err = c.request(ctx, "pages", b, &out)
	var transport *url.Error
	if (errors.As(err, &transport) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) && ctx.Err() == nil {
		out = Receipt{}
		err = c.request(ctx, "pages", b, &out)
	}
	sum := sha256.Sum256(b)
	if err == nil && (out.SHA256 != hex.EncodeToString(sum[:]) || out.RunID != r.RunID || out.PageID != r.ID || out.Final != r.Final || out.Revision != r.ExpectedRevision+1) {
		err = errors.New("page receipt mismatch")
	}
	return out, err
}

func (c *Client) Parent(ctx context.Context, id int64) (*remote.Change, error) {
	result, err := c.reader.Query(ctx, "gitcrawl", c.source, remote.QueryRequest{Name: "gitcrawl.threads.current", Args: map[string]any{"ids": []string{strconv.FormatInt(id, 10)}}, Limit: 1})
	if err != nil {
		return nil, err
	}
	return remotedelivery.AcquisitionParent(result, id)
}

// CheckRepository binds the configured PG source to the repository verified by
// Octopool before a new run can reserve its durable slot.
func (c *Client) CheckRepository(ctx context.Context, name, providerID string) (int64, error) {
	result, err := c.reader.Query(ctx, "gitcrawl", c.source, remote.QueryRequest{Name: "gitcrawl.repositories.current", Limit: 1})
	if err != nil {
		return 0, err
	}
	if len(result.Rows) != 1 || result.Cursor != "" || result.Stats.NextCursor != "" {
		return 0, errors.New("source repository is not singular")
	}
	values := map[string]any{}
	if len(result.Rows[0]) != len(result.Columns) {
		return 0, errors.New("invalid repository row")
	}
	for i, column := range result.Columns {
		values[column] = result.Rows[0][i]
	}
	full, ok := values["full_name"].(string)
	githubID, idOK := values["github_repo_id"].(string)
	native, nativeOK := values["id"].(string)
	if !ok || !idOK || !nativeOK || !strings.EqualFold(full, name) || githubID != providerID {
		return 0, errors.New("provider and PG repository identities differ")
	}
	return positiveID(native)
}
