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

// RequestTimeout bounds ordinary acquisition API calls. Run and page commits
// use DefaultCommitTimeout instead: the server checks publication eligibility
// over the whole candidate snapshot before it answers a final page.
const (
	RequestTimeout       = 30 * time.Second
	DefaultCommitTimeout = 10 * time.Minute
)

type RunRequest struct {
	ID               string          `json:"id"`
	Mode             string          `json:"mode,omitempty"`
	ExpectedRevision int64           `json:"expectedRevision"`
	Horizon          time.Time       `json:"horizon"`
	Scope            json.RawMessage `json:"scope"`
}
type State struct {
	Revision                int64           `json:"revision"`
	Sequence                int64           `json:"sequence"`
	Checkpoint              json.RawMessage `json:"checkpoint"`
	CompletedThrough        *time.Time      `json:"completedThrough"`
	ActiveRun               *RunRequest     `json:"activeRun,omitempty"`
	CompletedRun            *RunRequest     `json:"completedRun,omitempty"`
	ParentCheckpoint        json.RawMessage `json:"parentCheckpoint,omitempty"`
	CompletedCommitBackfill *RunRequest     `json:"completedCommitBackfill,omitempty"`
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
	Lifecycle        *LifecyclePage    `json:"lifecycle,omitempty"`
	Commits          *CommitPage       `json:"commits,omitempty"`
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
	http, commit            *http.Client
	reader, lifecycleReader *remote.Client
	readerCredential        remote.TokenProvider
}

func NewClient(endpoint, source, publisherToken, readerToken string, transport http.RoundTripper) (*Client, error) {
	u, err := acquisitionEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if source == "" || len(source) > 128 || strings.ContainsAny(source, "/\\?#\x00") || publisherToken == "" || readerToken == "" || publisherToken == readerToken || strings.ContainsAny(publisherToken+readerToken, " \t\r\n") {
		return nil, errors.New("explicit source and publisher/reader credentials required")
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	h := &http.Client{Transport: transport, Timeout: RequestTimeout, CheckRedirect: noRedirect}
	commit := &http.Client{Transport: transport, Timeout: DefaultCommitTimeout, CheckRedirect: noRedirect}
	credential := remote.StaticToken(readerToken)
	reader, err := remote.NewClient(remote.Options{Endpoint: u.String(), HTTPClient: h, TokenProvider: credential})
	if err != nil {
		return nil, err
	}
	return &Client{u.String(), source, publisherToken, h, commit, reader, reader, credential}, nil
}

func acquisitionEndpoint(endpoint string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"))) {
		return nil, errors.New("acquisition requires a fixed HTTPS endpoint or loopback HTTP")
	}
	return u, nil
}

// SetLifecycleReader routes only the candidate-number query to the admitted
// reader. Native parent verification and every write keep their original owner.
func (c *Client) SetLifecycleReader(endpoint, token string) error {
	u, err := acquisitionEndpoint(endpoint)
	if err != nil {
		return err
	}
	credential := c.readerCredential
	if token != "" {
		if token == c.token || strings.ContainsAny(token, " \t\r\n") {
			return errors.New("invalid lifecycle reader credential")
		}
		credential = remote.StaticToken(token)
	}
	reader, err := remote.NewClient(remote.Options{Endpoint: u.String(), HTTPClient: c.http, TokenProvider: credential})
	if err == nil {
		c.lifecycleReader = reader
	}
	return err
}

// SetCommitTimeout sets the per-request bound for run and page commits. The
// caller context still bounds every request.
func (c *Client) SetCommitTimeout(d time.Duration) error {
	if d <= 0 || d > 12*time.Hour {
		return errors.New("commit timeout must be positive and at most 12h")
	}
	c.commit.Timeout = d
	return nil
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
	h := c.http
	if kind == "runs" || kind == "pages" {
		h = c.commit
	}
	response, err := h.Do(req)
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

// Parents reads one completed snapshot for the selected native IDs. Missing
// parents remain absent; neither row order nor missing IDs imply identity.
func (c *Client) Parents(ctx context.Context, ids []int64) (map[int64]remote.Change, error) {
	if len(ids) == 0 || len(ids) > 50 {
		return nil, errors.New("parent lookup requires 1..50 IDs")
	}
	selected := make([]string, len(ids))
	wanted := make(map[int64]bool, len(ids))
	for i, id := range ids {
		if id <= 0 || wanted[id] {
			return nil, errors.New("invalid duplicate parent ID")
		}
		wanted[id] = true
		selected[i] = strconv.FormatInt(id, 10)
	}
	result, err := c.reader.Query(ctx, "gitcrawl", c.source, remote.QueryRequest{Name: "gitcrawl.threads.current", Args: map[string]any{"ids": selected}, Limit: len(ids)})
	if err != nil {
		return nil, err
	}
	if len(result.Rows) > len(ids) || result.Cursor != "" || result.Stats.NextCursor != "" {
		return nil, errors.New("incomplete selected parent lookup")
	}
	out := make(map[int64]remote.Change, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) == 0 {
			return nil, errors.New("empty selected parent row")
		}
		value, ok := row[0].(string)
		id, parseErr := strconv.ParseInt(value, 10, 64)
		if !ok || parseErr != nil || !wanted[id] {
			return nil, errors.New("unexpected selected parent identity")
		}
		if _, duplicate := out[id]; duplicate {
			return nil, errors.New("duplicate selected parent")
		}
		one := result
		one.Rows = [][]any{row}
		change, err := remotedelivery.AcquisitionParent(one, id)
		if err != nil {
			return nil, err
		}
		out[id] = *change
	}
	return out, nil
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
