package acquireapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A final page commit can wait on server-side eligibility checks for longer
// than ordinary calls; only run and page commits get the longer bound.
func TestCommitUsesCommitTimeout(t *testing.T) {
	const delay = 300 * time.Millisecond
	var pages atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		switch {
		case strings.HasSuffix(r.URL.Path, "/pages"):
			pages.Add(1)
			raw, _ := io.ReadAll(r.Body)
			var page PageRequest
			if err := json.Unmarshal(raw, &page); err != nil {
				t.Error(err)
			}
			sum := sha256.Sum256(raw)
			json.NewEncoder(w).Encode(Receipt{RunID: page.RunID, PageID: page.ID, SHA256: hex.EncodeToString(sum[:]), Revision: page.ExpectedRevision + 1, Final: page.Final})
		default:
			json.NewEncoder(w).Encode(State{})
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "fixture", "publisher", "reader", nil)
	if err != nil {
		t.Fatal(err)
	}
	if client.http.Timeout != RequestTimeout || client.commit.Timeout != DefaultCommitTimeout {
		t.Fatalf("default timeouts = %v/%v", client.http.Timeout, client.commit.Timeout)
	}
	client.http.Timeout = delay / 3
	if err = client.SetCommitTimeout(10 * delay); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = client.State(ctx); !isTimeout(err) {
		t.Fatalf("state should keep the ordinary bound, got %v", err)
	}
	page := PageRequest{RunID: "run", ID: "page", ExpectedRevision: 4, Checkpoint: json.RawMessage(`{}`), Final: true}
	receipt, err := client.Commit(ctx, page)
	if err != nil || receipt.Revision != 5 || !receipt.Final {
		t.Fatalf("commit = %+v, %v", receipt, err)
	}
	if pages.Load() != 1 {
		t.Fatalf("commit attempts = %d", pages.Load())
	}

	// The commit bound still applies, and the caller context bounds it too.
	if err = client.SetCommitTimeout(delay / 3); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Commit(ctx, page); !isTimeout(err) {
		t.Fatalf("commit should time out, got %v", err)
	}
	if err = client.SetCommitTimeout(10 * delay); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, delay/3)
	defer cancel()
	if _, err = client.Commit(short, page); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commit should follow the caller deadline, got %v", err)
	}
}

func TestSetCommitTimeoutRejectsInvalidBounds(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1", "fixture", "publisher", "reader", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []time.Duration{0, -time.Second, 13 * time.Hour} {
		if client.SetCommitTimeout(d) == nil {
			t.Fatalf("accepted %v", d)
		}
	}
	if client.commit.Timeout != DefaultCommitTimeout {
		t.Fatalf("invalid bound changed timeout to %v", client.commit.Timeout)
	}
}

func isTimeout(err error) bool {
	var transport *url.Error
	return errors.As(err, &transport) && transport.Timeout()
}
