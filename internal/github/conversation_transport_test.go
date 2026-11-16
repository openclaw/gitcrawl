package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"testing"
)

func TestConversationTransportPreservesCauseWithoutPrivateDiagnostics(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.EOF, syscall.ECONNRESET, errors.New("private-token-and-response")} {
		client, err := NewOctopoolConversations("https://private.invalid", "fixture", "private-token", "fixture/repo")
		if err != nil {
			t.Fatal(err)
		}
		client.relay.client = &http.Client{Transport: contentRetryTransport(func(*http.Request) (*http.Response, error) {
			return nil, &url.Error{Op: "Post", URL: "https://private.invalid/private-token", Err: cause}
		})}
		_, err = client.requestConversations(context.Background(), "query {}", nil, nil)
		if !errors.Is(err, cause) {
			t.Fatalf("lost transport identity: %v", err)
		}
		for _, secret := range []string{"private.invalid", "private-token", "private-token-and-response"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("private diagnostics leaked: %s", err)
			}
		}
		if !strings.Contains(err.Error(), "Octopool GraphQL request failed:") {
			t.Fatal(err)
		}
	}
}

func TestConversationQueryBudget(t *testing.T) {
	client, err := NewOctopoolConversations("https://relay.invalid", "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	numbers := make([]int, MaxConversationBatch)
	for i := range numbers {
		numbers[i] = 200000 + i
		client.issues[numbers[i]] = map[string]any{}
	}
	var query string
	client.relay.client = &http.Client{Transport: contentRetryTransport(func(request *http.Request) (*http.Response, error) {
		var payload struct {
			GraphQL struct {
				Query string `json:"query"`
			} `json:"graphql"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		query = payload.GraphQL.Query
		return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
	})}
	_ = client.prepareOnce(context.Background(), numbers, nil)
	if len(query) == 0 || len(query) > 16<<10 {
		t.Fatalf("query bytes=%d", len(query))
	}
	for _, size := range []int{21, 25} {
		oversized := make([]int, size)
		copy(oversized, numbers)
		if err := client.prepareOnce(context.Background(), oversized, nil); err == nil || !strings.Contains(err.Error(), "1..20") {
			t.Fatalf("batch size %d was not rejected before request: %v", size, err)
		}
	}
	encoded, _ := json.Marshal(query)
	t.Logf("generated query (%d bytes): %s", len(query), encoded)
}
