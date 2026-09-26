package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHistoryFailureEvidenceRetainsStructureWithoutSecretsOrBodies(t *testing.T) {
	data := map[string]any{"id": "PR_fixture", "body": "private prose", "Authorization": "Bearer secret", "author": map[string]any{"login": "private-login"}, "comments": map[string]any{"totalCount": 2, "nodes": []any{map[string]any{"id": "C1", "body": "retained-by-hash"}}, "pageInfo": map[string]any{"hasNextPage": false}}}
	evidence := SafeHistoryEvidence(data)
	for _, secret := range []string{"private prose", "Bearer secret", "private-login", "retained-by-hash"} {
		if strings.Contains(string(evidence), secret) {
			t.Fatal("private data in safe receipt")
		}
	}
	if !json.Valid(evidence) || !strings.Contains(string(evidence), `"totalCount":2`) || !strings.Contains(string(evidence), `"sha256"`) {
		t.Fatal("missing structural evidence")
	}
	err := historyFailure("validation", 7, data, errors.New("GraphQL history #7: incomplete history comments count"))
	class, message, stored := HistoryFailureDetails(err)
	if class != "validation" || !strings.Contains(message, "incomplete history comments count") || string(stored) != string(evidence) {
		t.Fatalf("receipt %s %s", class, message)
	}
}

func TestMalformedGraphQLResponseHasPrivateBoundedReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"body":"private malformed response"`))
	}))
	defer server.Close()
	var out map[string]any
	err := New(Options{BaseURL: server.URL}).doGraphQL(context.Background(), "query { rateLimit { cost } }", nil, nil, &out)
	if err == nil {
		t.Fatal("malformed JSON accepted")
	}
	class, _, evidence := HistoryFailureDetails(err)
	if class != "response_decode" || !strings.Contains(string(evidence), "prefix_sha256") || strings.Contains(string(evidence), "private malformed response") {
		t.Fatalf("bad rejection receipt: %s %s", class, evidence)
	}
}

func TestReviewThreadMissingStateFailsClosed(t *testing.T) {
	node := map[string]any{"id": "T1", "__typename": "PullRequestReviewThread", "comments": historyTestConnection()}
	h := historySession{}
	if err := h.hydrate(context.Background(), node); err == nil {
		t.Fatal("unknown resolution became false")
	}
	for _, field := range []string{"isResolved", "isOutdated", "viewerCanResolve", "viewerCanUnresolve", "viewerCanReply"} {
		node[field] = false
	}
	node["isResolved"] = true
	if err := h.hydrate(context.Background(), node); err != nil {
		t.Fatal(err)
	}
}
