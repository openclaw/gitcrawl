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
	var preserved map[string]json.RawMessage
	if err := json.Unmarshal(stored, &preserved); err != nil {
		t.Fatal(err)
	}
	if preserved["cause"] == nil {
		t.Fatal("missing safe cause metadata")
	}
	delete(preserved, "cause")
	originalFields, _ := json.Marshal(preserved)
	if class != "validation" || !strings.Contains(message, "incomplete history comments count") || string(originalFields) != string(evidence) {
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

func TestGraphQLRejectedResponseKeepsOnlyBoundedSafeErrorMetadata(t *testing.T) {
	failures := []any{}
	for i := 0; i < 12; i++ {
		typ := "NOT_FOUND"
		if i == 1 {
			typ = "private-identity"
		}
		failures = append(failures, map[string]any{"type": typ, "message": "private provider prose", "path": []any{"repository", "n0", "comments", 0, "body", "private-identity", "IC_private_identity", 1000000000, "omitted"}})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"n0": nil, "n1": map[string]any{"body": "private peer prose"}}}, "errors": failures})
	}))
	defer server.Close()
	out := map[string]any{"unchanged": true}
	err := New(Options{BaseURL: server.URL}).doGraphQL(context.Background(), "query { repository { id } }", nil, nil, &out)
	if err == nil || len(out) != 1 || out["unchanged"] != true {
		t.Fatal("partial data accepted")
	}
	class, _, evidence := HistoryFailureDetails(err)
	if class != "partial_response" {
		t.Fatal(class)
	}
	for _, private := range []string{"private provider prose", "private peer prose", "private-identity", "IC_private_identity", "omitted"} {
		if strings.Contains(string(evidence), private) {
			t.Fatal("unsafe rejection metadata retained", private)
		}
	}
	var receipt struct {
		Errors struct {
			Count int `json:"count"`
			Items []struct {
				Type      *string `json:"type"`
				Path      []any   `json:"path"`
				Truncated bool    `json:"path_truncated"`
			} `json:"items"`
			Truncated bool `json:"truncated"`
		} `json:"graphql_errors"`
	}
	if err = json.Unmarshal(evidence, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Errors.Count != 12 || len(receipt.Errors.Items) != 8 || !receipt.Errors.Truncated {
		t.Fatal("error bound lost")
	}
	first := receipt.Errors.Items[0]
	if first.Type == nil || *first.Type != "NOT_FOUND" || len(first.Path) != 8 || !first.Truncated || first.Path[0] != "repository" || first.Path[1] != "n0" || first.Path[3] != float64(0) || first.Path[5] != nil || first.Path[6] != nil || first.Path[7] != nil || receipt.Errors.Items[1].Type != nil {
		t.Fatalf("incorrect safe metadata: %+v", receipt.Errors)
	}
}

func TestGraphQLRejectedNullOrAbsentDataRetainsSafeEnvelope(t *testing.T) {
	for _, payload := range []string{
		`{"errors":[{"type":"NOT_FOUND","path":["repository","n0"],"message":"private prose"}]}`,
		`{"data":null,"errors":[{"type":"NOT_FOUND","path":["repository","n0"],"message":"private prose"}]}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(payload)) }))
		var out map[string]any
		err := New(Options{BaseURL: server.URL}).doGraphQL(context.Background(), "query { repository { id } }", nil, nil, &out)
		server.Close()
		if err == nil {
			t.Fatal("error envelope accepted")
		}
		class, _, evidence := HistoryFailureDetails(err)
		var record map[string]any
		if e := json.Unmarshal(evidence, &record); e != nil {
			t.Fatal(e)
		}
		if class != "partial_response" || record["version"] != float64(1) || record["graphql_errors"] == nil || record["structure"] != nil || strings.Contains(string(evidence), "private prose") {
			t.Fatalf("missing/redaction-invalid envelope: %s", evidence)
		}
	}
}
