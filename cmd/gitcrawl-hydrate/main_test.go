package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

func TestSelectNumbersRequiresExplicitDiscovery(t *testing.T) {
	for _, test := range []struct {
		include    bool
		selected   []int
		discovered int
	}{{false, []int{3}, 0}, {true, []int{1, 3, 5}, 2}} {
		selected, missing, discovered := selectNumbers([]int{8, 3}, []int{1, 3, 5}, test.include)
		if !reflect.DeepEqual(selected, test.selected) || !reflect.DeepEqual(missing, []int{8}) || discovered != test.discovered {
			t.Fatalf("selection = %v, missing=%v, discovered=%d", selected, missing, discovered)
		}
	}
}

func TestPrepareWindowFetchesInParallelAndPreservesBatchOrder(t *testing.T) {
	started := make(chan int, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path    string `json:"path"`
			GraphQL struct {
				Query string `json:"query"`
			} `json:"graphql"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		var body any
		switch request.Path {
		case "/repos/fixture/repo":
			body = map[string]any{"id": 123}
		case "/repos/fixture/repo/issues/7", "/repos/fixture/repo/issues/8":
			number := 7
			if strings.HasSuffix(request.Path, "/8") {
				number = 8
			}
			body = map[string]any{"id": number, "node_id": fmt.Sprint("I_", number), "number": number, "updated_at": "2026-01-01T00:00:00Z"}
		case "/graphql":
			number := 7
			if strings.Contains(request.GraphQL.Query, "number:8") {
				number = 8
			}
			started <- number
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			body = map[string]any{"data": map[string]any{"repository": map[string]any{"n0": map[string]any{"id": fmt.Sprint("I_", number), "__typename": "Issue", "number": number, "updatedAt": "2026-01-01T00:00:00Z", "repository": map[string]any{"nameWithOwner": "fixture/repo"}, "comments": map[string]any{"totalCount": 0, "nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false}}}}}}
		default:
			t.Errorf("unexpected path %s", request.Path)
			w.WriteHeader(400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "relay": map[string]any{"route_kind": "graphql_read", "cache": "miss"}, "identity": map[string]any{"kind": "github_app"}})
	}))
	defer server.Close()
	client, err := github.NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = client.InventoryNumbers(ctx, []int{7, 8}, nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var batches []*github.OctopoolConversations
	go func() { batches, err = prepareWindow(ctx, client, [][]int{{8}, {7}}, nil); close(done) }()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			close(release)
			t.Fatal("batch acquisition was not parallel")
		}
	}
	close(release)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	for index, number := range []int{8, 7} {
		if _, err = batches[index].GetIssue(ctx, "fixture", "repo", number, nil); err != nil {
			t.Fatal("batch order changed", err)
		}
	}
	if _, err = client.GetIssue(ctx, "fixture", "repo", 7, nil); err == nil {
		t.Fatal("workers changed shared child cache")
	}
}

func TestBatchReceiptCarriesExactSettledNumbers(t *testing.T) {
	var output bytes.Buffer
	stats := syncer.Stats{Numbers: []int{7, 19, 25}, ThreadsSynced: 3}
	if err := writeBatchComplete(json.NewEncoder(&output), stats); err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Event   string       `json:"event"`
		Numbers []int        `json:"numbers"`
		Stats   syncer.Stats `json:"stats"`
	}
	if err := json.Unmarshal(output.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Event != "batch_complete" || !reflect.DeepEqual(receipt.Numbers, stats.Numbers) || receipt.Stats.ThreadsSynced != 3 {
		t.Fatalf("unusable completion receipt: %+v", receipt)
	}
	output.Reset()
	stats.OwnerExcluded = 1
	if err := writeBatchComplete(json.NewEncoder(&output), stats); err == nil || output.Len() != 0 {
		t.Fatal("published completion after a concurrent policy exclusion")
	}
}

func TestSelectedRosterIsPrivateExclusiveAndDigestBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "selected.txt")
	digest, err := writeSelectedNumbers(path, []int{3, 7, 19})
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "3\n7\n19\n" || digest != fmt.Sprintf("%x", sha256.Sum256(content)) {
		t.Fatal("roster digest or numbers mismatch")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("roster mode %o", info.Mode().Perm())
	}
	if _, err = writeSelectedNumbers(path, []int{99}); err == nil {
		t.Fatal("overwrote original frozen roster")
	}
	content, _ = os.ReadFile(path)
	if string(content) != "3\n7\n19\n" {
		t.Fatal("original roster changed")
	}
}

func TestCanceledSiblingDoesNotMaskAdmissionFailure(t *testing.T) {
	for _, path := range []string{"numbers", "rows"} {
		t.Run(path, func(t *testing.T) {
			firstStarted := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Path    string `json:"path"`
					GraphQL struct {
						Query string `json:"query"`
					} `json:"graphql"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				var body any
				switch request.Path {
				case "/repos/fixture/repo":
					body = map[string]any{"id": 123}
				case "/repos/fixture/repo/issues/7", "/repos/fixture/repo/issues/8":
					n := 7
					if strings.HasSuffix(request.Path, "/8") {
						n = 8
					}
					body = map[string]any{"id": n, "node_id": fmt.Sprint("I_", n), "number": n, "updated_at": "2026-01-01T00:00:00Z"}
				case "/graphql":
					if strings.Contains(request.GraphQL.Query, "number:7") {
						close(firstStarted)
						<-r.Context().Done()
						return
					}
					<-firstStarted
					w.WriteHeader(http.StatusForbidden)
					return
				default:
					t.Errorf("unexpected path %s", request.Path)
					w.WriteHeader(400)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "relay": map[string]any{"cache": "miss"}})
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := github.NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
			if err != nil {
				t.Fatal(err)
			}
			if err = client.InventoryNumbers(ctx, []int{7, 8}, nil); err != nil {
				t.Fatal(err)
			}
			if path == "numbers" {
				_, err = prepareWindow(ctx, client, [][]int{{7}, {8}}, nil)
			} else {
				batches := [][]map[string]any{{{"id": 7, "node_id": "I_7", "number": 7, "updated_at": "2026-01-01T00:00:00Z"}}, {{"id": 8, "node_id": "I_8", "number": 8, "updated_at": "2026-01-01T00:00:00Z"}}}
				_, err = prepareRowWindow(ctx, client, batches, nil)
			}
			if err == nil || !strings.Contains(err.Error(), "relay status 403") {
				t.Fatalf("admission error masked by canceled sibling: %v", err)
			}
		})
	}
}
