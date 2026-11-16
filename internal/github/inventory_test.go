package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func inventoryTestLink(page int, relation string) string {
	return fmt.Sprintf(`<https://api.github.com/repositories/123/issues?direction=asc&page=%d&per_page=100&sort=created&state=all>; rel="%s"`, page, relation)
}
func TestInventoryUsesPooledParallelPagesInDeterministicOrder(t *testing.T) {
	started := make(chan int, 2)
	release := make(chan struct{})
	var active, maxActive atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path  string     `json:"path"`
			Query url.Values `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		var body any
		headers := map[string]string{}
		if request.Path == "/repos/fixture/repo" {
			body = map[string]any{"id": 123}
		} else {
			if request.Path != "/repos/fixture/repo/issues" || request.Query.Get("sort") != "created" || request.Query.Get("direction") != "asc" || request.Query.Get("state") != "all" {
				t.Errorf("query escaped inventory shape: %+v", request)
			}
			page, _ := strconv.Atoi(request.Query.Get("page"))
			if page == 2 || page == 3 {
				count := active.Add(1)
				for old := maxActive.Load(); count > old && !maxActive.CompareAndSwap(old, count); old = maxActive.Load() {
				}
				started <- page
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				active.Add(-1)
			}
			items := []any{}
			if page <= 3 {
				for number := (page-1)*100 + 1; number <= page*100; number++ {
					items = append(items, map[string]any{"id": json.Number(fmt.Sprint(int64(9007199254740993) + int64(number))), "number": number, "node_id": fmt.Sprint("I_", number)})
				}
			}
			if page == 4 {
				items = append(items, map[string]any{"id": json.Number("9007199254741294"), "number": 301, "node_id": "I_301"})
			}
			body = items
			if page < 4 {
				headers["link"] = strings.Replace(inventoryTestLink(page+1, "next"), "state=all>", "state=all&after=opaque>", 1)
			}
			if request.Query.Get("after") != "" {
				t.Error("constructed an opaque cursor")
			}

		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "headers": headers, "body": body, "relay": map[string]any{"cache": "miss"}})
	}))
	defer server.Close()
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { err = client.InventoryWithWorkers(ctx, 2, nil); close(done) }()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			close(release)
			t.Fatal("inventory pages were not parallel")
		}
	}
	close(release)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if maxActive.Load() != 2 || len(client.IssueNumbers()) != 301 {
		t.Fatalf("workers=%d numbers=%v", maxActive.Load(), client.IssueNumbers())
	}
	if fmt.Sprint(client.issues[301]["id"]) != "9007199254741294" {
		t.Fatal("large canonical identity changed")
	}
}

func TestInventoryRejectsChangedPaginationContract(t *testing.T) {
	values := url.Values{"state": {"all"}, "sort": {"created"}, "direction": {"asc"}, "per_page": {"100"}}
	client := New(Options{})
	good := inventoryTestLink(5, "last")
	for _, link := range []string{
		strings.Replace(good, "api.github.com", "foreign.invalid", 1),
		strings.Replace(good, "/repositories/123/", "/repositories/124/", 1),
		strings.Replace(good, "sort=created", "sort=updated", 1),
		strings.Replace(good, "state=all", "state=all&unexpected=opaque", 1),
		strings.Replace(good, "page=5", "page=0", 1),
		good + ", " + good,
	} {
		if _, _, err := client.inventoryLinks(link, "/repos/fixture/repo/issues", "123", values); err == nil {
			t.Errorf("accepted malformed inventory link %s", link)
		}
	}
	if last, _, err := client.inventoryLinks(good, "/repos/fixture/repo/issues", "123", values); err != nil || last != 5 {
		t.Fatalf("rejected verified numeric repository: %d %v", last, err)
	}
}

func TestInventoryFailedPageNeverPublishesPartialRows(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path  string     `json:"path"`
			Query url.Values `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		body := any(map[string]any{"id": 123})
		headers := map[string]string{}
		status := 200
		if strings.HasSuffix(request.Path, "/issues") {
			if request.Query.Get("page") == "1" {
				body = []any{map[string]any{"id": 1, "number": 1}}
				headers["link"] = inventoryTestLink(2, "next") + ", " + inventoryTestLink(2, "last")
			} else {
				body = map[string]any{"message": "denied"}
				status = 401
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "body_encoding": "json", "body": body, "headers": headers, "relay": map[string]any{"cache": "miss"}})
	}))
	defer server.Close()
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.InventoryWithWorkers(context.Background(), 2, nil); err == nil {
		t.Fatal("accepted failed inventory page")
	}
	if len(client.issues) != 0 || client.metadata != nil {
		t.Fatal("published partial inventory")
	}
}

func TestInventoryDetectsIgnoredOffsetBeforePublishing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path  string     `json:"path"`
			Query url.Values `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		var body any
		headers := map[string]string{}
		if request.Path == "/repos/fixture/repo" {
			body = map[string]any{"id": 123}
		} else {
			items := []any{}
			for n := 1; n <= 100; n++ {
				items = append(items, map[string]any{"id": n, "number": n})
			}
			body = items
			page, _ := strconv.Atoi(request.Query.Get("page"))
			headers["link"] = inventoryTestLink(page+1, "next")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "headers": headers, "relay": map[string]any{"cache": "miss"}})
	}))
	defer server.Close()
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.InventoryWithWorkers(context.Background(), 1, nil); err == nil || !strings.Contains(err.Error(), "repeated issue") {
		t.Fatalf("ignored offset accepted: %v", err)
	}
	if len(client.issues) != 0 {
		t.Fatal("published incomplete inventory")
	}
}

func TestInventoryRejectsRecordsAfterShortTail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path  string     `json:"path"`
			Query url.Values `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		var body any
		headers := map[string]string{}
		if request.Path == "/repos/fixture/repo" {
			body = map[string]any{"id": 123}
		} else {
			page, _ := strconv.Atoi(request.Query.Get("page"))
			items := []any{}
			if page == 1 {
				for n := 1; n <= 100; n++ {
					items = append(items, map[string]any{"id": n, "number": n})
				}
				headers["link"] = inventoryTestLink(2, "next")
			} else {
				items = append(items, map[string]any{"id": page + 100, "number": page + 100})
			}
			body = items
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "headers": headers, "relay": map[string]any{"cache": "miss"}})
	}))
	defer server.Close()
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.InventoryWithWorkers(context.Background(), 2, nil); err == nil || !strings.Contains(err.Error(), "after a terminal") {
		t.Fatalf("inconsistent tail accepted: %v", err)
	}
	if len(client.issues) != 0 {
		t.Fatal("published contradictory inventory")
	}
}

func TestInventoryRejectsNullPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		var body any
		if request.Path == "/repos/fixture/repo" {
			body = map[string]any{"id": 123}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "relay": map[string]any{"cache": "miss"}})
	}))
	defer server.Close()
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.InventoryWithWorkers(context.Background(), 1, nil); err == nil || !strings.Contains(err.Error(), "not an array") {
		t.Fatalf("null inventory accepted: %v", err)
	}
}
