package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func cursorFixture(number int) map[string]any {
	return map[string]any{"id": number + 100, "node_id": fmt.Sprint("I_", number), "number": number, "updated_at": "2026-01-01T00:00:00Z"}
}
func TestInventoryFollowsOnlyAuthenticCursorsPastPage100(t *testing.T) {
	var requests []url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path  string     `json:"path"`
			Query url.Values `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		body := any(map[string]any{"id": 123})
		headers := map[string]string{}
		if request.Path == "/repos/fixture/repo/issues" {
			requests = append(requests, request.Query)
			if request.Query.Get("after") == "" {
				if request.Query.Get("page") != "" {
					t.Error("constructed offset page")
				}
				body = []any{cursorFixture(1)}
				headers["link"] = `<https://api.github.com/repositories/123/issues?state=all&sort=created&direction=asc&per_page=100&page=100&after=provider-cursor>; rel="next"`
			} else {
				if request.Query.Get("after") != "provider-cursor" || request.Query.Get("page") != "100" {
					t.Error("changed provider cursor")
				}
				body = []any{cursorFixture(2)}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "headers": headers, "relay": map[string]any{"cache": "miss"}})
	}))
	defer server.Close()
	client, err := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.InventoryWithWorkers(context.Background(), 4, nil); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || !reflect.DeepEqual(client.IssueNumbers(), []int{1, 2}) {
		t.Fatalf("incomplete cursor inventory: %v", requests)
	}
}
func TestCheckpointResumesSavedPageAndRejectsChangedScope(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inventory.sqlite")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cp, err := openInventoryCheckpoint(ctx, path, "scope", "first", start)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal([]any{cursorFixture(1)})
	if err = cp.savePage(ctx, 1, "first", "cursor", raw); err != nil {
		t.Fatal(err)
	}
	if err = cp.complete(ctx, 1, []int{1}); err != nil {
		t.Fatal(err)
	}
	cp.Close()
	cp, err = openInventoryCheckpoint(ctx, path, "scope", "unused", start.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	next, eof, pages, err := cp.state(ctx)
	if err != nil || next != "cursor" || eof || pages != 1 || !cp.started.Equal(start) {
		t.Fatalf("resume lost state: %q %t %d %v", next, eof, pages, err)
	}
	got, done, err := cp.page(ctx, 1)
	if err != nil || string(got) != string(raw) || !reflect.DeepEqual(done, []int{1}) {
		t.Fatalf("resume lost full page: %v %v", done, err)
	}
	if _, err = openInventoryCheckpoint(ctx, path, "different-caller-or-archive", "first", start); err == nil {
		t.Fatal("accepted changed resume scope")
	}
}
func TestCheckpointPageAndCursorCommitAtomically(t *testing.T) {
	ctx := context.Background()
	cp, err := openInventoryCheckpoint(ctx, filepath.Join(t.TempDir(), "inventory.sqlite"), "scope", "first", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	if _, err = cp.base.DB().ExecContext(ctx, `create trigger reject_cursor before update on inventory_state begin select raise(abort,'fixture cursor failure'); end`); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal([]any{cursorFixture(1)})
	if err = cp.savePage(ctx, 1, "first", "next", raw); err == nil {
		t.Fatal("expected cursor write failure")
	}
	next, eof, pages, err := cp.state(ctx)
	if err != nil || next != "first" || eof || pages != 0 {
		t.Fatalf("partial page/cursor committed: %s %t %d %v", next, eof, pages, err)
	}
	if _, err = cp.base.DB().ExecContext(ctx, "drop trigger reject_cursor"); err != nil {
		t.Fatal(err)
	}
	if err = cp.savePage(ctx, 1, "first", "", raw); err != nil {
		t.Fatal(err)
	}
	if _, err = cp.base.DB().ExecContext(ctx, "update inventory_pages set raw='corrupt' where page=1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = cp.page(ctx, 1); err == nil {
		t.Fatal("accepted corrupt checkpoint page")
	}
}
func TestInventoryCursorRejectsChangedScope(t *testing.T) {
	client := New(Options{})
	query := url.Values{"state": {"all"}, "sort": {"created"}, "direction": {"asc"}, "per_page": {"100"}}
	good := `<https://api.github.com/repositories/123/issues?state=all&sort=created&direction=asc&per_page=100&page=100&after=opaque>; rel="next"`
	for _, link := range []string{strings.Replace(good, "api.github.com", "foreign.invalid", 1), strings.Replace(good, "/123/", "/124/", 1), strings.Replace(good, "sort=created", "sort=updated", 1), strings.Replace(good, "after=opaque", "unexpected=opaque", 1), good + ", " + good} {
		if _, err := client.inventoryNext(link, "/repos/fixture/repo/issues", "123", query); err == nil {
			t.Errorf("accepted changed cursor: %s", link)
		}
	}
	if _, err := client.inventoryNext(good, "/repos/fixture/repo/issues", "123", query); err != nil {
		t.Fatal(err)
	}
}
func TestCheckpointRetainsPageWhenConsumerStops(t *testing.T) {
	var pageCalls int
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
			pageCalls++
			body = []any{cursorFixture(pageCalls)}
			if pageCalls == 1 {
				headers["link"] = `<https://api.github.com/repositories/123/issues?state=all&sort=created&direction=asc&per_page=100&page=100&after=opaque>; rel="next"`
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "body_encoding": "json", "body": body, "headers": headers, "relay": map[string]any{"cache": "miss"}})
	}))
	defer server.Close()
	ctx := context.Background()
	scope := InventoryScope{Checkpoint: filepath.Join(t.TempDir(), "inventory.sqlite"), NativeIdentity: "fixture-inode", SelectionSHA256: "fixture-input", StartedAt: time.Now()}
	client, _ := NewOctopoolConversations(server.URL, "fixture", "fixture-token", "fixture/repo")
	stream, err := client.OpenInventory(ctx, scope, nil)
	if err != nil {
		t.Fatal(err)
	}
	stopped := errors.New("stop after first saved page")
	if err = stream.Walk(ctx, nil, func(InventoryPage) error { return stopped }); !errors.Is(err, stopped) {
		t.Fatal(err)
	}
	stream.Close()
	stream, err = client.OpenInventory(ctx, scope, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var pages []int
	if err = stream.Walk(ctx, nil, func(page InventoryPage) error { pages = append(pages, page.Index); return nil }); err != nil {
		t.Fatal(err)
	}
	if pageCalls != 2 || !reflect.DeepEqual(pages, []int{1, 2}) {
		t.Fatalf("re-fetched saved page: calls=%d pages=%v", pageCalls, pages)
	}
	other, _ := NewOctopoolConversations(server.URL, "fixture", "different-token", "fixture/repo")
	if changed, err := other.OpenInventory(ctx, scope, nil); err == nil {
		changed.Close()
		t.Fatal("accepted different caller scope")
	}
}
