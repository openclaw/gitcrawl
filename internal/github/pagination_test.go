package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPaginationRejectsCycles(t *testing.T) {
	for _, envelope := range []bool{false, true} {
		for _, cycleToFirst := range []bool{false, true} {
			name := "array"
			if envelope {
				name = "envelope"
			}
			if cycleToFirst {
				name += "/return-to-first-page"
			} else {
				name += "/repeat-next-page"
			}
			t.Run(name, func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					call := calls.Add(1)
					if call > 2 {
						http.Error(w, "unexpected extra request", http.StatusInternalServerError)
						return
					}
					query := "?page=2"
					if cycleToFirst && call == 2 {
						query = "?per_page=100"
					}
					w.Header().Set("Link", `<`+serverURL(r)+query+`>; rel="next"`)
					var payload any = []map[string]any{{"id": call}}
					if envelope {
						payload = map[string]any{"check_runs": payload}
					}
					_ = json.NewEncoder(w).Encode(payload)
				}))
				defer server.Close()
				client := New(Options{BaseURL: server.URL, PageDelay: -1})
				var rows []map[string]any
				var err error
				if envelope {
					rows, err = client.ListCommitCheckRuns(context.Background(), "example", "archive", "abc", nil)
				} else {
					rows, err = client.ListIssueComments(context.Background(), "example", "archive", 1, nil)
				}
				if err == nil || !strings.Contains(err.Error(), "repeated next link") {
					t.Fatalf("pagination error = %v, want repeated next link", err)
				}
				if rows != nil || calls.Load() != 2 {
					t.Fatalf("rows = %v, requests = %d; want no partial result and two requests", rows, calls.Load())
				}
			})
		}
	}
}
