package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMintAcquisitionCohortPublicCatalogAndSingleRepositoryTokens(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	public := false
	first := appRepository{56, "fixture/repo", &public, "public"}
	last := appRepository{157, "fixture/second", &public, "public"}
	firstPage := []appRepository{first}
	for id := int64(200); len(firstPage) < 100; id++ {
		firstPage = append(firstPage, appRepository{id, fmt.Sprintf("fixture/other-%d", id), &public, "public"})
	}
	for _, tc := range []struct {
		name      string
		second    appRepository
		badMint   bool
		wantPost  []int64
		linkPage  string
		remaining string
		firstOnly bool
	}{
		{name: "complete", second: last, wantPost: []int64{56, 157}},
		{name: "wrong ID", second: appRepository{158, last.FullName, &public, "public"}},
		{name: "private", second: appRepository{last.ID, last.FullName, new(true), "private"}},
		{name: "mint response drift", second: last, badMint: true, wantPost: []int64{56, 157}},
		{name: "repeated page", linkPage: "1", firstOnly: true},
		{name: "quota reserve", remaining: "30", firstOnly: true},
		{name: "full terminal page with missing ID", linkPage: "none", firstOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts []int64
			var pages []string
			var installationReads int
			transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/app/installations/34":
					installationReads++
					fmt.Fprint(w, `{"id":34,"app_id":12,"account":{"login":"fixture"},"suspended_at":null}`)
				case "/orgs/fixture/repos":
					q := r.URL.Query()
					if r.Header.Get("Authorization") != "" || q.Get("type") != "public" || q.Get("per_page") != "100" {
						t.Error("catalog request was not anonymous public metadata")
					}
					page := q.Get("page")
					pages = append(pages, page)
					if page == "1" {
						linkPage, remaining := tc.linkPage, tc.remaining
						if linkPage == "" {
							linkPage = "2"
						}
						if remaining == "" {
							remaining = "59"
						}
						if linkPage != "none" {
							w.Header().Set("Link", fmt.Sprintf(`<https://api.github.com/organizations/42/repos?page=%s>; rel="next"`, linkPage))
						}
						w.Header().Set("X-RateLimit-Limit", "60")
						w.Header().Set("X-RateLimit-Remaining", remaining)
						_ = json.NewEncoder(w).Encode(firstPage)
					} else {
						_ = json.NewEncoder(w).Encode([]appRepository{tc.second})
					}
				case "/app/installations/34/access_tokens":
					var body struct {
						IDs         []int64           `json:"repository_ids"`
						Permissions map[string]string `json:"permissions"`
					}
					if r.Method != http.MethodPost || r.Header.Get("Authorization") == "" || json.NewDecoder(r.Body).Decode(&body) != nil || len(body.IDs) != 1 || !readPermissions(body.Permissions) {
						t.Error("mint was not scoped to one repo and exact read permissions")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					posts = append(posts, body.IDs[0])
					minted := first
					if body.IDs[0] == last.ID && !tc.badMint {
						minted = last
					}
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(map[string]any{"token": "synthetic-credential", "expires_at": time.Now().Add(time.Hour), "repository_selection": "selected", "repositories": []appRepository{minted}, "permissions": acquisitionPermissions()})
				default:
					t.Error("unexpected request", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			cohort := []AcquisitionRepository{{first.ID, first.FullName}, {last.ID, last.FullName}}
			out, err := mintAcquisitionCohort(context.Background(), 12, 34, cohort, pemKey, &http.Client{Transport: transport})
			wantPages := []string{"1", "2"}
			if tc.firstOnly {
				wantPages = []string{"1"}
			}
			if !reflect.DeepEqual(posts, tc.wantPost) || !reflect.DeepEqual(pages, wantPages) || installationReads != 1 {
				t.Fatalf("unexpected effects: posts %v, pages %v, installation reads %d", posts, pages, installationReads)
			}
			if tc.name != "complete" {
				if err == nil || out != nil || strings.Contains(err.Error(), "synthetic-credential") {
					t.Fatalf("failure exposed a credential: %v", err)
				}
				return
			}
			if err != nil || len(out) != 2 || out[0].RepositoryID != first.ID || out[1].RepositoryID != last.ID {
				t.Fatalf("mint did not return both credentials in order: %v", err)
			}
		})
	}
}

func TestMintAcquisitionCohortRejectsConflictingRosterBeforeRequests(t *testing.T) {
	for _, cohort := range [][]AcquisitionRepository{
		{{1, "fixture/repo"}, {2, "other/repo"}},
		{{1, "fixture/repo"}, {1, "fixture/other"}},
		{{1, "fixture/repo"}, {2, "Fixture/REPO"}},
		{{1, "fixture/private-repo"}},
	} {
		out, err := mintAcquisitionCohort(context.Background(), 12, 34, cohort, nil, nil)
		if err == nil || out != nil || !strings.Contains(err.Error(), "unique public") {
			t.Fatalf("roster validation did not precede key handling: %v", err)
		}
	}
}
