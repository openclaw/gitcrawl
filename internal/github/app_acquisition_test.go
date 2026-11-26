package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Keep production requests on their real HTTPS origin until this fixture-only
// transport forwards them to a real local HTTP server.
type appFixtureTransport struct {
	t      *testing.T
	origin *url.URL
}

func (f appFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != "api.github.com" {
		f.t.Fatal("credential request escaped the GitHub API origin")
	}
	copyRequest := r.Clone(r.Context())
	copyURL := *r.URL
	copyURL.Scheme, copyURL.Host = f.origin.Scheme, f.origin.Host
	copyRequest.URL = &copyURL
	response, err := http.DefaultTransport.RoundTrip(copyRequest)
	if response != nil {
		response.Request = r
	}
	return response, err
}

func appHTTPFixture(t *testing.T, handler http.HandlerFunc) http.RoundTripper {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	u, _ := url.Parse(s.URL)
	return appFixtureTransport{t, u}
}

func fixtureAppToken() AcquisitionToken {
	return AcquisitionToken{"synthetic-installation-token", time.Now().Add(time.Hour), 12, 34, 56, "fixture/repo", acquisitionPermissions()}
}

const appRepoJSON = `{"id":56,"full_name":"fixture/repo","private":false,"visibility":"public"}`

func TestMintAcquisitionTokenExactSignedReadScope(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	rawKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	requests := []string{}
	transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		if strings.HasPrefix(r.URL.Path, "/app/") {
			parts := strings.Split(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), ".")
			if len(parts) != 3 {
				t.Fatal("missing JWT")
			}
			digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
			signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
			if rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature) != nil {
				t.Error("invalid JWT signature")
			}
			claims, _ := base64.RawURLEncoding.DecodeString(parts[1])
			var c struct {
				Iss      string
				Iat, Exp int64
			}
			if json.Unmarshal(claims, &c) != nil || c.Iss != "12" || c.Iat > time.Now().Add(-50*time.Second).Unix() || c.Exp > time.Now().Add(10*time.Minute).Unix() || c.Exp < time.Now().Unix() {
				t.Error("invalid JWT claims")
			}
		} else if r.Header.Get("Authorization") != "" {
			t.Error("App JWT leaked to public metadata endpoint")
		}
		switch r.URL.Path {
		case "/app/installations/34":
			fmt.Fprint(w, `{"id":34,"app_id":12,"account":{"login":"fixture"},"suspended_at":null}`)
		case "/repos/fixture/repo":
			fmt.Fprint(w, appRepoJSON)
		case "/app/installations/34/access_tokens":
			var body struct {
				RepositoryIDs []int64           `json:"repository_ids"`
				Permissions   map[string]string `json:"permissions"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || !reflect.DeepEqual(body.RepositoryIDs, []int64{56}) || !reflect.DeepEqual(body.Permissions, map[string]string{"metadata": "read", "contents": "read", "issues": "read", "pull_requests": "read"}) {
				t.Error("mint was not narrowed to one repository and four read permissions")
			}
			w.WriteHeader(201)
			fmt.Fprintf(w, `{"token":"synthetic-installation-token","expires_at":%q,"repository_selection":"selected","repositories":[%s],"permissions":{"metadata":"read","contents":"read","issues":"read","pull_requests":"read"}}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339), appRepoJSON)
		default:
			t.Error("unexpected request", r.URL.Path)
		}
	})
	token, err := mintAcquisitionToken(context.Background(), AcquisitionApp{12, 34, 56, "fixture/repo"}, rawKey, &http.Client{Transport: transport})
	if err != nil || token.Token != "synthetic-installation-token" || token.RepositoryID != 56 {
		t.Fatalf("mint failed: %v", err)
	}
	if !reflect.DeepEqual(requests, []string{"GET /app/installations/34", "GET /repos/fixture/repo", "POST /app/installations/34/access_tokens"}) {
		t.Fatal("unexpected authentication effects", requests)
	}
}

func TestMintAcquisitionTokenRejectsChangedAuthorityWithoutRetry(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	rawKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	validMint := fmt.Sprintf(`{"token":"MUST_NOT_APPEAR","expires_at":%q,"repository_selection":"selected","repositories":[%s],"permissions":{"metadata":"read","contents":"read","issues":"read","pull_requests":"read"}}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339), appRepoJSON)
	for _, test := range []struct {
		name, installation, repo, mint string
		posts                          int
	}{
		{"suspended", `{"id":34,"app_id":12,"account":{"login":"fixture"},"suspended_at":"now"}`, appRepoJSON, "", 0},
		{"other installation", `{"id":35,"app_id":12,"account":{"login":"fixture"}}`, appRepoJSON, "", 0},
		{"other app", `{"id":34,"app_id":13,"account":{"login":"fixture"}}`, appRepoJSON, "", 0},
		{"other owner", `{"id":34,"app_id":12,"account":{"login":"other"}}`, appRepoJSON, "", 0},
		{"private", "", strings.Replace(appRepoJSON, `"private":false`, `"private":true`, 1), "", 0},
		{"missing public flag", "", `{"id":56,"full_name":"fixture/repo","visibility":"public"}`, "", 0},
		{"wrong immutable ID", "", strings.Replace(appRepoJSON, `"id":56`, `"id":57`, 1), "", 0},
		{"wide token", "", appRepoJSON, `{"repository_selection":"all","token":"MUST_NOT_APPEAR"}`, 1},
		{"write token", "", appRepoJSON, strings.Replace(validMint, `"issues":"read"`, `"issues":"write"`, 1), 1},
		{"extra permission", "", appRepoJSON, strings.Replace(validMint, `"metadata":"read"`, `"metadata":"read","checks":"read"`, 1), 1},
		{"different token repository", "", appRepoJSON, strings.Replace(validMint, `"id":56`, `"id":57`, 1), 1},
		{"provider rejection", "", appRepoJSON, "rejection", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			posts := 0
			transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/app/installations/34":
					value := test.installation
					if value == "" {
						value = `{"id":34,"app_id":12,"account":{"login":"fixture"},"suspended_at":null}`
					}
					fmt.Fprint(w, value)
				case "/repos/fixture/repo":
					fmt.Fprint(w, test.repo)
				default:
					posts++
					if test.mint == "rejection" {
						w.WriteHeader(403)
						fmt.Fprint(w, "MUST_NOT_APPEAR")
						return
					}
					w.WriteHeader(201)
					fmt.Fprint(w, test.mint)
				}
			})
			out, err := mintAcquisitionToken(context.Background(), AcquisitionApp{12, 34, 56, "fixture/repo"}, rawKey, &http.Client{Transport: transport})
			if err == nil || out.Token != "" || strings.Contains(err.Error(), "MUST_NOT_APPEAR") || posts != test.posts {
				t.Fatalf("refusal failed: err=%v posts=%d", err, posts)
			}
		})
	}
}

func TestInstallationEnvelopeRefusesExpiryScopeAndSecretDiagnostics(t *testing.T) {
	for _, change := range []func(*AcquisitionToken){
		func(c *AcquisitionToken) { c.ExpiresAt = time.Now().Add(10 * time.Minute) },
		func(c *AcquisitionToken) { c.ExpiresAt = time.Now().Add(2 * time.Hour) },
		func(c *AcquisitionToken) { c.Permissions["contents"] = "write" },
		func(c *AcquisitionToken) { c.Permissions["administration"] = "read" },
		func(c *AcquisitionToken) { c.Repository = "fixture/private-repo" },
		func(c *AcquisitionToken) { c.RepositoryID = 0 },
	} {
		c := fixtureAppToken()
		change(&c)
		raw, _ := json.Marshal(c)
		if _, err := DecodeAcquisitionToken(raw, "fixture/repo", time.Now().Add(15*time.Minute)); err == nil || strings.Contains(err.Error(), c.Token) {
			t.Fatal("invalid credential accepted or exposed")
		}
	}
	valid := fixtureAppToken()
	raw, _ := json.Marshal(valid)
	if _, err := DecodeAcquisitionToken(raw, valid.Repository, time.Now().Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func TestAppAcquisitionRefreshesExpiredCoreQuota(t *testing.T) {
	for _, test := range []struct {
		name      string
		stale     bool
		remaining int
	}{
		{name: "refilled", remaining: 4900},
		{name: "still stale", stale: true, remaining: 4900},
		{name: "below reserve", remaining: 2499},
	} {
		t.Run(test.name, func(t *testing.T) {
			token := fixtureAppToken()
			probes, reads := 0, 0
			transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+token.Token {
					t.Error("quota and content must use the same credential")
				}
				switch r.URL.Path {
				case "/rate_limit":
					probes++
					reset := time.Now().Add(time.Hour)
					if probes == 1 || test.stale {
						reset = time.Now().Add(-time.Minute)
					}
					writeRateLimits(t, w, reset, test.remaining, 4900)
				case "/installation/repositories":
					reads++
					fmt.Fprint(w, `{"total_count":1,"repositories":[`+appRepoJSON+`]}`)
				case "/repos/fixture/repo":
					reads++
					fmt.Fprint(w, appRepoJSON)
				default:
					t.Error("unexpected request", r.URL.Path)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_, _, err := newAppAcquisition(ctx, token, transport)
			if test.stale {
				var expired *rateLimitStatusExpiredError
				if !errors.As(err, &expired) || probes != 2 || reads != 0 {
					t.Fatalf("err=%v probes=%d reads=%d", err, probes, reads)
				}
			} else if test.remaining < 2500 {
				var reserve *RateLimitReserveError
				if !errors.As(err, &reserve) || probes != 2 || reads != 0 {
					t.Fatalf("err=%v probes=%d reads=%d", err, probes, reads)
				}
			} else if err != nil || probes != 3 || reads != 2 {
				t.Fatalf("err=%v probes=%d reads=%d", err, probes, reads)
			}
		})
	}
}

func TestAppAcquisitionRefreshesCoreQuotaAfterRepositoryRead(t *testing.T) {
	probes, repositoryReads := 0, 0
	transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rate_limit":
			probes++
			writeRateLimits(t, w, time.Now().Add(time.Hour), 4900, 4900)
		case "/installation/repositories":
			fmt.Fprint(w, `{"total_count":1,"repositories":[`+appRepoJSON+`]}`)
		case "/repos/fixture/repo":
			repositoryReads++
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Remaining", "4900")
			w.Header().Set("X-RateLimit-Reset", fmt.Sprint(time.Now().Add(-time.Minute).Unix()))
			w.Header().Set("X-RateLimit-Resource", "core")
			fmt.Fprint(w, appRepoJSON)
		default:
			t.Error("unexpected request", r.URL.Path)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, _, err := newAppAcquisition(ctx, fixtureAppToken(), transport)
	if err != nil || probes != 3 || repositoryReads != 1 {
		t.Fatalf("err=%v probes=%d repositoryReads=%d", err, probes, repositoryReads)
	}
}

func TestAppAcquisitionNativeRESTGraphQLAndQuotaBoundary(t *testing.T) {
	remaining, graphRemaining := 4900, 4900
	apiCalls, graphCalls := 0, 0
	oid := strings.Repeat("a", 40)
	transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-installation-token" {
			t.Error("wrong credential")
		}
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "4900")
		w.Header().Set("X-RateLimit-Reset", fmt.Sprint(time.Now().Add(time.Hour).Unix()))
		switch r.URL.Path {
		case "/rate_limit":
			writeRateLimits(t, w, time.Now().Add(time.Hour), remaining, graphRemaining)
		case "/installation/repositories":
			fmt.Fprint(w, `{"total_count":1,"repositories":[`+appRepoJSON+`]}`)
		case "/repos/fixture/repo":
			fmt.Fprint(w, appRepoJSON)
		case "/repos/fixture/repo/issues", "/repositories/56/issues":
			apiCalls++
			if r.URL.Query().Get("page") == "" {
				w.Header().Set("Link", `<https://api.github.com/repositories/56/issues?per_page=20&sort=updated&direction=desc&state=all&page=2>; rel="next"`)
			}
			fmt.Fprint(w, `[{"id":123,"number":7,"title":"full title","body":"full body"}]`)
		case "/repos/fixture/repo/issues/7/comments", "/repos/fixture/repo/pulls/7/reviews", "/repos/fixture/repo/pulls/7/comments":
			apiCalls++
			fmt.Fprint(w, `[{"id":456,"body":"full child body"}]`)
		case "/graphql":
			graphCalls++
			var request struct {
				Query     string
				Variables map[string]any
			}
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.Variables["owner"] != "fixture" {
				t.Error("query lost scope")
			}
			w.Header().Set("X-RateLimit-Resource", "graphql")
			if strings.Contains(request.Query, "defaultBranchRef") {
				fmt.Fprintf(w, `{"data":{"repository":{"id":"R1","nameWithOwner":"fixture/repo","defaultBranchRef":{"name":"main","target":{"oid":%q}}}}}`, oid)
			} else {
				fmt.Fprint(w, `{"data":{"repository":{"nameWithOwner":"fixture/repo","issueOrPullRequest":{"__typename":"PullRequest","id":"PR-node","number":7,"repository":{"nameWithOwner":"fixture/repo"},"timelineItems":{"nodes":[{"__typename":"ClosedEvent","id":"E1","createdAt":"2026-09-08T01:00:00Z","future":"kept"}],"pageInfo":{"hasNextPage":false}}}}}}`)
			}
		default:
			t.Error("unexpected provider path", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	rest, graph, err := newAppAcquisition(ctx, fixtureAppToken(), transport)
	if err != nil {
		t.Fatal(err)
	}
	q := RESTPageRequest{Owner: "fixture", Repo: "repo", Family: "issues", PerPage: 20}
	page, err := rest.ReadRESTPage(ctx, q, nil)
	if err != nil || len(page.Rows) != 1 || page.Rows[0]["body"] != "full body" || page.NextCursor == "" {
		t.Fatalf("REST page failed: %v", err)
	}
	q.Cursor = page.NextCursor
	if _, err = rest.ReadRESTPage(ctx, q, nil); err != nil {
		t.Fatal("numeric pagination rejected", err)
	}
	for _, family := range []string{"issue_comment", "pull_review", "pull_review_comment"} {
		p, e := rest.ReadRESTPage(ctx, RESTPageRequest{Owner: "fixture", Repo: "repo", Family: family, Number: 7, PerPage: 20}, nil)
		if e != nil || len(p.Rows) != 1 || p.Rows[0]["body"] != "full child body" {
			t.Fatalf("%s: %v", family, e)
		}
	}
	if h, e := graph.ReadCommitHead(ctx, nil); e != nil || h.OID != oid {
		t.Fatalf("commit head: %v", e)
	}
	if p, e := graph.ReadLifecycle(ctx, lifecycleQuery(), nil); e != nil || len(p.Events) != 1 || !strings.Contains(string(p.Events[0]), `"future":"kept"`) {
		t.Fatalf("lifecycle: %v", e)
	}
	before := apiCalls
	remaining = 2500 // fixture rate_limit declares a 5000-point budget.
	_, err = rest.ReadRESTPage(ctx, q, nil)
	var reserve *RateLimitReserveError
	if !errors.As(err, &reserve) || apiCalls != before {
		t.Fatalf("REST reserve dispatched data: %v", err)
	}
	remaining = 4900
	graphRemaining = 2550
	before = graphCalls
	_, err = graph.ReadCommitHead(ctx, nil)
	if !errors.As(err, &reserve) || graphCalls != before {
		t.Fatalf("GraphQL reserve dispatched data: %v", err)
	}
}

func TestAppReadTransportRejectsRedirectAndForeignScope(t *testing.T) {
	calls := 0
	base := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Location", "https://outside.example/secret")
		w.WriteHeader(302)
	})
	transport := &appReadTransport{base: base, repositoryPath: "/repos/fixture/repo", repositoryID: "56"}
	client := &http.Client{Transport: transport, CheckRedirect: appNoRedirect}
	for _, target := range []string{"https://outside.example/repos/fixture/repo", "https://api.github.com/repos/fixture/other", "https://api.github.com/repositories/57/issues"} {
		if _, err := client.Get(target); err == nil {
			t.Fatal("foreign request accepted")
		}
	}
	response, err := client.Get("https://api.github.com/repos/fixture/repo")
	if err != nil || response.StatusCode != 302 || calls != 1 {
		t.Fatal("redirect followed", err, calls)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
}

func TestAppAcquisitionRejectsScopeAndUnknownQuotaBeforeCollection(t *testing.T) {
	for _, name := range []string{"extra repository", "private repository", "unknown quota", "missing reset", "revoked token"} {
		t.Run(name, func(t *testing.T) {
			transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/rate_limit":
					if name == "unknown quota" {
						fmt.Fprint(w, `{"resources":{}}`)
						return
					}
					if name == "missing reset" {
						fmt.Fprint(w, `{"resources":{"core":{"limit":5000,"remaining":4900}}}`)
						return
					}
					writeRateLimits(t, w, time.Now().Add(time.Hour), 4900, 4900)
				case "/installation/repositories":
					if name == "revoked token" {
						w.WriteHeader(401)
						fmt.Fprint(w, "synthetic-installation-token")
						return
					}
					if name == "extra repository" {
						fmt.Fprint(w, `{"total_count":2,"repositories":[`+appRepoJSON+`]}`)
						return
					}
					fmt.Fprint(w, `{"total_count":1,"repositories":[`+strings.Replace(appRepoJSON, `"private":false`, `"private":true`, 1)+`]}`)
				default:
					t.Error("rejected admission fetched provider data", r.URL.Path)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			_, _, err := newAppAcquisition(ctx, fixtureAppToken(), transport)
			if err == nil || strings.Contains(err.Error(), "synthetic-installation-token") {
				t.Fatal("scope/quota refusal failed", err)
			}
		})
	}
}

func TestAppGraphQLRejectionsKeepTypesWithoutProviderProse(t *testing.T) {
	for _, kind := range []string{"FORBIDDEN", "RATE_LIMITED", "oversized", "http"} {
		t.Run(kind, func(t *testing.T) {
			transport := appHTTPFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/rate_limit":
					writeRateLimits(t, w, time.Now().Add(time.Hour), 4900, 4900)
				case "/installation/repositories":
					fmt.Fprint(w, `{"total_count":1,"repositories":[`+appRepoJSON+`]}`)
				case "/repos/fixture/repo":
					fmt.Fprint(w, appRepoJSON)
				case "/graphql":
					switch kind {
					case "oversized":
						fmt.Fprintf(w, `{"data":{"padding":%q}}`, strings.Repeat("x", (3<<20)+1))
					case "http":
						w.WriteHeader(401)
						fmt.Fprint(w, "synthetic-secret-should-not-leak")
					default:
						fmt.Fprintf(w, `{"errors":[{"message":"synthetic-secret-should-not-leak","type":%q}]}`, kind)
					}
				default:
					t.Error("unexpected request", r.URL.Path)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			_, graph, err := newAppAcquisition(ctx, fixtureAppToken(), transport)
			if err != nil {
				t.Fatal(err)
			}
			head, err := graph.ReadCommitHead(ctx, nil)
			if err == nil || head.OID != "" || strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("provider rejection leaked or returned success", err)
			}
			if IsRateLimited(err) != (kind == "RATE_LIMITED") {
				t.Fatal("rate classification changed", err)
			}
			if kind == "oversized" {
				var failure *HistoryFailure
				if !errors.As(err, &failure) || failure.Stage != "response_size" {
					t.Fatal("size classification lost", err)
				}
			}
			if kind == "http" {
				var response *RequestError
				if !errors.As(err, &response) || response.Status != 401 {
					t.Fatal("HTTP classification lost", err)
				}
			}
		})
	}
}
