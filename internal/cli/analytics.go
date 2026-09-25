package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func (a *App) analyticsClient(ctx context.Context, cfg config.Config) (*gh.Client, error) {
	token := a.resolveGitHubToken(ctx, cfg)
	var provider func(context.Context) (string, error)
	var e error
	if a.githubTokenCommand != nil {
		provider, e = githubTokenProvider(*a.githubTokenCommand)
		if e != nil {
			return nil, e
		}
	}
	if provider != nil {
		fetch := provider
		var mu sync.Mutex
		var cached string
		var expires time.Time
		provider = func(ctx context.Context) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			if cached != "" && time.Now().Before(expires) {
				return cached, nil
			}
			value, e := fetch(ctx)
			if e != nil {
				return "", e
			}
			cached = value
			expires = time.Now().Add(45 * time.Minute)
			return value, nil
		}
		a.analyticsTokenProvider = provider
	}
	if provider == nil && token.Value == "" {
		return nil, fmt.Errorf("missing GitHub credential")
	}
	return gh.New(gh.Options{Token: token.Value, TokenProvider: provider, BaseURL: githubBaseURL(), RateLimit: a.observeGitHubRateLimit(ctx), RateLimitReserve: 1500}), nil
}
func (a *App) runAnalytics(ctx context.Context, args []string) error {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			_, err := fmt.Fprintln(a.Stdout, "Usage: gitcrawl [--config SOURCE_CONFIG] [--github-token-command TOKEN_HELPER] analytics owner/repo [--apply] [--enrich] [--watch|--once] [--json]\nWithout action flags: read-only publication audit. --apply repairs retained timestamps; --enrich collects actor evidence; --watch maintains GraphQL updates.")
			return err
		}
	}

	fs := flag.NewFlagSet("analytics", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	apply := fs.Bool("apply", false, "apply source publication corrections")
	enrich := fs.Bool("enrich", false, "collect missing provider identities and actor profiles")
	watch := fs.Bool("watch", false, "maintain GraphQL updates every two minutes")
	once := fs.Bool("once", false, "run one GraphQL update cycle")
	fs.Bool("json", false, "JSON output")
	if e := fs.Parse(normalizeCommandArgs(args, nil)); e != nil {
		return e
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("analytics requires owner/repo")
	}
	owner, repo, e := parseOwnerRepo(fs.Arg(0))
	if e != nil {
		return e
	}
	a.format = FormatJSON
	cfg, e := config.LoadRuntime(a.configPath)
	if e != nil {
		return e
	}
	if !*apply && !*enrich && !*watch && !*once {
		rt, e := a.openLocalRuntimeReadOnly(ctx)
		if e != nil {
			return e
		}
		defer rt.Store.Close()
		r, e := rt.Store.RepairPublication(ctx, false)
		if e != nil {
			return e
		}
		return a.writeOutput("analytics_repair_dry_run", r, false)
	}
	lock, e := os.OpenFile(filepath.Join(filepath.Dir(cfg.DBPath), "runner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = lockPortableFile(lock); e != nil {
		return fmt.Errorf("collector ownership lock busy: %w", e)
	}
	rt, e := a.openLocalRuntime(ctx)
	if e != nil {
		return e
	}
	defer rt.Store.Close()
	if *apply {
		r, e := rt.Store.RepairPublication(ctx, true)
		if e != nil {
			return e
		}
		if e = a.writeOutput("analytics_repair", r, false); e != nil {
			return e
		}
	}
	if !*enrich && !*watch && !*once {
		return nil
	}
	client, e := a.analyticsClient(ctx, cfg)
	if e != nil {
		return e
	}
	if *watch || *once {
		if e = rt.Store.SeedAnalyticsNodes(ctx, true); e != nil {
			return e
		}
		var existing int
		if e = rt.Store.DB().QueryRowContext(ctx, "SELECT count(*) FROM analytics_coverage WHERE repository=?", owner+"/"+repo).Scan(&existing); e != nil {
			return e
		}
		if existing == 0 {
			b, err := os.ReadFile(filepath.Join(filepath.Dir(a.configPath), "status.json"))
			if err == nil {
				var status struct {
					Phase     string `json:"phase"`
					Discovery []struct {
						Kind    string `json:"kind"`
						Done    int    `json:"done"`
						Total   int    `json:"total"`
						Updated string `json:"updated_at"`
					} `json:"discovery"`
				}
				if json.Unmarshal(b, &status) == nil && status.Phase == "complete" && len(status.Discovery) == 2 {
					through := ""
					issues, prs := 0, 0
					valid := true
					for _, d := range status.Discovery {
						valid = valid && d.Done == 1
						if through == "" || d.Updated < through {
							through = d.Updated
						}
						if d.Kind == "issues" {
							issues = d.Total
						} else if d.Kind == "pullRequests" {
							prs = d.Total
						} else {
							valid = false
						}
					}
					if valid && issues > 0 && prs > 0 {
						if e = rt.Store.SaveAnalyticsCoverage(ctx, owner+"/"+repo, through, issues, prs); e != nil {
							return e
						}
					}
				}
			}
		}
	}
	// Independent guarded clients permit disjoint evidence batches to overlap;
	// each preserves the normal quota reservation and uses the same cached token.
	actorClients := make([]*gh.Client, 8)
	for i := range actorClients {
		token := a.resolveGitHubToken(ctx, cfg)
		actorClients[i] = gh.New(gh.Options{Token: token.Value, TokenProvider: a.analyticsTokenProvider, BaseURL: githubBaseURL(), RateLimit: a.observeGitHubRateLimit(ctx), RateLimitReserve: 1500})
	}
	parallelWatch := *watch && *enrich
	if parallelWatch {
		pollCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		defer func() { cancel(); <-done }()
		go func() {
			defer close(done)
			for {
				pollErr := a.analyticsCycle(pollCtx, rt.Store, client, owner, repo)
				if pollErr != nil {
					fmt.Fprintf(a.Stderr, "{\"event\":\"github_update_failed\",\"error\":%q}\n", pollErr.Error())
				} else {
					fmt.Fprintln(a.Stderr, "{\"event\":\"github_update_complete\"}")
				}
				select {
				case <-pollCtx.Done():
					return
				case <-time.After(2 * time.Minute):
				}
			}
		}()
	}
	if *enrich {
		for _, profiles := range []bool{true, false} {
			if e = rt.Store.SeedAnalyticsNodes(ctx, profiles); e != nil {
				return e
			}
		}
		e = maintainAnalyticsEnrichment(ctx, parallelWatch, 2*time.Minute, func() error {
			for _, profiles := range []bool{true, false, true} {
				for {
					var ids []string
					if profiles {
						ids, e = rt.Store.AnalyticsProfileNodes(ctx, 800)
					} else {
						ids, e = rt.Store.AnalyticsIdentityNodes(ctx, 800)
					}
					if e != nil {
						return e
					}
					if len(ids) == 0 {
						break
					}
					var group sync.WaitGroup
					var failures []error
					var failureMu sync.Mutex
					for offset := 0; offset < len(ids); offset += 100 {
						part := append([]string(nil), ids[offset:min(offset+100, len(ids))]...)
						worker := actorClients[offset/100]
						group.Add(1)
						go func() {
							defer group.Done()
							nodes, err := worker.AnalyticsNodes(ctx, part, profiles)
							if err == nil {
								at := time.Now().UTC().Format(time.RFC3339Nano)
								if profiles {
									err = rt.Store.SaveActorProfiles(ctx, nodes, at)
								} else {
									err = rt.Store.SaveActorEvidence(ctx, nodes, at)
								}
							}
							if err != nil {
								failureMu.Lock()
								failures = append(failures, err)
								failureMu.Unlock()
							}
						}()
					}
					group.Wait()
					if len(failures) > 0 {
						fmt.Fprintf(a.Stderr, "{\"event\":\"actor_enrichment_retry\",\"failed_batches\":%d}\n", len(failures))
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(30 * time.Second):
						}
						continue
					}

					fmt.Fprintf(a.Stderr, "{\"event\":\"actor_enrichment\",\"profiles\":%t,\"nodes\":%d}\n", profiles, len(ids))

				}
			}
			return nil
		})
		if e != nil {
			return e
		}
	}

	if parallelWatch {
		<-ctx.Done()
		return ctx.Err()
	}
	for *watch || *once {
		e = a.analyticsCycle(ctx, rt.Store, client, owner, repo)
		if e != nil {
			if !*watch {
				return e
			}
			fmt.Fprintf(a.Stderr, "{\"event\":\"github_update_failed\",\"error\":%q}\n", e.Error())
		} else {
			fmt.Fprintln(a.Stderr, "{\"event\":\"github_update_complete\"}")
		}
		if !*watch {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Minute):
		}
	}
	return nil
}

type updateCheckpoint struct {
	Started string `json:"started"`
	Since   string `json:"since"`
	Kind    int    `json:"kind"`
	Cursor  string `json:"cursor"`
	Issues  int    `json:"issues"`
	PRs     int    `json:"prs"`
}

func (a *App) analyticsCycle(ctx context.Context, s *store.Store, c *gh.Client, owner, repo string) error {
	key := "updates:" + owner + "/" + repo
	value, e := s.AnalyticsState(ctx, key)
	if e != nil {
		return e
	}
	var cp updateCheckpoint
	if value != "" {
		if e = json.Unmarshal([]byte(value), &cp); e != nil {
			return e
		}
	}
	if cp.Started == "" {
		through, e := s.AnalyticsState(ctx, "through:"+owner+"/"+repo)
		if e != nil {
			return e
		}
		if through == "" { // Conservative baseline: earliest completed historical discovery, not the last row fetched.
			b, e := os.ReadFile(filepath.Join(filepath.Dir(a.configPath), "status.json"))
			if e == nil {
				var st struct {
					Discovery []struct {
						Updated string `json:"updated_at"`
						Done    int    `json:"done"`
					}
				}
				if json.Unmarshal(b, &st) == nil {
					for _, d := range st.Discovery {
						if d.Done == 1 && (through == "" || d.Updated < through) {
							through = d.Updated
						}
					}
				}
			}
		}
		if through == "" {
			return fmt.Errorf("verified historical discovery watermark required")
		}
		at, e := time.Parse(time.RFC3339Nano, through)
		if e != nil {
			return e
		}
		cp = updateCheckpoint{Started: time.Now().UTC().Format(time.RFC3339Nano), Since: at.Add(-5 * time.Minute).Format(time.RFC3339Nano)}
	}
	since, _ := time.Parse(time.RFC3339Nano, cp.Since)
	for cp.Kind < 2 {
		kind := []string{"issues", "pullRequests"}[cp.Kind]
		page, e := c.UpdatedNumbers(ctx, owner, repo, kind, cp.Cursor, since)
		if e != nil {
			return e
		}
		if cp.Kind == 0 {
			cp.Issues = page.Total
		} else {
			cp.PRs = page.Total
		}
		var wg sync.WaitGroup
		slots := make(chan struct{}, 16)
		var firstErr error
		var errMu sync.Mutex
		for i := 0; i < len(page.Numbers); i += 2 {
			numbers := append([]int(nil), page.Numbers[i:min(i+2, len(page.Numbers))]...)
			slots <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-slots }()
				defer wg.Done()
				e := a.syncAnalyticsBatch(ctx, s, owner, repo, numbers)
				if e != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = e
					}
					errMu.Unlock()
				}
			}()
		}
		wg.Wait()
		if firstErr != nil {
			return firstErr
		}
		fmt.Fprintf(a.Stderr, "{\"event\":\"github_update_page\",\"kind\":%q,\"threads\":%d}\n", kind, len(page.Numbers))

		if !page.More || (!page.Oldest.IsZero() && page.Oldest.Before(since)) {
			cp.Kind++
			cp.Cursor = ""
		} else {
			cp.Cursor = page.Cursor
		}
		b, _ := json.Marshal(cp)
		if e = s.SetAnalyticsState(ctx, key, string(b)); e != nil {
			return e
		}
		for batch := 0; batch < 5; batch++ {
			ids, e := s.AnalyticsProfileNodes(ctx, 100)
			if e != nil {
				return e
			}
			if len(ids) == 0 {
				break
			}
			nodes, e := c.AnalyticsNodes(ctx, ids, true)
			if e != nil {
				return e
			}
			if e = s.SaveActorProfiles(ctx, nodes, time.Now().UTC().Format(time.RFC3339Nano)); e != nil {
				return e
			}
		}

	}
	if e = s.SaveAnalyticsCoverage(ctx, owner+"/"+repo, cp.Started, cp.Issues, cp.PRs); e != nil {
		return e
	}
	if e = s.SetAnalyticsState(ctx, "through:"+owner+"/"+repo, cp.Started); e != nil {
		return e
	}
	return s.SetAnalyticsState(ctx, key, "")
}

// Smaller requests prevent high-fanout conversation queries exhausting GitHub's
// execution deadline. Split a persistently failing transient batch without
// accepting partial conversation evidence or changing transports.
func (a *App) syncAnalyticsBatch(ctx context.Context, s *store.Store, owner, repo string, numbers []int) error {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	token := a.resolveGitHubToken(ctx, cfg)
	client := gh.New(gh.Options{Token: token.Value, TokenProvider: a.analyticsTokenProvider, BaseURL: githubBaseURL(), RateLimit: a.observeGitHubRateLimit(ctx), RateLimitReserve: 1500})
	// The watch owner has already validated and opened this store. Reopening it
	// for every two threads repeats full-archive migration audits and serializes
	// otherwise independent network work. Native transactions still own writes.
	_, err = syncer.New(client, s).Sync(ctx, syncer.Options{Owner: owner, Repo: repo, GraphQLHistory: true, State: "all", Numbers: numbers, IncludeComments: true, IncludePRMetadata: true})
	if err == nil || ctx.Err() != nil || len(numbers) < 2 {
		return err
	}
	var response *gh.RequestError
	if errors.As(err, &response) && (response.Status == 502 || response.Status == 503 || response.Status == 504) {
		middle := len(numbers) / 2
		return errors.Join(a.syncAnalyticsBatch(ctx, s, owner, repo, numbers[:middle]), a.syncAnalyticsBatch(ctx, s, owner, repo, numbers[middle:]))
	}
	return err
}

// A drained queue is not the end of a watch: new actors and stale profiles
// become eligible later. Initial source scanning happens outside this loop.
func maintainAnalyticsEnrichment(ctx context.Context, watch bool, interval time.Duration, drain func() error) error {
	for {
		if err := drain(); err != nil {
			return err
		}
		if !watch {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
