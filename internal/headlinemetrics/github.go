package headlinemetrics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
)

// GitHubCollector uses the same HTTP client and credential provider as native
// Gitcrawl commands. Clone traffic is optional and requires authentication.
func GitHubCollector(client *github.Client, trafficEnabled bool) Collector {
	return func(ctx context.Context, c Config, ts string) ([]Row, error) {
		if err := c.Validate(); err != nil {
			return nil, err
		}
		observed, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, errors.New("invalid collection timestamp")
		}
		rows := []Row{}
		failed := false
		for _, t := range c.Targets {
			repo, err := client.RepositoryHeadline(ctx, t.Target)
			if err != nil {
				failed = true
				repo = github.RepositoryHeadline{}
			}
			prs, err := client.OpenPullCount(ctx, t.Target)
			if err != nil || prs.Incomplete {
				failed = true
				prs = github.PullCount{}
			}
			var issues *float64
			if validCount(repo.OpenIssuesAndPulls) != nil && validCount(prs.Count) != nil {
				issues = Value(*repo.OpenIssuesAndPulls - *prs.Count)
			}
			for _, m := range []struct {
				Name  string
				Value *float64
			}{
				{"stars", repo.Stars}, {"forks", repo.Forks}, {"watchers", repo.Watchers}, {"open_prs", prs.Count}, {"open_issues", issues},
			} {
				value := validCount(m.Value)
				if value == nil {
					failed = true
				}
				rows = append(rows, Counter(t, m.Name, value, ts, "github_rest"))
			}
			if trafficEnabled {
				traffic, err := client.CloneTraffic(ctx, t.Target)
				var requestErr *github.RequestError
				optional := errors.As(err, &requestErr) && (requestErr.Status == 403 || requestErr.Status == 404)
				if err != nil && !optional {
					failed = true
				}
				if err == nil {
					for _, v := range traffic.Clones {
						day, err := time.Parse(time.RFC3339Nano, v.Timestamp)
						if err != nil {
							failed = true
							continue
						}
						day = day.UTC().Truncate(24 * time.Hour)
						if !day.Before(observed.UTC().Truncate(24 * time.Hour)) {
							continue
						}
						value := validCount(v.Count)
						if value == nil {
							failed = true
						}
						r := Counter(t, "clones", value, day.Add(24*time.Hour-time.Millisecond).Format(time.RFC3339Nano), "github_traffic")
						r.Kind = "daily"
						r.ObservedAt = ts
						rows = append(rows, r)
					}
				}
			}
			// Follow all release pages. Do not silently truncate stable release history.
			for page := 1; ; page++ {
				releases, err := client.ReleasePage(ctx, t.Target, page)
				if err != nil {
					failed = true
					break
				}
				for _, v := range releases {
					if v.Draft || v.Prerelease {
						continue
					}
					at := v.Published
					if at == "" {
						at = v.Created
					}
					when, err := time.Parse(time.RFC3339Nano, at)
					label := v.Name
					if label == "" {
						label = v.Tag
					}
					if err != nil || v.ID <= 0 || label == "" {
						failed = true
						continue
					}
					rows = append(rows, Row{Type: "event", ID: fmt.Sprintf("github-release:%d", v.ID), Entity: t.Entity, Target: t.Target, Kind: "release", TS: when.UTC().Format(time.RFC3339Nano), ObservedAt: ts, Provenance: "github_releases", Label: label, URL: v.URL})
				}
				if len(releases) < 100 {
					break
				}
			}
		}
		if failed {
			return rows, errors.New("one or more GitHub metrics unavailable")
		}
		return rows, nil
	}
}

func validCount(value *float64) *float64 {
	if value == nil || math.Trunc(*value) != *value {
		return nil
	}
	return Value(*value)
}
