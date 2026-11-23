package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/acquireapi"
	"github.com/openclaw/gitcrawl/internal/github"
)

// Discovery exports identities for an explicit repair selection. It does not
// establish source coverage, persist archive state, or fetch child records.
type parentDiscovery struct {
	Selection     acquireapi.ParentSelection `json:"selection"`
	NumbersSHA256 string                     `json:"numbersSHA256,omitempty"`
	UpdatedSince  string                     `json:"updatedSince,omitempty"`
	ObservedAt    string                     `json:"observedAt,omitempty"`
	NextCursor    string                     `json:"nextCursor"`
	Pages         int                        `json:"pages"`
	Scanned       int                        `json:"scanned"`
	Complete      bool                       `json:"complete"`
}

func (a *App) runDiscoverParents(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("discover-parents", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repo := fs.String("repo", "", "owner/repository")
	file := fs.String("numbers-file", "", "missing parent numbers, separated by whitespace or commas")
	updated := fs.String("updated-since", "", "export all currently updated parent identities since RFC3339 (not historical coverage)")
	cursor := fs.String("cursor", "", "nextCursor from the preceding output for the same selection")
	pages := fs.Int("max-pages", 20, "maximum repository pages (1..1000)")
	pageSize := fs.Int("page-size", 100, "repository page size (1..100; preserve on resume)")
	timeout := fs.Duration("timeout", 10*time.Minute, "bounded invocation lifetime")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	owner, name, ok := strings.Cut(*repo, "/")
	if fs.NArg() != 0 || !ok || owner == "" || name == "" || strings.ContainsAny(owner+name, "/\\?#") || (*file == "") == (*updated == "") || *pages < 1 || *pages > 1000 || *pageSize < 1 || *pageSize > 100 || *timeout <= 0 || *timeout > time.Hour || len(*cursor) > 4096 {
		return usageErr(errors.New("discover-parents requires repo, either numbers-file or updated-since, and bounded pages/timeout"))
	}
	var since time.Time
	if *updated != "" {
		var err error
		since, err = time.Parse(time.RFC3339Nano, *updated)
		if err != nil || since.After(time.Now()) {
			return usageErr(errors.New("updated-since must be a prior RFC3339 time"))
		}
		*updated = since.UTC().Format(time.RFC3339Nano)
	}
	var raw []byte
	if *file != "" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		raw, err = io.ReadAll(io.LimitReader(f, (2<<20)+1))
		f.Close()
		if err != nil {
			return err
		}
		if len(raw) > 2<<20 {
			return usageErr(errors.New("number selection exceeds 2 MiB"))
		}
	}
	wanted := map[int]bool{}
	for _, word := range strings.FieldsFunc(string(raw), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\r' || r == '\t' }) {
		n, e := strconv.Atoi(word)
		if e != nil || n <= 0 || wanted[n] {
			return usageErr(errors.New("invalid or duplicate parent number"))
		}
		wanted[n] = true
	}
	if *file != "" && (len(wanted) == 0 || len(wanted) > 100000) {
		return usageErr(errors.New("number selection must contain 1..100000 parents"))
	}
	numbers := make([]int, 0, len(wanted))
	for n := range wanted {
		numbers = append(numbers, n)
	}
	sort.Ints(numbers)
	canonical, _ := json.Marshal(numbers)
	digest := sha256.Sum256(canonical)
	result := parentDiscovery{Selection: acquireapi.ParentSelection{Schema: "gitcrawl-parent-selection/v1", Repository: *repo, Parents: []acquireapi.SelectedParent{}}, NumbersSHA256: hex.EncodeToString(digest[:]), NextCursor: *cursor}
	ordering := "created"
	if *updated != "" {
		ordering = "updated"
		result.NumbersSHA256 = ""
		result.UpdatedSince = *updated
		result.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	provider, err := github.NewOctopool(os.Getenv("OCTOPOOL_URL"), os.Getenv("OCTOPOOL_POOL"), os.Getenv("OCTOPOOL_TOKEN"), *repo)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if _, err = provider.GetRepo(ctx, owner, name, nil); err != nil {
		return err
	}
	seen := map[int]acquireapi.SelectedParent{}
	ids := map[string]int{}
	cursors := map[string]bool{*cursor: true}
	for result.Pages < *pages {
		var page github.RESTPage
		page, err = provider.ReadRESTPage(ctx, github.RESTPageRequest{Owner: owner, Repo: name, Family: "issues", State: "all", Sort: ordering, Since: *updated, PerPage: *pageSize, Cursor: result.NextCursor}, nil)
		if err != nil {
			break
		}
		if len(page.Rows) > *pageSize {
			err = errors.New("repository page exceeds requested size")
			break
		}
		if page.NextCursor != "" && cursors[page.NextCursor] {
			err = errors.New("discovery repeated a cursor")
			break
		}
		batch := []acquireapi.SelectedParent{}
		for _, row := range page.Rows {
			number, numberOK := row["number"].(json.Number)
			n, numberErr := strconv.Atoi(string(number))
			id, idOK := row["id"].(json.Number)
			idValue, idErr := strconv.ParseInt(string(id), 10, 64)
			if !numberOK || numberErr != nil || n <= 0 || !idOK || idErr != nil || idValue <= 0 || strconv.FormatInt(idValue, 10) != string(id) {
				err = errors.New("invalid REST parent identity")
				break
			}
			if *updated != "" {
				at, _ := row["updated_at"].(string)
				parsed, parseErr := time.Parse(time.RFC3339Nano, at)
				if parseErr != nil || parsed.Before(since) {
					err = errors.New("invalid updated parent timestamp")
					break
				}
			} else if !wanted[n] {
				continue
			}
			kind := "issue"
			if pr, exists := row["pull_request"]; exists {
				if _, ok := pr.(map[string]any); !ok {
					err = errors.New("invalid REST pull request identity")
					break
				}
				kind = "pull_request"
			}
			p := acquireapi.SelectedParent{Number: n, Kind: kind, BackingIssueID: string(id)}
			if old, exists := seen[n]; exists {
				if old != p {
					err = errors.New("conflicting parent identity")
					break
				}
				continue
			}
			if other, exists := ids[string(id)]; exists && other != n {
				err = errors.New("parent identity reused by another number")
				break
			}
			// Validate the entire page before advancing its cursor or exposing it.
			seen[n] = p
			ids[string(id)] = n
			batch = append(batch, p)
		}
		if err != nil {
			break
		}
		for _, p := range batch {
			// A repeated identical row can appear across mutable list pages.
			result.Selection.Parents = append(result.Selection.Parents, p)
		}
		result.Pages++
		result.Scanned += len(page.Rows)
		result.NextCursor = page.NextCursor
		if page.NextCursor == "" {
			result.Complete = true
			break
		}
		cursors[page.NextCursor] = true
	}
	sort.Slice(result.Selection.Parents, func(i, j int) bool { return result.Selection.Parents[i].Number < result.Selection.Parents[j].Number })
	a.applyCommandJSON(*jsonOut)
	if outErr := a.writeOutput("discover-parents", result, false); outErr != nil {
		return outErr
	}
	if err != nil {
		return fmt.Errorf("discover-parents: %w", err)
	}
	return nil
}
