package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runClusters(ctx context.Context, args []string) error {
	return a.runClusterList(ctx, "clusters", args, false)
}

func (a *App) runDurableClusters(ctx context.Context, args []string) error {
	return a.runClusterList(ctx, "durable-clusters", args, true)
}

type clustersReport struct {
	Repository  string                `json:"repository"`
	GeneratedAt string                `json:"generated_at"`
	Sort        string                `json:"sort"`
	MinSize     int                   `json:"min_size"`
	Limit       int                   `json:"limit"`
	MemberLimit int                   `json:"member_limit"`
	HideClosed  bool                  `json:"hide_closed,omitempty"`
	Clusters    []store.ClusterDetail `json:"clusters"`
	Totals      clustersReportTotals  `json:"totals"`
}

type clustersReportTotals struct {
	ClusterCount int `json:"cluster_count"`
	MemberCount  int `json:"member_count"`
	OpenCount    int `json:"open_count"`
	ClosedCount  int `json:"closed_count"`
}

func clusterListIncludesClosed(durable bool, includeClosed bool, hideClosed bool) bool {
	if hideClosed {
		return false
	}
	if durable {
		return includeClosed
	}
	return true
}

func (a *App) runClusterList(ctx context.Context, command string, args []string, durable bool) error {
	fs := flag.NewFlagSet("clusters", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "", "minimum active member count")
	limitRaw := fs.String("limit", "", "maximum cluster rows")
	sortMode := fs.String("sort", "size", "sort mode: recent|oldest|size")
	includeClosed := fs.Bool("include-closed", false, "deprecated; clusters include closed rows by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "sort": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("%s requires owner/repo", command))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	sort := strings.TrimSpace(*sortMode)
	if sort != "recent" && sort != "oldest" && sort != "size" {
		return usageErr(fmt.Errorf("unsupported sort %q", sort))
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	options := store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: clusterListIncludesClosed(durable, *includeClosed, *hideClosed),
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	}
	var clusters []store.ClusterSummary
	if durable {
		clusters, err = rt.Store.ListClusterSummaries(ctx, options)
	} else {
		clusters, err = rt.Store.ListDisplayClusterSummaries(ctx, options)
	}
	if err != nil {
		return err
	}
	return a.writeOutput(command, map[string]any{
		"repository": repo.FullName,
		"clusters":   clusters,
	}, true)
}

func (a *App) runClustersReport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("clusters-report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "5", "minimum active member count")
	limitRaw := fs.String("limit", "10", "maximum cluster rows")
	memberLimitRaw := fs.String("member-limit", "8", "maximum member rows per cluster")
	bodyCharsRaw := fs.String("body-chars", "240", "maximum body snippet characters")
	sortMode := fs.String("sort", "size", "sort mode: size|recent|oldest")
	includeClosed := fs.Bool("include-closed", false, "deprecated; clusters include closed rows by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "member-limit": true, "body-chars": true, "sort": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("clusters-report requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}
	memberLimit, err := parseOptionalPositiveInt(*memberLimitRaw)
	if err != nil {
		return usageErr(err)
	}
	bodyChars, err := parseOptionalPositiveInt(*bodyCharsRaw)
	if err != nil {
		return usageErr(err)
	}
	sort := strings.TrimSpace(*sortMode)
	if sort != "recent" && sort != "oldest" && sort != "size" {
		return usageErr(fmt.Errorf("unsupported sort %q", sort))
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	summaries, err := rt.Store.ListDisplayClusterSummaries(ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: clusterListIncludesClosed(false, *includeClosed, *hideClosed),
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	})
	if err != nil {
		return err
	}
	report := clustersReport{
		Repository:  repo.FullName,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Sort:        sort,
		MinSize:     minSize,
		Limit:       limit,
		MemberLimit: memberLimit,
		HideClosed:  *hideClosed,
		Clusters:    make([]store.ClusterDetail, 0, len(summaries)),
	}
	for _, summary := range summaries {
		detail, err := rt.Store.ClusterDetail(ctx, store.ClusterDetailOptions{
			RepoID:        repo.ID,
			ClusterID:     summary.ID,
			Source:        summary.Source,
			IncludeClosed: clusterListIncludesClosed(false, *includeClosed, *hideClosed),
			MemberLimit:   memberLimit,
			BodyChars:     bodyChars,
		})
		if err != nil {
			return err
		}
		report.Clusters = append(report.Clusters, detail)
		report.Totals.MemberCount += detail.Cluster.MemberCount
		if detail.Cluster.Status == "closed" || detail.Cluster.ClosedAt != "" {
			report.Totals.ClosedCount++
		} else {
			report.Totals.OpenCount++
		}
	}
	report.Totals.ClusterCount = len(report.Clusters)
	return a.writeClustersReport(report)
}

func (a *App) writeClustersReport(report clustersReport) error {
	if a.format == FormatJSON {
		return a.writeOutput("clusters-report", report, true)
	}
	_, err := fmt.Fprint(a.Stdout, renderClustersReportMarkdown(report))
	return err
}

func mergeClusterSummaries(primary, secondary []store.ClusterSummary) []store.ClusterSummary {
	if len(primary) == 0 {
		return append([]store.ClusterSummary(nil), secondary...)
	}
	out := append([]store.ClusterSummary(nil), primary...)
	seen := make(map[string]bool, len(out)+len(secondary))
	for _, cluster := range out {
		seen[clusterSummaryKey(cluster)] = true
	}
	for _, cluster := range secondary {
		key := clusterSummaryKey(cluster)
		if !seen[key] {
			out = append(out, cluster)
			seen[key] = true
		}
	}
	return out
}

func clusterSummaryKey(cluster store.ClusterSummary) string {
	source := strings.TrimSpace(cluster.Source)
	if source == "" {
		source = "auto"
	}
	return source + ":" + strconv.FormatInt(cluster.ID, 10)
}

func sameClusterSummary(left, right store.ClusterSummary) bool {
	if left.ID == 0 || right.ID == 0 || left.ID != right.ID {
		return false
	}
	leftSource := strings.TrimSpace(left.Source)
	rightSource := strings.TrimSpace(right.Source)
	if leftSource == "" || rightSource == "" {
		return true
	}
	return leftSource == rightSource
}

func (a *App) runClusterDetail(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cluster-detail", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	clusterIDRaw := fs.String("id", "", "cluster id")
	sourceRaw := fs.String("source", "", "cluster source: auto|run|durable")
	memberLimitRaw := fs.String("member-limit", "", "maximum member rows")
	bodyCharsRaw := fs.String("body-chars", "", "maximum body snippet characters")
	includeClosed := fs.Bool("include-closed", false, "deprecated; closed cluster members are shown by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed members")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"id": true, "source": true, "member-limit": true, "body-chars": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("cluster-detail requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	clusterID, err := parseRequiredPositiveInt("id", *clusterIDRaw)
	if err != nil {
		return usageErr(err)
	}
	memberLimit, err := parseOptionalPositiveInt(*memberLimitRaw)
	if err != nil {
		return usageErr(err)
	}
	bodyChars, err := parseOptionalPositiveInt(*bodyCharsRaw)
	if err != nil {
		return usageErr(err)
	}
	if bodyChars <= 0 {
		bodyChars = 280
	}
	source, err := parseClusterDetailSource(*sourceRaw)
	if err != nil {
		return usageErr(err)
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return err
	}
	detail, err := rt.Store.ClusterDetail(ctx, store.ClusterDetailOptions{
		RepoID:        repo.ID,
		ClusterID:     int64(clusterID),
		Source:        source,
		IncludeClosed: *includeClosed || !*hideClosed,
		MemberLimit:   memberLimit,
		BodyChars:     bodyChars,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("cluster-detail", map[string]any{
		"repository": repo.FullName,
		"cluster":    detail.Cluster,
		"members":    detail.Members,
	}, true)
}

func parseClusterDetailSource(source string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "", "auto":
		return "", nil
	case "run", "raw", store.ClusterSourceRun:
		return store.ClusterSourceRun, nil
	case "durable", store.ClusterSourceDurable:
		return store.ClusterSourceDurable, nil
	default:
		return "", fmt.Errorf("unsupported cluster source %q", source)
	}
}
