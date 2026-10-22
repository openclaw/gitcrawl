package cli

import (
	"context"
	"fmt"
)

// RunClusterReader exposes only local cluster reads in the separately delivered
// native reader. It cannot invoke collectors, embeddings or release probes.
func (a *App) RunClusterReader(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return a.clusterReaderUsage()
	}
	var global gitcrawlRootArgs
	if err := parseKongArgs(&global, args, "gitcrawl-cluster-reader", a.Stdout, a.Stderr); err != nil {
		return usageErr(err)
	}
	if global.GitHubTokenCommand != nil {
		return usageErr(fmt.Errorf("cluster reader does not use provider credentials"))
	}
	rest := global.Args
	if global.Version {
		return a.writeOutput("version", map[string]string{"version": version}, false)
	}
	if len(rest) == 0 || rest[0] == "--help" || rest[0] == "-h" {
		return a.clusterReaderUsage()
	}
	command := rest[0]
	if command == "help" {
		if len(rest) != 2 {
			return a.clusterReaderUsage()
		}
		command = rest[1]
	}
	switch command {
	case "clusters", "durable-clusters", "clusters-report", "cluster-detail", "cluster-candidates":
	default:
		return usageErr(fmt.Errorf("cluster reader supports local cluster reads only"))
	}
	if rest[0] == "help" {
		return a.printCommandUsage(command)
	}
	if global.Config == "" {
		return usageErr(fmt.Errorf("cluster reader requires an explicit source config"))
	}
	format, err := resolveOutputFormat(global.Format, global.JSON)
	if err != nil {
		return usageErr(err)
	}
	a.configPath = global.Config
	a.format = format
	a.noColor = global.NoColor
	session := &portableCommandSession{}
	ctx = context.WithValue(ctx, portableCommandKey{}, session)
	defer session.close()
	switch command {
	case "clusters":
		return a.runClusterList(ctx, command, rest[1:], false)
	case "durable-clusters":
		return a.runClusterList(ctx, command, rest[1:], true)
	case "clusters-report":
		return a.runClustersReport(ctx, rest[1:])
	case "cluster-detail":
		return a.runClusterDetail(ctx, rest[1:])
	case "cluster-candidates":
		return a.runClusterCandidates(ctx, rest[1:], true)
	}
	return usageErr(fmt.Errorf("unsupported cluster read"))
}
func (a *App) clusterReaderUsage() error {
	_, err := fmt.Fprintln(a.Stdout, "Usage: gitcrawl-cluster-reader --config SOURCE_CONFIG clusters|durable-clusters|clusters-report|cluster-detail|cluster-candidates [options]\nLocal read-only cluster reader; no collection, embedding or provider commands.")
	return err
}
