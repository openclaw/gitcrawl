//go:build native_cluster_overlay

package main

import (
	"context"
	"fmt"
	"github.com/openclaw/gitcrawl/internal/cli"
	"os"
)

func main() {
	if err := cli.New().RunClusterReader(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(cli.ExitCode(err))
	}
}
