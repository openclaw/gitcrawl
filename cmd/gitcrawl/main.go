package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/gitcrawl/internal/cli"
)

func main() {
	ctx, stop := signalContext(context.Background())
	defer stop()
	args := os.Args[1:]
	name := strings.TrimSuffix(filepath.Base(os.Args[0]), ".exe")
	if name == "gh" || name == "gitcrawl-gh" {
		args = append([]string{"gh"}, args...)
	}
	if err := cli.New().Run(ctx, args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(cli.ExitCode(err))
	}
}
