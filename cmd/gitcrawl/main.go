package main

import (
	"context"
	"fmt"
	"os"

	"github.com/openclaw/gitcrawl/internal/cli"
)

func main() {
	if err := cli.New().Run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(cli.ExitCode(err))
	}
}
