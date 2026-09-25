package headlinemetrics

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestExecuteSerializesWriterProcesses(t *testing.T) {
	for _, mode := range []string{"complete", "kill"} {
		t.Run(mode, func(t *testing.T) {
			c := testConfig(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMetricsWriterProcessHelper$", "--", c.Database)
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			in, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				if !waited {
					cancel()
					_ = cmd.Wait()
				}
			}()
			line, err := bufio.NewReader(out).ReadString('\n')
			if err != nil || line != "collecting\n" {
				t.Fatalf("child = %q, %v, %s", line, err, stderr.String())
			}
			first, err := os.Stat(c.Database + ".writer.lock")
			if err != nil {
				t.Fatal(err)
			}
			collector := func(context.Context, Config, string) ([]Row, error) {
				t.Fatal("overlapping collector reached provider")
				return nil, nil
			}
			for _, command := range []string{"collect", "import"} {
				result, err := Execute(ctx, command, c, collector, strings.NewReader("invalid input"))
				if !errors.Is(err, errWriterBusy) || result.RowsWritten != 0 {
					t.Fatalf("overlap %s = %+v, %v", command, result, err)
				}
			}
			if result, err := Execute(ctx, "status", c, nil, nil); err != nil || !result.OK || result.Observations != 0 {
				t.Fatalf("concurrent reader = %+v, %v", result, err)
			}
			// A different metrics database has independent ownership.
			if _, err := Execute(ctx, "import", testConfig(t), nil, strings.NewReader("")); err != nil {
				t.Fatal(err)
			}
			if mode == "kill" {
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := in.Write([]byte("finish\n")); err != nil {
				t.Fatal(err)
			}
			_ = in.Close()
			err = cmd.Wait()
			waited = true
			if (mode == "complete") != (err == nil) {
				t.Fatalf("child exit = %v: %s", err, stderr.String())
			}
			if _, err := Execute(ctx, "import", c, nil, strings.NewReader("")); err != nil {
				t.Fatalf("writer after %s = %v", mode, err)
			}
			last, err := os.Stat(c.Database + ".writer.lock")
			if err != nil || !os.SameFile(first, last) || last.Mode().Perm() != 0600 {
				t.Fatalf("persistent private lock = %v, %v", last, err)
			}
			result, err := Execute(ctx, "status", c, nil, nil)
			want := 0
			if mode == "complete" {
				want = 1
			}
			if err != nil || result.Observations != want {
				t.Fatalf("retained observations = %+v, %v", result, err)
			}
		})
	}
}

func TestMetricsWriterProcessHelper(t *testing.T) {
	i := slices.Index(os.Args, "--")
	if i < 0 {
		return
	}
	c := Config{Database: os.Args[i+1], Targets: []Target{{Entity: "OpenClaw", Target: "openclaw/openclaw"}}}
	_, err := Execute(context.Background(), "collect", c, func(context.Context, Config, string) ([]Row, error) {
		fmt.Println("collecting")
		if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
			return nil, err
		}
		return []Row{testRow()}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMetricsWriterRejectsUnsafeLockPaths(t *testing.T) {
	for _, kind := range []string{"directory", "symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			c := testConfig(t)
			path := c.Database + ".writer.lock"
			target := filepath.Join(filepath.Dir(c.Database), "unrelated")
			if err := os.WriteFile(target, []byte("retained"), 0644); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink":
				err = os.Symlink(target, path)
			case "hardlink":
				err = os.Link(target, path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Execute(context.Background(), "import", c, nil, strings.NewReader("")); err == nil {
				t.Fatal("unsafe lock accepted")
			}
			if _, err := os.Stat(c.Database); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("database initialized before lock: %v", err)
			}
			if got, err := os.ReadFile(target); err != nil || string(got) != "retained" {
				t.Fatalf("unrelated file = %q, %v", got, err)
			}
		})
	}
}
