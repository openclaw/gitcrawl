package cli

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

func runGitCommandOutputWithEnvSeparate(ctx context.Context, workdir string, env []string, args ...string) (string, string, error) {
	if _, portable := ctx.Value(portableGitKey{}).(portableGitExecutable); portable {
		out, err := portableGitOutput(ctx, workdir, args...)
		return out, "", err
	}
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	cmd := exec.Command("git", args...)
	cmd.Dir = workdir
	cmd.Env = append(append([]string(nil), env...),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=ssh -o BatchMode=yes -o ConnectTimeout=10",
	)
	configureCommandGroup(cmd)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cleanupCommandGroup(cmd)
		return stdout.String(), stderr.String(), err
	}
	if err := attachCommandGroup(cmd); err != nil {
		killCommandGroup(cmd)
		_ = cmd.Wait()
		cleanupCommandGroup(cmd)
		return stdout.String(), stderr.String(), err
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case err := <-done:
		cleanupCommandGroup(cmd)
		return stdout.String(), stderr.String(), err
	case <-ctx.Done():
		killCommandGroup(cmd)
		err := <-done
		cleanupCommandGroup(cmd)
		if err == nil {
			err = ctx.Err()
		}
		return stdout.String(), stderr.String(), fmt.Errorf("%w: %v", ctx.Err(), err)
	}
}

func gitOutput(ctx context.Context, workdir string, args ...string) (string, error) {
	out, err := runGitCommandOutput(ctx, workdir, args...)
	if err != nil {
		return "", fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(out))
	}
	return strings.TrimSpace(out), nil
}
