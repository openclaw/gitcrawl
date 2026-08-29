package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

const portableGitOutputLimit = 8 << 20
const portableGitGrace = 750 * time.Millisecond

func portableGitArgs(args []string) []string {
	controls := []string{
		"-c", "maintenance.auto=false", "-c", "gc.auto=0",
		"-c", "core.hooksPath=" + os.DevNull, "-c", "core.fsmonitor=false",
		"-c", "submodule.recurse=false", "-c", "fetch.recurseSubmodules=false",
		"-c", "fetch.writeCommitGraph=false", "-c", "gc.writeCommitGraph=false",
		"-c", "merge.autostash=false", "-c", "core.askPass=",
		"-c", "core.autocrlf=false", "-c", "core.eol=lf",
		"-c", "core.protectHFS=true", "-c", "core.protectNTFS=true",
		"-c", "core.attributesFile=" + os.DevNull,
		"-c", "protocol.ext.allow=never",
	}
	return append(controls, args...)
}

func portableGitEnv() []string {
	var env []string
	for _, entry := range os.Environ() {
		name, value, _ := strings.Cut(entry, "=")
		// Preserve only config selectors that remove an entire scope. Other
		// paths and GIT_CONFIG_* injection still cannot select Git's inputs.
		if (name == "GIT_CONFIG_GLOBAL" || name == "GIT_CONFIG_SYSTEM") && value == os.DevNull ||
			name == "GIT_CONFIG_NOSYSTEM" && (value == "1" || strings.EqualFold(value, "true")) {
			env = append(env, entry)
			continue
		}
		if strings.HasPrefix(strings.ToUpper(name), "GIT_") || name == "SSH_ASKPASS" {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0", "GIT_NO_REPLACE_OBJECTS=1", "GIT_ATTR_NOSYSTEM=1",
		"GIT_LFS_SKIP_SMUDGE=1", "GCM_INTERACTIVE=never", "GIT_SSH_COMMAND=ssh -o BatchMode=yes -o ConnectTimeout=10")
}

type portableLimitedWriter struct {
	writer io.Writer
	left   int64
}

func (writer *portableLimitedWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > writer.left {
		return 0, fmt.Errorf("portable Git output exceeds limit")
	}
	written, err := writer.writer.Write(data)
	writer.left -= int64(written)
	return written, err
}

// Git diagnostics are deliberately not returned: transport errors can contain
// credential-bearing URLs, helper output, or unbounded remote messages.
func runPortableGit(ctx context.Context, workdir string, output io.Writer, args ...string) error {
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, portableOperationTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	executable, ok := ctx.Value(portableGitKey{}).(portableGitExecutable)
	if !ok {
		return fmt.Errorf("portable Git executable was not resolved")
	}
	info, err := os.Stat(executable.path)
	if err != nil || !os.SameFile(info, executable.info) || info.Size() != executable.info.Size() || !info.ModTime().Equal(executable.info.ModTime()) {
		return fmt.Errorf("portable Git executable changed during operation")
	}
	cmd := exec.Command(executable.path, portableGitArgs(args)...)
	cmd.Dir = workdir
	cmd.Env = portableGitEnv()
	cmd.Stdout = output
	var diagnostic portableGitDiagnostic
	cmd.Stderr = &diagnostic
	cmd.WaitDelay = time.Second
	if err := configurePortableProcess(cmd); err != nil {
		return err
	}
	defer cleanupCommandGroup(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start portable Git: %w", err)
	}
	defer killPortableProcess(cmd)
	if err := attachPortableProcess(cmd); err != nil {
		killPortableProcess(cmd)
		_ = cmd.Wait()
		return fmt.Errorf("contain portable Git: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			// Return fixed classifications, never raw remote/helper diagnostics.
			// Keep the legacy recovery classifiers intact.
			message := diagnostic.String()
			lower := strings.ToLower(message)
			if strings.Contains(message, "index.lock") && strings.Contains(lower, "file exists") {
				return fmt.Errorf("index.lock file exists: %w", err)
			}
			if strings.Contains(message, "Your local changes") || strings.Contains(message, "would be overwritten by merge") {
				return fmt.Errorf("Your local changes would be overwritten by merge: %w", err)
			}
			switch {
			case strings.Contains(lower, "does not appear to be a git repository") || strings.Contains(lower, "repository") && strings.Contains(lower, "does not exist") || strings.Contains(lower, "repository not found"):
				return fmt.Errorf("remote repository unavailable; verify the remote path and read access: %w", err)
			case strings.Contains(lower, "authentication failed") || strings.Contains(lower, "permission denied (publickey") || strings.Contains(lower, "could not read username"):
				return fmt.Errorf("Git authentication failed; check the credential helper or SSH identity and repository access: %w", err)
			case strings.Contains(lower, "no space left on device"):
				return fmt.Errorf("insufficient disk space for Git; restore free-space headroom before retrying: %w", err)
			case strings.Contains(lower, "could not resolve host") || strings.Contains(lower, "failed to connect") || strings.Contains(lower, "connection timed out") || strings.Contains(lower, "connection refused"):
				return fmt.Errorf("Git connection failed; check network connectivity and remote availability: %w", err)
			}
			return fmt.Errorf("Git command failed; verify Git version, repository state and remote access: %w", err)
		}
		return err
	case <-ctx.Done():
		terminatePortableProcess(cmd)
		timer := time.NewTimer(portableGitGrace)
		defer timer.Stop()
		select {
		case <-done:
			<-timer.C // Children may still be running their graceful cleanup.
			killPortableProcess(cmd)
		case <-timer.C:
			killPortableProcess(cmd)
			<-done
		}
		return context.Cause(ctx)
	}
}

type portableGitDiagnostic struct{ bytes.Buffer }

func (diagnostic *portableGitDiagnostic) Write(data []byte) (int, error) {
	size := len(data)
	if remaining := 8192 - diagnostic.Len(); remaining > 0 {
		_, _ = diagnostic.Buffer.Write(data[:min(size, remaining)])
	}
	return size, nil
}

func portableGitOutput(ctx context.Context, root string, args ...string) (string, error) {
	var output bytes.Buffer
	err := runPortableGit(ctx, root, &portableLimitedWriter{writer: &output, left: portableGitOutputLimit}, args...)
	return output.String(), err
}
