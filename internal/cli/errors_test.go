package cli

import (
	"errors"
	"testing"
)

func TestExitCodeForExplicitExitError(t *testing.T) {
	err := exitErr(7, errors.New("stop"))
	if got := ExitCode(err); got != 7 {
		t.Fatalf("ExitCode() = %d, want 7", got)
	}
	if got := err.Error(); got != "stop" {
		t.Fatalf("Error() = %q, want stop", got)
	}
}

func TestNotImplementedMessage(t *testing.T) {
	err := notImplemented("frobnicate")
	if got := err.Error(); got != "frobnicate is not implemented yet" {
		t.Fatalf("Error() = %q", got)
	}
}
