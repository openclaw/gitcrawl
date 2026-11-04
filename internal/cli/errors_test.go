package cli

import "testing"

func TestNotImplementedMessage(t *testing.T) {
	err := notImplemented("frobnicate")
	if got := err.Error(); got != "frobnicate is not implemented yet" {
		t.Fatalf("Error() = %q", got)
	}
}
