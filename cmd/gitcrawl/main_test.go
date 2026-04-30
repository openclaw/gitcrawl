package main

import (
	"bytes"
	"os"
	"testing"
)

func TestMainPrintsVersion(t *testing.T) {
	oldArgs := os.Args
	oldStdout := os.Stdout
	t.Cleanup(func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
	})
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = write
	os.Args = []string{"gitcrawl", "--version"}
	main()
	if err := write.Close(); err != nil {
		t.Fatalf("close stdout pipe: %v", err)
	}
	var out bytes.Buffer
	if _, err := out.ReadFrom(read); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if out.String() == "" {
		t.Fatal("version output was empty")
	}
}
