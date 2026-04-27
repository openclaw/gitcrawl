package cli

import (
	"reflect"
	"testing"
)

func TestNormalizeCommandArgsMovesFlagsBeforePositionals(t *testing.T) {
	got := normalizeCommandArgs([]string{"openclaw/openclaw", "--query", "download", "--json"}, map[string]bool{"query": true})
	want := []string{"--query", "download", "--json", "openclaw/openclaw"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args: got %#v want %#v", got, want)
	}
}

func TestNormalizeCommandArgsKeepsInlineValues(t *testing.T) {
	got := normalizeCommandArgs([]string{"openclaw/openclaw", "--limit=5"}, map[string]bool{"limit": true})
	want := []string{"--limit=5", "openclaw/openclaw"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args: got %#v want %#v", got, want)
	}
}
