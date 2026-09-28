package cli

import (
	"flag"
	"reflect"
	"testing"
)

func TestNormalizeCommandArgsPreservesEndOfOptions(t *testing.T) {
	for _, args := range [][]string{
		{"--limit", "5", "--", "--json"},
		{"first", "--limit", "5", "--", "--json", "-R", "last"},
	} {
		t.Run(args[0], func(t *testing.T) {
			fs := flag.NewFlagSet("search", flag.ContinueOnError)
			limit := fs.String("limit", "", "")
			jsonOut := fs.Bool("json", false, "")
			if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"limit": true})); err != nil {
				t.Fatal(err)
			}
			want := []string{"--json"}
			if args[0] == "first" {
				want = []string{"first", "--json", "-R", "last"}
			}
			if *limit != "5" || *jsonOut || !reflect.DeepEqual(fs.Args(), want) {
				t.Fatalf("limit=%q json=%v args=%q; want limit=5 json=false args=%q", *limit, *jsonOut, fs.Args(), want)
			}
		})
	}
}

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

func TestNormalizeCommandArgsMovesShortStringFlags(t *testing.T) {
	got := normalizeCommandArgs([]string{"hot loop", "-R", "openclaw/openclaw", "--state", "open"}, map[string]bool{"R": true, "state": true})
	want := []string{"-R", "openclaw/openclaw", "--state", "open", "hot loop"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args: got %#v want %#v", got, want)
	}
}
