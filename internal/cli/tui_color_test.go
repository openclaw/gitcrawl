package cli

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

func TestNoColorFlagControlsTUIRendering(t *testing.T) {
	previous := lipgloss.DefaultRenderer()
	renderer := lipgloss.NewRenderer(io.Discard)
	renderer.SetColorProfile(termenv.TrueColor)
	lipgloss.SetDefaultRenderer(renderer)
	defer lipgloss.SetDefaultRenderer(previous)

	app := New()
	app.Stdout, app.Stderr = io.Discard, io.Discard
	for _, noColor := range []bool{false, true, false} {
		args := []string{"version"}
		if noColor {
			args = append([]string{"--no-color"}, args...)
		}
		if err := app.Run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		var view string
		if err := app.withTUIRenderer(func() error {
			model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
				Repository: "example/archive", Clusters: sampleTUIClusters(),
			})
			model.width, model.height = 160, 40
			view = model.View()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(view, "example/archive") {
			t.Fatal("rendered view lost the repository")
		}
		if colored := hasTUIColors(view); colored == noColor {
			t.Fatalf("no-color=%t: colored output=%t", noColor, colored)
		}
		if lipgloss.DefaultRenderer() != renderer {
			t.Fatal("TUI did not restore the caller's renderer")
		}
	}
	if err := app.Run(context.Background(), []string{"--no-color", "version"}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("program failed")
	if err := app.withTUIRenderer(func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("program error = %v", err)
	}
	if lipgloss.DefaultRenderer() != renderer {
		t.Fatal("failed TUI did not restore the caller's renderer")
	}
}

func hasTUIColors(value string) bool {
	for _, sequence := range regexp.MustCompile("\x1b\\[[0-9;:]*m").FindAllString(value, -1) {
		for _, raw := range strings.FieldsFunc(sequence[2:len(sequence)-1], func(r rune) bool { return r == ';' || r == ':' }) {
			code, _ := strconv.Atoi(raw)
			if code >= 30 && code <= 49 || code >= 90 && code <= 107 || code == 58 {
				return true
			}
		}
	}
	return false
}
