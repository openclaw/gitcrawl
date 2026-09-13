package cli

import (
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

func (a *App) withTUIRenderer(run func() error) error {
	if !a.noColor {
		return run()
	}
	// Lip Gloss styles use a package-wide renderer; keep this override scoped
	// to the program so later commands retain the caller's color policy.
	previous := lipgloss.DefaultRenderer()
	renderer := lipgloss.NewRenderer(a.Stdout)
	renderer.SetColorProfile(termenv.Ascii)
	lipgloss.SetDefaultRenderer(renderer)
	defer lipgloss.SetDefaultRenderer(previous)
	return run()
}
