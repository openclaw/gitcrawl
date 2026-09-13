package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

func (m clusterBrowserModel) menuLines(width int) []string {
	palette := actionMenuColors(m.menuContext)
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(palette.accent)).
		Render(firstNonEmpty(m.menuTitle, "Actions"))
	lines := []string{title, dim(actionMenuSubtitle(m.menuContext)), ""}
	visible := m.menuVisibleCount()
	start := clampInt(m.menuOff, 0, max(0, len(m.menuItems)-visible))
	end := min(len(m.menuItems), start+visible)
	shortcut := 0
	for index := start; index < end; index++ {
		item := m.menuItems[index]
		if !item.selectable() {
			lines = append(lines, truncateCells("  "+dim(item.label), width))
			continue
		}
		shortcut++
		prefix := "  "
		if index == m.menuIndex {
			prefix = "> "
		}
		key := "   "
		if shortcut <= 9 {
			key = fmt.Sprintf("%d. ", shortcut)
		}
		line := truncateCells(prefix+key+item.label, width)
		if index == m.menuIndex {
			line = selectedMenuLineStyle(width, palette).Render(padCells(line, width))
		}
		lines = append(lines, line)
	}
	footer := "Enter/1-9 run  Esc close"
	if m.inMenuSubmenu() {
		footer = "Enter/1-9 run  b back  Esc close"
	}
	if len(m.menuItems) > visible {
		if m.inMenuSubmenu() {
			footer = fmt.Sprintf("Enter/1-9 run  b back  Esc close  Pg page  %d-%d/%d", start+1, end, len(m.menuItems))
		} else {
			footer = fmt.Sprintf("Enter/1-9 run  Esc close  Pg page  %d-%d/%d", start+1, end, len(m.menuItems))
		}
	}
	lines = append(lines, "", dim(footer))
	return lines
}

func (m clusterBrowserModel) renderFloatingMenu(view string) string {
	rect := m.menuRect
	if rect.w <= 0 || rect.h <= 0 {
		return view
	}
	lines := m.menuLines(max(1, rect.w-2))
	if len(lines) > max(0, rect.h-2) {
		lines = lines[:max(0, rect.h-2)]
	}
	box := floatingMenuStyle(rect.w, rect.h, actionMenuColors(m.menuContext)).Render(strings.Join(lines, "\n"))
	return overlayBlock(view, box, rect.x, rect.y, m.width)
}
