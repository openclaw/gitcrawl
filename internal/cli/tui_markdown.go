package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

func wrapPlain(value string, width int) []string {
	width = max(20, width)
	words := strings.Fields(value)
	if len(words) == 0 {
		return []string{""}
	}
	var lines []string
	var line string
	for _, word := range words {
		if lipgloss.Width(word) > width {
			if line != "" {
				lines = append(lines, line)
				line = ""
			}
			lines = append(lines, truncateCells(word, width))
			continue
		}
		if lipgloss.Width(line)+1+lipgloss.Width(word) > width && line != "" {
			lines = append(lines, line)
			line = word
			continue
		}
		if line == "" {
			line = word
		} else {
			line += " " + word
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

func markdownLines(value string, width int) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	width = max(20, width)
	var lines []string
	inFence := false
	blankRun := 0
	for _, rawLine := range strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n") {
		line := strings.TrimRight(stripTerminalControls(rawLine), " \t")
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inFence = !inFence
			lines = append(lines, dim("--- code ---"))
			blankRun = 0
			continue
		}
		if inFence {
			lines = append(lines, dim(truncateCells(line, width)))
			blankRun = 0
			continue
		}
		if trimmed == "" {
			blankRun++
			if blankRun <= 1 {
				lines = append(lines, "")
			}
			continue
		}
		blankRun = 0
		if match := markdownHeadingRE.FindStringSubmatch(trimmed); match != nil {
			lines = appendWrappedStyled(lines, "", renderInlineMarkdown(match[2]), width, bold)
			continue
		}
		if strings.HasPrefix(trimmed, ">") {
			quote := strings.TrimSpace(strings.TrimPrefix(trimmed, ">"))
			lines = appendWrappedStyled(lines, "> ", renderInlineMarkdown(quote), width, dim)
			continue
		}
		if match := markdownListRE.FindStringSubmatch(line); match != nil {
			indent := match[1]
			if lipgloss.Width(indent) > 4 {
				indent = strings.Repeat(" ", 4)
			}
			lines = appendWrappedStyled(lines, indent+"- ", renderInlineMarkdown(match[3]), width, nil)
			continue
		}
		lines = appendWrappedStyled(lines, "", renderInlineMarkdown(line), width, nil)
	}
	return trimTrailingBlankLines(lines)
}

func appendWrappedStyled(lines []string, prefix, value string, width int, styler func(string) string) []string {
	contentWidth := max(8, width-lipgloss.Width(prefix))
	wrapped := wrapPlain(value, contentWidth)
	if len(wrapped) == 0 {
		return lines
	}
	continuation := strings.Repeat(" ", lipgloss.Width(prefix))
	for index, line := range wrapped {
		prefixForLine := prefix
		if index > 0 {
			prefixForLine = continuation
		}
		if styler != nil {
			line = styler(line)
		}
		lines = append(lines, prefixForLine+line)
	}
	return lines
}

func renderInlineMarkdown(value string) string {
	value = markdownLinkRE.ReplaceAllString(value, "$1 <$2>")
	replacer := strings.NewReplacer(
		"`", "",
		"**", "",
		"__", "",
		"~~", "",
	)
	return strings.TrimSpace(replacer.Replace(value))
}

func firstMarkdownLink(value string) (string, bool) {
	links := markdownLinks(value)
	if len(links) == 0 {
		return "", false
	}
	return links[0], true
}

func markdownLinks(value string) []string {
	links := make([]string, 0, 2)
	seen := map[string]bool{}
	for _, match := range markdownLinkRE.FindAllStringSubmatch(value, -1) {
		if len(match) > 2 {
			link := stripTrailingURLPunctuation(match[2])
			if !seen[link] {
				links = append(links, link)
				seen[link] = true
			}
		}
	}
	for _, match := range bareLinkRE.FindAllStringSubmatch(value, -1) {
		if len(match) > 2 {
			link := stripTrailingURLPunctuation(match[2])
			if !seen[link] {
				links = append(links, link)
				seen[link] = true
			}
		}
	}
	return links
}

func formatLinkChoiceLabel(url string, index int) string {
	return fmt.Sprintf("%2d  %s", index+1, url)
}

func stripTrailingURLPunctuation(value string) string {
	return strings.TrimRight(value, ".,;:!?")
}

func stripTerminalControls(value string) string {
	return terminalControlRE.ReplaceAllString(value, "")
}

func trimTrailingBlankLines(lines []string) []string {
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}
