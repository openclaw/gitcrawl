package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/mattn/go-isatty"
	"github.com/openclaw/gitcrawl/internal/store"
	"golang.org/x/term"
)

type clusterBrowserPayload struct {
	Repository         string                 `json:"repository"`
	InferredRepository bool                   `json:"inferred_repository"`
	Mode               string                 `json:"mode"`
	Sort               string                 `json:"sort"`
	Clusters           []store.ClusterSummary `json:"clusters"`
}

type clusterBrowserModel struct {
	payload  clusterBrowserPayload
	selected int
	offset   int
	width    int
	height   int
}

func (a *App) canRunInteractiveTUI() bool {
	out, ok := a.Stdout.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(out.Fd()) && isatty.IsTerminal(os.Stdin.Fd())
}

func (a *App) runInteractiveTUI(payload clusterBrowserPayload) error {
	out, ok := a.Stdout.(*os.File)
	if !ok {
		return a.writeOutput("tui", payload, true)
	}
	return runClusterBrowserTUI(os.Stdin, out, payload)
}

func runClusterBrowserTUI(in *os.File, out *os.File, payload clusterBrowserPayload) error {
	oldState, err := term.MakeRaw(int(in.Fd()))
	if err != nil {
		return fmt.Errorf("enter raw terminal mode: %w", err)
	}
	defer func() {
		_ = term.Restore(int(in.Fd()), oldState)
	}()

	fmt.Fprint(out, "\x1b[?1049h\x1b[?25l")
	defer fmt.Fprint(out, "\x1b[?25h\x1b[?1049l")

	model := clusterBrowserModel{payload: payload}
	if err := renderClusterBrowser(out, &model); err != nil {
		return err
	}

	var buf [16]byte
	for {
		n, err := in.Read(buf[:])
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		if handleClusterBrowserInput(&model, string(buf[:n])) {
			return nil
		}
		if err := renderClusterBrowser(out, &model); err != nil {
			return err
		}
	}
}

func handleClusterBrowserInput(model *clusterBrowserModel, input string) bool {
	switch input {
	case "q", "Q", "\x03":
		return true
	case "j", "\x1b[B":
		moveClusterSelection(model, 1)
	case "k", "\x1b[A":
		moveClusterSelection(model, -1)
	case "\x06", "\x1b[6~":
		moveClusterSelection(model, visibleClusterRows(model)-1)
	case "\x02", "\x1b[5~":
		moveClusterSelection(model, -(visibleClusterRows(model) - 1))
	case "g":
		model.selected = 0
	case "G":
		if len(model.payload.Clusters) > 0 {
			model.selected = len(model.payload.Clusters) - 1
		}
	}
	keepSelectionVisible(model)
	return false
}

func moveClusterSelection(model *clusterBrowserModel, delta int) {
	if len(model.payload.Clusters) == 0 {
		model.selected = 0
		return
	}
	model.selected += delta
	if model.selected < 0 {
		model.selected = 0
	}
	if model.selected >= len(model.payload.Clusters) {
		model.selected = len(model.payload.Clusters) - 1
	}
}

func keepSelectionVisible(model *clusterBrowserModel) {
	rows := visibleClusterRows(model)
	if rows <= 0 {
		model.offset = 0
		return
	}
	if model.selected < model.offset {
		model.offset = model.selected
	}
	if model.selected >= model.offset+rows {
		model.offset = model.selected - rows + 1
	}
	if model.offset < 0 {
		model.offset = 0
	}
}

func renderClusterBrowser(out io.Writer, model *clusterBrowserModel) error {
	width, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || width <= 0 || height <= 0 {
		width, height = 100, 32
	}
	model.width = width
	model.height = height
	keepSelectionVisible(model)

	var b strings.Builder
	b.WriteString("\x1b[H\x1b[2J")
	writeTUILine(&b, width, "\x1b[1mGitcrawl\x1b[0m  "+model.payload.Repository+"  sort="+model.payload.Sort)
	if model.payload.InferredRepository {
		writeTUILine(&b, width, "repo inferred from local database")
	} else {
		writeTUILine(&b, width, "")
	}
	writeTUILine(&b, width, strings.Repeat("-", width))

	rows := visibleClusterRows(model)
	if len(model.payload.Clusters) == 0 {
		writeTUILine(&b, width, "no clusters found")
	} else {
		end := model.offset + rows
		if end > len(model.payload.Clusters) {
			end = len(model.payload.Clusters)
		}
		for i := model.offset; i < end; i++ {
			cluster := model.payload.Clusters[i]
			marker := " "
			styleStart := ""
			styleEnd := ""
			if i == model.selected {
				marker = ">"
				styleStart = "\x1b[7m"
				styleEnd = "\x1b[0m"
			}
			number := ""
			if cluster.RepresentativeNumber > 0 {
				number = fmt.Sprintf(" #%d", cluster.RepresentativeNumber)
			}
			line := fmt.Sprintf("%s %-24s %4d  %-12s%s  %s",
				marker,
				truncateRunes(cluster.StableSlug, 24),
				cluster.MemberCount,
				truncateRunes(cluster.RepresentativeKind, 12),
				number,
				firstNonEmpty(cluster.RepresentativeTitle, cluster.Title),
			)
			writeTUIStyledLine(&b, width, styleStart, line, styleEnd)
		}
	}

	for currentVisualLineCount(b.String()) < height-6 {
		writeTUILine(&b, width, "")
	}
	writeTUILine(&b, width, strings.Repeat("-", width))
	writeClusterDetail(&b, width, model)
	writeTUILine(&b, width, "j/k or arrows move  g/G jump  ctrl-f/ctrl-b page  q quit")
	_, err = io.WriteString(out, b.String())
	return err
}

func writeClusterDetail(b *strings.Builder, width int, model *clusterBrowserModel) {
	if len(model.payload.Clusters) == 0 {
		writeTUILine(b, width, "")
		writeTUILine(b, width, "")
		return
	}
	cluster := model.payload.Clusters[model.selected]
	writeTUILine(b, width, fmt.Sprintf("%s  %s  members=%d  status=%s",
		cluster.StableSlug,
		threadRef(cluster),
		cluster.MemberCount,
		firstNonEmpty(cluster.Status, "unknown"),
	))
	writeTUILine(b, width, firstNonEmpty(cluster.RepresentativeTitle, cluster.Title))
	writeTUILine(b, width, "updated "+cluster.UpdatedAt)
}

func threadRef(cluster store.ClusterSummary) string {
	if cluster.RepresentativeNumber == 0 {
		return ""
	}
	if cluster.RepresentativeKind == "" {
		return fmt.Sprintf("#%d", cluster.RepresentativeNumber)
	}
	return fmt.Sprintf("%s #%d", cluster.RepresentativeKind, cluster.RepresentativeNumber)
}

func visibleClusterRows(model *clusterBrowserModel) int {
	rows := model.height - 9
	if rows < 1 {
		return 1
	}
	return rows
}

func writeTUILine(b *strings.Builder, width int, line string) {
	if width <= 0 {
		width = 80
	}
	b.WriteString(truncateRunes(line, width))
	b.WriteString("\r\n")
}

func writeTUIStyledLine(b *strings.Builder, width int, styleStart, line, styleEnd string) {
	if width <= 0 {
		width = 80
	}
	b.WriteString(styleStart)
	b.WriteString(truncateRunes(line, width))
	b.WriteString(styleEnd)
	b.WriteString("\r\n")
}

func truncateRunes(value string, max int) string {
	if max <= 0 {
		return ""
	}
	if utf8.RuneCountInString(value) <= max {
		return value
	}
	if max <= 3 {
		return strings.Repeat(".", max)
	}
	runes := []rune(value)
	return string(runes[:max-3]) + "..."
}

func currentVisualLineCount(value string) int {
	return strings.Count(value, "\n")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
