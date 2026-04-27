package cli

import (
	"context"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestTUILayoutStacksNarrowTerminals(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 80
	model.height = 24

	layout := model.layout()
	if !layout.stacked {
		t.Fatal("expected narrow terminal to use stacked layout")
	}
	if layout.members.x != 0 || layout.members.y <= layout.clusters.y {
		t.Fatalf("expected members pane below clusters, got clusters=%+v members=%+v", layout.clusters, layout.members)
	}

	view := model.View()
	for _, label := range []string{"[*] Clusters", "[ ] Members", "[ ] Detail"} {
		if !strings.Contains(view, label) {
			t.Fatalf("expected view to contain %q", label)
		}
	}
}

func TestTUIMouseSelectsClusterRows(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	layout := model.layout()

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	if model.selected != 0 {
		t.Fatalf("first row click selected %d, want 0", model.selected)
	}

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	if model.selected != 1 {
		t.Fatalf("second row click selected %d, want 1", model.selected)
	}
}

func TestTUIMouseIgnoresRightClick(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "recent",
		Clusters:   sampleTUIClusters(),
	})
	model.width = 140
	model.height = 32
	layout := model.layout()
	model.selected = 1

	model.handleMouse(tea.MouseMsg{
		X:      layout.clusters.x + 2,
		Y:      layout.clusters.y + 4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonRight,
	})
	if model.selected != 1 {
		t.Fatalf("right click changed selected cluster to %d", model.selected)
	}
}

func sampleTUIClusters() []store.ClusterSummary {
	return []store.ClusterSummary{
		{
			ID:                   1,
			StableSlug:           "alpha-bravo-charlie",
			Status:               "active",
			RepresentativeKind:   "issue",
			RepresentativeTitle:  "First issue",
			RepresentativeNumber: 11,
			MemberCount:          3,
			UpdatedAt:            "2026-04-27T10:00:00Z",
		},
		{
			ID:                   2,
			StableSlug:           "delta-echo-foxtrot",
			Status:               "active",
			RepresentativeKind:   "pull_request",
			RepresentativeTitle:  "Second PR",
			RepresentativeNumber: 12,
			MemberCount:          5,
			UpdatedAt:            "2026-04-27T11:00:00Z",
		},
	}
}
