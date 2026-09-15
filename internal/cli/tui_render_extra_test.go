package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestFloatingMenuRenderingBranches(t *testing.T) {
	base := strings.Join([]string{
		"01234567890123456789",
		"01234567890123456789",
		"01234567890123456789",
		"01234567890123456789",
		"01234567890123456789",
		"01234567890123456789",
		"01234567890123456789",
		"01234567890123456789",
		"01234567890123456789",
	}, "\n")
	model := clusterBrowserModel{
		width:        20,
		height:       6,
		menuTitle:    "Actions",
		menuContext:  focusClusters,
		menuIndex:    2,
		menuOff:      1,
		menuFloating: true,
		menuRect:     tuiRect{x: 2, y: 1, w: 16, h: 8},
		menuItems: []tuiMenuItem{
			tuiMenuSection("Hidden"),
			{label: "Open", action: "open"},
			{label: "Close", action: "close"},
			{label: "Skip", action: ""},
			{label: "Refresh", action: "refresh"},
		},
	}
	rendered := model.renderFloatingMenu(base)
	if rendered == base || !strings.Contains(rendered, "Actions") || !strings.Contains(rendered, "Open") {
		t.Fatalf("rendered menu = %q", rendered)
	}
	if got := (clusterBrowserModel{}).renderFloatingMenu(base); got != base {
		t.Fatalf("empty rect should keep base view")
	}
	submenu := model
	submenu.menuTitle = "Repository"
	if lines := submenu.menuLines(14); !strings.Contains(strings.Join(lines, "\n"), "b back") {
		t.Fatalf("submenu lines = %#v", lines)
	}
	if got := actionMenuSubtitle(focusMembers); got != "selected member scope" {
		t.Fatalf("member subtitle = %q", got)
	}
	if got := actionMenuSubtitle(focusDetail); got != "detail scope" {
		t.Fatalf("detail subtitle = %q", got)
	}
	if got := actionMenuSubtitle(""); got != "current selection" {
		t.Fatalf("default subtitle = %q", got)
	}
	if palette := actionMenuColors(focusMembers); palette.accent == "" || palette.background == "" {
		t.Fatalf("member palette = %+v", palette)
	}
	if style := floatingMenuStyle(1, 1, actionMenuColors("")); style.GetWidth() != 1 || style.GetHeight() != 1 {
		t.Fatalf("minimum style size width=%d height=%d", style.GetWidth(), style.GetHeight())
	}
	if index, ok := visibleMenuShortcutIndex("2", model.menuItems, 1, 4); !ok || index != 2 {
		t.Fatalf("shortcut index=%d ok=%v", index, ok)
	}
	if _, ok := visibleMenuShortcutIndex("x", model.menuItems, 1, 4); ok {
		t.Fatal("non-numeric shortcut should not match")
	}
}

func TestTUIMenuNavigationAndWheelBranches(t *testing.T) {
	model := clusterBrowserModel{
		width:        100,
		height:       30,
		menuIndex:    0,
		menuOff:      4,
		menuFloating: true,
		menuRect:     tuiRect{x: 0, y: 0, w: 20, h: 8},
		menuItems: []tuiMenuItem{
			tuiMenuSection("top"),
			{label: "one", action: "one"},
			{label: "two", action: "two"},
			tuiMenuSection("middle"),
			{label: "three", action: "three"},
			{label: "four", action: "four"},
		},
		payload: clusterBrowserPayload{Clusters: []store.ClusterSummary{
			{ID: 10, Title: "first"},
			{ID: 11, Title: "second"},
		}},
	}
	if model.firstSelectableMenuIndex() != 1 || model.lastSelectableMenuIndex() != 5 {
		t.Fatalf("selectable bounds first=%d last=%d", model.firstSelectableMenuIndex(), model.lastSelectableMenuIndex())
	}
	if got := model.nextSelectableMenuIndex(1); got != 1 {
		t.Fatalf("next selectable = %d", got)
	}
	if got := model.nearestSelectableMenuIndex(3, 1); got != 4 {
		t.Fatalf("nearest forward = %d", got)
	}
	if got := model.nearestSelectableMenuIndex(3, -1); got != 2 {
		t.Fatalf("nearest backward = %d", got)
	}
	empty := clusterBrowserModel{}
	if got := empty.nearestSelectableMenuIndex(10, 1); got != 0 {
		t.Fatalf("empty nearest = %d", got)
	}
	model.menuIndex = 5
	model.keepMenuVisible()
	if model.menuOff > model.menuIndex {
		t.Fatalf("menu off=%d index=%d", model.menuOff, model.menuIndex)
	}
	layout := tuiLayout{
		clusters: tuiRect{x: 0, y: 2, w: 20, h: 8},
		members:  tuiRect{x: 20, y: 2, w: 20, h: 8},
		detail:   tuiRect{x: 40, y: 2, w: 20, h: 8},
	}
	if got := model.actionMenuContextAt(layout, 1, 3); got != focusClusters {
		t.Fatalf("cluster context = %q", got)
	}
	if got := model.actionMenuContextAt(layout, 21, 3); got != focusMembers {
		t.Fatalf("member context = %q", got)
	}
	if got := model.actionMenuContextAt(layout, 41, 3); got != focusDetail {
		t.Fatalf("detail context = %q", got)
	}
	if got := model.actionMenuContextAt(layout, 99, 99); got != "" {
		t.Fatalf("outside context = %q", got)
	}
	if index, ok := model.menuIndexAtMouse(layout, 1, 4); !ok || index != 6 {
		t.Fatalf("menu index at mouse index=%d ok=%v", index, ok)
	}
	model.menuFloating = false
	if index, ok := model.menuIndexAtMouse(layout, 41, 6); !ok || index != 5 {
		t.Fatalf("detail menu index at mouse index=%d ok=%v", index, ok)
	}
	if _, ok := model.menuIndexAtMouse(layout, 99, 99); ok {
		t.Fatal("outside mouse should not hit menu")
	}
	if step := (clusterBrowserModel{width: 100, height: 30}).pageStep(); step <= 0 {
		t.Fatalf("cluster page step = %d", step)
	}
	detailModel := clusterBrowserModel{focus: focusDetail}
	detailModel.detailView.Height = 3
	if step := detailModel.pageStep(); step != 3 {
		t.Fatalf("detail page step = %d", step)
	}
	model.selected = 0
	cmd := model.moveClusterByWheel(1)
	if cmd == nil || model.selected != 1 || model.status != "Cluster 11" {
		t.Fatalf("wheel move selected=%d status=%q cmd=%v", model.selected, model.status, cmd)
	}
	if cmd := model.moveClusterByWheel(1); cmd != nil {
		t.Fatalf("boundary wheel move should not tick: %v", cmd)
	}
	model.wheelDelta = -1
	model.wheelFocus = focusClusters
	if cmd := model.applyQueuedWheelScroll(); cmd == nil || model.focus != focusClusters {
		t.Fatalf("queued wheel cmd=%v focus=%q", cmd, model.focus)
	}
	model.wheelDelta = 0
	if cmd := model.applyQueuedWheelScroll(); cmd != nil {
		t.Fatalf("zero queued wheel should be nil: %v", cmd)
	}
}

func TestTUISelectionAndVisibilityHelperBranches(t *testing.T) {
	model := clusterBrowserModel{
		payload: clusterBrowserPayload{Limit: 2, Clusters: []store.ClusterSummary{
			{ID: 1, RepresentativeNumber: 101, MemberCount: 2, UpdatedAt: "2026-05-05T10:00:00Z"},
			{ID: 2, RepresentativeNumber: 202, MemberCount: 1, UpdatedAt: "2026-05-05T11:00:00Z"},
		}},
		allClusters: []store.ClusterSummary{
			{ID: 3, RepresentativeNumber: 303, MemberCount: 5, UpdatedAt: "2026-05-05T12:00:00Z"},
		},
		hasDetail: true,
		detail: store.ClusterDetail{
			Cluster: store.ClusterSummary{ID: 9, RepresentativeNumber: 909},
			Members: []store.ClusterMemberDetail{{
				Thread: store.Thread{Number: 909, State: "open"},
			}},
		},
		detailCache: map[string]store.ClusterDetail{
			clusterSummaryKey(store.ClusterSummary{ID: 8}): {Cluster: store.ClusterSummary{ID: 8}, Members: []store.ClusterMemberDetail{{Thread: store.Thread{Number: 808, State: "open"}}}},
		},
		memberRows: []memberRow{
			{label: "header"},
			{selectable: true, member: store.ClusterMemberDetail{Thread: store.Thread{Number: 202, State: "open"}}},
		},
	}
	if got := model.currentClusterID(); got != 1 {
		t.Fatalf("current cluster = %d", got)
	}
	if got := model.clusterRefreshLimit(); got != 2 {
		t.Fatalf("refresh limit = %d", got)
	}
	if got := model.findLoadedClusterIDForThreadNumber(909); got != 9 {
		t.Fatalf("detail cluster lookup = %d", got)
	}
	if got := model.findLoadedClusterIDForThreadNumber(808); got != 8 {
		t.Fatalf("cache cluster lookup = %d", got)
	}
	if got := model.findLoadedClusterIDForThreadNumber(303); got != 3 {
		t.Fatalf("working-set cluster lookup = %d", got)
	}
	if _, ok := model.clusterFromWorkingSet(404); ok {
		t.Fatal("missing cluster should not be found")
	}
	if !model.selectMemberByNumber(202) || model.memberIndex != 1 {
		t.Fatalf("member selection index = %d", model.memberIndex)
	}
	if model.selectMemberByNumber(999) {
		t.Fatal("missing member should not be selected")
	}
	openThread := store.Thread{State: "open"}
	closedThread := store.Thread{State: "closed"}
	localClosedThread := store.Thread{State: "open", ClosedAtLocal: "2026-05-05T00:00:00Z"}
	if !threadVisible(openThread, false) || threadVisible(closedThread, false) || threadVisible(localClosedThread, false) || !threadVisible(closedThread, true) {
		t.Fatal("thread visibility mismatch")
	}
	if got := memberDisplayState(store.ClusterMemberDetail{State: "removed", Thread: openThread}); got != "removed" {
		t.Fatalf("member state = %q", got)
	}
	if got := memberDisplayState(store.ClusterMemberDetail{Thread: localClosedThread}); got != "local" {
		t.Fatalf("local member state = %q", got)
	}
	if memberVisible(store.ClusterMemberDetail{State: "removed", Thread: openThread}, false) || !memberVisible(store.ClusterMemberDetail{State: "removed", Thread: closedThread}, true) {
		t.Fatal("member visibility mismatch")
	}
	noLimit := clusterBrowserModel{payload: clusterBrowserPayload{Clusters: model.payload.Clusters}, allClusters: model.allClusters}
	if got := noLimit.clusterRefreshLimit(); got < len(model.allClusters) {
		t.Fatalf("no-limit refresh limit = %d", got)
	}
}

func TestTUIJumpToThreadNumberLoadsClusterFromStore(t *testing.T) {
	st, repoID, clusterID := seedTUIDurableStore(t)
	defer st.Close()
	model := clusterBrowserModel{
		ctx:         context.Background(),
		store:       st,
		repoID:      repoID,
		detailCache: map[string]store.ClusterDetail{},
		payload:     clusterBrowserPayload{Limit: 1, Sort: "recent"},
		minSize:     99,
	}
	model.jumpToThreadNumber(0)
	if model.status != "Enter a positive issue or PR number" {
		t.Fatalf("bad jump status = %q", model.status)
	}
	model.jumpToThreadNumber(202)
	if model.focus != focusMembers || !strings.Contains(model.status, "Jumped to #202") {
		t.Fatalf("jump focus=%q status=%q", model.focus, model.status)
	}
	if len(model.payload.Clusters) == 0 || model.payload.Clusters[model.selected].ID != clusterID {
		t.Fatalf("selected clusters = %+v selected=%d want cluster %d", model.payload.Clusters, model.selected, clusterID)
	}
	if model.memberIndex < 0 || model.memberRows[model.memberIndex].thread().Number != 202 {
		t.Fatalf("member rows index=%d rows=%+v", model.memberIndex, model.memberRows)
	}
	if _, ok := model.detailCache[clusterSummaryKey(store.ClusterSummary{ID: clusterID, Source: store.ClusterSourceDurable})]; !ok {
		t.Fatalf("detail cache missing cluster %d", clusterID)
	}
	model.jumpToThreadNumber(999)
	if model.status == "" || strings.Contains(model.status, "Jumped") {
		t.Fatalf("missing jump status = %q", model.status)
	}
}

func TestTUISameNumericClusterIDsStaySourceScoped(t *testing.T) {
	raw := store.ClusterSummary{ID: 7, Source: store.ClusterSourceRun, RepresentativeNumber: 101, MemberCount: 2}
	durable := store.ClusterSummary{ID: 7, Source: store.ClusterSourceDurable, RepresentativeNumber: 201, MemberCount: 1}
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Clusters: []store.ClusterSummary{raw, durable},
	})
	model.detailCache[clusterSummaryKey(raw)] = store.ClusterDetail{
		Cluster: raw,
		Members: []store.ClusterMemberDetail{{Thread: store.Thread{Number: 101, State: "open"}}},
	}
	model.detailCache[clusterSummaryKey(durable)] = store.ClusterDetail{
		Cluster: durable,
		Members: []store.ClusterMemberDetail{{Thread: store.Thread{Number: 201, State: "open"}}},
	}

	model.selected = 1
	model.loadSelectedCluster()
	if model.detail.Cluster.Source != store.ClusterSourceDurable || len(model.detail.Members) != 1 || model.detail.Members[0].Thread.Number != 201 {
		t.Fatalf("durable selection loaded wrong same-ID detail: %#v", model.detail)
	}
	if !model.selectClusterForJump(durable) || model.selected != 1 {
		t.Fatalf("source-qualified jump selected index %d detail=%#v", model.selected, model.detail.Cluster)
	}
}

func TestTUIJumpKeyAndRefreshCommandBranches(t *testing.T) {
	input := textinput.New()
	input.SetValue("#0")
	model := clusterBrowserModel{searchInput: input, jumping: true}
	next, cmd := model.handleJumpKey(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd != nil || next.jumping || !strings.Contains(next.status, "expected positive") {
		t.Fatalf("bad enter next=%+v cmd=%v", next, cmd)
	}
	input = textinput.New()
	input.SetValue("https://github.com/openclaw/openclaw/issues/123")
	model = clusterBrowserModel{
		searchInput: input,
		jumping:     true,
		payload:     clusterBrowserPayload{Repository: "openclaw/openclaw", Clusters: []store.ClusterSummary{{ID: 1, RepresentativeNumber: 123}}},
		allClusters: []store.ClusterSummary{{ID: 1, RepresentativeNumber: 123}},
		detailCache: map[string]store.ClusterDetail{},
	}
	next, cmd = model.handleJumpKey(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd != nil || next.jumping || !strings.Contains(next.status, "outside loaded members") {
		t.Fatalf("valid enter next status=%q cmd=%v", next.status, cmd)
	}
	model = clusterBrowserModel{searchInput: textinput.New(), jumping: true}
	next, cmd = model.handleJumpKey(tea.KeyMsg{Type: tea.KeyEsc})
	if cmd != nil || next.jumping || next.status != "Jump cancelled" {
		t.Fatalf("esc next=%+v cmd=%v", next, cmd)
	}
	next, cmd = model.handleJumpKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")})
	if next.jumping != true {
		t.Fatalf("rune input should keep jump mode, next=%+v cmd=%v", next, cmd)
	}
	if (clusterBrowserModel{}).remoteRefreshTickCmd() == nil || (clusterBrowserModel{}).autoRefreshCmd() != nil || (clusterBrowserModel{store: &store.Store{}, repoID: 1}).autoRefreshCmd() == nil {
		t.Fatal("refresh tick commands should be scheduled")
	}
}

func TestInteractiveTUIFallsBackToJSONForNonFileOutput(t *testing.T) {
	app := New()
	var out bytes.Buffer
	app.Stdout = &out
	if app.canRunInteractiveTUI() {
		t.Fatal("buffer stdout should not be interactive")
	}
	payload := clusterBrowserPayload{Repository: "openclaw/openclaw", Mode: "clusters", Clusters: []store.ClusterSummary{{ID: 1, MemberCount: 2}}}
	if err := app.runInteractiveTUI(context.Background(), nil, 0, payload); err != nil {
		t.Fatalf("run tui fallback: %v", err)
	}
	if !strings.Contains(out.String(), `"repository": "openclaw/openclaw"`) || !strings.Contains(out.String(), `"clusters"`) {
		t.Fatalf("fallback tui output = %q", out.String())
	}
}
