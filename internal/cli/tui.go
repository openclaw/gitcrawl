package cli

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/vector"
)

var (
	markdownLinkRE    = regexp.MustCompile(`\[([^\]]+)\]\((https?://[^)\s]+)\)`)
	bareLinkRE        = regexp.MustCompile(`(^|[\s(<])(https?://[^\s<>)]+)`)
	markdownHeadingRE = regexp.MustCompile(`^(#{1,6})\s+(.+)$`)
	markdownListRE    = regexp.MustCompile(`^(\s*)([-*+]|\d+[.)])\s+(.+)$`)
	terminalControlRE = regexp.MustCompile(`[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]`)
	summaryKeyOrder   = []string{"key_summary", "problem_summary", "solution_summary", "maintainer_signal_summary", "dedupe_summary"}
)

const tuiAutoRefreshInterval = 15 * time.Second
const tuiWheelScrollDelay = 16 * time.Millisecond
const tuiWheelMaxBufferedDelta = 6
const tuiWheelSettleDelay = 90 * time.Millisecond

type tuiAutoRefreshMsg struct{}
type tuiRemoteRefreshTickMsg struct{}
type tuiWheelScrollMsg struct {
	seq int
}
type tuiWheelSettledMsg struct {
	seq int
}
type tuiNeighborsLoadedMsg struct {
	seq          int
	threadID     int64
	threadNumber int
	neighbors    []tuiNeighbor
	err          error
}

type tuiRemoteRefreshMsg struct {
	changed bool
	err     error
}

type clusterBrowserPayload struct {
	Repository         string                 `json:"repository"`
	InferredRepository bool                   `json:"inferred_repository"`
	Mode               string                 `json:"mode"`
	DBSource           string                 `json:"db_source,omitempty"`
	DBLocation         string                 `json:"db_location,omitempty"`
	DBRefreshSource    string                 `json:"-"`
	DBRuntimePath      string                 `json:"-"`
	ConfigPath         string                 `json:"-"`
	Sort               string                 `json:"sort"`
	Layout             string                 `json:"layout,omitempty"`
	MinSize            int                    `json:"min_size"`
	Limit              int                    `json:"limit,omitempty"`
	HideClosed         bool                   `json:"hide_closed,omitempty"`
	EmbedModel         string                 `json:"embed_model,omitempty"`
	EmbeddingBasis     string                 `json:"embedding_basis,omitempty"`
	VectorBackend      string                 `json:"vector_backend,omitempty"`
	Clusters           []store.ClusterSummary `json:"clusters"`
}

type tuiFocus string

const (
	focusClusters tuiFocus = "clusters"
	focusMembers  tuiFocus = "members"
	focusDetail   tuiFocus = "detail"
)

type tuiMemberSort string

const (
	memberSortKind   tuiMemberSort = "kind"
	memberSortRecent tuiMemberSort = "recent"
	memberSortOldest tuiMemberSort = "oldest"
	memberSortNumber tuiMemberSort = "number"
	memberSortState  tuiMemberSort = "state"
	memberSortTitle  tuiMemberSort = "title"
)

type tuiWideLayout string

const (
	wideLayoutColumns    tuiWideLayout = "columns"
	wideLayoutRightStack tuiWideLayout = "right-stack"
	wideLayoutFocus      tuiWideLayout = "focus"
)

type tuiRect struct {
	x int
	y int
	w int
	h int
}

type clusterBrowserModel struct {
	payload          clusterBrowserPayload
	allClusters      []store.ClusterSummary
	ctx              context.Context
	store            *store.Store
	repoID           int64
	focus            tuiFocus
	width            int
	height           int
	status           string
	search           string
	searching        bool
	searchBeforeEdit string
	jumping          bool
	showHelp         bool
	menuOpen         bool
	menuTitle        string
	menuContext      tuiFocus
	menuIndex        int
	menuOff          int
	menuItems        []tuiMenuItem
	menuFloating     bool
	menuRect         tuiRect
	quitRequested    bool
	showClosed       bool
	compactDetail    bool
	minSize          int
	memberSort       tuiMemberSort
	wideLayout       tuiWideLayout
	selected         int
	clusterOff       int
	memberRows       []memberRow
	memberOff        int
	memberIndex      int
	lastClickFocus   tuiFocus
	lastClickIndex   int
	lastClickX       int
	lastClickY       int
	lastClickAt      time.Time
	wheelScrollSeq   int
	wheelPending     bool
	wheelFocus       tuiFocus
	wheelDelta       int
	wheelSeq         int
	neighborLoadSeq  int
	detailView       viewport.Model
	detailContentKey string
	searchInput      textinput.Model
	detailCache      map[string]store.ClusterDetail
	neighborCache    map[int64][]tuiNeighbor
	pendingCmd       tea.Cmd
	neighborLoadStop context.CancelFunc
	queryNeighbors   func(context.Context, []vector.Item, []float64, vector.QueryOptions) ([]vector.Neighbor, error)
	detail           store.ClusterDetail
	hasDetail        bool
	remoteRefreshing bool
	remoteFrame      int
}

type memberRow struct {
	member     store.ClusterMemberDetail
	label      string
	selectable bool
}

const tuiDoubleClickWindow = 450 * time.Millisecond

const (
	tuiOpenRowFG            = "#f2c94c"
	tuiOpenRowBG            = "#14130f"
	tuiOpenSelectedFG       = "#f2c94c"
	tuiOpenSelectedBG       = "#1d1e18"
	tuiOpenSelectedBlurFG   = "#c3b66f"
	tuiOpenSelectedBlurBG   = "#171711"
	tuiClosedRowFG          = "#8793a3"
	tuiClosedRowBG          = "#0f141b"
	tuiClosedSelectedFG     = "#d6dde8"
	tuiClosedSelectedBG     = "#303744"
	tuiClosedSelectedBlurFG = "#aab2bf"
	tuiClosedSelectedBlurBG = "#242936"
	tuiMutedAccent          = "#8fb8d8"
)

type tuiNeighbor struct {
	Thread store.Thread
	Score  float64
}

func (a *App) canRunInteractiveTUI() bool {
	out, ok := a.Stdout.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(out.Fd()) && isatty.IsTerminal(os.Stdin.Fd())
}

func (a *App) runInteractiveTUI(ctx context.Context, st *store.Store, repoID int64, payload clusterBrowserPayload) error {
	out, ok := a.Stdout.(*os.File)
	if !ok {
		return a.writeOutput("tui", payload, true)
	}
	return a.withTUIRenderer(func() error {
		tuiCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		model := newClusterBrowserModel(tuiCtx, st, repoID, payload)
		program := tea.NewProgram(model, tea.WithInput(os.Stdin), tea.WithOutput(out), tea.WithAltScreen(), tea.WithMouseAllMotion())
		finalModel, err := program.Run()
		cancel()
		if final, ok := finalModel.(clusterBrowserModel); ok && final.store != nil && final.store != st {
			final.cancelNeighborLoad()
			_ = final.store.Close()
		}
		return err
	})
}

func newClusterBrowserModel(ctx context.Context, st *store.Store, repoID int64, payload clusterBrowserPayload) clusterBrowserModel {
	clusters := append([]store.ClusterSummary(nil), payload.Clusters...)
	payload.Clusters = clusters
	search := textinput.New()
	search.Prompt = "/ "
	search.Placeholder = "filter clusters"
	search.CharLimit = 80
	search.Width = 40
	model := clusterBrowserModel{
		payload:        payload,
		allClusters:    clusters,
		ctx:            ctx,
		store:          st,
		repoID:         repoID,
		focus:          focusClusters,
		status:         "Ready",
		showClosed:     !payload.HideClosed,
		minSize:        max(1, payload.MinSize),
		memberSort:     memberSortKind,
		wideLayout:     normalizeTUILayout(payload.Layout),
		memberIndex:    -1,
		detailView:     viewport.New(1, 1),
		searchInput:    search,
		detailCache:    map[string]store.ClusterDetail{},
		neighborCache:  map[int64][]tuiNeighbor{},
		queryNeighbors: vector.QueryWithOptions,
	}
	if payload.DBSource == "remote" && payload.DBRefreshSource != "" && payload.DBRuntimePath != "" {
		model.remoteRefreshing = true
		model.status = "Refreshing remote data"
	}
	model.applyClusterFilters()
	model.loadSelectedCluster()
	return model
}

func (m clusterBrowserModel) Init() tea.Cmd {
	cmds := []tea.Cmd{m.autoRefreshCmd()}
	if m.remoteRefreshing {
		cmds = append(cmds, m.remoteRefreshCmd(), m.remoteRefreshTickCmd())
	}
	return tea.Batch(cmds...)
}

func (m clusterBrowserModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tuiAutoRefreshMsg:
		if m.menuOpen || m.searching || m.jumping {
			return m, m.autoRefreshCmd()
		}
		m.autoRefreshFromStore()
		m.keepVisible()
		m.syncComponents()
		return m, m.autoRefreshCmd()
	case tuiWheelScrollMsg:
		if msg.seq != m.wheelScrollSeq {
			return m, nil
		}
		cmd := m.applyQueuedWheelScroll()
		m.keepVisible()
		m.syncComponents()
		return m, cmd
	case tuiWheelSettledMsg:
		if msg.seq != m.wheelSeq {
			return m, nil
		}
		m.loadSelectedCluster()
		m.keepVisible()
		m.syncComponents()
		return m, nil
	case tuiRemoteRefreshTickMsg:
		if !m.remoteRefreshing {
			return m, nil
		}
		m.remoteFrame++
		return m, m.remoteRefreshTickCmd()
	case tuiRemoteRefreshMsg:
		m.remoteRefreshing = false
		if msg.err != nil {
			m.status = "Remote refresh failed: " + msg.err.Error()
			return m, nil
		}
		if msg.changed {
			if err := m.reopenRuntimeStore(); err != nil {
				m.status = "Remote refresh loaded but reopen failed: " + err.Error()
				return m, nil
			}
			m.refreshFromStore()
			m.keepVisible()
			m.syncComponents()
			m.status = "Remote data refreshed"
			return m, nil
		}
		m.status = "Remote data already current"
		return m, nil
	case tuiNeighborsLoadedMsg:
		if msg.seq != m.neighborLoadSeq {
			return m, nil
		}
		m.neighborLoadStop = nil
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.applyLoadedNeighbors(msg.threadID, msg.threadNumber, msg.neighbors)
		m.keepVisible()
		m.syncComponents()
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.syncComponents()
		m.keepVisible()
	case tea.KeyMsg:
		m.cancelQueuedWheelScroll()
		if m.menuOpen {
			var cmd tea.Cmd
			next, cmd := m.updateMenu(msg)
			m = next.(clusterBrowserModel)
			m.keepVisible()
			m.syncComponents()
			return m, cmd
		}
		if m.searching {
			var cmd tea.Cmd
			m, cmd = m.handleSearchKey(msg)
			m.keepVisible()
			m.syncComponents()
			return m, cmd
		}
		if m.jumping {
			var cmd tea.Cmd
			m, cmd = m.handleJumpKey(msg)
			m.keepVisible()
			m.syncComponents()
			return m, cmd
		}
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab", "right":
			m.focus = nextFocus(m.focus, 1)
		case "shift+tab", "left":
			m.focus = nextFocus(m.focus, -1)
		case "up", "k":
			m.move(-1)
		case "down", "j":
			m.move(1)
		case "pgup", "ctrl+b":
			m.move(-m.pageStep())
		case "pgdown", "ctrl+f":
			m.move(m.pageStep())
		case "home", "g":
			m.jumpEdge(false)
		case "end", "G":
			m.jumpEdge(true)
		case "enter":
			if m.focus == focusClusters {
				m.focus = focusMembers
			} else if m.focus == focusMembers {
				m.focus = focusDetail
				cmd := m.requestSelectedThreadNeighbors(10, 0.2)
				m.keepVisible()
				m.syncComponents()
				return m, cmd
			}
		case "o":
			m.runAction("open")
		case "c":
			m.runAction("copy-url")
		case "a":
			m.clearMenuPlacement()
			m.openActionMenu()
		case "s":
			if m.payload.Sort == "recent" {
				m.payload.Sort = "size"
			} else {
				m.payload.Sort = "recent"
			}
			m.sortClustersPreservingSelection()
			m.loadSelectedCluster()
			m.status = "Sort: " + m.payload.Sort
		case "m":
			m.memberSort = nextMemberSort(m.memberSort)
			m.sortMembers()
			m.status = "Member sort: " + string(m.memberSort)
		case "n":
			cmd := m.requestSelectedThreadNeighbors(10, 0.2)
			m.keepVisible()
			m.syncComponents()
			return m, cmd
		case "d":
			m.toggleDetailMode()
		case "l":
			m.toggleWideLayout()
		case "p":
			m.openRepositoryMenu()
		case "r":
			m.refreshFromStore()
		case "f":
			m.minSize = nextMinSize(m.minSize)
			m.applyClusterFilters()
			m.status = fmt.Sprintf("Min size: %s", minSizeLabel(m.minSize))
		case "x":
			m.toggleClosedVisibility()
		case "/":
			cmd := m.startFilterInput()
			m.keepVisible()
			m.syncComponents()
			return m, cmd
		case "#":
			cmd := m.startJumpInput()
			m.keepVisible()
			m.syncComponents()
			return m, cmd
		case "esc":
			if m.showHelp {
				m.showHelp = false
			}
		case "h", "?":
			m.showHelp = !m.showHelp
			if m.showHelp {
				m.status = "Help"
			} else {
				m.status = "Ready"
			}
		}
		m.keepVisible()
		m.syncComponents()
	case tea.MouseMsg:
		cmd := m.handleMouse(msg)
		if m.quitRequested {
			return m, tea.Quit
		}
		m.keepVisible()
		m.syncComponents()
		return m, cmd
	}
	return m, nil
}

func (m clusterBrowserModel) remoteRefreshCmd() tea.Cmd {
	sourceDBPath := m.payload.DBRefreshSource
	runtimeDBPath := m.payload.DBRuntimePath
	return func() tea.Msg {
		changed, err := refreshPortableRuntimeDB(m.ctx, sourceDBPath, runtimeDBPath, true, m.payload.ConfigPath)
		return tuiRemoteRefreshMsg{changed: changed, err: err}
	}
}

func (m clusterBrowserModel) remoteRefreshTickCmd() tea.Cmd {
	return tea.Tick(120*time.Millisecond, func(time.Time) tea.Msg {
		return tuiRemoteRefreshTickMsg{}
	})
}

func (m *clusterBrowserModel) reopenRuntimeStore() error {
	if strings.TrimSpace(m.payload.DBRuntimePath) == "" {
		return nil
	}
	m.invalidateNeighborLoad()
	next, err := store.OpenReadOnly(m.ctx, m.payload.DBRuntimePath)
	if err != nil {
		return err
	}
	if m.store != nil {
		_ = m.store.Close()
	}
	m.store = next
	m.detailCache = map[string]store.ClusterDetail{}
	m.neighborCache = map[int64][]tuiNeighbor{}
	return nil
}
