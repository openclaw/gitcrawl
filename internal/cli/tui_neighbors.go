package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/vector"
)

func (m *clusterBrowserModel) loadSelectedThreadNeighbors(limit int, threshold float64) {
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return
	}
	threadID, threadNumber, neighbors, err := loadThreadNeighbors(m.ctx, m.store, m.repoID, m.payload, m.queryNeighbors, thread.Number, limit, threshold)
	if err != nil {
		m.status = err.Error()
		return
	}
	m.applyLoadedNeighbors(threadID, threadNumber, neighbors)
}

func (m *clusterBrowserModel) requestSelectedThreadNeighbors(limit int, threshold float64) tea.Cmd {
	thread, ok := m.selectedThread()
	if !ok {
		m.status = "No selected thread"
		return nil
	}
	if m.store == nil || m.repoID == 0 {
		m.status = "Neighbors unavailable for this view"
		return nil
	}
	if limit <= 0 {
		limit = 10
	}
	if threshold <= 0 {
		threshold = 0.2
	}
	m.cancelNeighborLoad()
	m.neighborLoadSeq++
	seq := m.neighborLoadSeq
	loadCtx, cancel := context.WithCancel(m.ctx)
	m.neighborLoadStop = cancel
	repoID := m.repoID
	payload := m.payload
	queryNeighbors := m.queryNeighbors
	threadNumber := thread.Number
	m.focus = focusDetail
	m.detailView.GotoTop()
	m.status = fmt.Sprintf("Loading neighbors for #%d...", threadNumber)
	return func() tea.Msg {
		defer cancel()
		threadID, resolvedNumber, neighbors, err := loadThreadNeighbors(loadCtx, m.store, repoID, payload, queryNeighbors, threadNumber, limit, threshold)
		return tuiNeighborsLoadedMsg{
			seq:          seq,
			threadID:     threadID,
			threadNumber: resolvedNumber,
			neighbors:    neighbors,
			err:          err,
		}
	}
}

func (m *clusterBrowserModel) applyLoadedNeighbors(threadID int64, threadNumber int, neighbors []tuiNeighbor) {
	m.neighborCache[threadID] = neighbors
	if thread, ok := m.selectedThread(); ok && thread.ID == threadID {
		m.focus = focusDetail
		m.detailView.GotoTop()
	}
	m.status = fmt.Sprintf("Loaded %d neighbors for #%d", len(neighbors), threadNumber)
}

func (m *clusterBrowserModel) takePendingCmd() tea.Cmd {
	cmd := m.pendingCmd
	m.pendingCmd = nil
	return cmd
}

func (m *clusterBrowserModel) cancelNeighborLoad() {
	if m.neighborLoadStop != nil {
		m.neighborLoadStop()
		m.neighborLoadStop = nil
	}
}

func (m *clusterBrowserModel) invalidateNeighborLoad() {
	m.cancelNeighborLoad()
	m.neighborLoadSeq++
}

func loadThreadNeighbors(ctx context.Context, st *store.Store, repoID int64, payload clusterBrowserPayload, queryNeighbors func(context.Context, []vector.Item, []float64, vector.QueryOptions) ([]vector.Neighbor, error), number, limit int, threshold float64) (int64, int, []tuiNeighbor, error) {
	if st == nil || repoID == 0 {
		return 0, 0, nil, errors.New("neighbors unavailable for this view")
	}
	if limit <= 0 {
		limit = 10
	}
	if threshold <= 0 {
		threshold = 0.2
	}
	targetThread, targetVector, err := st.ThreadVectorByNumber(ctx, store.ThreadVectorQuery{
		RepoID: repoID,
		Model:  payload.EmbedModel,
		Basis:  payload.EmbeddingBasis,
	}, number)
	if err != nil {
		var fallbackErr error
		targetThread, targetVector, fallbackErr = st.ThreadVectorByNumber(ctx, store.ThreadVectorQuery{RepoID: repoID}, number)
		if fallbackErr != nil {
			return 0, 0, nil, err
		}
	}
	vectors, err := st.ListThreadVectorsFiltered(ctx, store.ThreadVectorQuery{
		RepoID:     repoID,
		Model:      targetVector.Model,
		Basis:      targetVector.Basis,
		Dimensions: targetVector.Dimensions,
	})
	if err != nil {
		return 0, 0, nil, err
	}
	items := make([]vector.Item, 0, len(vectors))
	for _, stored := range vectors {
		items = append(items, vector.Item{ThreadID: stored.ThreadID, Vector: stored.Vector})
	}
	if queryNeighbors == nil {
		queryNeighbors = vector.QueryWithOptions
	}
	backend := strings.TrimSpace(payload.VectorBackend)
	if backend == "" {
		backend = "exact"
	}
	candidates, err := queryNeighbors(ctx, items, targetVector.Vector, vector.QueryOptions{
		Backend:         backend,
		Limit:           limit * 2,
		ExcludeThreadID: targetThread.ID,
	})
	if err != nil {
		return 0, 0, nil, err
	}
	filtered := make([]vector.Neighbor, 0, limit)
	for _, candidate := range candidates {
		if candidate.Score < threshold {
			continue
		}
		filtered = append(filtered, candidate)
		if len(filtered) >= limit {
			break
		}
	}
	ids := make([]int64, 0, len(filtered))
	for _, candidate := range filtered {
		ids = append(ids, candidate.ThreadID)
	}
	threads, err := st.ThreadsByIDs(ctx, repoID, ids)
	if err != nil {
		return 0, 0, nil, err
	}
	neighbors := make([]tuiNeighbor, 0, len(filtered))
	for _, candidate := range filtered {
		neighborThread, ok := threads[candidate.ThreadID]
		if !ok {
			continue
		}
		neighbors = append(neighbors, tuiNeighbor{Thread: neighborThread, Score: candidate.Score})
	}
	return targetThread.ID, targetThread.Number, neighbors, nil
}
