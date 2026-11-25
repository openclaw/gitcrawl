package acquireapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
)

type cycleGraphFixture struct {
	*emptyCommitProvider
	LifecycleProvider
}

type cycleFixtureAPI struct{ *Client }

// The existing lifecycle fixture owns parent query validation. Conversation
// acquisition sees no previous local fields in this fixture.
func (cycleFixtureAPI) Parent(context.Context, int64) (*remote.Change, error) { return nil, nil }

func TestCycleResumesBeforeRotatingAndPreservesHistoricalOffset(t *testing.T) {
	f := newLifecycleFixture(t)
	f.api.state.CompletedThrough = nil
	numbers := []int{7, 8, 9}
	sha, _ := selectionHash(numbers)
	f.api.state.Checkpoint, _ = json.Marshal(checkpoint{Version: 1, Phase: "select", History: history{SHA: sha, Offset: 1}})
	ctx, cancel := runContext()
	defer cancel()
	options := CycleOptions{Conversations: options(), LifecycleNumbers: []int{7}, CommitMaxPages: 1, LifecycleMaxPages: 1}
	options.Conversations.Numbers, options.Conversations.MaxThreads = numbers, 1
	options.Conversations.Now = func() time.Time { return time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC) }
	rest := &fixtureProvider{body: "original"}
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	api := cycleFixtureAPI{f.client}
	turn := func(mode string, complete, resumed bool) CycleResult {
		t.Helper()
		got, err := RunCycle(ctx, api, rest, graph, options)
		if err != nil || got.Mode != mode || got.Complete != complete || got.Resumed != resumed || got.LifecycleParents != 1 {
			t.Fatalf("turn %s: %+v %v", mode, got, err)
		}
		if !complete && len(got.DeferredModes) != 3 || complete && len(got.DeferredModes) != 0 {
			t.Fatalf("dishonest deferred modes: %+v", got)
		}
		return got
	}
	first := turn("incremental", false, false)
	if graph.calls != 0 || len(f.providerCursors) != 0 || f.api.state.CompletedThrough != nil {
		t.Fatal("another family or horizon advanced before the active run completed")
	}
	second := turn("incremental", true, true)
	if second.RunID != first.RunID || rest.parents != 1 {
		t.Fatal("resumed turn rewound inventory")
	}
	horizon := *f.api.state.CompletedThrough
	turn("commits", true, false)
	third := turn("lifecycle", false, false)
	options.LifecycleNumbers = nil
	if _, err := RunCycle(ctx, api, rest, graph, options); err == nil || f.api.state.ActiveRun.ID != third.RunID || len(f.providerCursors) != 1 {
		t.Fatal("a window cycle took over an active fixed selection")
	}
	options.LifecycleNumbers = []int{7}
	fourth := turn("lifecycle", true, true)
	if third.RunID != fourth.RunID || !reflect.DeepEqual(f.providerCursors, []string{"", "next"}) {
		t.Fatal("lifecycle turn lost its original cursor")
	}
	withoutHistory := options
	withoutHistory.Conversations.Numbers = nil
	if _, err := RunCycle(ctx, api, rest, graph, withoutHistory); err == nil || f.api.begins != 3 {
		t.Fatal("omitting the admitted historical file advanced the cycle", err)
	}
	rest.requested = nil
	turn("historical", true, false)
	if !reflect.DeepEqual(rest.requested, []int{8}) || !f.api.state.CompletedThrough.Equal(horizon) {
		t.Fatal("historical offset or supplemental horizon changed", rest.requested)
	}
	mode, _, err := nextCycleMode(f.api.state, options.Conversations)
	if err != nil || mode != "incremental" || f.api.begins != 4 {
		t.Fatal("rotation did not give every admitted mode one completed turn", mode, err, f.api.begins)
	}
}

func TestCycleWithoutHistoricalSelectionResumesLifecycleThenIncremental(t *testing.T) {
	f := newLifecycleFixture(t)
	ctx, cancel := runContext()
	defer cancel()
	o := CycleOptions{Conversations: options(), CommitMaxPages: 1, LifecycleMaxPages: 1}
	o.Conversations.Now = func() time.Time { return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) }
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	api := cycleFixtureAPI{f.client}
	rest := &fixtureProvider{}
	if _, err := RunCommits(ctx, api, graph, CommitOptions{Repository: o.Conversations.Repository, Since: o.Conversations.Since, MaxPages: 1}); err != nil {
		t.Fatal(err)
	}
	f.windowPin = fmt.Sprintf("pg-gitcrawl-current-v1:seed:%d", f.api.state.Sequence)
	f.windowNumbers = []int{7}
	first, err := RunCycle(ctx, api, rest, graph, o)
	if err != nil || first.Mode != "lifecycle" || first.Complete || !slices.Equal(first.DeferredModes, []string{"incremental", "commits"}) {
		t.Fatalf("cycle without history did not start the lifecycle turn: %+v %v", first, err)
	}
	o.OneWindow = true
	resumed, err := RunCycleTurns(ctx, api, rest, graph, o, 3)
	if err != nil || len(resumed) != 1 || !resumed[0].Resumed || !resumed[0].Complete || resumed[0].RunID != first.RunID ||
		!slices.Equal(f.providerCursors, []string{"", "next"}) || rest.parents != 0 {
		t.Fatalf("batch crossed the resumed lifecycle window: %+v %v", resumed, err)
	}
	third, err := RunCycle(ctx, api, rest, graph, o)
	if err != nil || third.Mode != "incremental" || len(rest.requested) != 0 {
		t.Fatalf("cycle without history did not rotate to incremental: %+v %v", third, err)
	}
}

func TestCycleRefusesUnknownActiveOwner(t *testing.T) {
	f := newLifecycleFixture(t)
	f.api.state.ActiveRun = &RunRequest{ID: "other", Mode: "historical-parents"}
	ctx, cancel := runContext()
	defer cancel()
	o := CycleOptions{Conversations: options(), LifecycleNumbers: []int{7}, CommitMaxPages: 1, LifecycleMaxPages: 1}
	o.Conversations.Numbers = []int{7}
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	_, err := RunCycle(ctx, f.client, &fixtureProvider{}, graph, o)
	if err == nil || graph.calls != 0 || len(f.providerCursors) != 0 || f.api.begins != 0 {
		t.Fatal("cycle replaced an unadmitted active owner", err)
	}
}

func TestCycleRejectsNewNanosecondBoundBeforeEffectsAndRecoversOldWindow(t *testing.T) {
	f := newLifecycleFixture(t)
	ctx, cancel := runContext()
	defer cancel()
	o := CycleOptions{Conversations: options(), LifecycleNumbers: []int{7}, CommitMaxPages: 1, LifecycleMaxPages: 1}
	o.Conversations.Numbers = []int{7}
	o.Conversations.Since = o.Conversations.Since.Add(time.Nanosecond)
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	_, err := RunCycle(ctx, f.client, &fixtureProvider{}, graph, o)
	if err == nil || f.api.requests != 0 || graph.calls != 0 || len(f.providerCursors) != 0 {
		t.Fatal("new unsupported precision reached an effect", err)
	}
	raw, _ := json.Marshal(scope{Version: 1, Mode: "incremental", Repository: o.Conversations.Repository, Since: o.Conversations.Since.Format(time.RFC3339Nano)})
	f.api.state.CompletedRun = &RunRequest{ID: "legacy", Horizon: *f.api.state.CompletedThrough, Scope: raw}
	mode, since, err := nextCycleMode(f.api.state, o.Conversations)
	if err != nil || mode != "commits" || !since.Equal(o.Conversations.Since.Truncate(time.Microsecond)) || since.After(o.Conversations.Since) {
		t.Fatal("legacy precise window wedged or lost its lower edge", mode, since, err)
	}
	raw, _ = json.Marshal(scope{Version: 1, Mode: "incremental", Repository: o.Conversations.Repository, Since: f.api.state.CompletedThrough.Format(time.RFC3339Nano)})
	f.api.state.CompletedRun.Scope = raw
	mode, since, err = nextCycleMode(f.api.state, o.Conversations)
	if err != nil || mode != "commits" || !since.Equal(f.api.state.CompletedThrough.Add(-time.Microsecond)) {
		t.Fatal("empty legacy window wedged supplemental turns", mode, since, err)
	}
}

func TestCycleCollectsPinnedWindowAcrossRunsAndResumesAcceptedPage(t *testing.T) {
	f := newLifecycleFixture(t)
	ctx, cancel := runContext()
	defer cancel()
	options := CycleOptions{Conversations: options(), CommitMaxPages: 1, LifecycleMaxPages: 1}
	options.Conversations.Numbers = []int{8}
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	api := cycleFixtureAPI{f.client}
	rest := &fixtureProvider{}
	if _, err := RunCommits(ctx, api, graph, CommitOptions{Repository: options.Conversations.Repository, Since: options.Conversations.Since, MaxPages: 1}); err != nil {
		t.Fatal(err)
	}
	f.candidateToken = "candidate-reader"
	var readerCalls int
	reader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q remote.QueryRequest
		raw, err := io.ReadAll(r.Body)
		if err != nil || json.Unmarshal(raw, &q) != nil || q.Name != "gitcrawl.lifecycle.candidates" || r.URL.Path != "/v1/apps/gitcrawl/archives/fixture/query" {
			t.Error("alternate reader received a different operation, source or credential")
			http.Error(w, "wrong operation", http.StatusBadRequest)
			return
		}
		readerCalls++
		r.Body = io.NopCloser(bytes.NewReader(raw))
		f.archiveHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(reader.Close)
	if err := f.client.SetLifecycleReader(reader.URL, ""); err != nil {
		t.Fatal(err)
	}
	f.windowPin = fmt.Sprintf("pg-gitcrawl-current-v1:seed:%d", f.api.state.Sequence)
	for i := 0; i < 502; i++ {
		f.windowNumbers = append(f.windowNumbers, 7+i*2)
	}
	next := func() CycleResult {
		t.Helper()
		got, err := RunCycle(ctx, api, rest, graph, options)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if _, err := RunCycle(ctx, api, rest, graph, options); err == nil || f.api.state.ActiveRun != nil || len(f.providerNumbers) != 0 {
		t.Fatal("reader with another audience advanced a lifecycle run")
	}
	if err := f.client.SetLifecycleReader(reader.URL, f.candidateToken); err != nil {
		t.Fatal(err)
	}
	first := next()
	if first.Mode != "lifecycle" || first.Complete || first.Resumed || first.LifecycleParents < 1 || len(f.providerNumbers) != 1 {
		t.Fatal("did not start the first window parent", first)
	}
	rawParentProgress := `{"version":1,"phase":"previous-mode"}`
	checkProof := func(run *RunRequest) {
		t.Helper()
		var scope LifecycleScope
		if json.Unmarshal(run.Scope, &scope) != nil || string(scope.ParentCheckpoint) != rawParentProgress || run.Horizon != *f.api.state.CompletedThrough {
			t.Fatal("window lost original parent progress/horizon")
		}
	}
	checkProof(f.api.state.ActiveRun)
	all := f.windowNumbers
	f.windowNumbers = all[1:]
	if _, err := RunCycle(ctx, api, rest, graph, options); err == nil || len(f.providerNumbers) != 1 || f.api.state.ActiveRun.ID != first.RunID {
		t.Fatal("changed candidate membership advanced an active run")
	}
	f.windowNumbers = all
	f.terminalFirst = true
	options.LifecycleMaxPages = 1000
	resumed := next()
	if resumed.RunID != first.RunID || !resumed.Resumed || !resumed.Complete || f.providerCursors[1] != "next" {
		t.Fatal("lost accepted timeline page", resumed)
	}
	for i := 0; i < 503; i++ {
		result := next()
		if result.Mode == "historical" {
			break
		}
		if result.Mode != "lifecycle" || result.Threads < 1 || !result.Complete {
			t.Fatal("window did not advance", result)
		}
		checkProof(f.api.state.CompletedRun)
	}
	want := append([]int{f.windowNumbers[0]}, f.windowNumbers...)
	if !slices.Equal(f.providerNumbers, want) || len(f.candidateQueries) < 4 || readerCalls != len(f.candidateQueries)+1 || !slices.Equal(rest.requested, []int{8}) {
		t.Fatalf("more than 500 parents must finish before history: reads=%d candidates=%d history=%v", len(f.providerNumbers), len(f.candidateQueries), rest.requested)
	}
	var merged bool
	for _, page := range f.api.pages {
		var req PageRequest
		if json.Unmarshal(page, &req) == nil && req.Lifecycle != nil {
			for _, event := range req.Lifecycle.Events {
				var raw struct {
					Type string `json:"__typename"`
				}
				if json.Unmarshal([]byte(event.RawJSON), &raw) == nil && raw.Type == "MergedEvent" {
					merged = true
				}
			}
		}
	}
	if !merged {
		t.Fatal("provider's merged event did not reach the native page")
	}
}

func TestCycleDistinguishesEmptyWindowFromUnavailableInventory(t *testing.T) {
	f := newLifecycleFixture(t)
	ctx, cancel := runContext()
	defer cancel()
	o := CycleOptions{Conversations: options(), CommitMaxPages: 1, LifecycleMaxPages: 1}
	o.Conversations.Numbers = []int{8}
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	api := cycleFixtureAPI{f.client}
	rest := &fixtureProvider{}
	if _, err := RunCommits(ctx, api, graph, CommitOptions{Repository: o.Conversations.Repository, Since: o.Conversations.Since, MaxPages: 1}); err != nil {
		t.Fatal(err)
	}
	f.windowPin = fmt.Sprintf("pg-gitcrawl-current-v1:seed:%d", f.api.state.Sequence)
	f.failCandidates = true
	if _, err := RunCycle(ctx, api, rest, graph, o); err == nil || f.api.begins != 1 || len(f.providerNumbers) != 0 {
		t.Fatal("unavailable candidate query was treated as empty")
	}
	f.failCandidates = false
	got, err := RunCycle(ctx, api, rest, graph, o)
	if err != nil || got.Mode != "historical" || !got.Complete || f.api.begins != 2 || len(f.providerNumbers) != 0 || !slices.Equal(rest.requested, []int{8}) {
		t.Fatal("empty window must advance real historical work without creating a lifecycle run", got, err)
	}
}

func TestCycleTurnsStopsOnUnfinishedRunAndResumesBeforeRotating(t *testing.T) {
	f := newLifecycleFixture(t)
	f.api.state.CompletedThrough = nil
	f.api.state.Checkpoint, _ = json.Marshal(checkpoint{Version: 1, Phase: "select"})
	ctx, cancel := runContext()
	defer cancel()
	o := CycleOptions{Conversations: options(), LifecycleNumbers: []int{7}, CommitMaxPages: 1, LifecycleMaxPages: 1}
	o.Conversations.MaxThreads = 1
	o.Conversations.Now = func() time.Time { return time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC) }
	api := cycleFixtureAPI{f.client}
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	rest := &fixtureProvider{body: "original"}
	batch := func(limit int) []CycleResult {
		t.Helper()
		turns, err := RunCycleTurns(ctx, api, rest, graph, o, limit)
		if err != nil {
			t.Fatal(err)
		}
		return turns
	}
	first := batch(4)
	if len(first) != 1 || first[0].Mode != "incremental" || first[0].Complete || graph.calls != 0 || f.api.begins != 1 {
		t.Fatalf("first batch crossed the unfinished parent run: %+v", first)
	}
	second := batch(4)
	if len(second) != 3 || second[0].Mode != "incremental" || !second[0].Complete || !second[0].Resumed || second[0].RunID != first[0].RunID ||
		second[1].Mode != "commits" || !second[1].Complete || second[2].Mode != "lifecycle" || second[2].Complete || f.api.begins != 3 {
		t.Fatalf("second batch skipped the checkpoint or crossed the unfinished lifecycle run: %+v", second)
	}
	last := batch(1)
	if len(last) != 1 || !last[0].Complete || !last[0].Resumed || last[0].RunID != second[2].RunID || !slices.Equal(f.providerCursors, []string{"", "next"}) || f.api.begins != 3 {
		t.Fatalf("following batch did not resume the original lifecycle cursor: %+v", last)
	}
}

func TestCycleOneWindowYieldsAfterEmptyLifecycleSelection(t *testing.T) {
	f := newLifecycleFixture(t)
	f.api.state.CompletedThrough = nil
	f.api.state.Checkpoint, _ = json.Marshal(checkpoint{Version: 1, Phase: "select"})
	ctx, cancel := runContext()
	defer cancel()
	o := CycleOptions{Conversations: options(), CommitMaxPages: 1, LifecycleMaxPages: 1, OneWindow: true}
	o.Conversations.Now = func() time.Time { return time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC) }
	api := cycleFixtureAPI{f.client}
	graph := &cycleGraphFixture{&emptyCommitProvider{}, f.provider}
	rest := &fixtureProvider{empty: true}
	first, err := RunCycle(ctx, api, rest, graph, o)
	if err != nil || first.Mode != "incremental" || !first.Complete {
		t.Fatalf("could not establish incremental publication: %+v %v", first, err)
	}
	f.windowPin = fmt.Sprintf("pg-gitcrawl-current-v1:seed:%d", f.api.state.Sequence+1)
	o.Conversations.Now = func() time.Time { return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) }
	turns, err := RunCycleTurns(ctx, api, rest, graph, o, 3)
	if err != nil || len(turns) != 1 || turns[0].Mode != "commits" || !turns[0].Complete ||
		rest.parents != 1 || f.api.begins != 2 || len(f.candidateQueries) != 1 || len(f.providerNumbers) != 0 {
		t.Fatalf("batch missed supplemental work or crossed into a second incremental scan: %+v %v", turns, err)
	}
}
