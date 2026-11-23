package acquireapi

import (
	"context"
	"encoding/json"
	"reflect"
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
	fourth := turn("lifecycle", true, true)
	if third.RunID != fourth.RunID || !reflect.DeepEqual(f.providerCursors, []string{"", "next"}) {
		t.Fatal("lifecycle turn lost its original cursor")
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
