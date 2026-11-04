package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestTUIRemainingActionAndErrorBranches(t *testing.T) {
	thread := store.Thread{
		ID: 1, Number: 10, Kind: "issue", State: "open", Title: "Thread title",
		Body: "Body with https://example.com/docs", HTMLURL: "https://github.com/openclaw/openclaw/issues/10",
		UpdatedAt: "2026-05-08T00:00:00Z",
	}
	cluster := store.ClusterSummary{
		ID: 7, Source: store.ClusterSourceRun, StableSlug: "cluster-7", Status: "active",
		Title: "Cluster title", RepresentativeNumber: 10, RepresentativeKind: "issue",
		RepresentativeTitle: "Thread title", MemberCount: 1, UpdatedAt: "2026-05-08T00:00:00Z",
	}
	detail := store.ClusterDetail{
		Cluster: cluster,
		Members: []store.ClusterMemberDetail{{
			Thread:      thread,
			Role:        "member",
			State:       "active",
			BodySnippet: "Body with https://example.com/docs",
			Summaries:   map[string]string{"problem_summary": "summary"},
		}},
	}
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		Sort:       "size",
		MinSize:    1,
		Clusters:   []store.ClusterSummary{cluster},
	})
	model.detailCache[clusterSummaryKey(cluster)] = detail
	model.loadSelectedCluster()
	model.memberIndex = 0
	model.neighborCache[thread.ID] = []tuiNeighbor{{Thread: thread, Score: 0.9}}

	for _, action := range []string{"sort-oldest", "member-sort-oldest", "toggle-closed", "close-menu"} {
		if !model.runAction(action) {
			t.Fatalf("action %s was not handled", action)
		}
	}
	if model.payload.Sort != "oldest" || model.memberSort != memberSortOldest {
		t.Fatalf("sort actions failed sort=%q member=%q", model.payload.Sort, model.memberSort)
	}

	t.Setenv("PATH", "")
	errorActions := []string{
		"open-cluster-representative",
		"copy-cluster-url",
		"copy-thread-detail",
		"copy-body-preview",
		"copy-summaries",
		"copy-neighbors",
		"copy-cluster-id",
		"copy-cluster-name",
		"copy-cluster-title",
		"copy-member-list",
		"copy-cluster",
		"copy-visible-clusters",
		"copy-reference-links",
		"open",
		"copy-url",
		"copy-markdown",
		"copy-title",
		"open-first-link",
		"copy-first-link",
	}
	for _, action := range errorActions {
		model.status = ""
		handled := model.runMenuItem(tuiMenuItem{label: action, action: action, value: "https://example.com/docs"})
		if !handled || model.status == "" {
			t.Fatalf("error action %s handled=%v status=%q", action, handled, model.status)
		}
	}
	model.openReferenceLinkMenu("copy")
	model.runAction("back-to-actions")
	if model.menuTitle != "Actions" {
		t.Fatalf("back to actions failed title=%q", model.menuTitle)
	}
	model.runMenuItem(tuiMenuItem{label: "Open picked", action: "open-picked-link", value: "https://example.com/docs"})
	model.runMenuItem(tuiMenuItem{label: "Copy picked", action: "copy-picked-link", value: "https://example.com/docs"})

	model.closeSelectedClusterLocally()
	if !strings.Contains(model.status, "only available for durable clusters") {
		t.Fatalf("raw cluster local close status=%q", model.status)
	}
	model.reopenSelectedClusterLocally()
	model.excludeSelectedClusterMemberLocally()
	model.includeSelectedClusterMemberLocally()
	model.setSelectedClusterCanonicalLocally()
	if !strings.Contains(model.status, "only available for durable clusters") {
		t.Fatalf("raw member local action status=%q", model.status)
	}
}

func TestTUIRemainingHelperBranches(t *testing.T) {
	model := newClusterBrowserModel(context.Background(), nil, 0, clusterBrowserPayload{
		Repository: "openclaw/openclaw",
		MinSize:    1,
		Limit:      1,
		Clusters: []store.ClusterSummary{
			{ID: 1, Status: "active", RepresentativeNumber: 101, MemberCount: 1, UpdatedAt: "2026-05-08T00:00:00Z"},
		},
	})
	if cluster, ok := model.selectedCluster(); !ok || cluster.ID != 1 {
		t.Fatalf("current cluster id = %d", cluster.ID)
	}
	if model.clusterRefreshLimit() != 1 {
		t.Fatalf("cluster refresh limit = %d", model.clusterRefreshLimit())
	}
	model.ensureClusterInWorkingSet(store.ClusterSummary{ID: 2, Status: "closed", ClosedAt: "2026-05-08T00:00:00Z", MemberCount: 2})
	if !model.selectClusterForJump(store.ClusterSummary{ID: 2}) || !model.showClosed || model.minSize != 1 {
		t.Fatalf("jump selection showClosed=%v minSize=%d selected=%d", model.showClosed, model.minSize, model.selected)
	}
	model.payload.Clusters = nil
	if cluster, ok := model.selectedCluster(); ok || model.clusterSignature() != "" {
		t.Fatalf("empty cluster helpers id=%d sig=%q", cluster.ID, model.clusterSignature())
	}
	if _, ok := model.clusterSummaryFromWorkingSet(store.ClusterSummary{ID: 999}); ok {
		t.Fatal("missing working-set cluster should not resolve")
	}
	model.applyClusterRefresh(nil, "")
	if model.payload.Clusters == nil {
		t.Fatal("nil refresh should normalize clusters")
	}
	model.autoRefreshFromStore()
	if model.status != "Refresh unavailable for this view" {
		t.Fatalf("auto refresh status=%q", model.status)
	}
	if cmd := model.autoRefreshCmd(); cmd != nil {
		t.Fatalf("auto refresh command without store = %v", cmd)
	}
	model.switchRepository("")
	if model.status != "Repository picker unavailable for this view" {
		t.Fatalf("switch repository no store status=%q", model.status)
	}
	if label := (clusterBrowserModel{}).clusterPositionLabel(); label != "0" {
		t.Fatalf("zero cluster position label = %q", label)
	}
	if label := model.clusterPositionLabel(); label != "0" {
		t.Fatalf("empty model cluster position label = %q", label)
	}
	memberModel := model
	memberModel.memberRows = []memberRow{}
	if label := memberModel.memberPositionLabel(); label != "0" {
		t.Fatalf("zero member position label = %q", label)
	}
	if got := formatRelativeTime(time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)); got != "30m ago" {
		t.Fatalf("minute age = %q", got)
	}
	if got := formatRelativeTime(time.Now().Add(-75 * 24 * time.Hour).Format(time.RFC3339Nano)); !strings.Contains(got, "mo ago") {
		t.Fatalf("month age = %q", got)
	}
	if got := formatRelativeTime(""); got != "never" {
		t.Fatalf("empty age = %q", got)
	}
	if got := formatRelativeTime("bad-time"); got != "bad-time" {
		t.Fatalf("bad age = %q", got)
	}
	if got := wrapPlain("", 10); len(got) != 1 || got[0] != "" {
		t.Fatalf("empty wrap = %+v", got)
	}
	if got := clampInt(5, 10, 1); got != 10 {
		t.Fatalf("inverted clamp = %d", got)
	}
	if got := padCells("abcdef", 0); got != "" {
		t.Fatalf("zero pad = %q", got)
	}
	if got := fitBlock("a\nb", 2, 1); got != "a " {
		t.Fatalf("fit block = %q", got)
	}
}
