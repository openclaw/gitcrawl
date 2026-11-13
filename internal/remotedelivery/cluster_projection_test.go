//go:build darwin || linux

package remotedelivery

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

func clusterReportFixture(t *testing.T) (context.Context, store.NativeClusterContext) {
	t.Helper()
	ctx, s := nativeFixture(t)
	_, err := s.DB().ExecContext(ctx, `update repositories set raw_json='{"private":false}'; update threads set kind='issue',body=NULL;
 insert into cluster_groups(id,repo_id,stable_key,stable_slug,status,representative_thread_id,created_at,updated_at) values(1,1,'one','one','active',1,'now','now');
 insert into cluster_memberships(cluster_id,thread_id,role,state,added_by,added_reason_json,created_at,updated_at) values(1,1,'canonical','active','fixture','{}','now','now'),(1,2,'member','active','fixture','{}','now','now');`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().ExecContext(ctx, "update threads set body=?,title=? where id=2", "☃\x00excerpt", "title\x00with NUL"); err != nil {
		t.Fatal(err)
	}
	file, err := store.NativeReportFileIdentity(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	c, err := store.CaptureNativeClusterContext(ctx, s.Path(), store.NativeClusterContextRequest{Repository: "fixture/repo", File: file, Limits: store.NativeReportLimits{MaxRows: 10000, MaxBytes: 8 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, c
}

func TestClusterSeedNativeRoundTrip(t *testing.T) {
	ctx, c := clusterReportFixture(t)
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 123, time.UTC)
	report, err := ProjectClusterContext(ctx, c, stamp)
	if err != nil {
		t.Fatal(err)
	}
	if report.SHA256 == c.SHA256 || report.Rows != 6 {
		t.Fatal("wire inventory confused with native digest or lost rows")
	}
	seed, err := PrepareClusterSeed(ctx, report, seedRequest())
	if err != nil {
		t.Fatal(err)
	}
	m := seed.Plan.Manifest()
	if m.NativeThrough != "gitcrawl-cluster-v1:"+report.ScopeSHA256+":"+report.SHA256 || m.RepositoryID != "1" || m.Profile != store.NativeClusterContextProfile {
		t.Fatal("wrong cluster identity")
	}
	var changes []remote.Change
	for _, body := range seed.Chunks {
		chunk, err := seed.Plan.ValidateChunk(body)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, chunk.Changes...)
	}
	if _, err := seed.Plan.ValidateComplete(seed.Complete); err != nil {
		t.Fatal(err)
	}
	if len(changes) != 6 {
		t.Fatal("missing rows")
	}
	selection := changes[0].Row
	if selection[5].Value == nil || *selection[5].Value != "legacy_unknown" || selection[8].Kind != "null" || selection[24].Kind != "null" {
		t.Fatal("unknown qualification became complete")
	}
	if changes[4].Row[7].Kind != "null" || changes[5].Row[7].Value == nil || *changes[5].Row[7].Value != "☃\x00excerpt" || *changes[5].Row[4].Value != "title\x00with NUL" {
		t.Fatal("scalar fidelity lost")
	}
	again, err := ProjectClusterContext(ctx, c, stamp)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := PrepareClusterSeed(ctx, again, seedRequest())
	if err != nil || !reflect.DeepEqual(seed.Manifest, replay.Manifest) || !reflect.DeepEqual(seed.Chunks, replay.Chunks) {
		t.Fatal("same capture/stamp changed exact bytes", err)
	}
	c.Threads[1].Title = "changed"
	changed, err := ProjectClusterContext(ctx, c, stamp)
	if err != nil {
		t.Fatal(err)
	}
	if changed.ScopeSHA256 != report.ScopeSHA256 || changed.SHA256 == report.SHA256 {
		t.Fatal("content/scope digest did not bind independently")
	}
}

func TestClusterSeedBoundsAndProfileIsolation(t *testing.T) {
	ctx, c := clusterReportFixture(t)
	report, err := ProjectClusterContext(ctx, c, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	seed, err := PrepareClusterSeed(ctx, report, seedRequest())
	if err != nil {
		t.Fatal(err)
	}
	request := seedRequest()
	request.MaxBytes = 1
	if out, err := PrepareClusterSeed(ctx, report, request); !errors.Is(err, ErrPrivateSeedLimit) || !reflect.DeepEqual(out, PrivateSeed{}) {
		t.Fatal("partial bounded seed", err)
	}
	report.Tables[0].Name = "repositories"
	if _, err := PrepareClusterSeed(ctx, report, seedRequest()); err == nil {
		t.Fatal("private table accepted")
	}
	if strings.Contains(string(seed.Manifest), "gitcrawl-native-v1:") {
		t.Fatal("fabricated native journal")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ProjectClusterContext(canceled, c, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestClusterProjectionRejectsOversizeScalar(t *testing.T) {
	ctx, c := clusterReportFixture(t)
	c.Threads[0].Title = strings.Repeat("x", clusterMaxScalarBytes+1)
	if out, err := ProjectClusterContext(ctx, c, time.Now()); !errors.Is(err, ErrClusterProjection) || !reflect.DeepEqual(out, ClusterReport{}) {
		t.Fatal("oversize public scalar was retained", err)
	}
}
