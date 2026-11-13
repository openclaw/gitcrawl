//go:build darwin || linux

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func deliveryWalk(items ...NativeReportDeliveryEvidence) func(func(NativeReportDeliveryEvidence) error) error {
	return func(yield func(NativeReportDeliveryEvidence) error) error {
		for _, item := range items {
			if err := yield(item); err != nil {
				return err
			}
		}
		return nil
	}
}

func deliveryLimits() NativeReportDeliveryLimits {
	return NativeReportDeliveryLimits{MaxRows: 100, MaxBytes: 8192}
}

func deliveryFixture(t *testing.T) (*Store, context.Context, NativeReportCapture) {
	t.Helper()
	s := changeContractFixture(t, 0)
	ctx, request := nativeReportRequest(t, s)
	for _, id := range []int64{1, 2} {
		if _, err := s.UpsertComment(ctx, Comment{ThreadID: id, GitHubID: fmt.Sprint(id), CommentType: "issue_comment", Body: "retained", RawJSON: fmt.Sprintf(`{"node_id":"comment-%d"}`, id)}); err != nil {
			t.Fatal(err)
		}
	}
	// Purge lazily installs these guards. Prepare them before capturing so the
	// policy tests exercise exclusion refusal independently of schema-cookie
	// refusal, which is covered by the incarnation tests below.
	if err := s.ensureThreadExclusionGuards(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx, takeNativeReport(t, ctx, s, request)
}

func TestNativeReportDeliveryRealPurgeAndOrdinaryProgress(t *testing.T) {
	s, ctx, c := deliveryFixture(t)
	evidence := NativeReportDeliveryEvidence{ThreadNumbers: []int64{10}, RawJSON: []string{`{"node_id":"comment-1"}`}}
	check := func(e NativeReportDeliveryEvidence) error {
		return CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, deliveryLimits(), deliveryWalk(e))
	}
	if err := check(evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 2, GitHubID: "2", CommentType: "issue_comment", Body: "ordinary progress", RawJSON: `{"node_id":"comment-2"}`}); err != nil {
		t.Fatal(err)
	}
	ctx, request := nativeReportRequest(t, s)
	progress := takeNativeReport(t, ctx, s, request)
	if progress.Anchor.Cursor.Sequence <= c.Anchor.Cursor.Sequence || int64(len(changeContractEntriesAfter(t, s.db, c.Anchor.Cursor.Sequence))) >= progress.Anchor.Cursor.Sequence-c.Anchor.Cursor.Sequence {
		t.Fatal("fixture did not exercise legitimate table-mode gaps")
	}
	if err := check(evidence); err != nil {
		t.Fatal("ordinary progress blocked", err)
	}
	plan, err := s.PlanThreadPurge(ctx, request.Repository, []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, request.Repository, []int{10}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	for _, e := range []NativeReportDeliveryEvidence{{ThreadNumbers: []int64{10}}, {RawJSON: evidence.RawJSON}} {
		if err := check(e); !errors.Is(err, ErrThreadExcluded) {
			t.Fatal("purged staged content allowed", err)
		}
	}
	if err := check(NativeReportDeliveryEvidence{ThreadNumbers: []int64{20}, RawJSON: []string{`{"node_id":"comment-2"}`}}); err != nil {
		t.Fatal("unaffected sibling blocked", err)
	}
}

func TestNativeReportDeliveryGlobalNodesUseNativeJSONSemantics(t *testing.T) {
	s, ctx, c := deliveryFixture(t)
	repo, err := s.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "other", FullName: "Fixture/Other", RawJSON: "{}", UpdatedAt: "2026-01-01"})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{10, 20, 30} {
		raw := fmt.Sprintf(`{"node_id":"foreign-%d"}`, n)
		if n == 20 {
			raw = `{"node_id":123}`
		}
		if _, err := s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: fmt.Sprint(n), Number: n, Kind: "issue", State: "open", Title: "retained", RawJSON: raw, LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: fmt.Sprint(n), UpdatedAt: "2026-01-01"}); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := s.PlanThreadPurge(ctx, "fixture/other", []int{10, 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/other", []int{10, 20}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	// Nodes are global, even when the owner exclusion belongs to another repo.
	for _, tc := range []struct {
		raw      string
		excluded bool
	}{
		{`{"node_id":"foreign-10"}`, true},
		{`{"node_id":123}`, true}, {`{"node_id":"123"}`, true},
		{`{"node_id":123.0}`, false},
		{`{"node_id":"foreign-10","node_id":"allowed"}`, true},
		{`{"node_id":"allowed","node_id":"foreign-10"}`, false},
		{`{"id":"foreign-10","_graphql":{"id":"foreign-10"}}`, false},
		{`{"node_id":"foreign-10"`, false},
		{`{"node_id":null}`, false}, {`["foreign-10"]`, false}, {"", false},
	} {
		err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, deliveryLimits(), deliveryWalk(NativeReportDeliveryEvidence{ThreadNumbers: []int64{10}, RawJSON: []string{tc.raw}}))
		if tc.excluded && !errors.Is(err, ErrThreadExcluded) || !tc.excluded && err != nil {
			t.Fatal("native node interpretation changed", err)
		}
	}
}

func TestNativeReportDeliveryOneSnapshotAcrossConcurrentPurge(t *testing.T) {
	s, ctx, c := deliveryFixture(t)
	e := NativeReportDeliveryEvidence{ThreadNumbers: []int64{10}, RawJSON: []string{`{"node_id":"comment-1"}`}}
	err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, deliveryLimits(), func(yield func(NativeReportDeliveryEvidence) error) error {
		if err := yield(e); err != nil {
			return err
		}
		// Another real connection commits while the checker holds its snapshot.
		plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
		if err != nil {
			return err
		}
		if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, plan.PlanID); err != nil {
			return err
		}
		return yield(e)
	})
	if err != nil {
		t.Fatal("stream observed mixed snapshots", err)
	}
	if err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, deliveryLimits(), deliveryWalk(e)); !errors.Is(err, ErrThreadExcluded) {
		t.Fatal("success reused as later grant", err)
	}
}

func TestNativeReportDeliveryChecksAfterSQLBatchBoundary(t *testing.T) {
	s, ctx, c := deliveryFixture(t)
	e := NativeReportDeliveryEvidence{}
	for range 501 {
		e.ThreadNumbers = append(e.ThreadNumbers, 20)
		e.RawJSON = append(e.RawJSON, `{"node_id":"comment-2"}`)
	}
	limits := NativeReportDeliveryLimits{MaxRows: 1002, MaxBytes: 64 << 10}
	check := func(e NativeReportDeliveryEvidence) error {
		return CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, limits, deliveryWalk(e))
	}
	if err := check(e); err != nil {
		t.Fatal("allowed references across batches", err)
	}
	plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	e.ThreadNumbers[500] = 10
	if err := check(e); !errors.Is(err, ErrThreadExcluded) {
		t.Fatal("last number escaped batching", err)
	}
	e.ThreadNumbers[500] = 20
	e.RawJSON[500] = `{"node_id":"comment-1"}`
	if err := check(e); !errors.Is(err, ErrThreadExcluded) {
		t.Fatal("last raw node escaped batching", err)
	}
}

func TestNativeReportDeliveryBoundsCancellationAndWalkerFailure(t *testing.T) {
	s, ctx, c := deliveryFixture(t)
	e := NativeReportDeliveryEvidence{ThreadNumbers: []int64{10}, RawJSON: []string{"{}"}}
	l := NativeReportDeliveryLimits{MaxRows: 4, MaxBytes: 4}
	if err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, l, deliveryWalk(e, e)); err != nil {
		t.Fatal("exact aggregate bounds refused", err)
	}
	for _, limit := range []NativeReportDeliveryLimits{{MaxRows: 3, MaxBytes: 4}, {MaxRows: 4, MaxBytes: 3}, {MaxRows: 0, MaxBytes: 4}, {MaxRows: 4, MaxBytes: 0}} {
		if err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, limit, deliveryWalk(e, e)); !errors.Is(err, ErrNativeReportLimit) {
			t.Fatal("bound did not refuse check", err)
		}
	}
	if err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, l, func(yield func(NativeReportDeliveryEvidence) error) error {
		_ = yield(e)
		_ = yield(e)
		_ = yield(e)
		return nil
	}); !errors.Is(err, ErrNativeReportLimit) {
		t.Fatal("ignored yield failure became success", err)
	}
	if err := CheckNativeReportDelivery(context.Background(), s.Path(), c.Anchor, l, deliveryWalk(e)); err == nil {
		t.Fatal("missing deadline accepted")
	}
	for _, before := range []bool{true, false} {
		callCtx, cancel := context.WithCancel(ctx)
		if before {
			cancel()
		}
		err := CheckNativeReportDelivery(callCtx, s.Path(), c.Anchor, l, func(yield func(NativeReportDeliveryEvidence) error) error {
			if err := yield(e); err != nil {
				return err
			}
			cancel()
			return nil
		})
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation lost", err)
		}
	}
	walkErr := errors.New("fixture stopped")
	if err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, l, func(yield func(NativeReportDeliveryEvidence) error) error {
		if err := yield(e); err != nil {
			return err
		}
		return walkErr
	}); !errors.Is(err, walkErr) {
		t.Fatal("walker failure became success", err)
	}
	for _, n := range []int64{0, -1} {
		if err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, l, deliveryWalk(NativeReportDeliveryEvidence{ThreadNumbers: []int64{n}})); !errors.Is(err, ErrNativeReportSource) {
			t.Fatal("invalid number accepted", err)
		}
	}
}

func TestNativeReportDeliveryIdentityAndIncarnationRefusal(t *testing.T) {
	s, ctx, c := deliveryFixture(t)
	for _, mutate := range []func(*NativeReportAnchor){
		func(a *NativeReportAnchor) { a.File.Inode += "0" }, func(a *NativeReportAnchor) { a.RepositoryID++ },
		func(a *NativeReportAnchor) { a.Repository = "fixture/other" }, func(a *NativeReportAnchor) { a.ScopeSHA256 = strings.Repeat("0", 64) },
		func(a *NativeReportAnchor) { a.Cursor.Epoch = "other" }, func(a *NativeReportAnchor) { a.Cursor.Generation++ },
		func(a *NativeReportAnchor) { a.Cursor.SchemaVersion++ }, func(a *NativeReportAnchor) { a.Cursor.Sequence++ },
		func(a *NativeReportAnchor) { a.Cursor.PrunedThrough = a.Cursor.Sequence + 1 },
	} {
		a := c.Anchor
		mutate(&a)
		called := false
		err := CheckNativeReportDelivery(ctx, s.Path(), a, deliveryLimits(), func(func(NativeReportDeliveryEvidence) error) error { called = true; return nil })
		if !errors.Is(err, ErrNativeReportSource) || called {
			t.Fatal("invalid anchor reached iterator", err)
		}
	}
	for _, mutate := range []string{
		`update change_contract_state set generation=generation+1`,
		`update change_contract_state set epoch='other'`,
		`update change_contract_sequence set pruned_through=value+1`,
		`update change_contract_sequence set value=0,pruned_through=0`,
		`drop trigger change_contract_log_prune`,
		`pragma user_version=15`,
	} {
		t.Run(mutate, func(t *testing.T) {
			s, ctx, c := deliveryFixture(t)
			if _, err := s.db.ExecContext(ctx, mutate); err != nil {
				t.Fatal(err)
			}
			// Test armed validation independently of the schema-cookie guard.
			if strings.HasPrefix(mutate, "drop trigger") {
				if err := s.db.QueryRowContext(ctx, `pragma schema_version`).Scan(&c.Anchor.Cursor.SchemaVersion); err != nil {
					t.Fatal(err)
				}
			}
			if err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, deliveryLimits(), deliveryWalk()); err == nil {
				t.Fatal("changed incarnation accepted")
			}
		})
	}
}

func TestNativeReportDeliveryPreservesStandaloneNativeBytes(t *testing.T) {
	s, ctx, c := deliveryFixture(t)
	if _, err := s.db.ExecContext(ctx, `pragma journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []NativeReportDeliveryLimits{deliveryLimits(), {MaxRows: 1, MaxBytes: 1}} {
		err := CheckNativeReportDelivery(ctx, s.Path(), c.Anchor, l, deliveryWalk(NativeReportDeliveryEvidence{ThreadNumbers: []int64{10}, RawJSON: []string{"{}"}}))
		if l.MaxRows == 1 && !errors.Is(err, ErrNativeReportLimit) || l.MaxRows != 1 && err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || info.Mode() != afterInfo.Mode() {
		t.Fatal("checker changed source bytes/mode")
	}
	file, err := NativeReportFileIdentity(s.Path())
	if err != nil || file != c.Anchor.File {
		t.Fatal("physical identity changed", err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(s.Path() + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("standalone readonly check created sidecar", err)
		}
	}
}
