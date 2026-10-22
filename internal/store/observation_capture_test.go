package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func captureFixture(t *testing.T) *Store {
	t.Helper()
	s := exclusionFixture(t)
	if err := s.InstallObservationCapture(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}
func captureExec(t *testing.T, s *Store, sql string, args ...any) {
	t.Helper()
	if err := s.WithTx(context.Background(), func(tx *Store) error { _, err := tx.q().ExecContext(context.Background(), sql, args...); return err }); err != nil {
		t.Fatal(err)
	}
}
func captureCount(t *testing.T, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.q().QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestObservationCaptureRegistry(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "observation-capture-registry-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != observationRegistry || captureDigest() != "cd6d17e41da24b2a70bc1e13d908399249066c57c0be80f957c1b9027cd6763d" {
		t.Fatal("frozen registry changed")
	}
}
func TestObservationCaptureCommitABAAndOverlay(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	for _, bio := range []string{"A", "B", "A", "A"} {
		captureExec(t, s, `UPDATE actor_profiles SET bio=?,observed_at='same-clock' WHERE node_id='shared-actor'`, bio)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations`); n != 4 {
		t.Fatalf("observations=%d", n)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_images`); n != 3 {
		t.Fatalf("images=%d", n)
	}
	captureExec(t, s, `UPDATE actor_profiles SET observed_at='new-clock' WHERE node_id='shared-actor'`)
	if n := captureCount(t, s, `SELECT count(*) FROM capture_images`); n != 3 {
		t.Fatalf("clock duplicated image: %d", n)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations WHERE json_extract(clocks_json,'$.observed_at[1]')='new-clock'`); n != 1 {
		t.Fatal("lost clock")
	}
	if _, err := s.DB().Exec(`UPDATE actor_profiles SET bio='unframed'`); err == nil || !strings.Contains(err.Error(), "capture writer context") {
		t.Fatalf("unframed write accepted: %v", err)
	}
	before := captureCount(t, s, `SELECT count(*) FROM capture_observations`)
	err := s.WithTx(ctx, func(tx *Store) error {
		if _, err := tx.q().ExecContext(ctx, `UPDATE actor_profiles SET bio='rollback'`); err != nil {
			return err
		}
		return errors.New("fixture rollback")
	})
	if err == nil || captureCount(t, s, `SELECT count(*) FROM capture_observations`) != before {
		t.Fatal("rollback leaked")
	}
	if err := validateCaptureSchema(ctx, s.DB()); err != nil {
		t.Fatal(err)
	}
	if _, err := captureCapability(ctx, s.DB()); err != nil {
		t.Fatal(err)
	}
}

func TestObservationCaptureReviewFinalCommitPreservesNativeHistory(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	at := "2026-02-01T00:00:00Z"
	before := captureCount(t, s, `SELECT count(*) FROM analytics_fetch_attempts`)
	err := s.WithTx(ctx, func(tx *Store) error {
		for _, body := range []string{"first", "final"} {
			r := PullRequestReviewThread{ReviewThreadID: "R1", FirstCommentBody: body, CommentsJSON: `[]`, RawJSON: `{}`, FetchedAt: at, ViewerCanReply: true, FirstCommentCreatedAt: "provider-created", FirstCommentUpdatedAt: "provider-updated"}
			if err := tx.UpsertPullRequestReviewThreads(ctx, 1, at, []PullRequestReviewThread{r}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM pull_request_review_thread_revisions WHERE thread_id=1`); n != 2 {
		t.Fatalf("native history collapsed: %d", n)
	}
	if captureCount(t, s, `SELECT count(*) FROM analytics_fetch_attempts`) != before {
		t.Fatal("native receipts changed")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations`); n != 2 {
		t.Fatalf("internal SQL leaked into committed observations: %d", n)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_commits WHERE observation_count=2`); n != 1 {
		t.Fatal("not a single frame")
	}
	var body string
	err = s.DB().QueryRow(`SELECT json_extract(i.value_json,'$.first_comment_body[1]') FROM capture_supports c JOIN capture_images i ON i.image_id=c.image_id WHERE c.role='review_thread'`).Scan(&body)
	if err != nil || body != "final" {
		t.Fatalf("support=%q err=%v", body, err)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_supports WHERE role='review_membership'`); n != 1 {
		t.Fatal("membership support absent")
	}
	// A missing member rolls native metadata, history and capture back together.
	err = s.WithTx(ctx, func(tx *Store) error {
		_, err := tx.q().ExecContext(ctx, `UPDATE pull_request_review_thread_syncs SET review_thread_ids_json='["missing"]' WHERE thread_id=1`)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "capture support") {
		t.Fatalf("accepted incomplete membership: %v", err)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations`); n != 2 {
		t.Fatal("partial frame survived")
	}
}

func captureBlobRevision(t *testing.T, s *Store) {
	t.Helper()
	captureExec(t, s, `INSERT INTO blobs(id,sha256,media_type,size_bytes,storage_kind,inline_text,created_at) VALUES(101,'hash','application/json',1,'inline','A','born'); INSERT INTO thread_revisions(id,thread_id,source_updated_at,content_hash,title_hash,body_hash,labels_hash,raw_json_blob_id,observation_sequence,created_at) VALUES(101,1,'provider','content','title','body','labels',101,1,'born')`)
}
func TestObservationCaptureBlobRepairBothOrders(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprint(before), func(t *testing.T) {
			s := captureFixture(t)
			captureBlobRevision(t, s)
			repair := `UPDATE blobs SET inline_text='repaired',size_bytes=8 WHERE id=101;`
			observe := `UPDATE thread_revisions SET observation_sequence=2 WHERE id=101;`
			if before {
				captureExec(t, s, repair+observe)
			} else {
				captureExec(t, s, observe+repair)
			}
			var value string
			if err := s.DB().QueryRow(`SELECT json_extract(i.value_json,'$.inline_text[1]') FROM capture_supports p JOIN capture_images i ON i.image_id=p.image_id WHERE p.role='github_blob' ORDER BY p.seq DESC LIMIT 1`).Scan(&value); err != nil || value != "repaired" {
				t.Fatalf("support=%q %v", value, err)
			}
			captureExec(t, s, `UPDATE blobs SET inline_text='later' WHERE id=101`)
			if n := captureCount(t, s, `SELECT count(*) FROM capture_supports p JOIN capture_images i ON i.image_id=p.image_id WHERE json_extract(i.value_json,'$.inline_text[1]')='repaired'`); n != 1 {
				t.Fatal("historical dependency changed")
			}
			if n := captureCount(t, s, `SELECT count(*) FROM thread_revisions`); n != 1 {
				t.Fatal("native revision writer altered")
			}
		})
	}
}

func TestObservationCaptureSemanticValuesAndObservationOverlay(t *testing.T) {
	s := captureFixture(t)
	captureBlobRevision(t, s)
	ctx := context.Background()
	r := PullRequestReviewThread{ReviewThreadID: "R1", CommentsJSON: `[]`, RawJSON: `{}`, FetchedAt: "2026-01-01T00:00:00Z"}
	if err := s.UpsertPullRequestReviewThreads(ctx, 1, r.FetchedAt, []PullRequestReviewThread{r}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`UPDATE actor_profiles SET created_at='provider-born'`,
		`UPDATE thread_revisions SET source_updated_at='provider-edit'`,
		`UPDATE thread_revisions SET created_at='corrected-birth'`,
		`UPDATE blobs SET created_at='corrected-birth'`,
		`UPDATE pull_request_review_threads SET first_comment_created_at='provider-born'`,
		`UPDATE pull_request_review_threads SET first_comment_updated_at='provider-edit'`,
		`UPDATE pull_request_review_threads SET viewer_can_reply=1`,
		`UPDATE pull_request_review_threads SET deleted_at='deleted',deletion_reason='provider_receipt'`,
		`UPDATE pull_request_review_threads SET deleted_at='deletion-correction'`,
	} {
		before := captureCount(t, s, `SELECT count(*) FROM capture_images`)
		captureExec(t, s, query)
		if n := captureCount(t, s, `SELECT count(*) FROM capture_images`); n != before+1 {
			t.Fatalf("semantic field reused image: %s (%d -> %d)", query, before, n)
		}
	}
	for _, query := range []string{`UPDATE actor_profiles SET observed_at='new'`, `UPDATE thread_revisions SET observation_sequence=999`, `UPDATE pull_request_review_threads SET fetched_at='new'`, `UPDATE pull_request_review_thread_syncs SET fetched_at='new'`} {
		before := captureCount(t, s, `SELECT count(*) FROM capture_images`)
		obs := captureCount(t, s, `SELECT count(*) FROM capture_observations`)
		captureExec(t, s, query)
		if captureCount(t, s, `SELECT count(*) FROM capture_images`) != before || captureCount(t, s, `SELECT count(*) FROM capture_observations`) != obs+1 {
			t.Fatalf("overlay not retained efficiently: %s", query)
		}
	}
}

func TestObservationCaptureRekeyTemporaryKeysAndPurgeRefusal(t *testing.T) {
	s := captureFixture(t)
	captureExec(t, s, `UPDATE actor_profiles SET node_id='new-actor' WHERE node_id='shared-actor'; INSERT INTO actor_profiles(node_id,login,actor_type,observed_at,raw_json) VALUES('temporary','temp','User','clock','{}'); DELETE FROM actor_profiles WHERE node_id='temporary'`)
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations`); n != 2 {
		t.Fatalf("rekey/final absence emitted %d observations", n)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations WHERE operation='withdraw'`); n != 1 {
		t.Fatal("old key not withdrawn")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_commits WHERE reset_reason='identity_change'`); n != 1 {
		t.Fatal("missing durable ownership ambiguity marker")
	}
	if _, err := s.PurgeThreads(context.Background(), "fixture/repo", []int{10}, "fixture"); err == nil || !strings.Contains(err.Error(), "ownership") {
		t.Fatalf("ambiguous purge accepted: %v", err)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM threads`); n != 2 {
		t.Fatal("refusal mutated native rows")
	}
}

func TestObservationCapturePurgeRedactsHistoricalImagesAndRevokesEpoch(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	for _, v := range []string{"first private observation", "second private observation"} {
		captureExec(t, s, `UPDATE actor_identity_evidence SET raw_json=? WHERE node_id='node10'`, v)
	}
	captureExec(t, s, `UPDATE actor_profiles SET bio='shared profile'`)
	before, err := captureCapability(ctx, s.DB())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "capture-owner-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.CaptureImageIDs) != 2 {
		t.Fatalf("closure=%v", plan.CaptureImageIDs)
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_images WHERE redaction_reason='owner_requested' AND value_json IS NULL`) != 2 {
		t.Fatal("old exclusive bytes retained")
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_images WHERE table_name='actor_profiles' AND value_json IS NOT NULL`) != 1 {
		t.Fatal("shared profile redacted")
	}
	if err = s.CheckObservationCapability(ctx, before); err == nil {
		t.Fatal("old epoch admitted")
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_observations WHERE cause='owner_requested' AND operation='withdraw'`) != 2 {
		t.Fatal("owner withdrawal misclassified")
	}
	if err = s.SaveActorEvidence(ctx, []map[string]any{{"id": "node10", "author": map[string]any{"id": "x"}}}, "later"); err != nil {
		t.Fatal(err)
	}
	if captureCount(t, s, `SELECT count(*) FROM actor_identity_evidence WHERE node_id='node10'`) != 0 {
		t.Fatal("purged target recreated")
	}
}

func TestObservationCapturePinnedBaselinePrefixAndReset(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	for _, v := range []string{"A", "B", "A"} {
		captureExec(t, s, `UPDATE actor_profiles SET bio=?`, v)
	}
	reader, err := OpenReadOnly(ctx, s.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	snapshot, err := reader.OpenObservationSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	captureExec(t, s, `UPDATE actor_profiles SET bio='after-H'`)
	var buf bytes.Buffer
	seal, err := snapshot.WriteBaseline(ctx, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if seal.Streams["capture_observations"].Rows != 3 || strings.Contains(buf.String(), "after-H") {
		t.Fatalf("drifting/incomplete prefix: %+v", seal.Streams)
	}
	if !strings.Contains(buf.String(), `"stream":"seal"`) {
		t.Fatal("missing seal")
	}
	if err = s.CheckObservationCapability(ctx, snapshot.Capability); err != nil {
		t.Fatal(err)
	}
	if err = s.ResetObservationCapture(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckObservationCapability(ctx, snapshot.Capability); err == nil {
		t.Fatal("reset admitted stale package")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations`); n != 4 {
		t.Fatal("reset pruned historical observations")
	}
}

func TestObservationCaptureRestartIntegrityAndDeferredSeal(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	captureExec(t, s, `UPDATE actor_profiles SET bio='persisted'`)
	reopened, err := Open(ctx, s.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	captureExec(t, reopened, `UPDATE actor_profiles SET bio='after restart'`)
	if diag := InspectSchema(ctx, s.Path()); !diag.Current || diag.Newer || diag.SupportedVersion != observationSchemaVersion {
		t.Fatalf("installed capture misclassified by native health: %+v", diag)
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_observations`) != 2 {
		t.Fatal("restart lost feed")
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := &Store{db: s.DB(), queries: tx, captureCache: &captureSchemaCache{}}
	if _, err = ts.beginObservationCapture(ctx); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE actor_profiles SET bio='unsealed'`); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err == nil {
		t.Fatal("deferred seal fence allowed incomplete frame")
	}
	tx.Rollback()
	var retainedBio string
	if err := s.DB().QueryRow(`SELECT bio FROM actor_profiles WHERE node_id='shared-actor'`).Scan(&retainedBio); err != nil || retainedBio != "after restart" {
		t.Fatalf("failed seal published native data: %q %v", retainedBio, err)
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_observations`) != 2 {
		t.Fatal("unsealed frame leaked")
	}
	if _, err = s.DB().Exec(`DROP TRIGGER capture_native_actor_profiles_update`); err != nil {
		t.Fatal(err)
	}
	if diag := InspectSchema(ctx, s.Path()); diag.Current || diag.State != "error" {
		t.Fatalf("broken capture reported healthy: %+v", diag)
	}
	err = reopened.WithTx(ctx, func(tx *Store) error {
		_, err := tx.q().ExecContext(ctx, `UPDATE actor_profiles SET bio='unguarded'`)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "DDL") {
		t.Fatalf("missing guard ignored: %v", err)
	}
}

func TestObservationCaptureConcurrentWriterFrames(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- s.SaveActorProfiles(ctx, []map[string]any{{"id": fmt.Sprintf("actor-%d", i), "login": "fixture", "__typename": "User", "createdAt": "2026-01-01T00:00:00Z"}}, "same-clock")
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_commits WHERE observation_count=1`); n != 4 {
		t.Fatalf("frames=%d", n)
	}
	if _, err := captureCapability(ctx, s.DB()); err != nil {
		t.Fatal(err)
	}
}

func TestObservationCaptureDDLPacket(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "observation-capture-schema.sql")
	if os.Getenv("GITCRAWL_UPDATE_CAPTURE_FIXTURES") == "1" {
		if err := os.WriteFile(path, []byte(ObservationCaptureDDL()), 0644); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != ObservationCaptureDDL() {
		t.Fatal("DDL packet drift")
	}
}

func TestObservationCaptureCancellationAndStreamFailure(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	err := s.WithTx(cancelled, func(tx *Store) error {
		_, err := tx.q().ExecContext(cancelled, `UPDATE actor_profiles SET bio='cancelled'`)
		cancel()
		return err
	})
	if err == nil {
		t.Fatal("cancelled capture committed")
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_observations`) != 0 {
		t.Fatal("cancelled observation survived")
	}
	snap, err := s.OpenObservationSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if _, err = snap.WriteBaseline(ctx, failingCaptureWriter{}); err == nil {
		t.Fatal("failed package sealed")
	}
	var buf bytes.Buffer
	if _, err = snap.WriteBaseline(cancelled, &buf); err == nil || buf.Len() != 0 {
		t.Fatal("cancelled export wrote bytes")
	}
}

type failingCaptureWriter struct{}

func (failingCaptureWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestObservationCaptureReadOnlyNoInstallAndCopyBoundary(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	read, err := OpenReadOnly(ctx, s.Path())
	if err != nil {
		t.Fatal(err)
	}
	read.Close()
	if captureCount(t, s, `SELECT count(*) FROM sqlite_schema WHERE name='capture_meta'`) != 0 {
		t.Fatal("read installed capture")
	}
	if err = s.InstallObservationCapture(ctx); err != nil {
		t.Fatal(err)
	}
	if err = DetachObservationCaptureCopy(ctx, s.Path(), s.Path(), s.DB()); err == nil {
		t.Fatal("live detachment accepted")
	}
	if _, err = s.PrunePortablePayloads(ctx, PortablePruneOptions{}); err == nil || !strings.Contains(err.Error(), "capture-owned") {
		t.Fatalf("live prune=%v", err)
	}
	copyPath := filepath.Join(t.TempDir(), "derived.db")
	if _, err = s.DB().Exec(`VACUUM INTO ?`, copyPath); err != nil {
		t.Fatal(err)
	}
	if err = DetachObservationCaptureCopy(ctx, s.Path(), copyPath, s.DB()); err == nil {
		t.Fatal("mismatched path allowed source handle to detach")
	}
	db, err := sql.Open("sqlite", copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = DetachObservationCaptureCopy(ctx, s.Path(), copyPath, db); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name GLOB 'capture_*' OR name GLOB '_capture_*'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("private capture survived: %d %v", n, err)
	}
	if _, err = captureCapability(ctx, s.DB()); err != nil {
		t.Fatal("copy detachment affected source", err)
	}
}

func TestObservationCaptureTypedLargeKeyAndNUL(t *testing.T) {
	s := captureFixture(t)
	captureExec(t, s, `INSERT INTO blobs(id,sha256,media_type,size_bytes,storage_kind,inline_text,created_at) VALUES(9007199254740993,'large','text',0,'inline',?,'clock')`, "a\x00b<&>")
	var key, value string
	if err := s.DB().QueryRow(`SELECT key_json,value_json FROM capture_images WHERE table_name='blobs'`).Scan(&key, &value); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(key, "9007199254740993") {
		t.Fatal("integer rounded")
	}
	var fields map[string][]any
	if err := json.Unmarshal([]byte(value), &fields); err != nil {
		t.Fatal(err)
	}
	if fields["inline_text"][1] != "a\x00b<&>" {
		t.Fatal("text changed")
	}
}

func TestObservationCaptureNativeEnrichmentHistoryAndSameCommitImages(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	threads, err := s.ListThreads(ctx, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	var thread Thread
	for _, candidate := range threads {
		if candidate.ID == 1 {
			thread = candidate
		}
	}
	thread.UpdatedAtGitHub = "2026-01-01T00:00:00Z"
	var ids []int64
	// Native history is independent of capture's final-key projection: all three
	// immutable revisions persist even inside one surrounding native transaction.
	err = s.WithTx(ctx, func(tx *Store) error {
		for i, state := range []bool{false, true, false} {
			e := ThreadEvidence{Thread: thread, ObservationSequence: int64(i + 1), ReviewThreads: []PullRequestReviewThread{{ReviewThreadID: "R", IsResolved: state, CommentsJSON: `[]`}}}
			r, err := tx.UpsertThreadRevisionAndFingerprint(ctx, e, "2026-01-01T00:00:01Z")
			if err != nil {
				return err
			}
			ids = append(ids, r.RevisionID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] == ids[1] || ids[0] == ids[2] || ids[1] == ids[2] {
		t.Fatalf("native transitions collapsed: %v", ids)
	}
	if n := captureCount(t, s, `SELECT count(*) FROM thread_revisions WHERE thread_id=1`); n != 3 {
		t.Fatal("lost native revisions")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations WHERE table_name='thread_revisions'`); n != 3 {
		t.Fatal("lost distinct revision-key afterimages")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_commits WHERE observation_count>0`); n != 1 {
		t.Fatal("one native transaction split")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_supports WHERE role='github_blob'`); n != 3 {
		t.Fatal("missing captured native dependencies")
	}
}

func TestObservationCaptureReplaceAndReviewRekeyBindings(t *testing.T) {
	s := captureFixture(t)
	captureExec(t, s, `INSERT OR REPLACE INTO actor_profiles(node_id,login,actor_type,observed_at,raw_json) VALUES('shared-actor','new-login','User','same','{}')`)
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations`); n != 1 {
		t.Fatal("REPLACE lost final row or duplicated internal delete")
	}
	captureExec(t, s, `INSERT INTO pull_request_review_threads(thread_id,review_thread_id,comments_json,raw_json,fetched_at) VALUES(1,'old','[]','{}','clock'); INSERT INTO pull_request_review_thread_syncs VALUES(1,'clock','["old"]')`)
	captureExec(t, s, `UPDATE pull_request_review_thread_syncs SET review_thread_ids_json='["new"]'; UPDATE pull_request_review_threads SET review_thread_id='new'`)
	if n := captureCount(t, s, `SELECT count(*) FROM capture_supports s JOIN capture_images i ON i.image_id=s.image_id WHERE s.role='review_thread' AND json_extract(i.value_json,'$.review_thread_id[1]')='new'`); n != 1 {
		t.Fatal("rekey support bound old image")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_supports s JOIN capture_images i ON i.image_id=s.image_id WHERE s.role='review_thread' AND json_extract(i.value_json,'$.review_thread_id[1]')='old'`); n != 1 {
		t.Fatal("rekey rewrote historical support")
	}
}

func TestObservationCaptureEmptyUnknownAndFailClosedTypeSchema(t *testing.T) {
	s := captureFixture(t)
	captureExec(t, s, `INSERT INTO pull_request_review_thread_syncs VALUES(1,'clock',NULL)`)
	captureExec(t, s, `UPDATE pull_request_review_thread_syncs SET review_thread_ids_json='[]'`)
	if n := captureCount(t, s, `SELECT count(*) FROM capture_images WHERE table_name='pull_request_review_thread_syncs'`); n != 2 {
		t.Fatal("empty and unknown collapsed")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_supports`); n != 0 {
		t.Fatal("fabricated members")
	}
	err := s.WithTx(context.Background(), func(tx *Store) error {
		_, err := tx.q().ExecContext(context.Background(), `UPDATE actor_profiles SET node_id=NULL`)
		return err
	})
	if err == nil {
		t.Fatal("NULL key accepted")
	}
	if _, err = s.DB().Exec(`ALTER TABLE actor_profiles ADD COLUMN derived TEXT GENERATED ALWAYS AS ('value') VIRTUAL`); err != nil {
		t.Fatal(err)
	}
	err = s.WithTx(context.Background(), func(tx *Store) error {
		_, err := tx.q().ExecContext(context.Background(), `UPDATE actor_profiles SET bio='ignored field'`)
		return err
	})
	if err == nil {
		t.Fatal("unregistered generated column ignored")
	}
}

func TestObservationCaptureInstallationRefusalsAreAtomic(t *testing.T) {
	for _, tc := range []struct{ name, prepare string }{
		{"partial-schema", `CREATE TABLE capture_heads(unrelated TEXT)`},
		{"new-native-column", `ALTER TABLE actor_profiles ADD COLUMN extra TEXT`},
		{"missing-native-column", `ALTER TABLE actor_profiles DROP COLUMN bio`},
		{"foreign-keys-off", `PRAGMA foreign_keys=OFF`},
		{"newer-schema", `PRAGMA user_version=99`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := exclusionFixture(t)
			s.DB().SetMaxOpenConns(1)
			if _, err := s.DB().Exec(tc.prepare); err != nil {
				t.Fatal(err)
			}
			if err := s.InstallObservationCapture(context.Background()); err == nil {
				t.Fatal("unsupported installation accepted")
			}
			if captureCount(t, s, `SELECT count(*) FROM sqlite_schema WHERE name='capture_meta'`) != 0 {
				t.Fatal("partial installation persisted")
			}
			if captureCount(t, s, `SELECT count(*) FROM threads`) != 2 {
				t.Fatal("installation changed native content")
			}
		})
	}
	s := captureFixture(t)
	if err := s.InstallObservationCapture(context.Background()); err == nil {
		t.Fatal("reinstall accepted")
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_commits`) != 1 {
		t.Fatal("reinstall changed epoch")
	}
}

func TestObservationCaptureCapabilityCorruptionFailsClosed(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"registry", `UPDATE capture_meta SET registry_json='{}'`},
		{"invalidated", `UPDATE capture_meta SET state='invalidated'`},
		{"context", `DELETE FROM _capture_context`},
		{"schema", `DROP INDEX capture_images_key; CREATE INDEX capture_images_key ON capture_images(image_id)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := captureFixture(t)
			ctx := context.Background()
			if _, err := s.DB().Exec(tc.sql); err != nil {
				t.Fatal(err)
			}
			if snap, err := s.OpenObservationSnapshot(ctx); err == nil {
				snap.Close()
				t.Fatal("corrupt capability opened")
			}
			err := s.WithTx(ctx, func(tx *Store) error {
				_, err := tx.q().ExecContext(ctx, `UPDATE actor_profiles SET bio='should not commit'`)
				return err
			})
			if err == nil {
				t.Fatal("corrupt capability allowed native write")
			}
			if captureCount(t, s, `SELECT count(*) FROM actor_profiles WHERE bio IS NOT NULL`) != 0 {
				t.Fatal("guard failure changed native data")
			}
			if reopened, err := Open(ctx, s.Path()); err == nil {
				reopened.Close()
				t.Fatal("restart ignored corrupt capability")
			}
		})
	}
}

func TestObservationCaptureStrictCodecAndMembership(t *testing.T) {
	table, _ := captureTableNamed("blobs")
	for _, key := range []string{`null`, `[]`, `[["integer"]]`, `[["text","1"]]`, `[["integer",1]]`, `[["integer","+1"]]`, `[["integer","9223372036854775808"]]`} {
		if _, _, err := table.keyWhere(key); err == nil {
			t.Fatalf("invalid key accepted: %s", key)
		}
	}
	if _, err := captureTableNamed("unregistered"); err == nil {
		t.Fatal("unknown table accepted")
	}
	for _, r := range []captureRow{{Value: "\xff", Clocks: `{}`}, {Value: `{`, Clocks: `{}`}, {Value: `{}`, Clocks: `{}`}} {
		if err := validateCaptureRow(table, r); err == nil {
			t.Fatal("invalid afterimage accepted")
		}
	}
	s := captureFixture(t)
	ctx := context.Background()
	captureExec(t, s, `INSERT INTO pull_request_review_thread_syncs VALUES(1,'clock',NULL); INSERT INTO pull_request_review_threads(thread_id,review_thread_id,comments_json,raw_json,fetched_at) VALUES(2,'peer','[]','{}','clock')`)
	for _, members := range []string{`null`, `[1]`, `[""]`, `[" spaced "]`, `["x","x"]`, `["peer"]`} {
		err := s.WithTx(ctx, func(tx *Store) error {
			_, err := tx.q().ExecContext(ctx, `UPDATE pull_request_review_thread_syncs SET review_thread_ids_json=? WHERE thread_id=1`, members)
			return err
		})
		if err == nil {
			t.Fatalf("incomplete/cross-parent membership accepted: %s", members)
		}
		if captureCount(t, s, `SELECT count(*) FROM pull_request_review_thread_syncs WHERE thread_id=1 AND review_thread_ids_json IS NULL`) != 1 {
			t.Fatal("failed membership replaced unknown")
		}
	}
}

func TestObservationCaptureRevokedCopyForkAndImmutableHistory(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	captureBlobRevision(t, s)
	for _, query := range []string{`UPDATE capture_images SET value_json='{}'`, `DELETE FROM capture_images`, `UPDATE capture_observations SET clocks_json='{}'`, `DELETE FROM capture_observations`, `UPDATE capture_supports SET clocks_json='{}'`, `DELETE FROM capture_supports`, `UPDATE capture_commits SET sealed_at=''`, `DELETE FROM capture_commits`} {
		if _, err := s.DB().Exec(query); err == nil {
			t.Fatalf("immutable evidence changed: %s", query)
		}
	}
	copyPath := filepath.Join(t.TempDir(), "copy.db")
	if _, err := s.DB().Exec(`VACUUM INTO ?`, copyPath); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before, err := captureCapability(ctx, s.DB())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ResetObservationCapture(ctx, true); err != nil {
		t.Fatal(err)
	}
	after, err := captureCapability(ctx, s.DB())
	if err != nil {
		t.Fatal(err)
	}
	if before.ArchiveID == after.ArchiveID || before.Epoch == after.Epoch {
		t.Fatal("fork reused lineage")
	}
	if err = DetachObservationCaptureCopy(ctx, s.Path(), copyPath, db); err == nil {
		t.Fatal("revoked copy published")
	}
	if n := captureCount(t, s, `SELECT count(*) FROM capture_observations`); n != 2 {
		t.Fatal("fork discarded retained history")
	}
}

func TestObservationCaptureBaselineLateFailureAndClosedSnapshot(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	snap, err := s.OpenObservationSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writer := &limitedCaptureWriter{remaining: 2}
	if _, err = snap.WriteBaseline(ctx, writer); err == nil {
		t.Fatal("interrupted baseline accepted")
	}
	if strings.Contains(writer.buf.String(), `"stream":"seal"`) {
		t.Fatal("interrupted baseline got a seal")
	}
	if err = snap.Close(); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if _, err = snap.WriteBaseline(ctx, &b); err == nil {
		t.Fatal("closed snapshot continued with a new snapshot")
	}
	captureExec(t, s, `UPDATE actor_profiles SET bio='collection continues'`)
}

func TestObservationCaptureBaselineExplicitEmptyStreams(t *testing.T) {
	s := captureFixture(t)
	snap, err := s.OpenObservationSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	var buf bytes.Buffer
	seal, err := snap.WriteBaseline(context.Background(), &buf)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"blobs", "thread_revisions", "pull_request_review_threads", "capture_observations", "capture_images", "capture_supports"} {
		count, ok := seal.Streams[name]
		if !ok || count.Rows != 0 || count.Bytes != 0 || count.SHA256 != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
			t.Fatalf("empty stream not explicitly sealed: %s %+v", name, count)
		}
	}
}

type limitedCaptureWriter struct {
	remaining int
	buf       bytes.Buffer
}

func (w *limitedCaptureWriter) Write(b []byte) (int, error) {
	if w.remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	w.remaining--
	return w.buf.Write(b)
}

func TestObservationCaptureTransactionalSchemaChangeRollsBack(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	err := s.WithTx(ctx, func(tx *Store) error {
		_, err := tx.q().ExecContext(ctx, `UPDATE actor_profiles SET bio='unsealed'; ALTER TABLE actor_profiles ADD COLUMN unregistered TEXT`)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("schema changed during frame: %v", err)
	}
	if captureCount(t, s, `SELECT count(*) FROM pragma_table_info('actor_profiles') WHERE name='unregistered'`) != 0 || captureCount(t, s, `SELECT count(*) FROM actor_profiles WHERE bio IS NOT NULL`) != 0 {
		t.Fatal("failed final schema check committed native changes")
	}
	if captureCount(t, s, `SELECT count(*) FROM capture_observations`) != 0 {
		t.Fatal("failed schema check published frame")
	}
}

func TestObservationCaptureLegacyCellMismatchDoesNotSealBaseline(t *testing.T) {
	for _, cell := range []string{`x'00ff'`, `CAST(x'ff' AS TEXT)`} {
		t.Run(cell, func(t *testing.T) {
			s := exclusionFixture(t)
			ctx := context.Background()
			if _, err := s.DB().Exec(`UPDATE actor_profiles SET raw_json=` + cell); err != nil {
				t.Fatal(err)
			}
			if err := s.InstallObservationCapture(ctx); err != nil {
				t.Fatal(err)
			}
			snap, err := s.OpenObservationSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			var b bytes.Buffer
			if _, err = snap.WriteBaseline(ctx, &b); err == nil {
				t.Fatal("incompatible native value silently coerced")
			}
			if strings.Contains(b.String(), `"stream":"seal"`) {
				t.Fatal("incomplete baseline sealed")
			}
		})
	}
}

func TestObservationCaptureSharedHistoryRefusalAndIdempotentPurge(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	captureExec(t, s, `UPDATE actor_identity_evidence SET raw_json='retained private evidence' WHERE node_id='node10'`)
	captureExec(t, s, `UPDATE actor_profiles SET bio='unrelated retained observer'`)
	// A restored/foreign legacy edge is deliberately not attributed by guessing.
	if _, err := s.DB().Exec(`INSERT INTO capture_supports SELECT o.seq,'review_membership',0,i.image_id,'{}' FROM capture_observations o,capture_images i WHERE o.table_name='actor_profiles' AND i.table_name='actor_identity_evidence'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "ambiguous-fixture"); err == nil || !strings.Contains(err.Error(), "unrelated") {
		t.Fatalf("shared evidence purge accepted: %v", err)
	}
	if captureCount(t, s, `SELECT count(*) FROM threads`) != 2 || captureCount(t, s, `SELECT count(*) FROM capture_images WHERE redaction_epoch IS NOT NULL`) != 0 {
		t.Fatal("refusal partially purged")
	}
	clean := captureFixture(t)
	if _, err := clean.PurgeThreads(ctx, "fixture/repo", []int{10}, "first"); err != nil {
		t.Fatal(err)
	}
	cap, err := captureCapability(ctx, clean.DB())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = clean.PurgeThreads(ctx, "fixture/repo", []int{10}, "repeat"); err != nil {
		t.Fatal(err)
	}
	if err = clean.CheckObservationCapability(ctx, cap); err != nil {
		t.Fatal("idempotent purge revoked a valid epoch", err)
	}
}

func TestObservationCaptureMalformedWireAndFooter(t *testing.T) {
	table, _ := captureTableNamed("actor_profiles")
	for _, r := range []captureRow{
		{Key: `[["text","actor"]]`, Value: `{"node_id":["text","actor"],"login":["text","x"],"actor_type":["text","User"],"name":["null",null],"bio":["null",null],"url":["null",null],"created_at":["null",null],"raw_json":["text","{}"]}`, Clocks: `{"observed_at":["integer","1"]}`},
		{Key: `[["text","actor"]]`, Value: `{"node_id":[],"login":[],"actor_type":[],"name":[],"bio":[],"url":[],"created_at":[],"raw_json":[]}`, Clocks: `{"observed_at":[]}`},
	} {
		if err := validateCaptureRow(table, r); err == nil {
			t.Fatal("malformed wire accepted")
		}
	}
	if _, _, err := table.keyWhere(`[[42,"actor"]]`); err == nil {
		t.Fatal("numeric type label accepted")
	}
	if _, _, err := captureField(captureRow{Value: `not-json`}, "x"); err == nil {
		t.Fatal("invalid support image accepted")
	}
	if _, _, err := captureField(captureRow{Value: `{}`}, "x"); err == nil {
		t.Fatal("missing support field accepted")
	}
	s := captureFixture(t)
	ctx := context.Background()
	r, err := readCaptureRow(ctx, s.q(), table, captureKey([2]string{"text", "shared-actor"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []captureRow{
		{Key: captureKey([2]string{"text", "other-actor"}), Value: r.Value, Clocks: r.Clocks},
		{Key: r.Key, Value: strings.Replace(r.Value, `["null",null]`, `["null","not null"]`, 1), Clocks: r.Clocks},
		{Key: r.Key, Value: strings.Replace(r.Value, `["text","shared-actor"]`, `["text",42]`, 1), Clocks: r.Clocks},
	} {
		if err := validateCaptureRow(table, bad); err == nil {
			t.Fatal("invalid typed value/key binding accepted")
		}
	}
	snap, err := s.OpenObservationSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if _, err = snap.WriteBaseline(ctx, shortFooterWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short footer accepted: %v", err)
	}
	if _, err = snap.WriteBaseline(ctx, shortAllWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short header accepted: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if other, err := s.OpenObservationSnapshot(cancelled); err == nil {
		other.Close()
		t.Fatal("cancelled snapshot opened")
	}
}

type shortFooterWriter struct{}

func (shortFooterWriter) Write(b []byte) (int, error) {
	if bytes.HasPrefix(b, []byte(`{"stream":"seal"`)) {
		return len(b) - 1, nil
	}
	return len(b), nil
}

type shortAllWriter struct{}

func (shortAllWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }

func TestObservationCapturePurgeRefusesHistoricalBlobAfterPointerRepair(t *testing.T) {
	s := captureFixture(t)
	captureBlobRevision(t, s)
	captureExec(t, s, `UPDATE thread_revisions SET raw_json_blob_id=NULL WHERE id=101; INSERT INTO thread_revisions(id,thread_id,content_hash,title_hash,body_hash,labels_hash,created_at) VALUES(102,2,'peer','title','body','labels','clock')`)
	if _, err := s.PurgeThreads(context.Background(), "fixture/repo", []int{10}, "historical-blob"); err == nil || !strings.Contains(err.Error(), "historical blob-backed") {
		t.Fatalf("historical exclusive blob silently retained: %v", err)
	}
	if captureCount(t, s, `SELECT count(*) FROM threads`) != 2 || captureCount(t, s, `SELECT count(*) FROM capture_images WHERE redaction_epoch IS NOT NULL`) != 0 {
		t.Fatal("refusal mutated native/capture data")
	}
}

func TestObservationCaptureBaselineDoesNotOccupyWriterPool(t *testing.T) {
	s := captureFixture(t)
	ctx := context.Background()
	snap, err := s.OpenObservationSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = s.WithTx(bounded, func(tx *Store) error {
		_, err := tx.q().ExecContext(bounded, `UPDATE actor_profiles SET bio='writer-advanced-after-H'`)
		return err
	})
	if err != nil {
		t.Fatalf("baseline blocked collection through the same store: %v", err)
	}
	var b bytes.Buffer
	if _, err = snap.WriteBaseline(ctx, &b); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "writer-advanced-after-H") {
		t.Fatal("separate connection lost pinned baseline")
	}
}
