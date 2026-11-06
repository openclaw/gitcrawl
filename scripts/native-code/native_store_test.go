//go:build native_code_overlay

package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNativeProducerCodeNoopAndRollback(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", GitHubRepoID: "1103012935", RawJSON: "{}", UpdatedAt: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	snap := CodeSnapshot{RepoID: repo, SourceRoot: "/synthetic", GitSHA: "a", FileCount: 1, ByteCount: 4, IndexedAt: "2026-01-01T00:00:00Z"}
	docs := []CodeDocument{{Path: "a.go", Language: "go", ContentHash: "h1", Text: "code", ByteSize: 4, UpdatedAt: snap.IndexedAt}}
	id, e := s.ReplaceCodeSnapshot(ctx, snap, docs)
	if e != nil {
		t.Fatal(e)
	}
	id2, same, e := s.NativeCodeSnapshotMatches(ctx, snap, docs)
	if e != nil || !same || id != id2 {
		t.Fatalf("noop %d %v %v", id2, same, e)
	}
	docs[0].Language = "text"
	_, same, e = s.NativeCodeSnapshotMatches(ctx, snap, docs)
	if e != nil || same {
		t.Fatal("language change missed")
	}
	docs[0].Language = "go"
	// Abort the second insert; the previous generation must survive.
	if _, e := s.DB().Exec("CREATE TRIGGER fail_fixture_code BEFORE INSERT ON code_documents WHEN NEW.path='reject.go' BEGIN SELECT RAISE(ABORT,'synthetic interruption'); END"); e != nil {
		t.Fatal(e)
	}
	next := append(append([]CodeDocument{}, docs...), CodeDocument{Path: "reject.go", Language: "go", ContentHash: "h2", Text: "next", ByteSize: 4, UpdatedAt: snap.IndexedAt})
	snap.FileCount = 2
	snap.ByteCount = 8
	if _, e = s.ReplaceCodeSnapshot(ctx, snap, next); e == nil || !strings.Contains(e.Error(), "synthetic interruption") {
		t.Fatalf("expected synthetic insert failure, got %v", e)
	}
	latest, e := s.LatestCodeSnapshot(ctx, repo)
	if e != nil || latest.ID != id || latest.FileCount != 1 {
		t.Fatal("partial replacement visible")
	}
	if _, e := s.DB().Exec("DROP TRIGGER fail_fixture_code"); e != nil {
		t.Fatal(e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = s.ReplaceCodeSnapshot(cancelled, snap, next); !errors.Is(e, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", e)
	}
	latest, e = s.LatestCodeSnapshot(ctx, repo)
	if e != nil || latest.ID != id || latest.FileCount != 1 {
		t.Fatalf("cancelled replacement changed generation: %+v, %v", latest, e)
	}
	id2, e = s.ReplaceCodeSnapshot(ctx, snap, next)
	if e != nil {
		t.Fatalf("healthy replacement after cancellation: %v", e)
	}
	matchedID, same, e := s.NativeCodeSnapshotMatches(ctx, snap, next)
	if e != nil || !same || matchedID != id2 {
		t.Fatalf("healthy replacement mismatch: %d %v %v", matchedID, same, e)
	}
}
func TestNativeProducerExistingWriterNeverMigrates(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "source.db")
	s, e := Open(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := s.DB().Exec("update thread_observation_sequence set value=123 where id=1"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.DB().Exec("update observation_schema_convergence set checked_observation_sequence=0 where id=1"); e != nil {
		t.Fatal(e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(p, 0640); e != nil {
		t.Fatal(e)
	}
	w, e := OpenNativeProducerWriter(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	if _, e = w.ReplaceCodeSnapshot(ctx, CodeSnapshot{RepoID: repo, SourceRoot: "synthetic", GitSHA: "s", IndexedAt: "t"}, nil); e != nil {
		t.Fatal(e)
	}
	var checked, allocated int64
	if e := w.DB().QueryRow("select c.checked_observation_sequence,s.value from observation_schema_convergence c join thread_observation_sequence s on s.id=c.id where c.id=1").Scan(&checked, &allocated); e != nil {
		t.Fatal(e)
	}
	if checked != 0 || allocated != 123 {
		t.Fatal("writer performed convergence repair")
	}
	info, e := os.Stat(p)
	if e != nil || info.Mode().Perm() != 0640 {
		t.Fatal("writer changed permissions")
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	if st, e := OpenNativeProducerWriter(ctx, missing); e == nil {
		st.Close()
		t.Fatal("missing source accepted")
	}
	if _, e = os.Stat(missing); !os.IsNotExist(e) {
		t.Fatal("source was created")
	}
}

func TestNativeCodeWriterFlushAndObsoleteReplacement(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "source.db")
	s, e := Open(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = OpenNativeProducerWriter(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var level, full, checkpoint int
	if e := s.DB().QueryRow("pragma synchronous").Scan(&level); e != nil {
		t.Fatal(e)
	}
	if e := s.DB().QueryRow("pragma fullfsync").Scan(&full); e != nil {
		t.Fatal(e)
	}
	if e := s.DB().QueryRow("pragma checkpoint_fullfsync").Scan(&checkpoint); e != nil {
		t.Fatal(e)
	}
	if level < 2 || runtime.GOOS == "darwin" && (full != 1 || checkpoint != 1) {
		t.Fatalf("writer durability %d %d %d", level, full, checkpoint)
	}
	first := CodeSnapshot{RepoID: repo, SourceRoot: "owned-cache", GitSHA: "first", FileCount: 1, ByteCount: 3, IndexedAt: "first"}
	if _, e = s.ReplaceCodeSnapshot(ctx, first, []CodeDocument{{RepoID: repo, Path: "old.go", Text: "old", ContentHash: "h", ByteSize: 3}}); e != nil {
		t.Fatal(e)
	}
	second := first
	second.GitSHA = "second"
	second.IndexedAt = "second"
	id, e := s.ReplaceCodeSnapshot(ctx, second, []CodeDocument{{RepoID: repo, Path: "renamed.go", Text: "new", ContentHash: "new", ByteSize: 3}})
	if e != nil {
		t.Fatal(e)
	}
	var old, foreign int
	if e := s.DB().QueryRow("select count(*) from code_documents where path='old.go'").Scan(&old); e != nil {
		t.Fatal(e)
	}
	if e := s.DB().QueryRow("select count(*) from code_documents where snapshot_id!=?", id).Scan(&foreign); e != nil {
		t.Fatal(e)
	}
	if old != 0 || foreign != 0 {
		t.Fatal("obsolete generation retained in current code index")
	}
}
