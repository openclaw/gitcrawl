//go:build native_cluster_overlay

package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNativeProducerCodeNoopAndRollback(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", GitHubRepoID: "1103012935", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
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
	// Inject cancellation after a real document insert; old generation must survive.
	s.DB().Exec("CREATE TRIGGER fail_fixture_code BEFORE INSERT ON code_documents WHEN NEW.path='reject.go' BEGIN SELECT RAISE(ABORT,'synthetic interruption'); END")
	next := append(append([]CodeDocument{}, docs...), CodeDocument{Path: "reject.go", Language: "go", ContentHash: "h2", Text: "next", ByteSize: 4, UpdatedAt: snap.IndexedAt})
	snap.FileCount = 2
	snap.ByteCount = 8
	if _, e = s.ReplaceCodeSnapshot(ctx, snap, next); e == nil {
		t.Fatal("synthetic failure ignored")
	}
	latest, e := s.LatestCodeSnapshot(ctx, repo)
	if e != nil || latest.ID != id || latest.FileCount != 1 {
		t.Fatal("partial replacement visible")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = s.ReplaceCodeSnapshot(cancelled, snap, next); e == nil {
		t.Fatal("cancelled write succeeded")
	}
}
func TestNativeProducerExportConsumerFence(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", GitHubRepoID: "1103012935", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	for n := 1; n <= 2; n++ {
		_, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: string(rune('A' + n)), Number: n, Kind: "issue", State: "open", Title: "fixture", Body: "exact\x00body", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "2026-01-01T00:00:00Z"})
		if e != nil {
			t.Fatal(e)
		}
	}
	p := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":["1"]}`)
	r, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 32, p)
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Tasks) != 1 || r.Tasks[0].ThreadBinding.ThreadID != "2" || !strings.ContainsRune(r.Tasks[0].Input, 0) {
		t.Fatal("consumer exclusion or NUL failed")
	}
	if r.SourceBinding.ConsumerExclusionsSHA256 != NativeSHA256(p) {
		t.Fatal("policy binding missing")
	}
	if r.Tasks[0].InputSHA256 != NativeSHA256([]byte(r.Tasks[0].Input)) {
		t.Fatal("input hash changed")
	}
}
func TestNativeProducerMeasuredCommit(t *testing.T) {
	if os.Getenv("GITCRAWL_SYNTHETIC_COMMIT") != "1" {
		t.Skip("explicit bounded size proof only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "synthetic-commit.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	const n = 52162
	const bytes = 514618215
	body := strings.Repeat("package fixture\nfunc ReadSnapshot() string { return \"synthetic source testing quota guard context history reader\" }\n", 100)
	docs := make([]CodeDocument, 0, n)
	var total int64
	for i := 0; i < n; i++ {
		size := bytes / n
		if i < bytes%n {
			size++
		}
		text := body[:size]
		h := sha256.Sum256([]byte(text))
		path := filepath.Join("synthetic", time.Unix(int64(i), 0).Format("150405")+".go")
		docs = append(docs, CodeDocument{Path: path, Language: "go", ContentHash: hex.EncodeToString(h[:]), Text: text, ByteSize: int64(size), UpdatedAt: "2026-01-01T00:00:00Z"})
		total += int64(size)
	}
	snap := CodeSnapshot{RepoID: repo, SourceRoot: "/synthetic/only", GitSHA: "synthetic", FileCount: n, ByteCount: total, IndexedAt: "2026-01-01T00:00:00Z"}
	started := time.Now()
	id, e := s.ReplaceCodeSnapshot(ctx, snap, docs)
	elapsed := time.Since(started)
	if e != nil {
		t.Fatal(e)
	}
	_, same, e := s.NativeCodeSnapshotMatches(ctx, snap, docs)
	if e != nil || !same {
		t.Fatal("postcommit digest mismatch")
	}
	b, _ := json.MarshalIndent(map[string]any{"synthetic_only": true, "documents": n, "text_bytes": total, "commit_seconds": elapsed.Seconds(), "deadline_seconds": 90, "snapshot_id": id, "postcommit_metadata_equal": true, "finished_at": time.Now().UTC()}, "", "  ")
	if e = os.WriteFile(os.Getenv("GITCRAWL_SYNTHETIC_RECEIPT"), b, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestNativeProducerImportFences(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", GitHubRepoID: "1103012935", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	for n := 1; n <= 2; n++ {
		_, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: string(rune('A' + n)), Number: n, Kind: "issue", State: "open", Title: "fixture", Body: "native\x00text", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "2026-01-01T00:00:00Z"})
		if e != nil {
			t.Fatal(e)
		}
	}
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 32, policy)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(req)
	raw = append(raw, '\n')
	res := NativeEmbeddingResponse{Format: "clawmmunity.native-embeddings.v1", RequestSHA256: NativeSHA256(raw), Model: req.Model, Dimensions: 1536, Tasks: len(req.Tasks), Ready: len(req.Tasks), Status: "complete"}
	for _, task := range req.Tasks {
		vector := make([]float64, 1536)
		vector[0] = 0.12345679
		res.Results = append(res.Results, struct {
			TaskID      string    `json:"task_id"`
			InputSHA256 string    `json:"input_sha256"`
			Vector      []float64 `json:"vector"`
		}{task.TaskID, task.InputSHA256, vector})
	}
	response, _ := json.Marshal(res)
	assertEmpty := func() {
		var n int
		s.DB().QueryRow("select count(*) from thread_vectors").Scan(&n)
		if n != 0 {
			t.Fatal("failed/preview import wrote vectors")
		}
	}
	preview, e := s.PreviewNativeEmbeddingResponse(ctx, raw, response, policy)
	if e != nil || preview.Imported != 2 || preview.CommittedAt != "" {
		t.Fatalf("preview %+v %v", preview, e)
	}
	assertEmpty()
	for _, tc := range []string{"raw_hash", "dimension", "duplicate", "zero", "consumer", "clock"} {
		t.Run(tc, func(t *testing.T) {
			var bad NativeEmbeddingResponse
			json.Unmarshal(response, &bad)
			p := policy
			switch tc {
			case "raw_hash":
				bad.RequestSHA256 = "wrong"
			case "dimension":
				bad.Results[0].Vector = bad.Results[0].Vector[:1535]
			case "duplicate":
				bad.Results[1] = bad.Results[0]
			case "zero":
				bad.Results[0].Vector[0] = 0
			case "consumer":
				p = []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":["2"]}`)
			case "clock":
				s.DB().Exec("update threads set updated_at='2026-01-02T00:00:00Z' where id=2")
			}
			b, _ := json.Marshal(bad)
			if _, e := s.ImportNativeEmbeddingResponse(ctx, raw, b, p); e == nil {
				t.Fatal("invalid response/binding accepted")
			}
			assertEmpty()
			if tc == "clock" {
				s.DB().Exec("update threads set updated_at='2026-01-01T00:00:00Z' where id=2")
			}
		})
	}
	receipt, e := s.ImportNativeEmbeddingResponse(ctx, raw, response, policy)
	if e != nil || receipt.Imported != 2 || receipt.CommittedAt == "" {
		t.Fatalf("import %+v %v", receipt, e)
	}
	var before string
	s.DB().QueryRow("select updated_at from thread_vectors where thread_id=1").Scan(&before)
	receipt, e = s.ImportNativeEmbeddingResponse(ctx, raw, response, policy)
	if e != nil || receipt.Imported != 0 || receipt.Unchanged != 2 {
		t.Fatalf("idempotency %+v %v", receipt, e)
	}
	var after string
	s.DB().QueryRow("select updated_at from thread_vectors where thread_id=1").Scan(&after)
	if before != after {
		t.Fatal("repeat import clock churn")
	}
}
func TestNativeProducerExportProgressAndNoop(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	for n := 1; n <= 3; n++ {
		_, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: string(rune('A' + n)), Number: n, Kind: "issue", State: "open", Title: "fixture", Body: "body", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "2026-01-01T00:00:00Z"})
		if e != nil {
			t.Fatal(e)
		}
	}
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":["2"]}`)
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 1, policy)
		if e != nil || len(req.Tasks) != 1 {
			t.Fatalf("batch %d: %v %+v", i, e, req)
		}
		task := req.Tasks[0]
		if seen[task.ThreadBinding.ThreadID] {
			t.Fatal("prefix repeated instead of progressing")
		}
		seen[task.ThreadBinding.ThreadID] = true
		vector := make([]float64, 1536)
		vector[0] = 1
		id, _ := strconv.ParseInt(task.ThreadBinding.ThreadID, 10, 64)
		if e = s.UpsertThreadVector(ctx, ThreadVector{ThreadID: id, Basis: req.Basis, Model: req.Model, Dimensions: 1536, ContentHash: task.NativeContentHash, Vector: vector, CreatedAt: "t", UpdatedAt: "t"}); e != nil {
			t.Fatal(e)
		}
	}
	req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 1, policy)
	if e != nil || len(req.Tasks) != 0 {
		t.Fatal("fresh corpus was not a no-op")
	}
	s.DB().Exec("update threads set body='changed source' where id=1")
	req, e = s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 1, policy)
	if e != nil || len(req.Tasks) != 1 || req.Tasks[0].ThreadBinding.ThreadID != "1" {
		t.Fatal("stale input missed")
	}
	s.DB().Exec("update threads set body='body' where id=1")
	s.DB().Exec("update thread_vectors set dimensions=10 where thread_id=3")
	req, e = s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 1, policy)
	if e != nil || len(req.Tasks) != 1 || req.Tasks[0].ThreadBinding.ThreadID != "3" {
		t.Fatal("dimension repair missed")
	}
}
func TestNativeProducerExistingWriterNeverMigrates(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "source.db")
	s, e := Open(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	s.DB().Exec("update thread_observation_sequence set value=123 where id=1")
	s.DB().Exec("update observation_schema_convergence set checked_observation_sequence=0 where id=1")
	s.Close()
	os.Chmod(p, 0640)
	w, e := OpenNativeProducerWriter(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	if _, e = w.ReplaceCodeSnapshot(ctx, CodeSnapshot{RepoID: repo, SourceRoot: "synthetic", GitSHA: "s", IndexedAt: "t"}, nil); e != nil {
		t.Fatal(e)
	}
	var checked, allocated int64
	w.DB().QueryRow("select c.checked_observation_sequence,s.value from observation_schema_convergence c join thread_observation_sequence s on s.id=c.id where c.id=1").Scan(&checked, &allocated)
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

func TestNativeProducerPointInputEquivalence(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: "one", Number: 1, Kind: "issue", State: "open", Title: "title", Body: "native", HTMLURL: "https://example.invalid", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	for _, body := range []string{"", "  ", "native\x00input", strings.Repeat("界", 7001)} {
		for _, raw := range []string{"", "  ", "document\x00raw", strings.Repeat("😅", 2000)} {
			for _, doc := range []bool{false, true} {
				s.DB().Exec("delete from documents")
				s.DB().Exec("update threads set body=?", body)
				if doc {
					_, e = s.DB().Exec("insert into documents(thread_id,title,body,raw_text,dedupe_text,updated_at) values(1,'doc',?,?, 'dedupe','t')", body, raw)
					if e != nil {
						t.Fatal(e)
					}
				}
				want, e := s.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{RepoID: repo, Number: 1, Model: "text-embedding-3-small", Force: true})
				if e != nil {
					t.Fatal(e)
				}
				got, e := s.nativeTitleOriginalTask(ctx, repo, 1, "text-embedding-3-small")
				if e != nil || len(want) != 1 || !reflect.DeepEqual(got, want[0]) {
					t.Fatalf("point input differs: %v", e)
				}
				policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
				exported, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 10, policy)
				if e != nil || len(exported.Tasks) != 1 {
					t.Fatal("stream export", e)
				}
				x := exported.Tasks[0]
				binding, clock, e := s.nativeTaskBinding(ctx, 1)
				if e != nil || x.Input != got.Text || x.NativeContentHash != got.ContentHash || x.InputRunes != got.TextRunes || x.TextTruncated != got.TextTruncated || x.ThreadBinding != binding || !reflect.DeepEqual(x.ClockBinding, clock) {
					t.Fatal("stream native projection differs", e)
				}
			}
		}
	}
	for _, sql := range []string{"update threads set state='closed'", "update threads set state='open',closed_at_local='t'"} {
		s.DB().Exec(sql)
		if _, e = s.nativeTitleOriginalTask(ctx, repo, 1, "text-embedding-3-small"); e == nil {
			t.Fatal("ineligible accepted")
		}
	}
}
func TestNativeProducerLargePacketPreview(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	tx, e := s.DB().Begin()
	if e != nil {
		t.Fatal(e)
	}
	for n := 1; n <= 2000; n++ {
		_, e = tx.Exec(`insert into threads(repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) values(?,? ,?,'issue','open','title','body','https://example.invalid','[]','[]','{}','h','t')`, repo, strconv.Itoa(n), n)
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	req, e := s.ExportNativeEmbeddingRequest(ctx, "openclaw/openclaw", 2000, policy)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(req)
	res := NativeEmbeddingResponse{Format: "clawmmunity.native-embeddings.v1", RequestSHA256: NativeSHA256(raw), Model: req.Model, Dimensions: 1536, Tasks: len(req.Tasks), Ready: len(req.Tasks), Status: "complete"}
	for _, task := range req.Tasks {
		v := make([]float64, 1536)
		v[0] = .123
		res.Results = append(res.Results, struct {
			TaskID      string    `json:"task_id"`
			InputSHA256 string    `json:"input_sha256"`
			Vector      []float64 `json:"vector"`
		}{task.TaskID, task.InputSHA256, v})
	}
	response, _ := json.Marshal(res)
	deadline, done := context.WithTimeout(ctx, 15*time.Second)
	defer done()
	start := time.Now()
	got, e := s.PreviewNativeEmbeddingResponse(deadline, raw, response, policy)
	if e != nil || got.Imported != 2000 {
		t.Fatalf("large preview: %+v %v", got, e)
	}
	t.Logf("2000-task preview seconds=%.3f", time.Since(start).Seconds())
	var n int
	s.DB().QueryRow("select count(*) from thread_vectors").Scan(&n)
	if n != 0 {
		t.Fatal("preview wrote")
	}
	// Last-key mutation must still reject the whole packet before any write.
	s.DB().Exec("update documents set raw_text='repair' where thread_id=2000")
	s.DB().Exec("update threads set body='repair' where id=2000")
	if _, e = s.ImportNativeEmbeddingResponse(ctx, raw, response, policy); e == nil {
		t.Fatal("last-key repair accepted")
	}
	s.DB().QueryRow("select count(*) from thread_vectors").Scan(&n)
	if n != 0 {
		t.Fatal("partial import")
	}
}
func TestNativeProducerExportNativeOrderAndTail(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "fixture.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	seedClusterAdmission(t, s)
	repo, e := s.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: `{"private":false,"node_id":"R_FIXTURE"}`, UpdatedAt: "t"})
	if e != nil {
		t.Fatal(e)
	}
	tx, e := s.DB().Begin()
	if e != nil {
		t.Fatal(e)
	}
	for n := 1; n <= 9500; n++ {
		_, e = tx.Exec(`insert into threads(repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at,updated_at_gh) values(?,?,?,'issue','open',?,'body','https://example.invalid','[]','[]','{}','h',?,?)`, repo, strconv.Itoa(n), n, "title"+strconv.Itoa(n), strconv.Itoa(n%4), nil)
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	policy := []byte(`{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":[]}`)
	want, e := s.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{RepoID: repo, Force: true, Basis: "title_original", Model: "text-embedding-3-small"})
	if e != nil {
		t.Fatal(e)
	}
	if len(want) != 9500 {
		t.Fatal(len(want))
	}
	// Install the first9000 fresh vectors; later export must pass the full prefix.
	vector := make([]float64, 1536)
	vector[0] = .25
	raw, _ := json.Marshal(vector)
	tx, e = s.DB().Begin()
	if e != nil {
		t.Fatal(e)
	}
	for _, task := range want[:9000] {
		_, e = tx.Exec(`insert into thread_vectors(thread_id,basis,model,dimensions,content_hash,vector_json,vector_backend,created_at,updated_at) values(?,'title_original','text-embedding-3-small',1536,?,?,'exact','t','t')`, task.ThreadID, task.ContentHash, string(raw))
		if e != nil {
			t.Fatal(e)
		}
	}
	tx.Commit()
	start := time.Now()
	bound, done := context.WithTimeout(ctx, 30*time.Second)
	defer done()
	got, e := s.ExportNativeEmbeddingRequest(bound, "openclaw/openclaw", 2000, policy)
	if e != nil || len(got.Tasks) != 500 {
		t.Fatalf("tail %d %v", len(got.Tasks), e)
	}
	for i, task := range got.Tasks {
		w := want[i+9000]
		if task.ThreadBinding.ThreadID != strconv.FormatInt(w.ThreadID, 10) || task.Input != w.Text || task.NativeContentHash != w.ContentHash {
			t.Fatal("native ordering/input changed")
		}
	}
	t.Logf("9500eligible/9000fresh prefix/500tail export seconds=%.3f", time.Since(start).Seconds())
}

func seedClusterAdmission(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	at := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	for _, key := range []string{"core_baseline_verified:openclaw/openclaw", "through:openclaw/openclaw:issues", "through:openclaw/openclaw:pullRequests"} {
		value := at
		if key == "core_baseline_verified:openclaw/openclaw" {
			value = "1"
		}
		if e := s.SetAnalyticsState(ctx, key, value); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := s.DB().Exec("insert or replace into analytics_coverage(repository,through,issues,pull_requests,complete,observed_at) values('openclaw/openclaw',?,0,0,1,?)", at, at); e != nil {
		t.Fatal(e)
	}
	for _, op := range []string{"discover_issues", "discover_pullRequests"} {
		if _, e := s.DB().Exec("insert into analytics_fetch_attempts(repository,number,operation,started_at,finished_at,status,evidence_json) values('openclaw/openclaw',0,?,?,?,'success','{}')", op, at, at); e != nil {
			t.Fatal(e)
		}
	}
}
