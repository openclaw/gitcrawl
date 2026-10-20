//go:build native_cluster_overlay

package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

// This adapter belongs to the isolated native producer; it is overlaid onto
// pinned schema16 source and is not part of the held capture candidate.
type NativeSourceBinding struct {
	AvailabilitySHA256       string `json:"availability_sha256"`
	SourceCommit             string `json:"source_commit"`
	ArchivePath              string `json:"archive_path"`
	Device                   string `json:"device"`
	Inode                    string `json:"inode"`
	SchemaVersion            int    `json:"schema_version"`
	RepoID                   string `json:"repo_id"`
	ProviderRepoID           string `json:"provider_repo_id"`
	OwnerExclusionsSHA256    string `json:"owner_exclusions_sha256"`
	ConsumerExclusionsSHA256 string `json:"consumer_exclusions_sha256"`
}
type NativeThreadBinding struct {
	ThreadID string `json:"thread_id"`
	GitHubID string `json:"github_id"`
	Number   int    `json:"number"`
	Kind     string `json:"kind"`
}
type NativeClockBinding struct {
	ObservationSequence         string  `json:"observation_sequence"`
	EvidenceObservationSequence string  `json:"evidence_observation_sequence"`
	EvidenceSourceUpdatedAt     string  `json:"evidence_source_updated_at"`
	UpdatedAtGitHub             *string `json:"updated_at_gh"`
	UpdatedAt                   string  `json:"updated_at"`
}
type NativeEmbeddingTask struct {
	TaskID            string              `json:"task_id"`
	Input             string              `json:"input"`
	InputSHA256       string              `json:"input_sha256"`
	NativeContentHash string              `json:"native_content_hash"`
	ThreadBinding     NativeThreadBinding `json:"thread_binding"`
	ClockBinding      NativeClockBinding  `json:"clock_binding"`
	InputBytes        int                 `json:"input_bytes"`
	InputRunes        int                 `json:"input_runes"`
	TextTruncated     bool                `json:"text_truncated"`
	OriginalTextRunes int                 `json:"original_text_runes"`
}
type NativeScope struct {
	IncludeClosed  bool `json:"include_closed"`
	MaxTasks       int  `json:"max_tasks"`
	MaxPacketBytes int  `json:"max_packet_bytes"`
	MaxInputRunes  int  `json:"max_input_runes"`
	MaxInputBytes  int  `json:"max_input_bytes"`
}
type NativeEmbeddingRequest struct {
	Format            string                `json:"format"`
	RequestID         string                `json:"request_id"`
	CreatedAt         string                `json:"created_at"`
	Repository        string                `json:"repository"`
	Model             string                `json:"model"`
	Dimensions        int                   `json:"dimensions"`
	Basis             string                `json:"basis"`
	NativeHashVersion string                `json:"native_hash_version"`
	Scope             NativeScope           `json:"scope"`
	SourceBinding     NativeSourceBinding   `json:"source_binding"`
	Tasks             []NativeEmbeddingTask `json:"tasks"`
}

func NativeSHA256(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (s *Store) nativeSourceBinding(ctx context.Context, repo string) (NativeSourceBinding, error) {
	p, e := filepath.EvalSymlinks(s.path)
	if e != nil {
		return NativeSourceBinding{}, e
	}
	p, e = filepath.Abs(p)
	if e != nil {
		return NativeSourceBinding{}, e
	}
	info, e := os.Stat(p)
	if e != nil {
		return NativeSourceBinding{}, e
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return NativeSourceBinding{}, fmt.Errorf("source file identity unavailable")
	}
	b := NativeSourceBinding{SourceCommit: "6c9c1bea7d094397dcdfecfb50e693460592a5aa", ArchivePath: p, Device: fmt.Sprint(stat.Dev), Inode: fmt.Sprint(stat.Ino)}
	if e = s.q().QueryRowContext(ctx, "pragma user_version").Scan(&b.SchemaVersion); e != nil {
		return b, e
	}
	if b.SchemaVersion != 16 {
		return b, fmt.Errorf("native producer requires schema16, got%d", b.SchemaVersion)
	}
	var id int64
	if e = s.q().QueryRowContext(ctx, "select id,coalesce(github_repo_id,'') from repositories where full_name=?", repo).Scan(&id, &b.ProviderRepoID); e != nil {
		return b, e
	}
	b.RepoID = strconv.FormatInt(id, 10)
	rows, e := s.q().QueryContext(ctx, "select number,kind,original_thread_id,github_id from thread_exclusions where repository=lower(?) order by number", repo)
	if e != nil {
		return b, e
	}
	defer rows.Close()
	values := []any{}
	for rows.Next() {
		var n int
		var kind, gid string
		var tid int64
		if e = rows.Scan(&n, &kind, &tid, &gid); e != nil {
			return b, e
		}
		values = append(values, []any{n, kind, strconv.FormatInt(tid, 10), gid})
	}
	if e = rows.Err(); e != nil {
		return b, e
	}
	raw, _ := json.Marshal(values)
	b.OwnerExclusionsSHA256 = NativeSHA256(raw)
	availability, e := s.ReadClusterAvailability(ctx, repo, time.Now().UTC())
	if e != nil {
		return b, e
	}
	b.AvailabilitySHA256 = availability.SemanticDigest
	return b, nil
}
func (s *Store) nativeTaskBinding(ctx context.Context, tid int64) (NativeThreadBinding, NativeClockBinding, error) {
	var t NativeThreadBinding
	var c NativeClockBinding
	var obs, evidence int64
	var gh sql.NullString
	e := s.q().QueryRowContext(ctx, `select id,github_id,number,kind,observation_sequence,evidence_observation_sequence,evidence_source_updated_at,updated_at_gh,updated_at from threads where id=?`, tid).Scan(&t.ThreadID, &t.GitHubID, &t.Number, &t.Kind, &obs, &evidence, &c.EvidenceSourceUpdatedAt, &gh, &c.UpdatedAt)
	c.ObservationSequence = strconv.FormatInt(obs, 10)
	c.EvidenceObservationSequence = strconv.FormatInt(evidence, 10)
	if gh.Valid {
		v := gh.String
		c.UpdatedAtGitHub = &v
	}
	return t, c, e
}
func (s *Store) ExportNativeEmbeddingRequest(ctx context.Context, repo string, limit int, consumerRaw []byte) (NativeEmbeddingRequest, error) {
	var policy struct {
		Format  string   `json:"format"`
		Dataset string   `json:"dataset"`
		IDs     []string `json:"excluded_thread_ids"`
	}
	if len(consumerRaw) == 0 || len(consumerRaw) > 1<<20 {
		return NativeEmbeddingRequest{}, fmt.Errorf("bounded consumer exclusion input required")
	}
	if e := json.Unmarshal(consumerRaw, &policy); e != nil {
		return NativeEmbeddingRequest{}, e
	}
	if policy.Format != "gitcrawl.embedding.exclusions.v1" || policy.Dataset != "gitcrawl/history/threads" || policy.IDs == nil {
		return NativeEmbeddingRequest{}, fmt.Errorf("invalid consumer exclusion policy")
	}
	blocked := map[string]bool{}
	for _, id := range policy.IDs {
		n, e := strconv.ParseInt(id, 10, 64)
		if e != nil || n <= 0 || strconv.FormatInt(n, 10) != id {
			return NativeEmbeddingRequest{}, fmt.Errorf("invalid exclusion ID")
		}
		blocked[id] = true
	}
	if repo != "openclaw/openclaw" {
		return NativeEmbeddingRequest{}, fmt.Errorf("producer scope is openclaw/openclaw")
	}
	if limit < 1 || limit > 2000 {
		return NativeEmbeddingRequest{}, fmt.Errorf("task cap must be1..2000")
	}
	var request NativeEmbeddingRequest
	e := s.WithTx(ctx, func(tx *Store) error {
		binding, e := tx.nativeSourceBinding(ctx, repo)
		if e != nil {
			return e
		}
		binding.ConsumerExclusionsSHA256 = NativeSHA256(consumerRaw)
		repoID, _ := strconv.ParseInt(binding.RepoID, 10, 64)
		nonce := make([]byte, 16)
		if _, e = rand.Read(nonce); e != nil {
			return e
		}
		request = NativeEmbeddingRequest{Format: "gitcrawl.embedding.request.v1", RequestID: hex.EncodeToString(nonce), CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), Repository: repo, Model: "text-embedding-3-small", Dimensions: 1536, Basis: "title_original", NativeHashVersion: embeddingContentHashVersion, Scope: NativeScope{false, limit, 16 << 20, MaxEmbeddingTextRunes, MaxEmbeddingTextBytes}, SourceBinding: binding, Tasks: []NativeEmbeddingTask{}}
		availability, e := tx.ReadClusterAvailability(ctx, repo, time.Now().UTC())
		if e != nil {
			return e
		}
		for id := range availability.Blocked {
			blocked[strconv.FormatInt(id, 10)] = true
		}
		request.Tasks, e = tx.nativeTitleOriginalExport(ctx, repoID, limit, blocked, request)
		if e != nil {
			return e
		}
		raw, e := json.Marshal(request)
		if e != nil {
			return e
		}
		if len(raw)+1 > 16<<20 {
			return fmt.Errorf("request exceeds16MiB; retry a smaller explicit task cap")
		}
		return nil
	})
	return request, e
}

func (s *Store) NativeCodeWriterReady(ctx context.Context) error {
	var version int
	if e := s.q().QueryRowContext(ctx, "pragma user_version").Scan(&version); e != nil {
		return e
	}
	if version != 16 {
		return fmt.Errorf("schema16 required")
	}
	if s.hasTable(ctx, "capture_commits") {
		return fmt.Errorf("capture-owned source is outside this isolated producer contract")
	}
	changes, e := inspectStructuralCompatibilityMigrations(ctx, s, version, inspectPRDetailSchema(ctx, s))
	if e != nil {
		return e
	}
	if len(changes) != 0 {
		return fmt.Errorf("native schema needs migration; refusing code-only writer")
	}
	return nil
}
func (s *Store) NativeCodeSnapshotMatches(ctx context.Context, snap CodeSnapshot, docs []CodeDocument) (int64, bool, error) {
	old, e := s.LatestCodeSnapshot(ctx, snap.RepoID)
	if e == sql.ErrNoRows {
		return 0, false, nil
	}
	if e != nil {
		return 0, false, e
	}
	if old.RepoID != snap.RepoID || old.SourceRoot != snap.SourceRoot || old.GitSHA != snap.GitSHA || old.WorktreeDirty != snap.WorktreeDirty || old.FileCount != snap.FileCount || old.ByteCount != snap.ByteCount {
		return old.ID, false, nil
	}
	wanted := map[string]CodeDocument{}
	for _, d := range docs {
		if _, ok := wanted[d.Path]; ok {
			return 0, false, fmt.Errorf("duplicate code path")
		}
		wanted[d.Path] = d
	}
	rows, e := s.q().QueryContext(ctx, "select repo_id,path,language,content_hash,byte_size from code_documents where snapshot_id=?", old.ID)
	if e != nil {
		return 0, false, e
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var repo, size int64
		var path, lang, hash string
		if e = rows.Scan(&repo, &path, &lang, &hash, &size); e != nil {
			return 0, false, e
		}
		d, ok := wanted[path]
		if !ok || repo != snap.RepoID || lang != d.Language || hash != d.ContentHash || size != d.ByteSize {
			return old.ID, false, nil
		}
		seen++
	}
	if e = rows.Err(); e != nil {
		return 0, false, e
	}
	return old.ID, seen == len(docs), nil
}

type NativeEmbeddingResponse struct {
	Format        string `json:"format"`
	RequestSHA256 string `json:"request_sha256"`
	Model         string `json:"model"`
	Dimensions    int    `json:"dimensions"`
	Tasks         int    `json:"tasks"`
	Ready         int    `json:"ready"`
	Pending       int    `json:"pending"`
	Failed        int    `json:"failed"`
	Status        string `json:"status"`
	Results       []struct {
		TaskID      string    `json:"task_id"`
		InputSHA256 string    `json:"input_sha256"`
		Vector      []float64 `json:"vector"`
	} `json:"results"`
}
type NativeImportReceipt struct {
	RequestSHA256            string `json:"request_sha256"`
	ResponseSHA256           string `json:"response_sha256"`
	Imported                 int    `json:"imported"`
	Unchanged                int    `json:"unchanged"`
	ConsumerExclusionsSHA256 string `json:"consumer_exclusions_sha256"`
	VectorEncoding           string `json:"vector_encoding"`
	CommittedAt              string `json:"committed_at"`
}

func (s *Store) ImportNativeEmbeddingResponse(ctx context.Context, requestRaw, responseRaw, consumerRaw []byte) (NativeImportReceipt, error) {
	return s.nativeEmbeddingResponse(ctx, requestRaw, responseRaw, consumerRaw, true)
}
func (s *Store) PreviewNativeEmbeddingResponse(ctx context.Context, requestRaw, responseRaw, consumerRaw []byte) (NativeImportReceipt, error) {
	return s.nativeEmbeddingResponse(ctx, requestRaw, responseRaw, consumerRaw, false)
}
func (s *Store) nativeEmbeddingResponse(ctx context.Context, requestRaw, responseRaw, consumerRaw []byte, apply bool) (NativeImportReceipt, error) {
	receipt := NativeImportReceipt{RequestSHA256: NativeSHA256(requestRaw), ResponseSHA256: NativeSHA256(responseRaw), ConsumerExclusionsSHA256: NativeSHA256(consumerRaw), VectorEncoding: "processor OpenAI base64 float32 / PostgreSQL vector(1536), decimal components preserved; native Cosine normalization"}
	if len(requestRaw) > 16<<20 || len(responseRaw) > 80<<20 || len(consumerRaw) > 1<<20 {
		return receipt, fmt.Errorf("bounded packet limits exceeded")
	}
	var req NativeEmbeddingRequest
	var res NativeEmbeddingResponse
	var policy struct {
		Format  string   `json:"format"`
		Dataset string   `json:"dataset"`
		IDs     []string `json:"excluded_thread_ids"`
	}
	for _, x := range []struct {
		b []byte
		v any
	}{{requestRaw, &req}, {responseRaw, &res}, {consumerRaw, &policy}} {
		if e := json.Unmarshal(x.b, x.v); e != nil {
			return receipt, e
		}
	}
	if policy.Format != "gitcrawl.embedding.exclusions.v1" || policy.Dataset != "gitcrawl/history/threads" || policy.IDs == nil {
		return receipt, fmt.Errorf("consumer exclusion input required")
	}
	blocked := map[string]bool{}
	for _, id := range policy.IDs {
		blocked[id] = true
	}
	if req.Format != "gitcrawl.embedding.request.v1" || req.Repository != "openclaw/openclaw" || req.Model != "text-embedding-3-small" || req.Dimensions != 1536 || req.Basis != "title_original" || req.NativeHashVersion != embeddingContentHashVersion || req.Scope.IncludeClosed || req.Scope.MaxInputRunes != 6000 || req.Scope.MaxInputBytes != 7000 || len(req.Tasks) == 0 || len(req.Tasks) > 2000 {
		return receipt, fmt.Errorf("native request contract mismatch")
	}
	if res.Format != "clawmmunity.native-embeddings.v1" || res.RequestSHA256 != receipt.RequestSHA256 || res.Model != req.Model || res.Dimensions != req.Dimensions || res.Status != "complete" || res.Pending != 0 || res.Failed != 0 || res.Ready != len(req.Tasks) || res.Tasks != len(req.Tasks) || len(res.Results) != len(req.Tasks) {
		return receipt, fmt.Errorf("broker response incomplete or not request-bound")
	}
	var strict struct {
		Results []struct {
			Vector []json.RawMessage `json:"vector"`
		} `json:"results"`
	}
	if e := json.Unmarshal(responseRaw, &strict); e != nil {
		return receipt, e
	}
	for _, r := range strict.Results {
		for _, component := range r.Vector {
			if bytes.Equal(bytes.TrimSpace(component), []byte("null")) {
				return receipt, fmt.Errorf("null vector component")
			}
		}
	}
	outputs := map[string][]float64{}
	hashes := map[string]string{}
	for _, v := range res.Results {
		if _, ok := outputs[v.TaskID]; ok {
			return receipt, fmt.Errorf("duplicate response task")
		}
		if len(v.Vector) != 1536 {
			return receipt, fmt.Errorf("vector dimension mismatch")
		}
		nonzero := false
		for _, f := range v.Vector {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return receipt, fmt.Errorf("nonfinite vector")
			}
			nonzero = nonzero || f != 0
		}
		if !nonzero {
			return receipt, fmt.Errorf("zero vector")
		}
		outputs[v.TaskID] = v.Vector
		hashes[v.TaskID] = v.InputSHA256
	}
	seen := map[string]bool{}
	for _, task := range req.Tasks {
		if seen[task.TaskID] {
			return receipt, fmt.Errorf("duplicate request task")
		}
		seen[task.TaskID] = true
		if blocked[task.ThreadBinding.ThreadID] {
			return receipt, fmt.Errorf("consumer-excluded task")
		}
		if task.InputSHA256 != NativeSHA256([]byte(task.Input)) || task.NativeContentHash != embeddingContentHash(req.Basis, req.Model, task.Input) || len(task.Input) > 7000 || utf8.RuneCountInString(task.Input) > 6000 || hashes[task.TaskID] != task.InputSHA256 {
			return receipt, fmt.Errorf("input hash/bounds mismatch")
		}
		copy := task
		copy.TaskID = ""
		raw, _ := json.Marshal(struct {
			RequestID string
			Source    NativeSourceBinding
			Task      NativeEmbeddingTask
		}{req.RequestID, req.SourceBinding, copy})
		if NativeSHA256(raw) != task.TaskID {
			return receipt, fmt.Errorf("native task binding mismatch")
		}
	}
	e := s.nativeClusterWithTx(ctx, func(tx *Store) error {
		receipt.Imported = 0
		receipt.Unchanged = 0
		current, e := tx.nativeSourceBinding(ctx, req.Repository)
		if e != nil {
			return e
		}
		current.ConsumerExclusionsSHA256 = NativeSHA256(consumerRaw)
		if current.AvailabilitySHA256 != req.SourceBinding.AvailabilitySHA256 {
			return fmt.Errorf("native availability changed")
		}
		if current != req.SourceBinding {
			return fmt.Errorf("native source identity or exclusion set changed")
		}
		repoID, _ := strconv.ParseInt(current.RepoID, 10, 64)
		availability, e := tx.ReadClusterAvailability(ctx, req.Repository, time.Now().UTC())
		if e != nil {
			return e
		}
		// Validate ALL bindings before the first write. A stale peer rejects the batch.
		for _, task := range req.Tasks {
			id, e := strconv.ParseInt(task.ThreadBinding.ThreadID, 10, 64)
			if e != nil {
				return e
			}
			if availability.Blocked[id] {
				return fmt.Errorf("native availability changed: unavailable task")
			}
			t, c, e := tx.nativeTaskBinding(ctx, id)
			if e != nil {
				return e
			}
			if t != task.ThreadBinding || !reflect.DeepEqual(c, task.ClockBinding) {
				return fmt.Errorf("stale native identity/clock task %s", task.TaskID)
			}
			now, e := tx.nativeTitleOriginalTask(ctx, repoID, id, req.Model)
			if e != nil {
				return e
			}
			if now.ContentHash != task.NativeContentHash || now.Text != task.Input {
				return fmt.Errorf("native input/eligibility changed for task %s", task.TaskID)
			}
		}
		at := time.Now().UTC().Format(time.RFC3339Nano)
		for _, task := range req.Tasks {
			id, _ := strconv.ParseInt(task.ThreadBinding.ThreadID, 10, 64)
			vector := outputs[task.TaskID]
			raw, _ := json.Marshal(vector)
			var oldHash string
			var oldDim int
			var oldBytes []byte
			e := tx.q().QueryRowContext(ctx, "select content_hash,dimensions,vector_json from thread_vectors where thread_id=? and basis=? and model=?", id, req.Basis, req.Model).Scan(&oldHash, &oldDim, &oldBytes)
			if e != nil && e != sql.ErrNoRows {
				return e
			}
			if e == nil && oldHash == task.NativeContentHash && oldDim == 1536 {
				old, e := decodeStoredVector(oldBytes)
				if e == nil && reflect.DeepEqual(old, vector) {
					receipt.Unchanged++
					continue
				}
			}
			if !apply {
				receipt.Imported++
				continue
			}
			_, e = tx.q().ExecContext(ctx, `insert into thread_vectors(thread_id,basis,model,dimensions,content_hash,vector_json,vector_backend,created_at,updated_at) values(?,?,?,?,?,?,'exact',?,?) on conflict(thread_id,basis,model) do update set dimensions=excluded.dimensions,content_hash=excluded.content_hash,vector_json=excluded.vector_json,vector_backend=excluded.vector_backend,updated_at=excluded.updated_at`, id, req.Basis, req.Model, 1536, task.NativeContentHash, string(raw), at, at)
			if e != nil {
				return e
			}
			receipt.Imported++
		}
		if apply {
			receipt.CommittedAt = at
		}
		return nil
	})
	return receipt, e
}

func (s *Store) nativeVectorMatchesTask(ctx context.Context, task EmbeddingTask, basis, model string, dimensions int) (bool, error) {
	var hash string
	var dims int
	var encoded []byte
	e := s.q().QueryRowContext(ctx, "select content_hash,dimensions,vector_json from thread_vectors where thread_id=? and basis=? and model=?", task.ThreadID, basis, model).Scan(&hash, &dims, &encoded)
	if e == sql.ErrNoRows {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if hash != task.ContentHash || dims != dimensions {
		return false, nil
	}
	vector, e := decodeStoredVector(encoded)
	if e != nil || len(vector) != dimensions {
		return false, nil
	}
	nonzero := false
	for _, v := range vector {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false, nil
		}
		nonzero = nonzero || v != 0
	}
	return nonzero, nil
}

// OpenNativeProducerWriter opens only an existing schema16 archive. Unlike Open,
// it performs NO migration, convergence repair, permission change, or creation.
func OpenNativeProducerWriter(ctx context.Context, path string) (*Store, error) {
	info, e := os.Stat(path)
	if e != nil {
		return nil, e
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("existing regular native archive required")
	}
	absolute, e := filepath.Abs(path)
	if e != nil {
		return nil, e
	}
	u := url.URL{Scheme: "file", Path: absolute}
	q := u.Query()
	q.Set("mode", "rw")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "synchronous(FULL)")
	if runtime.GOOS == "darwin" {
		q.Add("_pragma", "fullfsync(1)")
		q.Add("_pragma", "checkpoint_fullfsync(1)")
	}
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, path: path}
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, e
	}
	if e = s.NativeCodeWriterReady(ctx); e != nil {
		db.Close()
		return nil, e
	}
	var journal string
	if e = db.QueryRowContext(ctx, "pragma journal_mode").Scan(&journal); e != nil || journal != "wal" {
		db.Close()
		return nil, fmt.Errorf("existing WAL archive required")
	}
	return s, nil
}

// The broker contract is fixed to title_original. This indexed point read is
// the same input projection and native text/hash builder as listEmbeddingTaskPage;
// unrelated summary/revision subqueries and optional-number scan are unnecessary.
func (s *Store) nativeTitleOriginalTask(ctx context.Context, repoID, id int64, model string) (EmbeddingTask, error) {
	var task EmbeddingTask
	var body, rawText, dedupe string
	e := s.q().QueryRowContext(ctx, `select t.id,t.number,t.kind,t.title,
 coalesce(d.body,t.body,''),coalesce(d.raw_text,t.body,''),coalesce(d.dedupe_text,t.title||' '||coalesce(t.body,''))
 from threads t left join documents d on d.thread_id=t.id
 where t.id=? and t.repo_id=? and t.state='open' and t.closed_at_local is null`, id, repoID).Scan(&task.ThreadID, &task.Number, &task.Kind, &task.Title, &body, &rawText, &dedupe)
	if e != nil {
		return task, fmt.Errorf("native task input/eligibility: %w", e)
	}
	text, meta, e := embeddingTextForBasisWithMeta("title_original", task.Title, body, rawText, dedupe, "")
	if e != nil {
		return task, e
	}
	if strings.TrimSpace(text) == "" {
		return task, fmt.Errorf("native task input is empty")
	}
	task.Text = text
	task.ContentHash = embeddingContentHash("title_original", model, text)
	task.TextTruncated = meta.Truncated
	task.OriginalTextRunes = meta.OriginalRunes
	task.TextRunes = meta.Runes
	return task, nil
}

// One ordered eligible-row cursor avoids repeating the generic summary/revision
// projection on every page. Scope/order/body fallback and native text/hash
// builders are identical to title_original listEmbeddingTaskPage. All fields
// and vector freshness checks belong to this same source read transaction.
func (s *Store) nativeTitleOriginalExport(ctx context.Context, repoID int64, limit int, blocked map[string]bool, req NativeEmbeddingRequest) ([]NativeEmbeddingTask, error) {
	rows, e := s.q().QueryContext(ctx, `select t.id,t.github_id,t.number,t.kind,t.title,
 coalesce(d.body,t.body,''),coalesce(d.raw_text,t.body,''),coalesce(d.dedupe_text,t.title||' '||coalesce(t.body,'')),
 t.observation_sequence,t.evidence_observation_sequence,t.evidence_source_updated_at,t.updated_at_gh,t.updated_at,
 tv.content_hash,tv.dimensions,tv.vector_json,
 exists(select 1 from thread_exclusions x where x.repository=lower(?) and x.number=t.number)
 from threads t left join documents d on d.thread_id=t.id
 left join thread_vectors tv on tv.thread_id=t.id and tv.basis=? and tv.model=?
 where t.repo_id=? and t.state='open' and t.closed_at_local is null
 order by coalesce(t.updated_at_gh,t.updated_at) desc,t.number desc,t.id desc`, req.Repository, req.Basis, req.Model, repoID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []NativeEmbeddingTask{}
	for rows.Next() {
		var t NativeThreadBinding
		var c NativeClockBinding
		var title, body, rawText, dedupe string
		var obs, evidence int64
		var gh, oldHash sql.NullString
		var dims sql.NullInt64
		var vectorRaw []byte
		var excluded bool
		if e = rows.Scan(&t.ThreadID, &t.GitHubID, &t.Number, &t.Kind, &title, &body, &rawText, &dedupe, &obs, &evidence, &c.EvidenceSourceUpdatedAt, &gh, &c.UpdatedAt, &oldHash, &dims, &vectorRaw, &excluded); e != nil {
			return nil, e
		}
		if blocked[t.ThreadID] {
			continue
		}
		if excluded {
			return nil, fmt.Errorf("excluded task reached export")
		}
		text, meta, e := embeddingTextForBasisWithMeta(req.Basis, title, body, rawText, dedupe, "")
		if e != nil {
			return nil, e
		}
		if strings.TrimSpace(text) == "" {
			continue
		}
		hash := embeddingContentHash(req.Basis, req.Model, text)
		if oldHash.Valid && oldHash.String == hash && dims.Valid && dims.Int64 == int64(req.Dimensions) {
			vector, e := decodeStoredVector(vectorRaw)
			valid := e == nil && len(vector) == req.Dimensions
			nonzero := false
			for _, n := range vector {
				valid = valid && !math.IsNaN(n) && !math.IsInf(n, 0)
				nonzero = nonzero || n != 0
			}
			if valid && nonzero {
				continue
			}
		}
		c.ObservationSequence = strconv.FormatInt(obs, 10)
		c.EvidenceObservationSequence = strconv.FormatInt(evidence, 10)
		if gh.Valid {
			v := gh.String
			c.UpdatedAtGitHub = &v
		}
		x := NativeEmbeddingTask{Input: text, InputSHA256: NativeSHA256([]byte(text)), NativeContentHash: hash, ThreadBinding: t, ClockBinding: c, InputBytes: len(text), InputRunes: meta.Runes, TextTruncated: meta.Truncated, OriginalTextRunes: meta.OriginalRunes}
		raw, e := json.Marshal(struct {
			RequestID string
			Source    NativeSourceBinding
			Task      NativeEmbeddingTask
		}{req.RequestID, req.SourceBinding, x})
		if e != nil {
			return nil, e
		}
		x.TaskID = NativeSHA256(raw)
		out = append(out, x)
		if len(out) == limit {
			break
		}
	}
	return out, rows.Err()
}

// ValidateNativeEmbeddingRequest fences queued/new work before broker admission.
func (s *Store) ValidateNativeEmbeddingRequest(ctx context.Context, raw, consumer []byte) error {
	if len(raw) > 16<<20 || len(consumer) > 1<<20 {
		return fmt.Errorf("bounded native request required")
	}
	var req NativeEmbeddingRequest
	if e := json.Unmarshal(raw, &req); e != nil {
		return e
	}
	if req.Format != "gitcrawl.embedding.request.v1" || req.Repository != "openclaw/openclaw" || req.Basis != "title_original" || req.Model != "text-embedding-3-small" || req.Dimensions != 1536 || len(req.Tasks) > 2000 {
		return fmt.Errorf("native request contract mismatch")
	}
	return s.nativeClusterWithTx(ctx, func(tx *Store) error {
		current, e := tx.nativeSourceBinding(ctx, req.Repository)
		if e != nil {
			return e
		}
		current.ConsumerExclusionsSHA256 = NativeSHA256(consumer)
		if current.AvailabilitySHA256 != req.SourceBinding.AvailabilitySHA256 {
			return fmt.Errorf("native availability changed")
		}
		if current != req.SourceBinding {
			return fmt.Errorf("native source identity or exclusion set changed")
		}
		a, e := tx.ReadClusterAvailability(ctx, req.Repository, time.Now().UTC())
		if e != nil {
			return e
		}
		repoID, _ := strconv.ParseInt(current.RepoID, 10, 64)
		var p struct {
			IDs []string `json:"excluded_thread_ids"`
		}
		if e = json.Unmarshal(consumer, &p); e != nil {
			return e
		}
		blocked := map[string]bool{}
		for _, id := range p.IDs {
			blocked[id] = true
		}
		for _, t := range req.Tasks {
			id, e := strconv.ParseInt(t.ThreadBinding.ThreadID, 10, 64)
			if e != nil || a.Blocked[id] || blocked[t.ThreadBinding.ThreadID] {
				return fmt.Errorf("native availability changed: blocked task")
			}
			binding, clock, e := tx.nativeTaskBinding(ctx, id)
			if e != nil {
				return e
			}
			if binding != t.ThreadBinding || !reflect.DeepEqual(clock, t.ClockBinding) {
				return fmt.Errorf("stale native identity/clock task %s", t.TaskID)
			}
			task, e := tx.nativeTitleOriginalTask(ctx, repoID, id, req.Model)
			if e != nil {
				return e
			}
			if task.Text != t.Input || task.ContentHash != t.NativeContentHash || t.InputSHA256 != NativeSHA256([]byte(t.Input)) {
				return fmt.Errorf("native input changed")
			}
			copy := t
			copy.TaskID = ""
			b, _ := json.Marshal(struct {
				RequestID string
				Source    NativeSourceBinding
				Task      NativeEmbeddingTask
			}{req.RequestID, req.SourceBinding, copy})
			if t.TaskID != NativeSHA256(b) {
				return fmt.Errorf("native task binding mismatch")
			}
		}
		return nil
	})
}
