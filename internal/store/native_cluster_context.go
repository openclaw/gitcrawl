package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"hash"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

const NativeClusterContextProfile = "gitcrawl.public-durable-cluster-context/v1"
const NativeClusterContextView = "durable-active/v1"

var ErrNativeClusterContext = errors.New("cluster context requires a matching public native archive and valid durable selection")

type NativeClusterContextRequest struct {
	Repository string
	File       NativeReportFile
	// Limits cover compact scalar reads plus qualification history, not full
	// body lengths, wire bytes or process RSS. A deadline is also required.
	Limits NativeReportLimits
}

// Nil flags mean qualification was not recorded, not false. Even a valid
// scope is historical evidence, not current provider or publication authority.
type NativeClusterContextScope struct {
	Mode, PolicyDigest, SourceThrough, SourceObservedAt              string
	ProviderComplete, FullNativeScopeComplete, SelectedInputComplete bool
	IdentityReconciliationComplete                                   *bool
	PublicationStatus                                                string
	RetainedPriorMemberships                                         int64
}
type NativeClusterContextSelection struct {
	Source, ScopeStatus string
	RunID               *int64
	Scope               *NativeClusterContextScope
	AnalysisRunID       *int64
	AnalysisStatus      string
	AnalysisScope       *NativeClusterContextScope
}
type NativeClusterContextGroup struct {
	ID                     int64
	Status                 string
	ClosedAt               *string
	RepresentativeThreadID *int64
	MemberCount            int64
	Quarantined            bool
}
type NativeClusterContextMember struct {
	GroupID, ThreadID int64
	State, Role       string
	LastSeenRunID     *int64
	Generation        string
}
type NativeClusterContextThread struct {
	ID, Number                     int64
	Kind, State, Title, LabelsJSON string
	NodeID                         *string // Native guarded JSON extraction with SQLite TEXT affinity.
	UpdatedAt                      *string // Native provider updated_at_gh, not local acquisition time.
	Body                           *string // At most 800 UTF-8 runes; NULL and empty remain distinct.
}
type NativeClusterContext struct {
	Profile, View, Repository, GitHubRepositoryID string
	File                                          NativeReportFile // Local custody only; never exported by a wire adapter.
	RepositoryID                                  int64
	Selection                                     NativeClusterContextSelection
	Groups                                        []NativeClusterContextGroup
	Members                                       []NativeClusterContextMember
	Threads                                       []NativeClusterContextThread
	SHA256                                        string // Local typed-content digest, not a journal or delivery cursor.
	Rows, Bytes                                   int64  // Compact output scalars; limits also charge selection reads.
}

// CaptureNativeClusterContext reads a full bounded durable-active view in one
// transaction. It never migrates, rearms tracking, repairs a representative or
// acquires provider data. Legacy selection is retained as explicitly unknown;
// an empty legacy view must not be used as verified absence of clusters.
func CaptureNativeClusterContext(ctx context.Context, path string, request NativeClusterContextRequest) (NativeClusterContext, error) {
	if err := ctx.Err(); err != nil {
		return NativeClusterContext{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return NativeClusterContext{}, errors.New("cluster context requires a deadline")
	}
	if request.Limits.MaxRows <= 0 || request.Limits.MaxBytes <= 0 {
		return NativeClusterContext{}, ErrNativeReportLimit
	}
	parts := strings.Split(request.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ToLower(request.Repository) != request.Repository || strings.ContainsAny(request.Repository, " \t\r\n\\\x00") || !utf8.ValidString(request.Repository) {
		return NativeClusterContext{}, ErrNativeClusterContext
	}
	file, err := NativeReportFileIdentity(path)
	if err != nil || file != request.File {
		return NativeClusterContext{}, ErrNativeClusterContext
	}
	source, err := OpenReadOnly(ctx, path)
	if err != nil {
		return NativeClusterContext{}, err
	}
	defer source.Close()
	tx, err := source.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return NativeClusterContext{}, err
	}
	defer tx.Rollback()
	bound := &Store{db: source.db, queries: tx, path: path}
	out, err := captureNativeClusterContext(ctx, bound, request)
	if err != nil {
		return NativeClusterContext{}, err
	}
	after, err := NativeReportFileIdentity(path)
	if err != nil || after != file {
		return NativeClusterContext{}, ErrNativeClusterContext
	}
	if err := ctx.Err(); err != nil {
		return NativeClusterContext{}, err
	}
	return out, nil
}

type clusterContextBudget struct {
	limits      NativeReportLimits
	rows, bytes int64
}

func (b *clusterContextBudget) charge(rows, bytes int64) error {
	if rows < 0 || bytes < 0 || rows > b.limits.MaxRows-b.rows || bytes > b.limits.MaxBytes-b.bytes {
		return ErrNativeReportLimit
	}
	b.rows += rows
	b.bytes += bytes
	return nil
}
func (b *clusterContextBudget) preflight(ctx context.Context, q dbQueries, from, size string, args ...any) error {
	var rows, bytes int64
	if err := q.QueryRowContext(ctx, "select count(*),coalesce(sum("+size+"),0) from "+from, args...).Scan(&rows, &bytes); err != nil {
		return err
	}
	return b.charge(rows, bytes)
}

const clusterContextGroups = `cluster_groups g where g.repo_id=? and g.status='active' and g.closed_at is null`
const clusterContextNumberExcluded = `exists(select 1 from thread_exclusions e where e.repository=? and e.number=t.number)`
const clusterContextNode = `cast(json_extract(case when json_valid(t.raw_json) then t.raw_json else '{}' end,'$.node_id') as text)`
const clusterContextNodeExcluded = `exists(select 1 from thread_excluded_nodes e where e.node_id=json_extract(case when json_valid(t.raw_json) then t.raw_json else '{}' end,'$.node_id'))`

func captureNativeClusterContext(ctx context.Context, s *Store, request NativeClusterContextRequest) (NativeClusterContext, error) {
	out := NativeClusterContext{Profile: NativeClusterContextProfile, View: NativeClusterContextView, Repository: request.Repository, File: request.File,
		Groups: []NativeClusterContextGroup{}, Members: []NativeClusterContextMember{}, Threads: []NativeClusterContextThread{}}
	q := s.q()
	var version int
	if err := q.QueryRowContext(ctx, "pragma user_version").Scan(&version); err != nil {
		return out, err
	}
	if version != schemaVersion || s.hasTable(ctx, "portable_metadata") {
		return out, ErrNativeClusterContext
	}
	b := clusterContextBudget{limits: request.Limits}
	// Inspect private as a JSON boolean, never a truthy/coerced value. Unknown
	// privacy and conflicting retained metadata cannot become public evidence.
	if err := b.preflight(ctx, q, "repositories where lower(full_name)=?", "32+length(cast(full_name as blob))+coalesce(length(cast(github_repo_id as blob)),0)", request.Repository); err != nil {
		return out, err
	}
	var count int
	var public bool
	if err := q.QueryRowContext(ctx, `select count(*),coalesce(min(id),0),coalesce(min(github_repo_id),''),coalesce(min(case when json_valid(raw_json) then json_type(raw_json,'$.private')='false' and (json_type(raw_json,'$.visibility') is null or json_extract(raw_json,'$.visibility')='public') else 0 end),0) from repositories where lower(full_name)=?`, request.Repository).Scan(&count, &out.RepositoryID, &out.GitHubRepositoryID, &public); err != nil {
		return out, err
	}
	if count != 1 || out.RepositoryID <= 0 || !public || out.GitHubRepositoryID == "" || !utf8.ValidString(out.GitHubRepositoryID) {
		return out, ErrNativeClusterContext
	}
	// ClusterReadSelection owns validation and may inspect historical anchors.
	// Bound that input before it allocates/decodes stats; do not replace its rules.
	runs := `cluster_runs where repo_id=? and scope='durable' and status in ('success','completed','blocked_identity')`
	if err := b.preflight(ctx, q, runs, `64+coalesce(length(cast(stats_json as blob)),0)+length(cast(started_at as blob))+coalesce(length(cast(finished_at as blob)),0)`, out.RepositoryID); err != nil {
		return out, err
	}
	read, err := s.ClusterReadSelection(ctx, out.RepositoryID, ClusterSourceDurable)
	if err != nil {
		return out, err
	}
	out.Selection = compactClusterSelection(read)
	if err := b.preflight(ctx, q, clusterContextGroups, "64+length(cast(g.status as blob))", out.RepositoryID); err != nil {
		return out, err
	}
	rows, err := q.QueryContext(ctx, "select g.id,g.status,g.closed_at,g.representative_thread_id from "+clusterContextGroups+" order by g.id", out.RepositoryID)
	if err != nil {
		return out, err
	}
	ids := map[int64]bool{}
	groupIndex := map[int64]int{}
	for rows.Next() {
		var g NativeClusterContextGroup
		if err = rows.Scan(&g.ID, &g.Status, &g.ClosedAt, &g.RepresentativeThreadID); err != nil {
			break
		}
		if g.ID <= 0 {
			err = ErrNativeClusterContext
			break
		}
		g.Quarantined = clusterIdentityContext(g.ID, read).IdentityQuarantineStatus == "quarantined"
		groupIndex[g.ID] = len(out.Groups)
		out.Groups = append(out.Groups, g)
		if g.RepresentativeThreadID != nil {
			ids[*g.RepresentativeThreadID] = true
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return out, err
	}
	// A representative is an identity pointer, not a candidate to silently drop
	// or replace. Check it even when closed or not in the visible member set.
	var invalid, nodeLedgerNonempty bool
	if err = q.QueryRowContext(ctx, "select exists(select 1 from thread_excluded_nodes)").Scan(&nodeLedgerNonempty); err != nil {
		return out, err
	}
	// Unknown node identity is advisory only while the authoritative global
	// node ledger is empty; it cannot prove non-exclusion once that changes.
	unprovableNode := "(? and (" + clusterContextNode + " is null or length(cast(" + clusterContextNode + " as blob))=0))"
	if err = q.QueryRowContext(ctx, `select exists(select 1 from cluster_groups g left join threads t on t.id=g.representative_thread_id where g.repo_id=? and g.status='active' and g.closed_at is null and g.representative_thread_id is not null and (t.id is null or t.repo_id<>g.repo_id or `+clusterContextNumberExcluded+` or `+clusterContextNodeExcluded+` or `+unprovableNode+`))`, out.RepositoryID, request.Repository, nodeLedgerNonempty).Scan(&invalid); err != nil {
		return out, err
	}
	if invalid {
		return out, ErrNativeClusterContext
	}
	members := `cluster_memberships m join cluster_groups g on g.id=m.cluster_id join threads t on t.id=m.thread_id where g.repo_id=? and g.status='active' and g.closed_at is null and m.state='active' and ` + durableVisibleMemberPredicate("g", "m", "t") + ` and not ` + clusterContextNumberExcluded
	if err = q.QueryRowContext(ctx, "select exists(select 1 from "+members+" and (t.repo_id<>g.repo_id or "+clusterContextNodeExcluded+" or "+unprovableNode+"))", out.RepositoryID, request.Repository, nodeLedgerNonempty).Scan(&invalid); err != nil {
		return out, err
	}
	if invalid {
		return out, ErrNativeClusterContext
	}
	if err = b.preflight(ctx, q, members, "64+length(cast(m.state as blob))+length(cast(m.role as blob))", out.RepositoryID, request.Repository); err != nil {
		return out, err
	}
	rows, err = q.QueryContext(ctx, "select m.cluster_id,m.thread_id,m.state,m.role,m.last_seen_run_id from "+members+" order by m.cluster_id,m.thread_id", out.RepositoryID, request.Repository)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var m NativeClusterContextMember
		if err = rows.Scan(&m.GroupID, &m.ThreadID, &m.State, &m.Role, &m.LastSeenRunID); err != nil {
			break
		}
		var marker *string
		marker, m.Generation, err = clusterMemberGeneration(m.LastSeenRunID, read)
		if err != nil {
			break
		}
		_, _, err = clusterMemberRetention(marker, clusterIdentityContext(m.GroupID, read), m.Generation)
		if err != nil {
			break
		}
		if m.ThreadID <= 0 || !utf8.ValidString(m.Role) {
			err = ErrNativeClusterContext
			break
		}
		out.Groups[groupIndex[m.GroupID]].MemberCount++
		out.Members = append(out.Members, m)
		ids[m.ThreadID] = true
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return out, err
	}
	ordered := make([]int64, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	slices.Sort(ordered)
	for start := 0; start < len(ordered); start += 500 {
		end := min(start+500, len(ordered))
		args := make([]any, 0, end-start)
		for _, id := range ordered[start:end] {
			args = append(args, id)
		}
		from := "threads t where t.id in (" + strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + ")"
		size := `64+length(cast(kind as blob))+length(cast(state as blob))+length(cast(title as blob))+length(cast(labels_json as blob))+coalesce(length(cast(updated_at_gh as blob)),0)+min(coalesce(length(cast(body as blob)),0),3200)+coalesce(length(cast(` + clusterContextNode + ` as blob)),0)`
		if err = b.preflight(ctx, q, from, size, args...); err != nil {
			return out, err
		}
		rows, err = q.QueryContext(ctx, `select id,number,kind,state,title,labels_json,updated_at_gh,substr(cast(body as blob),1,3200),body is null,`+clusterContextNode+` from `+from+" order by id", args...)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var t NativeClusterContextThread
			var prefix []byte
			var bodyNull bool
			if err = rows.Scan(&t.ID, &t.Number, &t.Kind, &t.State, &t.Title, &t.LabelsJSON, &t.UpdatedAt, &prefix, &bodyNull, &t.NodeID); err != nil {
				break
			}
			if t.Number <= 0 || !utf8.ValidString(t.Kind) || !utf8.ValidString(t.State) || !utf8.ValidString(t.Title) || !utf8.ValidString(t.LabelsJSON) || (t.UpdatedAt != nil && !utf8.ValidString(*t.UpdatedAt)) || (t.NodeID != nil && !utf8.ValidString(*t.NodeID)) {
				err = ErrNativeClusterContext
				break
			}
			if !bodyNull {
				var excerpt string
				excerpt, err = clusterContextExcerpt(prefix)
				if err != nil {
					break
				}
				t.Body = &excerpt
			}
			out.Threads = append(out.Threads, t)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return out, err
		}
	}
	if len(out.Threads) != len(ids) {
		return out, ErrNativeClusterContext
	}
	out.Rows = 1 + int64(len(out.Groups)+len(out.Members)+len(out.Threads))
	out.SHA256, out.Bytes = clusterContextDigest(out)
	if out.Rows > request.Limits.MaxRows || out.Bytes > request.Limits.MaxBytes {
		return out, ErrNativeReportLimit
	}
	return out, nil
}

func clusterContextExcerpt(prefix []byte) (string, error) {
	i := 0
	for count := 0; i < len(prefix) && count < 800; count++ {
		r, size := utf8.DecodeRune(prefix[i:])
		if r == utf8.RuneError && size == 1 {
			return "", ErrNativeClusterContext
		}
		i += size
	}
	return string(prefix[:i]), nil
}

func compactClusterSelection(read ClusterReadContext) NativeClusterContextSelection {
	out := NativeClusterContextSelection{Source: read.Selection.Source, ScopeStatus: read.ScopeStatus, Scope: compactClusterScope(read.ClusterScope)}
	if read.Selection.RunID != nil {
		n, _ := positiveClusterKey(*read.Selection.RunID)
		out.RunID = &n
	}
	if read.Analysis != nil {
		n, _ := positiveClusterKey(read.Analysis.RunID)
		out.AnalysisRunID = &n
		out.AnalysisStatus = read.Analysis.Status
		out.AnalysisScope = compactClusterScope(read.Analysis.ClusterScope)
	}
	return out
}
func compactClusterScope(s *ClusterScope) *NativeClusterContextScope {
	if s == nil {
		return nil
	}
	return &NativeClusterContextScope{Mode: s.Mode, PolicyDigest: s.PolicyDigest, SourceThrough: s.SourceAsOf.Through, SourceObservedAt: s.SourceAsOf.ObservedAt,
		ProviderComplete: s.ProviderComplete, FullNativeScopeComplete: s.FullNativeScopeComplete, SelectedInputComplete: s.SelectedInputComplete,
		IdentityReconciliationComplete: s.IdentityReconciliationComplete, PublicationStatus: s.PublicationStatus, RetainedPriorMemberships: int64(s.RetainedPriorMemberships)}
}

// This local digest deliberately binds only compact content and its allowlisted
// interpretation. It is separate from the wire profile's inventory digest.
func clusterContextDigest(c NativeClusterContext) (string, int64) {
	h := sha256.New()
	var size int64
	add := func(values ...any) {
		for _, v := range values {
			size += clusterContextHashValue(h, v)
		}
	}
	add(c.Profile, c.View, c.Repository, c.RepositoryID, c.GitHubRepositoryID)
	add("selection", c.Selection.Source, c.Selection.ScopeStatus, c.Selection.RunID, c.Selection.AnalysisRunID, c.Selection.AnalysisStatus)
	for _, scope := range []*NativeClusterContextScope{c.Selection.Scope, c.Selection.AnalysisScope} {
		if scope == nil {
			add(nil)
			continue
		}
		add(true, scope.Mode, scope.PolicyDigest, scope.SourceThrough, scope.SourceObservedAt, scope.ProviderComplete, scope.FullNativeScopeComplete, scope.SelectedInputComplete, scope.IdentityReconciliationComplete, scope.PublicationStatus, scope.RetainedPriorMemberships)
	}
	add("groups", int64(len(c.Groups)))
	for _, g := range c.Groups {
		add(g.ID, g.Status, g.ClosedAt, g.RepresentativeThreadID, g.MemberCount, g.Quarantined)
	}
	add("members", int64(len(c.Members)))
	for _, m := range c.Members {
		add(m.GroupID, m.ThreadID, m.State, m.Role, m.LastSeenRunID, m.Generation)
	}
	add("threads", int64(len(c.Threads)))
	for _, t := range c.Threads {
		add(t.ID, t.Number, t.Kind, t.State, t.Title, t.LabelsJSON, t.UpdatedAt, t.Body, t.NodeID)
	}
	return hex.EncodeToString(h.Sum(nil)), size
}
func clusterContextHashValue(h hash.Hash, v any) int64 {
	switch x := v.(type) {
	case *string:
		if x == nil {
			return clusterContextHashValue(h, nil)
		}
		return clusterContextHashValue(h, *x)
	case *int64:
		if x == nil {
			return clusterContextHashValue(h, nil)
		}
		return clusterContextHashValue(h, *x)
	case *bool:
		if x == nil {
			return clusterContextHashValue(h, nil)
		}
		return clusterContextHashValue(h, *x)
	case nil:
		h.Write([]byte{0})
		return 1
	case string:
		h.Write([]byte{1})
		nativeReportHashText(h, x)
		return 9 + int64(len(x))
	case int64:
		h.Write([]byte{2})
		nativeReportHashInt(h, uint64(x))
		return 9
	case bool:
		h.Write([]byte{3})
		nativeReportHashText(h, strconv.FormatBool(x))
		return 9 + int64(len(strconv.FormatBool(x)))
	default:
		panic("unsupported cluster context scalar")
	}
}
