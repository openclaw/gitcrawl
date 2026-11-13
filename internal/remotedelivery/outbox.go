package remotedelivery

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store"
)

var ErrSeedOutbox = errors.New("seed outbox identity, structure or receipt differs; retain the file for reconciliation")

const BaselineUnqualified = "unqualified"
const BaselineQualified = "qualified"

// SeedRegistration is explicit destination/custody metadata, not permission to
// send. Physical identity stays local; no source path or credential is stored.
type SeedRegistration struct {
	Origin, Archive, BindingID string
	File                       store.NativeReportFile
	Repository                 string
}

type StageLimits struct{ MaxEncodedBytes int64 }
type PartRef struct {
	SeedID string
	Index  int
}
type PendingPart struct {
	Ref       PartRef
	Body      []byte
	SHA256    string
	PartCount int
}
type SeedState struct {
	Registration                                                SeedRegistration
	SeedID                                                      string
	Anchor                                                      store.NativeReportAnchor
	NativeThrough, NativeSHA256                                 string
	NativeRows, NativeBytes                                     int64
	PartCount, Receipts                                         int
	BaselineState, BaselineNative, BaselineSHA256, ServerCursor string
}

// Outbox owns a separate private SQLite file. Its owner must keep its pathname
// bound to that physical file while open. No native source is opened, and no
// method grants outbound authority or claims provider acquisition coverage.
type Outbox struct{ base *crawlstore.Store }

// CreateSeedOutbox prepares the complete seed before filesystem effects, then
// exclusively claims and initializes a new file in one FULL SQLite transaction.
// The caller must not mutate capture during this call. A failure after the
// exclusive claim retains the task-created partial file, never unlinks/retries.
func CreateSeedOutbox(ctx context.Context, path string, r SeedRegistration, capture store.NativeReportCapture, limits StageLimits) (*Outbox, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := seedOutboxPath(path); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = os.ErrExist
		}
		return nil, err
	}
	if err := validateSeedRegistration(r); err != nil {
		return nil, err
	}
	if r.File != capture.Anchor.File || r.Repository != capture.Anchor.Repository {
		return nil, ErrSeedOutbox
	}
	report, err := ProjectPrivateReport(capture)
	if err != nil {
		return nil, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(random[:])
	seed, err := PreparePrivateSeed(ctx, report, PrivateSeedRequest{Archive: r.Archive, BindingID: r.BindingID, SeedID: id, MaxBytes: limits.MaxEncodedBytes})
	if err != nil {
		return nil, err
	}
	parts := append([][]byte{seed.Manifest}, seed.Chunks...)
	parts = append(parts, seed.Complete)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	o := &Outbox{base: base}
	registration, _ := json.Marshal(r)
	anchor, _ := json.Marshal(capture.Anchor)
	err = o.write(ctx, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, seedOutboxSchema); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `insert into binding(singleton,origin,archive,binding_id,registration_json,baseline_state) values(1,?,?,?,?, 'unqualified')`, r.Origin, r.Archive, r.BindingID, registration); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `insert into captures(seed_id,anchor_json,native_through,native_sha256,native_rows,native_bytes,part_count) values(?,?,?,?,?,?,?)`, id, anchor, seed.Plan.Manifest().NativeThrough, capture.SHA256, capture.Rows, capture.Bytes, len(parts)); err != nil {
			return err
		}
		statement, err := conn.PrepareContext(ctx, `insert into parts(seed_id,part_index,body,payload_sha256) values(?,?,?,?)`)
		if err != nil {
			return err
		}
		defer statement.Close()
		for i, body := range parts {
			if _, err := statement.ExecContext(ctx, id, i, body, privateSeedHash(body)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		base.Close()
		return nil, err
	}
	return o, nil
}

// ReadSeedOutboxState validates and inspects current seed custody without
// opening a writer, migrating schema, changing modes or acquiring credentials.
// Normal SQLite WAL reader sidecars may still be created by the storage engine.
func ReadSeedOutboxState(ctx context.Context, path string) (SeedState, error) {
	if err := ctx.Err(); err != nil {
		return SeedState{}, err
	}
	if err := seedOutboxPath(path); err != nil {
		return SeedState{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return SeedState{}, err
	}
	if !info.Mode().IsRegular() {
		return SeedState{}, ErrSeedOutbox
	}
	ro, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return SeedState{}, err
	}
	var state SeedState
	err = ro.WithTx(ctx, func(tx *sql.Tx) error {
		var tables int
		if err := tx.QueryRowContext(ctx, `select count(*) from sqlite_schema where type='table' and name not like 'sqlite_%'`).Scan(&tables); err != nil {
			return err
		}
		if tables != 3 {
			return ErrSeedOutbox
		}
		state, err = readSeedState(ctx, tx)
		if err != nil {
			return err
		}
		if err := validateSeedRegistration(state.Registration); err != nil {
			return err
		}
		var body []byte
		var hash string
		if err := tx.QueryRowContext(ctx, `select body,payload_sha256 from parts where seed_id=? and part_index=0`, state.SeedID).Scan(&body, &hash); err != nil {
			return err
		}
		if privateSeedHash(body) != hash {
			return ErrSeedOutbox
		}
		plan, err := remote.DecodeGitHubSeedManifest(body)
		if err != nil {
			return ErrSeedOutbox
		}
		m := plan.Manifest()
		a := state.Anchor
		c := a.Cursor
		through := strings.Join([]string{"gitcrawl-native-v1", c.Epoch, strconv.FormatInt(c.Generation, 10), strconv.FormatInt(c.SchemaVersion, 10), strconv.FormatInt(c.Sequence, 10), strconv.FormatInt(c.PrunedThrough, 10), a.ScopeSHA256, state.NativeSHA256}, ":")
		count := 2
		for _, table := range m.Tables {
			count += len(table.Chunks)
		}
		if m.Archive != state.Registration.Archive || m.BindingID != state.Registration.BindingID || m.SeedID != state.SeedID || m.Repository != a.Repository || a.Repository != state.Registration.Repository || a.File != state.Registration.File || m.RepositoryID != strconv.FormatInt(a.RepositoryID, 10) || m.ScopeSHA256 != a.ScopeSHA256 || m.NativeSHA256 != state.NativeSHA256 || m.NativeThrough != state.NativeThrough || through != m.NativeThrough || count != state.PartCount {
			return ErrSeedOutbox
		}
		return nil
	})
	closeErr := ro.Close()
	if err != nil {
		return SeedState{}, err
	}
	if closeErr != nil {
		return SeedState{}, closeErr
	}
	return state, nil
}

// OpenSeedOutbox recognizes the current outbox read-only before a writable
// helper may chmod or change journal mode. There are no implicit migrations.
func OpenSeedOutbox(ctx context.Context, path string) (*Outbox, error) {
	if _, err := ReadSeedOutboxState(ctx, path); err != nil {
		return nil, err
	}
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	return &Outbox{base: base}, nil
}

func seedOutboxPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return ErrSeedOutbox
	}
	return nil
}

func validateSeedRegistration(r SeedRegistration) error {
	u, err := url.Parse(r.Origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(r.Origin, "#") || r.File.Platform == "" || r.File.Device == "" || r.File.Inode == "" {
		return ErrSeedOutbox
	}
	if u.Scheme == "http" && !net.ParseIP(u.Hostname()).IsLoopback() {
		return ErrSeedOutbox
	}
	for _, value := range []string{r.Origin, r.File.Platform, r.File.Device, r.File.Inode} {
		if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return ErrSeedOutbox
		}
	}
	return nil // shared seed codec validates archive/binding/repository metadata
}

func (o *Outbox) Close() error { return o.base.Close() }

type seedQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func checkSeedOutboxVersion(ctx context.Context, q seedQuery) error {
	var app, version int
	if err := q.QueryRowContext(ctx, `pragma application_id`).Scan(&app); err != nil {
		return err
	}
	if err := q.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return err
	}
	if app != seedOutboxApplicationID || version != seedOutboxVersion {
		return ErrSeedOutbox
	}
	return nil
}

func readSeedState(ctx context.Context, q seedQuery) (SeedState, error) {
	// Recheck in the caller's transaction: an open handle may outlive a later
	// explicit migration. Seed-only operations must not interpret that schema.
	if err := checkSeedOutboxVersion(ctx, q); err != nil {
		return SeedState{}, err
	}
	var s SeedState
	var registration, anchor []byte
	var origin, archive, binding string
	var baseline, nativeHash sql.NullString
	var completed bool
	err := q.QueryRowContext(ctx, `select b.origin,b.archive,b.binding_id,b.registration_json,b.baseline_state,b.baseline_native,b.baseline_sha256,b.server_cursor,c.seed_id,c.anchor_json,c.native_through,c.native_sha256,c.native_rows,c.native_bytes,c.part_count,c.completed from binding b cross join captures c where b.singleton=1`).Scan(&origin, &archive, &binding, &registration, &s.BaselineState, &baseline, &nativeHash, &s.ServerCursor, &s.SeedID, &anchor, &s.NativeThrough, &s.NativeSHA256, &s.NativeRows, &s.NativeBytes, &s.PartCount, &completed)
	if err != nil {
		return SeedState{}, err
	}
	if json.Unmarshal(registration, &s.Registration) != nil || json.Unmarshal(anchor, &s.Anchor) != nil || origin != s.Registration.Origin || archive != s.Registration.Archive || binding != s.Registration.BindingID {
		return SeedState{}, ErrSeedOutbox
	}
	var count, first, last int
	if err := q.QueryRowContext(ctx, `select count(*),min(part_index),max(part_index),count(receipt_json) from parts where seed_id=?`, s.SeedID).Scan(&count, &first, &last, &s.Receipts); err != nil {
		return SeedState{}, err
	}
	if count != s.PartCount || first != 0 || last != count-1 {
		return SeedState{}, ErrSeedOutbox
	}
	switch s.BaselineState {
	case BaselineUnqualified:
		if completed || baseline.Valid || nativeHash.Valid || s.ServerCursor != "" || s.Receipts == s.PartCount {
			return SeedState{}, ErrSeedOutbox
		}
	case BaselineQualified:
		if !completed || !baseline.Valid || !nativeHash.Valid || baseline.String != s.NativeThrough || nativeHash.String != s.NativeSHA256 || s.Receipts != s.PartCount || s.ServerCursor != seedPartBatch(s.SeedID, s.PartCount-1, s.PartCount) {
			return SeedState{}, ErrSeedOutbox
		}
		s.BaselineNative, s.BaselineSHA256 = baseline.String, nativeHash.String
	default:
		return SeedState{}, ErrSeedOutbox
	}
	return s, nil
}

func (o *Outbox) State(ctx context.Context) (SeedState, error) {
	var state SeedState
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error { var err error; state, err = readSeedState(ctx, tx); return err })
	return state, err
}

// NextPart releases its transaction before returning exact saved bytes. More
// than one worker may receive the same part; this does not authorize a send.
func (o *Outbox) NextPart(ctx context.Context) (*PendingPart, error) {
	var part *PendingPart
	err := o.base.WithTx(ctx, func(tx *sql.Tx) error {
		state, err := readSeedState(ctx, tx)
		if err != nil {
			return err
		}
		if state.BaselineState == BaselineQualified {
			return nil
		}
		part = &PendingPart{Ref: PartRef{SeedID: state.SeedID}, PartCount: state.PartCount}
		if err := tx.QueryRowContext(ctx, `select part_index,body,payload_sha256 from parts where seed_id=? and receipt_json is null order by part_index limit 1`, state.SeedID).Scan(&part.Ref.Index, &part.Body, &part.SHA256); err != nil {
			return err
		}
		if privateSeedHash(part.Body) != part.SHA256 {
			return ErrSeedOutbox
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return part, nil
}

// RecordReceipt settles a trusted caller's receiver receipt. It does not prove
// transport authentication. Only exact ordered completion qualifies a delivery
// baseline, never current source/exclusion authority or provider coverage.
func (o *Outbox) RecordReceipt(ctx context.Context, ref PartRef, receipt remote.SeedReceipt) error {
	return o.write(ctx, func(conn *sql.Conn) error {
		state, err := readSeedState(ctx, conn)
		if err != nil {
			return err
		}
		if ref.SeedID != state.SeedID || ref.Index < 0 || ref.Index >= state.PartCount {
			return ErrSeedOutbox
		}
		var hash string
		var recorded []byte
		if err := conn.QueryRowContext(ctx, `select payload_sha256,receipt_json from parts where seed_id=? and part_index=?`, ref.SeedID, ref.Index).Scan(&hash, &recorded); err != nil {
			return err
		}
		batch := seedPartBatch(ref.SeedID, ref.Index, state.PartCount)
		// Native bootstrap is pristine at the receiver, even beside historical
		// artifacts. The begin receipt is 1; each following part advances once.
		if receipt.App != "gitcrawl" || receipt.Archive != state.Registration.Archive || receipt.BindingID != state.Registration.BindingID || receipt.BatchID != batch || receipt.Cursor != batch || receipt.PayloadSHA256 != hash || receipt.Sequence != strconv.Itoa(ref.Index+1) {
			return ErrSeedOutbox
		}
		if recorded != nil {
			var previous remote.SeedReceipt
			if json.Unmarshal(recorded, &previous) != nil {
				return ErrSeedOutbox
			}
			previous.Replayed = receipt.Replayed
			if previous != receipt {
				return ErrSeedOutbox
			}
			return nil
		}
		var next int
		if err := conn.QueryRowContext(ctx, `select min(part_index) from parts where seed_id=? and receipt_json is null`, ref.SeedID).Scan(&next); err != nil {
			return err
		}
		if state.BaselineState != BaselineUnqualified || next != ref.Index {
			return ErrSeedOutbox
		}
		body, _ := json.Marshal(receipt)
		if _, err := conn.ExecContext(ctx, `update parts set receipt_json=? where seed_id=? and part_index=?`, body, ref.SeedID, ref.Index); err != nil {
			return err
		}
		if ref.Index != state.PartCount-1 {
			return nil
		}
		if _, err := conn.ExecContext(ctx, `update captures set completed=1 where seed_id=?`, ref.SeedID); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `update binding set baseline_state='qualified',baseline_native=?,baseline_sha256=?,server_cursor=? where singleton=1`, state.NativeThrough, state.NativeSHA256, receipt.Cursor)
		return err
	})
}

func seedPartBatch(id string, index, count int) string {
	suffix := strconv.Itoa(index - 1)
	if index == 0 {
		suffix = "begin"
	} else if index == count-1 {
		suffix = "complete"
	}
	return "seed1:" + id + ":" + suffix
}

func (o *Outbox) write(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := o.base.DB().Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `pragma synchronous=FULL`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `begin immediate`); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `rollback`) }()
	if err := fn(conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `commit`)
	return err
}
