package remotedelivery

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/crawlkit/store"
)

var ErrReportVerification = errors.New("private report parity differs; retain frozen outbox and receiver for reconciliation")
var ErrReportVerificationLimit = errors.New("private report verification exceeds its row or encoded-byte bound")

type VerificationLimits struct{ MaxRows, MaxBytes int64 }

// ReportVerification covers the seven complete current tables at one published
// receiver snapshot. It does not qualify acquisition freshness or revision history.
type ReportVerification struct {
	SnapshotID       string `json:"snapshot_id"`
	NativeThrough    string `json:"native_through"`
	InventorySHA256  string `json:"inventory_sha256"`
	ReceiverSequence string `json:"receiver_sequence"`
	Rows             int64  `json:"rows"`
	EncodedBytes     int64  `json:"encoded_bytes"`
}

// VerifyReport reads existing custody only. The operator freezes capture and
// delivery for the run; a fixed SQLite connection detects concurrent commits,
// and the pathname and qualified state are checked again before returning proof.
func VerifyReport(ctx context.Context, path string, token remote.TokenProvider, client *http.Client, limits VerificationLimits) (ReportVerification, error) {
	var empty ReportVerification
	if _, ok := ctx.Deadline(); !ok || limits.MaxRows <= 0 || limits.MaxBytes <= 0 {
		return empty, ErrReportVerificationLimit
	}
	if err := seedOutboxPath(path); err != nil {
		return empty, err
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() {
		return empty, ErrSeedOutbox
	}
	ro, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return empty, err
	}
	defer ro.Close()
	conn, err := ro.DB().Conn(ctx)
	if err != nil {
		return empty, err
	}
	defer conn.Close()
	var version int64
	if err = conn.QueryRowContext(ctx, `pragma data_version`).Scan(&version); err != nil {
		return empty, err
	}
	state, err := frozenReportState(ctx, conn)
	if err != nil {
		return empty, err
	}
	var rows, keyBytes int64
	if err = conn.QueryRowContext(ctx, `select count(*),coalesce(sum(length(encoded_key)+length(row_sha256)),0) from inventory_rows`).Scan(&rows, &keyBytes); err != nil {
		return empty, err
	}
	if rows > limits.MaxRows || keyBytes > limits.MaxBytes {
		return empty, ErrReportVerificationLimit
	}
	codec, err := privateInventoryCodec()
	if err != nil {
		return empty, err
	}
	digest, err := digestInventoryRows(ctx, conn, codec, state.BaselineAnchor.ScopeSHA256, limits.MaxRows)
	if err != nil {
		return empty, err
	}
	if digest != state.InventorySHA256 {
		return empty, ErrSeedOutbox
	}
	var seed string
	if err = conn.QueryRowContext(ctx, `select seed_id from captures where completed=1`).Scan(&seed); err != nil {
		return empty, err
	}
	pin := "pg-gitcrawl-current-v1:" + seed + ":" + strconv.FormatInt(state.ReceiverSequence, 10)
	// Crawlkit owns credentials and query encoding. This adapter only bounds
	// actual response bytes, including unknown fields and JSON whitespace.
	httpClient := http.Client{Timeout: 30 * time.Second}
	if client != nil {
		httpClient = *client
	}
	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	budget := &verificationTransport{base: transport, remaining: limits.MaxBytes}
	httpClient.Transport = budget
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	reader, err := remote.NewClient(remote.Options{Endpoint: state.Registration.Origin, TokenProvider: token, HTTPClient: &httpClient})
	if err != nil {
		return empty, err
	}
	binding := func() error {
		result, err := reader.Query(ctx, "gitcrawl", state.Registration.Archive, remote.QueryRequest{Name: "gitcrawl.report-inputs.binding", Limit: 1, SnapshotID: pin})
		if err != nil {
			return err
		}
		return verifyReportBinding(result, state, seed, pin)
	}
	if err = binding(); err != nil {
		return empty, err
	}
	var seenRows int64
	for _, layout := range privateLayouts {
		columns := strings.Fields(layout.columns)
		seen := make(map[string]bool)
		cursors := make(map[string]bool)
		cursor := ""
		var previousSequence, previousOrdinal int64
		for {
			page, err := reader.Query(ctx, "gitcrawl", state.Registration.Archive, remote.QueryRequest{Name: "gitcrawl." + layout.name + ".current", Limit: 500, Cursor: cursor, SnapshotID: pin})
			if err != nil {
				return empty, err
			}
			if !slices.Equal(page.Columns, append(slices.Clone(columns), "sequence", "ordinal")) || !verificationSnapshot(page, pin) || len(page.Rows) > 500 {
				return empty, ErrReportVerification
			}
			for _, row := range page.Rows {
				if err := ctx.Err(); err != nil {
					return empty, err
				}
				if seenRows >= limits.MaxRows {
					return empty, ErrReportVerificationLimit
				}
				if len(row) != len(columns)+2 {
					return empty, ErrReportVerification
				}
				sequence, e1 := verificationInteger(row[len(columns)])
				ordinal, e2 := verificationInteger(row[len(columns)+1])
				if e1 != nil || e2 != nil || sequence < 1 || sequence > state.ReceiverSequence || ordinal < 0 || ordinal > 499 || sequence < previousSequence || sequence == previousSequence && ordinal <= previousOrdinal {
					return empty, ErrReportVerification
				}
				previousSequence, previousOrdinal = sequence, ordinal
				values := make([]remote.ChangeValue, len(columns))
				for i, column := range columns {
					values[i].Kind = "null"
					if row[i] != nil {
						value, ok := row[i].(string)
						if !ok { // The reader encodes all int64 scalars as decimal strings.
							return empty, ErrReportVerification
						}
						kind := "text"
						if strings.Contains(" "+layout.integers+" ", " "+column+" ") {
							kind = "integer"
						}
						values[i] = remote.ChangeValue{Kind: kind, Value: &value}
					}
				}
				var key []remote.ChangeValue
				for _, column := range strings.Fields(layout.keys) {
					key = append(key, values[slices.Index(columns, column)])
				}
				encoded, err := codec.EncodeKey(layout.name, key)
				if err != nil || seen[string(encoded)] {
					return empty, ErrReportVerification
				}
				hash, err := codec.RowDigest(layout.name, key, values)
				if err != nil {
					return empty, ErrReportVerification
				}
				var expected []byte
				if err = conn.QueryRowContext(ctx, `select row_sha256 from inventory_rows where table_name=? and encoded_key=?`, layout.name, encoded).Scan(&expected); err != nil || !bytes.Equal(expected, hash[:]) {
					return empty, ErrReportVerification
				}
				seen[string(encoded)] = true
				seenRows++
			}
			if page.Cursor == "" {
				break
			}
			if len(page.Rows) == 0 || len(page.Cursor) > 4096 || cursors[page.Cursor] {
				return empty, ErrReportVerification
			}
			cursors[page.Cursor] = true
			cursor = page.Cursor
		}
	}
	if seenRows != rows {
		return empty, ErrReportVerification
	}
	if err = binding(); err != nil {
		return empty, err
	}
	afterState, err := frozenReportState(ctx, conn)
	if err != nil {
		return empty, err
	}
	if afterState != state {
		return empty, ErrSeedOutbox
	}
	var afterVersion int64
	if err = conn.QueryRowContext(ctx, `pragma data_version`).Scan(&afterVersion); err != nil {
		return empty, err
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || before.ModTime() != after.ModTime() || version != afterVersion {
		return empty, ErrSeedOutbox
	}
	return ReportVerification{SnapshotID: pin, NativeThrough: state.BaselineNative, InventorySHA256: digest, ReceiverSequence: strconv.FormatInt(state.ReceiverSequence, 10), Rows: rows, EncodedBytes: limits.MaxBytes - budget.remaining}, nil
}

func frozenReportState(ctx context.Context, conn *sql.Conn) (ReportOutboxState, error) {
	state, err := readReportState(ctx, conn)
	if err != nil {
		return ReportOutboxState{}, err
	}
	if state.ActiveChangeID != "" {
		return ReportOutboxState{}, ErrSeedOutbox
	}
	var pending, version int
	if err := conn.QueryRowContext(ctx, `select (select count(*) from captures where completed=0)+(select count(*) from parts where receipt_json is null)`).Scan(&pending); err != nil || pending != 0 {
		return ReportOutboxState{}, ErrSeedOutbox
	}
	if err := conn.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return ReportOutboxState{}, err
	}
	if version == 3 {
		if err := conn.QueryRowContext(ctx, `select (select count(*) from change_captures where completed=0)+(select count(*) from change_parts where receipt_json is null)`).Scan(&pending); err != nil || pending != 0 {
			return ReportOutboxState{}, ErrSeedOutbox
		}
	}
	return state, nil
}

func verificationSnapshot(result remote.QueryResult, pin string) bool {
	return result.Stats.SnapshotID == pin && result.Stats.SchemaVersion == 1 && result.Stats.ObservationOrder == "completed-publication/observation-sequence-ordinal" && result.Stats.RowsWritten == 0 && result.Stats.NextCursor == "" && len(result.Values) == 0 && (result.Snapshot == nil || result.Snapshot.ID == pin)
}

func verifyReportBinding(result remote.QueryResult, state ReportOutboxState, seed, pin string) error {
	if !verificationSnapshot(result, pin) || result.Cursor != "" || len(result.Rows) != 1 || len(result.Rows[0]) != len(result.Columns) {
		return ErrReportVerification
	}
	fields := make(map[string]any, len(result.Columns))
	for i, name := range result.Columns {
		if _, exists := fields[name]; exists {
			return ErrReportVerification
		}
		fields[name] = result.Rows[0][i]
	}
	for name, expected := range map[string]string{
		"profile": "gitcrawl-report-inputs/v1", "archive": state.Registration.Archive, "binding_id": state.Registration.BindingID,
		"seed_id": seed, "native_profile": PrivateReportProfile, "native_schema": "16",
		"repository": state.Registration.Repository, "repository_id": strconv.FormatInt(state.BaselineAnchor.RepositoryID, 10), "scope_sha256": state.BaselineAnchor.ScopeSHA256,
		"published_native": state.BaselineNative, "inventory_sha256": state.InventorySHA256, "published_sequence": strconv.FormatInt(state.ReceiverSequence, 10),
		"transport_sequence": strconv.FormatInt(state.ReceiverSequence, 10), "transport_cursor": state.ServerCursor, "publication_batch_id": state.ServerCursor,
	} {
		if value, ok := fields[name].(string); !ok || value != expected {
			return ErrReportVerification
		}
	}
	if partial, ok := fields["partial_capture"].(bool); !ok || partial {
		return ErrReportVerification
	}
	for _, name := range []string{"pending_capture", "pending_native_from", "pending_native_through", "pending_part_count", "pending_next_part"} {
		if value, exists := fields[name]; !exists || value != nil {
			return ErrReportVerification
		}
	}
	return nil
}

func verificationInteger(value any) (int64, error) {
	text, ok := value.(string)
	if !ok {
		return 0, ErrReportVerification
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != text {
		return 0, ErrReportVerification
	}
	return n, nil
}

type verificationTransport struct {
	base      http.RoundTripper
	remaining int64
}

func (t *verificationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.remaining <= 0 {
		return nil, ErrReportVerificationLimit
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	limit := min(t.remaining, int64(8<<20))
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, ErrReportVerificationLimit
	}
	t.remaining -= int64(len(body))
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}
