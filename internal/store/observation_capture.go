package store

// Capture is an opt-in, source-owned publication protocol. Native row/history
// writers are unchanged; their final committed rows are projected by WithTx.
import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const observationSchemaVersion = 17

//go:embed observation_capture_registry.json
var observationRegistry string

type captureColumn struct {
	Name, Type, Storage string
	Nullable            bool
}
type captureTable struct {
	Name    string
	Key     []string
	Columns []captureColumn
}
type captureRegistrySpec struct{ Tables []captureTable }

var registeredCaptureTables = func() []captureTable {
	var r captureRegistrySpec
	if err := json.Unmarshal([]byte(observationRegistry), &r); err != nil {
		panic(err)
	}
	return r.Tables
}()

func captureTables() []captureTable { return registeredCaptureTables }
func captureTableNamed(name string) (captureTable, error) {
	for _, t := range captureTables() {
		if t.Name == name {
			return t, nil
		}
	}
	return captureTable{}, fmt.Errorf("unknown capture table %q", name)
}
func captureID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func captureDigest() string {
	sum := sha256.Sum256([]byte(observationRegistry))
	return hex.EncodeToString(sum[:])
}
func captureQuote(s string) string   { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func captureLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func captureCell(expr string) string {
	return `CASE typeof(` + expr + `) WHEN 'null' THEN json_array('null',NULL) WHEN 'integer' THEN json_array('integer',CAST(` + expr + ` AS TEXT)) WHEN 'text' THEN json_array('text',` + expr + `) WHEN 'blob' THEN json_array('blob',hex(` + expr + `)) WHEN 'real' THEN json_array('real',printf('%!.26g',` + expr + `)) END`
}
func (t captureTable) keyExpr(prefix string) string {
	var cells []string
	for _, k := range t.Key {
		cells = append(cells, `json(`+captureCell(prefix+captureQuote(k))+`)`)
	}
	return `json_array(` + strings.Join(cells, ",") + `)`
}
func (t captureTable) rowExpr(overlay bool) string {
	var cells []string
	for _, c := range t.Columns {
		if (c.Storage == "observation_overlay") == overlay {
			cells = append(cells, captureLiteral(c.Name), `json(`+captureCell(captureQuote(c.Name))+`)`)
		}
	}
	return `json_object(` + strings.Join(cells, ",") + `)`
}
func (t captureTable) keyWhere(key string) (string, []any, error) {
	var cells [][]json.RawMessage
	if err := json.Unmarshal([]byte(key), &cells); err != nil || len(cells) != len(t.Key) {
		return "", nil, errors.New("invalid capture key")
	}
	var where []string
	var args []any
	for i, k := range t.Key {
		if len(cells[i]) != 2 {
			return "", nil, errors.New("invalid capture key cell")
		}
		var typ, value string
		if json.Unmarshal(cells[i][0], &typ) != nil || json.Unmarshal(cells[i][1], &value) != nil {
			return "", nil, errors.New("invalid capture key type")
		}
		var expected string
		for _, c := range t.Columns {
			if c.Name == k {
				expected = c.Type
			}
		}
		if typ != expected {
			return "", nil, errors.New("capture key type mismatch")
		}
		var arg any = value
		if typ == "integer" {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || strconv.FormatInt(n, 10) != value {
				return "", nil, errors.New("invalid capture integer")
			}
			arg = n
		}
		where = append(where, captureQuote(k)+" IS ?")
		args = append(args, arg)
	}
	return strings.Join(where, " AND "), args, nil
}

type captureRow struct {
	Key    string `json:"key_json"`
	Value  string `json:"value_json"`
	Clocks string `json:"clocks_json"`
}

func validateCaptureRow(t captureTable, r captureRow) error {
	if !utf8.ValidString(r.Value) || !utf8.ValidString(r.Clocks) {
		return errors.New("capture text is not UTF-8")
	}
	var value, clocks map[string][]json.RawMessage
	if json.Unmarshal([]byte(r.Value), &value) != nil || json.Unmarshal([]byte(r.Clocks), &clocks) != nil {
		return errors.New("invalid capture row encoding")
	}
	if len(value)+len(clocks) != len(t.Columns) {
		return errors.New("capture column mismatch")
	}
	for _, c := range t.Columns {
		m := value
		if c.Storage == "observation_overlay" {
			m = clocks
		}
		cell := m[c.Name]
		if len(cell) != 2 {
			return fmt.Errorf("invalid capture column %s", c.Name)
		}
		var typ string
		if json.Unmarshal(cell[0], &typ) != nil {
			return errors.New("invalid capture cell type")
		}
		if typ == "null" && c.Nullable {
			if string(cell[1]) != "null" {
				return errors.New("capture NULL cell has a non-NULL value")
			}
			continue
		}
		if typ != c.Type {
			return fmt.Errorf("capture type mismatch: %s.%s", t.Name, c.Name)
		}
		var encoded string
		if json.Unmarshal(cell[1], &encoded) != nil || !utf8.ValidString(encoded) {
			return errors.New("capture cell requires exact encoded text")
		}
		if typ == "integer" {
			n, err := strconv.ParseInt(encoded, 10, 64)
			if err != nil || strconv.FormatInt(n, 10) != encoded {
				return errors.New("invalid capture INTEGER cell")
			}
		}
	}
	if _, _, err := t.keyWhere(r.Key); err != nil {
		return err
	}
	var keys [][]json.RawMessage
	if err := json.Unmarshal([]byte(r.Key), &keys); err != nil {
		return err
	}
	for i, k := range t.Key {
		var keyValue, rowValue string
		if json.Unmarshal(keys[i][1], &keyValue) != nil || json.Unmarshal(value[k][1], &rowValue) != nil || keyValue != rowValue {
			return errors.New("capture key and image binding mismatch")
		}
	}
	return nil
}
func readCaptureRow(ctx context.Context, q dbQueries, t captureTable, key string) (captureRow, error) {
	where, args, err := t.keyWhere(key)
	if err != nil {
		return captureRow{}, err
	}
	r := captureRow{}
	err = q.QueryRowContext(ctx, "SELECT "+t.keyExpr("")+","+t.rowExpr(false)+","+t.rowExpr(true)+" FROM "+captureQuote(t.Name)+" WHERE "+where, args...).Scan(&r.Key, &r.Value, &r.Clocks)
	if err != nil {
		return r, err
	}
	return r, validateCaptureRow(t, r)
}

// ObservationCapability is valid only in the snapshot that read it. Coordinates
// are decimal strings on the wire; Epoch is the active capability, not an image's origin.
type ObservationCapability struct {
	ArchiveID      string `json:"archive_id"`
	Epoch          string `json:"epoch"`
	RegistryDigest string `json:"registry_digest"`
	EpochStartTx   string `json:"epoch_start_tx_id"`
	EpochStartSeq  string `json:"epoch_start_seq"`
	TxID           string `json:"h_tx_id"`
	Seq            string `json:"h_seq"`
}

func captureCapability(ctx context.Context, q dbQueries) (ObservationCapability, error) {
	var c ObservationCapability
	var state, registry string
	var version int
	err := q.QueryRowContext(ctx, `SELECT format_version,archive_id,epoch,state,registry_json,registry_digest,CAST(epoch_start_tx_id AS TEXT),CAST(epoch_start_seq AS TEXT) FROM capture_meta WHERE singleton=1`).Scan(&version, &c.ArchiveID, &c.Epoch, &state, &registry, &c.RegistryDigest, &c.EpochStartTx, &c.EpochStartSeq)
	if err != nil {
		return c, err
	}
	if version != 1 || state != "ready" || c.ArchiveID == "" || c.Epoch == "" || registry != observationRegistry || c.RegistryDigest != captureDigest() {
		return c, errors.New("capture capability mismatch or invalidated")
	}
	var open, dirty int
	if err = q.QueryRowContext(ctx, `SELECT open FROM _capture_context WHERE singleton=1`).Scan(&open); err != nil {
		return c, err
	}
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM _capture_dirty`).Scan(&dirty); err != nil {
		return c, err
	}
	if open != 0 || dirty != 0 {
		return c, errors.New("unsealed capture context")
	}
	err = q.QueryRowContext(ctx, `SELECT CAST(coalesce(max(tx_id),0) AS TEXT),(SELECT CAST(coalesce(max(seq),0) AS TEXT) FROM capture_observations) FROM capture_commits`).Scan(&c.TxID, &c.Seq)
	return c, err
}
func capturePresent(ctx context.Context, q dbQueries) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name='capture_meta' AND type='table'`).Scan(&n)
	return n == 1, err
}

// InstallObservationCapture is intentionally explicit. Open/read-only opens do
// not install tracking, and no shipped schedule invokes this method implicitly.
func (s *Store) InstallObservationCapture(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != schemaVersion {
		return errors.New("capture install requires unchanged native schema 16")
	}
	if err = validateCaptureNativeSchema(ctx, tx); err != nil {
		return err
	}
	var present bool
	if present, err = capturePresent(ctx, tx); err != nil {
		return err
	}
	if present {
		return errors.New("capture already installed")
	}
	if err = captureConnection(ctx, tx); err != nil {
		return err
	}
	for _, ddl := range captureDDL() {
		if _, err = tx.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("install capture: %w", err)
		}
	}
	archive, err := captureID()
	if err != nil {
		return err
	}
	epoch, err := captureID()
	if err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, `INSERT INTO capture_commits(epoch,kind,previous_epoch,reset_reason,observation_count,sealed_at) VALUES(?,'epoch_reset',NULL,'installation',0,?)`, epoch, time.Now().UTC().Format(timeLayout))
	if err != nil {
		return err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO capture_meta VALUES(1,1,?,?,'ready',?,0,?,?)`, archive, epoch, id, observationRegistry, captureDigest())
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO _capture_context(singleton,open,mode) VALUES(1,0,'ordinary'); PRAGMA user_version=17`); err != nil {
		return err
	}
	return tx.Commit()
}
func captureConnection(ctx context.Context, q dbQueries) error {
	var fk int
	if err := q.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil {
		return err
	}
	if fk != 1 {
		return errors.New("capture requires foreign keys")
	}
	_, err := q.ExecContext(ctx, "PRAGMA recursive_triggers=ON")
	return err
}

type captureTransaction struct {
	ID                            int64
	Epoch, Previous, Kind, Reason string
}

func (s *Store) beginObservationCapture(ctx context.Context) (*captureTransaction, error) {
	present, err := capturePresent(ctx, s.q())
	if err != nil || !present {
		if err == nil {
			var version int
			err = s.q().QueryRowContext(ctx, "PRAGMA user_version").Scan(&version)
			if err == nil && version == observationSchemaVersion {
				err = errors.New("capture metadata missing")
			}
		}
		return nil, err
	}
	if err = captureConnection(ctx, s.q()); err != nil {
		return nil, err
	}
	if err = s.validateCaptureCached(ctx); err != nil {
		return nil, err
	}
	cap, err := captureCapability(ctx, s.q())
	if err != nil {
		return nil, err
	}
	// Allocate with SQLite AUTOINCREMENT, then defer the descriptor until sealing.
	// Context/dirty/observation deferred FKs make a missed seal fail at COMMIT.
	r, err := s.q().ExecContext(ctx, `INSERT INTO capture_commits(epoch,kind,observation_count,sealed_at) VALUES(?,'observation',0,'')`, cap.Epoch)
	if err != nil {
		return nil, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return nil, err
	}
	if _, err = s.q().ExecContext(ctx, `DELETE FROM capture_commits WHERE tx_id=?`, id); err != nil {
		return nil, err
	}
	_, err = s.q().ExecContext(ctx, `UPDATE _capture_context SET open=1,tx_id=?,mode='ordinary' WHERE singleton=1`, id)
	return &captureTransaction{ID: id, Epoch: cap.Epoch, Kind: "observation"}, err
}
