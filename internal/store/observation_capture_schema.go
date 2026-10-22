package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

func captureDDL() []string {
	ddl := []string{
		`CREATE TABLE capture_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1),format_version INTEGER NOT NULL CHECK(format_version=1),archive_id TEXT NOT NULL,epoch TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('ready','invalidated')),epoch_start_tx_id INTEGER NOT NULL,epoch_start_seq INTEGER NOT NULL,registry_json TEXT NOT NULL,registry_digest TEXT NOT NULL)`,
		`CREATE TABLE capture_commits(tx_id INTEGER PRIMARY KEY AUTOINCREMENT,epoch TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('observation','owner_purge','epoch_reset')),previous_epoch TEXT,reset_reason TEXT,first_seq INTEGER,last_seq INTEGER,observation_count INTEGER NOT NULL CHECK(observation_count>=0),sealed_at TEXT NOT NULL,CHECK((observation_count=0 AND first_seq IS NULL AND last_seq IS NULL) OR (observation_count>0 AND first_seq>0 AND last_seq>=first_seq)))`,
		`CREATE TABLE capture_images(image_id INTEGER PRIMARY KEY AUTOINCREMENT,image_epoch TEXT NOT NULL,table_name TEXT NOT NULL,key_json TEXT NOT NULL,value_json TEXT,redaction_epoch TEXT,redaction_reason TEXT,CHECK((value_json IS NOT NULL AND redaction_epoch IS NULL AND redaction_reason IS NULL) OR (value_json IS NULL AND redaction_epoch IS NOT NULL AND redaction_reason='owner_requested')))`,
		`CREATE TABLE capture_heads(table_name TEXT NOT NULL,key_json TEXT NOT NULL,present INTEGER NOT NULL CHECK(present IN (0,1)),image_id INTEGER REFERENCES capture_images(image_id),clocks_json TEXT NOT NULL,last_seq INTEGER,PRIMARY KEY(table_name,key_json),CHECK((present=1 AND image_id IS NOT NULL) OR (present=0 AND image_id IS NULL)))`,
		`CREATE TABLE capture_observations(seq INTEGER PRIMARY KEY AUTOINCREMENT,tx_id INTEGER NOT NULL REFERENCES capture_commits(tx_id) DEFERRABLE INITIALLY DEFERRED,epoch TEXT NOT NULL,table_name TEXT NOT NULL,key_json TEXT NOT NULL,operation TEXT NOT NULL CHECK(operation IN ('upsert','withdraw')),image_id INTEGER REFERENCES capture_images(image_id),clocks_json TEXT NOT NULL,cause TEXT NOT NULL CHECK(cause IN ('native_write','owner_requested')),CHECK((operation='upsert' AND image_id IS NOT NULL) OR (operation='withdraw' AND image_id IS NULL)),UNIQUE(tx_id,table_name,key_json))`,
		`CREATE TABLE capture_supports(seq INTEGER NOT NULL REFERENCES capture_observations(seq),role TEXT NOT NULL CHECK(role IN ('github_blob','review_thread','review_membership')),ordinal INTEGER NOT NULL CHECK(ordinal>=0),image_id INTEGER NOT NULL REFERENCES capture_images(image_id),clocks_json TEXT NOT NULL,PRIMARY KEY(seq,role,ordinal))`,
		`CREATE TABLE _capture_context(singleton INTEGER PRIMARY KEY CHECK(singleton=1),open INTEGER NOT NULL CHECK(open IN (0,1)),tx_id INTEGER REFERENCES capture_commits(tx_id) DEFERRABLE INITIALLY DEFERRED,mode TEXT NOT NULL CHECK(mode IN ('ordinary','identity_change','owner_purge','epoch_reset')),CHECK((open=0 AND tx_id IS NULL) OR (open=1 AND tx_id IS NOT NULL)))`,
		`CREATE TABLE _capture_dirty(tx_id INTEGER NOT NULL REFERENCES capture_commits(tx_id) DEFERRABLE INITIALLY DEFERRED,table_name TEXT NOT NULL,key_json TEXT NOT NULL,initial_present INTEGER NOT NULL CHECK(initial_present IN (0,1)),first_touch INTEGER NOT NULL,PRIMARY KEY(tx_id,table_name,key_json))`,
		`CREATE INDEX capture_commits_epoch ON capture_commits(epoch,tx_id)`,
		`CREATE INDEX capture_observations_table ON capture_observations(table_name,seq)`,
		`CREATE INDEX capture_observations_tx ON capture_observations(tx_id,seq)`,
		`CREATE INDEX capture_images_key ON capture_images(table_name,key_json,image_id)`,
		`CREATE INDEX capture_images_redaction ON capture_images(redaction_epoch,image_id)`,
		`CREATE INDEX capture_supports_image ON capture_supports(image_id,seq)`,
		`CREATE TRIGGER capture_image_update BEFORE UPDATE ON capture_images WHEN NOT (EXISTS(SELECT 1 FROM _capture_context WHERE open=1 AND mode='owner_purge') AND NEW.image_id=OLD.image_id AND NEW.image_epoch=OLD.image_epoch AND NEW.table_name=OLD.table_name AND NEW.key_json=OLD.key_json AND NEW.value_json IS NULL AND NEW.redaction_reason='owner_requested' AND NEW.redaction_epoch IS NOT NULL) BEGIN SELECT RAISE(ABORT,'immutable capture image'); END`,
		`CREATE TRIGGER capture_image_delete BEFORE DELETE ON capture_images BEGIN SELECT RAISE(ABORT,'immutable capture image'); END`,
		`CREATE TRIGGER capture_observation_update BEFORE UPDATE ON capture_observations BEGIN SELECT RAISE(ABORT,'immutable capture observation'); END`,
		`CREATE TRIGGER capture_observation_delete BEFORE DELETE ON capture_observations BEGIN SELECT RAISE(ABORT,'immutable capture observation'); END`,
		`CREATE TRIGGER capture_support_update BEFORE UPDATE ON capture_supports BEGIN SELECT RAISE(ABORT,'immutable capture support'); END`,
		`CREATE TRIGGER capture_support_delete BEFORE DELETE ON capture_supports BEGIN SELECT RAISE(ABORT,'immutable capture support'); END`,
		`CREATE TRIGGER capture_commit_update BEFORE UPDATE ON capture_commits BEGIN SELECT RAISE(ABORT,'immutable capture commit'); END`,
		`CREATE TRIGGER capture_commit_delete BEFORE DELETE ON capture_commits WHEN OLD.sealed_at<>'' BEGIN SELECT RAISE(ABORT,'immutable capture commit'); END`,
	}
	guard := `SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM _capture_context c JOIN capture_meta m ON m.singleton=c.singleton WHERE c.open=1 AND m.state='ready') THEN RAISE(ABORT,'capture writer context required') END; `
	for _, t := range captureTables() {
		mark := func(prefix, present string) string {
			// A native UPSERT's conflict policy can override trigger OR IGNORE.
			// Explicit absence avoids a conflict while preserving first-touch state.
			return `INSERT INTO _capture_dirty SELECT tx_id,` + captureLiteral(t.Name) + `,` + t.keyExpr(prefix) + `,` + present + `,(SELECT coalesce(max(rowid),0)+1 FROM _capture_dirty) FROM _capture_context c WHERE singleton=1 AND NOT EXISTS(SELECT 1 FROM _capture_dirty d WHERE d.tx_id=c.tx_id AND d.table_name=` + captureLiteral(t.Name) + ` AND d.key_json=` + t.keyExpr(prefix) + `); `
		}
		var where, changed, valid []string
		for _, k := range t.Key {
			where = append(where, captureQuote(k)+` IS NEW.`+captureQuote(k))
			changed = append(changed, `OLD.`+captureQuote(k)+` IS NOT NEW.`+captureQuote(k))
		}
		for _, c := range t.Columns {
			check := `typeof(NEW.` + captureQuote(c.Name) + `)<>` + captureLiteral(c.Type)
			if c.Nullable {
				check = `(` + check + ` AND NEW.` + captureQuote(c.Name) + ` IS NOT NULL)`
			}
			valid = append(valid, check)
			if c.Name == "thread_id" {
				changed = append(changed, `OLD.thread_id IS NOT NEW.thread_id`)
			}
		}
		validate := `SELECT CASE WHEN ` + strings.Join(valid, " OR ") + ` THEN RAISE(ABORT,'capture native cell type mismatch') END; `
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			body := guard
			if event != "DELETE" {
				body += validate
			}
			if event == "UPDATE" {
				body += mark("OLD.", "1") + `UPDATE _capture_context SET mode='identity_change' WHERE mode='ordinary' AND (` + strings.Join(changed, " OR ") + `); ` + mark("NEW.", `EXISTS(SELECT 1 FROM `+captureQuote(t.Name)+` WHERE `+strings.Join(where, " AND ")+`)`)
			}
			if event == "DELETE" {
				body += mark("OLD.", "1")
			}
			ddl = append(ddl, `CREATE TRIGGER capture_native_`+t.Name+`_`+strings.ToLower(event)+` BEFORE `+event+` ON `+captureQuote(t.Name)+` BEGIN `+body+`END`)
		}
		// Automatic INTEGER PRIMARY KEY allocation is undefined in BEFORE INSERT.
		// REPLACE's deletion or an earlier update already records initial presence;
		// genuinely inserted rows become dirty with their real key only afterward.
		ddl = append(ddl, `CREATE TRIGGER capture_native_`+t.Name+`_inserted AFTER INSERT ON `+captureQuote(t.Name)+` BEGIN `+guard+mark("NEW.", "0")+`END`)
	}
	return ddl
}

// ObservationCaptureDDL exposes the exact additive DDL for compatibility review.
func ObservationCaptureDDL() string { return strings.Join(captureDDL(), ";\n\n") + ";\n" }

func validateCaptureNativeSchema(ctx context.Context, q dbQueries) error {
	for _, t := range captureTables() {
		rows, err := q.QueryContext(ctx, "PRAGMA table_xinfo("+captureQuote(t.Name)+")")
		if err != nil {
			return err
		}
		i := 0
		for rows.Next() {
			var cid, notnull, pk, hidden int
			var name, typ string
			var def any
			if err = rows.Scan(&cid, &name, &typ, &notnull, &def, &pk, &hidden); err != nil {
				rows.Close()
				return err
			}
			if i >= len(t.Columns) {
				rows.Close()
				return fmt.Errorf("capture schema extra column in %s", t.Name)
			}
			c := t.Columns[i]
			keypos := 0
			for j, k := range t.Key {
				if k == c.Name {
					keypos = j + 1
				}
			}
			if hidden != 0 || c.Name != name || !strings.EqualFold(c.Type, typ) || pk != keypos || (pk == 0 && (notnull == 0) != c.Nullable) {
				rows.Close()
				return fmt.Errorf("capture native schema mismatch %s.%s", t.Name, name)
			}
			i++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if i != len(t.Columns) {
			return fmt.Errorf("capture schema missing columns in %s", t.Name)
		}
	}
	return nil
}
func validateCaptureSchema(ctx context.Context, q dbQueries) error {
	var version int
	if err := q.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != observationSchemaVersion {
		return fmt.Errorf("capture requires schema version %d", observationSchemaVersion)
	}
	if err := validateCaptureNativeSchema(ctx, q); err != nil {
		return err
	}
	want := map[string]string{}
	for _, ddl := range captureDDL() {
		want[strings.SplitN(strings.Fields(ddl)[2], "(", 2)[0]] = ddl
	}
	rows, err := q.QueryContext(ctx, `SELECT name,sql FROM sqlite_schema WHERE name GLOB 'capture_*' OR name GLOB '_capture_*'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, ddl string
		if err = rows.Scan(&name, &ddl); err != nil {
			return err
		}
		if want[name] != ddl {
			return fmt.Errorf("capture DDL mismatch: %s", name)
		}
		delete(want, name)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(want) != 0 {
		return fmt.Errorf("capture DDL missing %d objects", len(want))
	}
	return nil
}

type captureSchemaCache struct {
	mu     sync.Mutex
	cookie int
	valid  bool
}

func (s *Store) validateCaptureCached(ctx context.Context) error {
	if s.captureCache == nil {
		return validateCaptureSchema(ctx, s.q())
	}
	s.captureCache.mu.Lock()
	defer s.captureCache.mu.Unlock()
	var cookie int
	if err := s.q().QueryRowContext(ctx, "PRAGMA schema_version").Scan(&cookie); err != nil {
		return err
	}
	if s.captureCache.valid && s.captureCache.cookie == cookie {
		return nil
	}
	if err := validateCaptureSchema(ctx, s.q()); err != nil {
		return err
	}
	s.captureCache.cookie = cookie
	s.captureCache.valid = true
	return nil
}
