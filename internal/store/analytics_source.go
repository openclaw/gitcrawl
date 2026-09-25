package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Publication describes provider-authored timestamps, not capture/repair time.
type Publication struct {
	Submitted string `json:"submitted_at_gh,omitempty"`
	Published string `json:"publication_at_gh,omitempty"`
}

func ProviderTime(value any) string {
	s, ok := value.(string)
	if !ok || strings.TrimSpace(s) != s {
		return ""
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
func CommentPublication(kind, raw string) Publication {
	var p map[string]any
	if json.Unmarshal([]byte(raw), &p) != nil {
		return Publication{}
	}
	g, _ := p["_graphql"].(map[string]any)
	pick := func(values ...any) string {
		for _, v := range values {
			if s := ProviderTime(v); s != "" {
				return s
			}
		}
		return ""
	}
	if kind == "pull_review" {
		if strings.EqualFold(fmt.Sprint(p["state"]), "pending") {
			return Publication{}
		}
		at := pick(p["submitted_at"], g["submittedAt"])
		return Publication{Submitted: at, Published: at}
	}
	return Publication{Published: pick(g["publishedAt"], p["published_at"], p["created_at"], g["createdAt"])}
}
func (s *Store) ensureAnalyticsSourceSchema(ctx context.Context) error {
	for _, table := range []string{"comments", "comment_revisions"} {
		for _, col := range []string{"submitted_at_gh", "publication_at_gh"} {
			if err := s.ensureColumn(ctx, table, col, "text"); err != nil {
				return err
			}
		}
	}
	_, err := s.q().ExecContext(ctx, `CREATE TABLE IF NOT EXISTS actor_identity_evidence(
  node_id TEXT PRIMARY KEY,actor_node_id TEXT,login TEXT,actor_type TEXT,observed_at TEXT NOT NULL,raw_json TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS actor_profiles(node_id TEXT PRIMARY KEY,login TEXT NOT NULL,actor_type TEXT NOT NULL,name TEXT,bio TEXT,url TEXT,created_at TEXT,observed_at TEXT NOT NULL,raw_json TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS analytics_coverage(repository TEXT PRIMARY KEY,through TEXT NOT NULL,issues INTEGER,pull_requests INTEGER,complete INTEGER NOT NULL,observed_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS analytics_repair_receipts(name TEXT PRIMARY KEY,cursor INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS analytics_pending_nodes(node_id TEXT NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(kind,node_id));
 CREATE TABLE IF NOT EXISTS analytics_collection_state(name TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL);`)
	return err
}
func (s *Store) queueAnalyticsActor(ctx context.Context, raw string) error {
	var payload struct {
		NodeID string `json:"node_id"`
		User   struct {
			NodeID string `json:"node_id"`
		} `json:"user"`
	}
	if json.Unmarshal([]byte(raw), &payload) != nil {
		return nil
	}
	id, kind := payload.User.NodeID, "profile"
	if id == "" {
		id, kind = payload.NodeID, "identity"
	}
	if id == "" {
		return nil
	}
	_, err := s.q().ExecContext(ctx, "INSERT OR IGNORE INTO analytics_pending_nodes(node_id,kind) VALUES(?,?)", id, kind)
	return err
}
func (s *Store) upsertCommentPublication(ctx context.Context, id int64, kind, raw string) error {
	p := CommentPublication(kind, raw)
	if err := s.queueAnalyticsActor(ctx, raw); err != nil {
		return err
	}
	_, err := s.q().ExecContext(ctx, "UPDATE comments SET submitted_at_gh=?,publication_at_gh=? WHERE id=?", nullString(p.Submitted), nullString(p.Published), id)
	return err
}

type PublicationRepair struct {
	Scanned     int `json:"scanned"`
	Changed     int `json:"changed"`
	WouldChange int `json:"would_change"`
	Unknown     int `json:"unknown"`
}

// RepairPublication uses retained native payloads. Raw JSON, IDs and original
// recorded_at values are never rewritten, and no synthetic revision is appended.
func (s *Store) RepairPublication(ctx context.Context, apply bool) (PublicationRepair, error) {
	result := PublicationRepair{}
	pending := false
	if apply {
		value, e := s.AnalyticsState(ctx, "publication_repair_in_progress")
		if e != nil {
			return result, e
		}
		pending = value == "1"
		if e = s.SetAnalyticsState(ctx, "publication_repair_in_progress", "1"); e != nil {
			return result, e
		}
	}
	for _, table := range []string{"comments", "comment_revisions"} {
		var cursor int64
		var hasFields int
		if err := s.q().QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info(?) WHERE name IN('submitted_at_gh','publication_at_gh')", table).Scan(&hasFields); err != nil {
			return result, err
		}
		for {
			fields := ",NULL,NULL"
			if hasFields == 2 {
				fields = ",c.submitted_at_gh,c.publication_at_gh"
			}
			query := "SELECT c.id,c.comment_type,c.raw_json" + fields + " FROM comments c WHERE c.id>? ORDER BY c.id LIMIT 1000"
			if table == "comment_revisions" {
				fields = ",NULL,NULL"
				if hasFields == 2 {
					fields = ",r.submitted_at_gh,r.publication_at_gh"
				}
				query = "SELECT r.id,c.comment_type,r.raw_json" + fields + " FROM comment_revisions r JOIN comments c ON c.id=r.comment_id WHERE r.id>? ORDER BY r.id LIMIT 1000"
			}
			rows, err := s.q().QueryContext(ctx, query, cursor)
			if err != nil {
				return result, err
			}
			type item struct {
				id                   int64
				kind, raw            string
				submitted, published sql.NullString
			}
			var batch []item
			for rows.Next() {
				var r item
				if err := rows.Scan(&r.id, &r.kind, &r.raw, &r.submitted, &r.published); err != nil {
					rows.Close()
					return result, err
				}
				batch = append(batch, r)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return result, err
			}
			if len(batch) == 0 {
				break
			}
			if apply {
				err = s.WithTx(ctx, func(tx *Store) error {
					for _, r := range batch {
						p := CommentPublication(r.kind, r.raw)
						if p.Submitted != r.submitted.String || p.Published != r.published.String {
							result.WouldChange++
						}
						result.Scanned++
						if r.kind == "pull_review" && p.Submitted == "" {
							result.Unknown++
						}
						res, e := tx.q().ExecContext(ctx, "UPDATE "+table+" SET submitted_at_gh=?,publication_at_gh=? WHERE id=? AND raw_json=? AND (submitted_at_gh IS NOT ? OR publication_at_gh IS NOT ?)", nullString(p.Submitted), nullString(p.Published), r.id, r.raw, nullString(p.Submitted), nullString(p.Published))
						if e != nil {
							return e
						}
						n, e := res.RowsAffected()
						if e != nil {
							return e
						}
						result.Changed += int(n)
					}
					_, e := tx.q().ExecContext(ctx, "INSERT INTO analytics_repair_receipts(name,cursor,updated_at) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,updated_at=excluded.updated_at", table, batch[len(batch)-1].id, time.Now().UTC().Format(time.RFC3339Nano))
					return e
				})
				if err != nil {
					return result, err
				}
			} else {
				for _, r := range batch {
					result.Scanned++
					p := CommentPublication(r.kind, r.raw)
					if p.Submitted != r.submitted.String || p.Published != r.published.String {
						result.WouldChange++
					}
					if r.kind == "pull_review" && p.Submitted == "" {
						result.Unknown++
					}
				}
			}
			cursor = batch[len(batch)-1].id
		}
	}
	if apply {
		if result.Changed > 0 || pending {
			if e := s.SetAnalyticsState(ctx, "publication_repair_generation", time.Now().UTC().Format(time.RFC3339Nano)); e != nil {
				return result, e
			}
		}
		if e := s.SetAnalyticsState(ctx, "publication_repair_in_progress", "0"); e != nil {
			return result, e
		}
	}
	return result, nil
}

// Use the native node itself as the identity evidence key: login reuse is not an
// identity join. Deleted/unavailable actors are retained as explicit unknowns.
func (s *Store) SaveActorEvidence(ctx context.Context, nodes []map[string]any, at string) error {
	return s.WithTx(ctx, func(tx *Store) error {
		for _, n := range nodes {
			id, _ := n["id"].(string)
			if id == "" {
				continue
			}
			a, _ := n["author"].(map[string]any)
			actor, _ := a["id"].(string)
			if actor != "" {
				if _, e := tx.q().ExecContext(ctx, "INSERT OR IGNORE INTO analytics_pending_nodes(node_id,kind) VALUES(?,'profile')", actor); e != nil {
					return e
				}
			}
			login, _ := a["login"].(string)
			typ, _ := a["__typename"].(string)
			raw, e := json.Marshal(n)
			if e != nil {
				return e
			}
			if _, e = tx.q().ExecContext(ctx, `INSERT INTO actor_identity_evidence VALUES(?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET actor_node_id=excluded.actor_node_id,login=excluded.login,actor_type=excluded.actor_type,observed_at=excluded.observed_at,raw_json=excluded.raw_json`, id, nullString(actor), nullString(login), nullString(typ), at, string(raw)); e != nil {
				return e
			}
		}
		return nil
	})
}
func (s *Store) SaveActorProfiles(ctx context.Context, nodes []map[string]any, at string) error {
	return s.WithTx(ctx, func(tx *Store) error {
		for _, n := range nodes {
			id, _ := n["id"].(string)
			login, _ := n["login"].(string)
			typ, _ := n["__typename"].(string)
			if id == "" {
				continue
			}
			raw, e := json.Marshal(n)
			if e != nil {
				return e
			}
			name, _ := n["name"].(string)
			bio, _ := n["bio"].(string)
			url, _ := n["url"].(string)
			_, e = tx.q().ExecContext(ctx, `INSERT INTO actor_profiles VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET login=excluded.login,actor_type=excluded.actor_type,name=excluded.name,bio=excluded.bio,url=excluded.url,created_at=excluded.created_at,observed_at=excluded.observed_at,raw_json=excluded.raw_json`, id, login, typ, nullString(name), nullString(bio), nullString(url), nullString(ProviderTime(n["createdAt"])), at, string(raw))
			if e != nil {
				return e
			}
		}
		return nil
	})
}
func (s *Store) SeedAnalyticsNodes(ctx context.Context, profiles bool) error {
	query := `INSERT OR IGNORE INTO analytics_pending_nodes(node_id,kind) SELECT json_extract(raw_json,'$.node_id'),'identity' FROM threads WHERE json_valid(raw_json) AND json_extract(raw_json,'$.user.node_id') IS NULL AND json_extract(raw_json,'$.node_id') IS NOT NULL
 UNION SELECT json_extract(raw_json,'$.node_id'),'identity' FROM comments WHERE json_valid(raw_json) AND json_extract(raw_json,'$.user.node_id') IS NULL AND json_extract(raw_json,'$.node_id') IS NOT NULL`
	if profiles {
		query = `INSERT OR IGNORE INTO analytics_pending_nodes(node_id,kind) SELECT json_extract(raw_json,'$.user.node_id'),'profile' FROM threads WHERE json_valid(raw_json) AND json_extract(raw_json,'$.user.node_id') IS NOT NULL
 UNION SELECT json_extract(raw_json,'$.user.node_id'),'profile' FROM comments WHERE json_valid(raw_json) AND json_extract(raw_json,'$.user.node_id') IS NOT NULL
 UNION SELECT actor_node_id,'profile' FROM actor_identity_evidence WHERE actor_node_id IS NOT NULL`
	}
	_, err := s.q().ExecContext(ctx, query)
	return err
}
func (s *Store) AnalyticsIdentityNodes(ctx context.Context, limit int) ([]string, error) {
	return s.analyticsNodes(ctx, limit, false)
}
func (s *Store) AnalyticsProfileNodes(ctx context.Context, limit int) ([]string, error) {
	return s.analyticsNodes(ctx, limit, true)
}
func (s *Store) analyticsNodes(ctx context.Context, limit int, profiles bool) ([]string, error) {
	query := `SELECT q.node_id FROM analytics_pending_nodes q WHERE kind='identity' AND NOT EXISTS(SELECT 1 FROM actor_identity_evidence e WHERE e.node_id=q.node_id) LIMIT ?`
	if profiles {
		query = `SELECT q.node_id FROM analytics_pending_nodes q WHERE kind='profile' AND NOT EXISTS(SELECT 1 FROM actor_profiles p WHERE p.node_id=q.node_id AND p.observed_at>=?) LIMIT ?`
	}
	var rows *sql.Rows
	var e error
	if profiles {
		rows, e = s.q().QueryContext(ctx, query, time.Now().UTC().Add(-24*time.Hour).Format(time.RFC3339Nano), limit)
	} else {
		rows, e = s.q().QueryContext(ctx, query, limit)
	}
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			return nil, e
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
func (s *Store) AnalyticsState(ctx context.Context, key string) (string, error) {
	var value string
	err := s.q().QueryRowContext(ctx, "SELECT value FROM analytics_collection_state WHERE name=?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}
func (s *Store) SetAnalyticsState(ctx context.Context, key, value string) error {
	_, err := s.q().ExecContext(ctx, "INSERT INTO analytics_collection_state VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", key, value, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
func (s *Store) SaveAnalyticsCoverage(ctx context.Context, repo, through string, issues, prs int) error {
	_, e := s.q().ExecContext(ctx, `INSERT INTO analytics_coverage VALUES(?,?,?,?,1,?) ON CONFLICT(repository) DO UPDATE SET through=excluded.through,issues=excluded.issues,pull_requests=excluded.pull_requests,complete=1,observed_at=excluded.observed_at`, repo, through, issues, prs, time.Now().UTC().Format(time.RFC3339Nano))
	return e
}
