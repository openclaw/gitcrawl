package github

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	crawlstore "github.com/openclaw/crawlkit/store"
)

const inventoryApplicationID = 1195918160
const inventorySchema = `
pragma application_id=1195918160;
create table inventory_state (
 id integer primary key check(id=1), descriptor text not null,
 started_at text not null, next_path text not null, finished integer not null default 0
);
create table inventory_pages (
 page integer primary key, request_path text not null unique,
 raw blob not null, sha256 text not null, completed text not null default '[]'
);`

type inventoryCheckpoint struct {
	base    *crawlstore.Store
	started time.Time
}

func openInventoryCheckpoint(ctx context.Context, path, descriptor, firstPath string, started time.Time) (*inventoryCheckpoint, error) {
	if path == "" {
		return nil, errors.New("full inventory requires --inventory-checkpoint")
	}
	fresh := false
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		if err = file.Close(); err != nil {
			return nil, err
		}
		fresh = true
	} else if err != nil {
		return nil, err
	} else if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("inventory checkpoint must be a private regular file")
	}
	if !fresh {
		check, err := crawlstore.OpenReadOnly(ctx, path)
		if err != nil {
			return nil, err
		}
		var appID int
		err = check.DB().QueryRowContext(ctx, "pragma application_id").Scan(&appID)
		check.Close()
		if err != nil || appID != inventoryApplicationID {
			return nil, errors.New("not a qualified inventory checkpoint; retain file")
		}
	}
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path, MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*inventoryCheckpoint, error) { base.Close(); return nil, err }
	if _, err = base.DB().ExecContext(ctx, "pragma synchronous=FULL"); err != nil {
		return fail(err)
	}
	if fresh {
		err = func() error {
			tx, err := base.DB().BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, inventorySchema); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "insert into inventory_state(id,descriptor,started_at,next_path) values(1,?,?,?)", descriptor, started.UTC().Format(time.RFC3339Nano), firstPath); err != nil {
				return err
			}
			return tx.Commit()
		}()
		if err != nil {
			return fail(err)
		}
	}
	var savedDescriptor, savedStart string
	if err = base.DB().QueryRowContext(ctx, "select descriptor,started_at from inventory_state where id=1").Scan(&savedDescriptor, &savedStart); err != nil {
		return fail(err)
	}
	if savedDescriptor != descriptor {
		return fail(errors.New("inventory checkpoint scope, input, caller or archive differs; retain checkpoint"))
	}
	originalStart, err := time.Parse(time.RFC3339Nano, savedStart)
	if err != nil {
		return fail(errors.New("invalid inventory checkpoint start"))
	}
	return &inventoryCheckpoint{base: base, started: originalStart}, nil
}
func (c *inventoryCheckpoint) Close() error { return c.base.Close() }
func (c *inventoryCheckpoint) state(ctx context.Context) (next string, finished bool, pages int, err error) {
	err = c.base.DB().QueryRowContext(ctx, "select next_path,finished,(select count(*) from inventory_pages) from inventory_state where id=1").Scan(&next, &finished, &pages)
	return
}
func (c *inventoryCheckpoint) page(ctx context.Context, index int) (raw []byte, completed []int, err error) {
	var hash, done string
	err = c.base.DB().QueryRowContext(ctx, "select raw,sha256,completed from inventory_pages where page=?", index).Scan(&raw, &hash, &done)
	if err != nil {
		return
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != hash {
		return nil, nil, errors.New("inventory checkpoint page checksum differs")
	}
	err = json.Unmarshal([]byte(done), &completed)
	return
}
func (c *inventoryCheckpoint) savePage(ctx context.Context, index int, request, next string, raw []byte) error {
	tx, err := c.base.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var saved string
	var finished bool
	if err = tx.QueryRowContext(ctx, "select next_path,finished from inventory_state where id=1").Scan(&saved, &finished); err != nil {
		return err
	}
	if finished || saved != request {
		return errors.New("inventory cursor changed; stop competing writer")
	}
	if next != "" {
		var count int
		if err = tx.QueryRowContext(ctx, "select count(*) from inventory_pages where request_path=?", next).Scan(&count); err != nil {
			return err
		}
		if next == request || count != 0 {
			return errors.New("inventory cursor repeated a saved request")
		}
	}
	if _, err = tx.ExecContext(ctx, "insert into inventory_pages(page,request_path,raw,sha256) values(?,?,?,?)", index, request, raw, fmt.Sprintf("%x", sha256.Sum256(raw))); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "update inventory_state set next_path=?,finished=? where id=1", next, next == ""); err != nil {
		return err
	}
	return tx.Commit()
}
func (c *inventoryCheckpoint) complete(ctx context.Context, index int, numbers []int) error {
	tx, err := c.base.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw []byte
	var saved string
	if err = tx.QueryRowContext(ctx, "select raw,completed from inventory_pages where page=?", index).Scan(&raw, &saved); err != nil {
		return err
	}
	rows, err := decodeInventoryPage(raw)
	if err != nil {
		return err
	}
	allowed := map[int]bool{}
	for _, row := range rows {
		n, _ := historyInt(row["number"])
		allowed[n] = true
	}
	var done []int
	if err = json.Unmarshal([]byte(saved), &done); err != nil {
		return err
	}
	seen := map[int]bool{}
	for _, n := range done {
		seen[n] = true
	}
	for _, number := range numbers {
		if !allowed[number] {
			return errors.New("completion key is not in checkpoint page")
		}
		if !seen[number] {
			done = append(done, number)
			seen[number] = true
		}
	}
	encoded, err := json.Marshal(done)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "update inventory_pages set completed=? where page=?", string(encoded), index); err != nil {
		return err
	}
	return tx.Commit()
}
