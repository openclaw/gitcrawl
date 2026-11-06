package storedb_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

func TestGeneratedQueriesUseTransaction(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	st.DB().SetMaxOpenConns(1)
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := st.DB().BeginTx(bounded, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if got, err := storedb.New(st.DB()).WithTx(tx).CountRepositories(bounded); err != nil || got != 0 {
		t.Fatalf("count repositories in tx = %d, %v", got, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
}
