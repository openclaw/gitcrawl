package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestRecordEvidenceIntegrityAndDepthBound(t *testing.T) {
	ctx := context.Background()
	st, _ := recordFixture(t)
	for i := 0; i < 35; i++ {
		raw := strings.Repeat("prefix", 100) + string(rune('A'+i)) + strings.Repeat("suffix", 100)
		if err := st.retainRecordCheck(ctx, "test", "key", "now", raw, nil); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(raw))
		got, err := st.EvidenceRaw(ctx, hex.EncodeToString(sum[:]))
		if err != nil || string(got) != raw {
			t.Fatalf("depth %d: %v", i, err)
		}
	}
	for _, tc := range []struct {
		name          string
		depth, prefix int
		delta         []byte
	}{
		{"bad-depth", 33, 0, nil}, {"bad-bounds", 1, 999999, nil}, {"bad-gzip", 1, 0, []byte("invalid")},
	} {
		if _, err := st.DB().Exec(`insert into record_evidence_blobs(sha256,base_sha256,prefix_bytes,delta_gzip,depth) select ?,sha256,?,?,? from record_evidence_blobs where depth=0 limit 1`, tc.name, tc.prefix, tc.delta, tc.depth); err != nil {
			t.Fatal(err)
		}
		if _, err := st.EvidenceRaw(ctx, tc.name); err == nil {
			t.Fatalf("accepted %s", tc.name)
		}
	}
	if _, err := st.DB().Exec(`insert into record_evidence_blobs(sha256,raw_bytes) values('wrong',X'61')`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EvidenceRaw(ctx, "wrong"); err == nil {
		t.Fatal("accepted corrupted anchor")
	}
	if err := st.retainRecordCheck(ctx, "test", "key", "now", "", make(chan int)); err == nil {
		t.Fatal("accepted non-serializable check")
	}
	for _, v := range []any{nil, true, false, json.Number("9007199254740993"), map[string]any{"future": 1}, make(chan int)} {
		_ = jsonScalarString(v)
	}
	if _, err := st.EvidenceRaw(ctx, "missing"); err == nil {
		t.Fatal("missing evidence accepted")
	}
}
