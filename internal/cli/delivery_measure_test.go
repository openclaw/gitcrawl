//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestDeliveryMeasureSeedReadOnly(t *testing.T) {
	db := deliveryNativeFixture(t)
	before, err := os.ReadFile(db)
	if err != nil {
		t.Fatal(err)
	}
	a := New()
	var out, diagnostic bytes.Buffer
	a.Stdout, a.Stderr = &out, &diagnostic
	a.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Fatal("measurement must not acquire credentials")
		return "", nil
	}
	if err := a.Run(context.Background(), []string{"delivery", "measure-seed", "--db", db, "--repo", "fixture/repo", "--timeout", "10s", "--json"}); err != nil {
		t.Fatal(err)
	}
	var result store.NativeReportMeasurement
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Rows != 3 || result.Bytes <= 0 || len(result.Tables) != 7 || result.Tables[2].Rows != 1 {
		t.Fatalf("missing complete selected scope: %+v", result)
	}
	if out.Len() > 8192 || diagnostic.Len() != 0 || strings.Contains(out.String(), "private-fixture") || strings.Contains(out.String(), db) {
		t.Fatal("measurement leaked content/path or unbounded output")
	}
	after, err := os.ReadFile(db)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("measurement changed native source", err)
	}
}

func TestDeliveryMeasureSeedRequiresExplicitReadOnlyInputs(t *testing.T) {
	for _, args := range [][]string{
		{}, {"--db", "missing", "--repo", "fixture/repo"},
		{"--db", "missing", "--repo", "fixture/repo", "--timeout", "-1s"},
		{"--db", "missing", "--repo", "fixture/repo", "--timeout", "1s", "--outbox", "private-output"},
		{"--db", "missing", "--repo", "fixture/repo", "--timeout", "1s", "--token-env", "PRIVATE_TOKEN"},
	} {
		a := New()
		var out bytes.Buffer
		a.Stdout, a.Stderr = &out, &out
		if err := a.Run(context.Background(), append([]string{"delivery", "measure-seed"}, args...)); err == nil {
			t.Fatal("invalid measurement arguments accepted", args)
		}
		if strings.Contains(out.String(), "private-output") || strings.Contains(out.String(), "PRIVATE_TOKEN") {
			t.Fatal("invalid flag value escaped diagnostics")
		}
	}
}
