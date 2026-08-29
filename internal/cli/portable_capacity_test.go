package cli

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPortableCapacityScansUniquePathsAndCancellation(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	first, second := filepath.Join(root, "first"), filepath.Join(nested, "second")
	writePortableSafetyFile(t, first, []byte("abc"))
	writePortableSafetyFile(t, second, []byte("12345"))
	roots := []string{root, nested, filepath.Join(root, "already-gone")}
	sizes, err := portableFileSizes(context.Background(), roots)
	if err != nil || !reflect.DeepEqual(sizes, map[string]int64{first: 3, second: 5}) {
		t.Fatalf("overlapping roots or vanished paths miscounted: %v, %v", sizes, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sizes, err := portableFileSizes(ctx, roots); !errors.Is(err, context.Canceled) || sizes != nil {
		t.Fatalf("canceled scan returned usable accounting: %v, %v", sizes, err)
	}
	if budget, err := newPortableBudget(ctx, roots, 0, 8); !errors.Is(err, context.Canceled) || budget != nil {
		t.Fatalf("canceled admission returned a budget: %+v, %v", budget, err)
	}
}

func TestPortableCapacityAdmissionAndMeasurementFailures(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name    string
		roots   []string
		reserve uint64
		growth  int64
		want    string
	}{
		{"reserve", []string{root}, math.MaxUint64, 1, "free-space reserve crossed"},
		{"reserve-plus-growth", []string{root}, 0, math.MaxInt64, "reserve plus growth budget"},
		{"missing-filesystem", []string{filepath.Join(root, "missing")}, 0, 1, "measure portable free space"},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget, err := newPortableBudget(context.Background(), test.roots, test.reserve, test.growth)
			if err == nil || !strings.Contains(err.Error(), test.want) || budget == nil {
				t.Fatalf("expected %q refusal with diagnostics, got %+v, %v", test.want, budget, err)
			}
			snapshot := budget.snapshot()
			if snapshot.ReserveBytes != test.reserve || snapshot.GrowthLimit != test.growth || snapshot.PeakGrowth != 0 {
				t.Fatalf("refusal lost requested limits: %+v", snapshot)
			}
			if test.name == "reserve-plus-growth" && (snapshot.FreeBefore == 0 || snapshot.FreeAfter != snapshot.FreeBefore) {
				t.Fatalf("admission omitted measured capacity: %+v", snapshot)
			}
		})
	}
}

func TestPortableCapacityPeakAndMonitor(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runtime.db")
	writePortableSafetyFile(t, path, []byte("base"))
	budget, err := newPortableBudget(context.Background(), []string{root}, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	writePortableSafetyFile(t, path, []byte("base1234"))
	if err := budget.check(context.Background()); err != nil {
		t.Fatalf("exact growth limit refused: %v", err)
	}
	writePortableSafetyFile(t, path, []byte("b"))
	if err := budget.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := budget.snapshot().PeakGrowth; got != 4 {
		t.Fatalf("shrinking erased peak growth: %d", got)
	}
	// Five new bytes exceed the budget even though the old file shrank.
	writePortableSafetyFile(t, filepath.Join(root, "new"), []byte("12345"))
	ctx, stop := budget.monitor(context.Background())
	defer stop()
	select {
	case <-ctx.Done():
		if cause := context.Cause(ctx); cause == nil || !strings.Contains(cause.Error(), "growth budget exceeded") {
			t.Fatalf("monitor lost capacity refusal: %v", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("monitor did not cancel excessive growth")
	}
	if got := budget.snapshot().PeakGrowth; got != 5 {
		t.Fatalf("monitor did not record observed growth: %d", got)
	}
	if data, err := os.ReadFile(filepath.Join(root, "new")); err != nil || string(data) != "12345" {
		t.Fatalf("monitor deleted data to satisfy budget: %q, %v", data, err)
	}
}
