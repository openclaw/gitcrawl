package cli

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const portableDefaultReserve = uint64(2 << 30)
const portableDefaultGrowth = int64(2 << 30)

type portableCapacity struct {
	ReserveBytes uint64 `json:"reserve_bytes"`
	GrowthLimit  int64  `json:"growth_limit_bytes"`
	PeakGrowth   int64  `json:"peak_growth_bytes"`
	FreeBefore   uint64 `json:"minimum_free_before_bytes"`
	FreeAfter    uint64 `json:"minimum_free_observed_bytes"`
}

type portableBudget struct {
	mu       sync.Mutex
	roots    []string
	baseline map[string]int64
	observed portableCapacity
}

// Measure positive per-path logical-byte growth. Deletions and shrinking old
// files cannot buy more budget. This walks metadata, never historical contents.
func portableFileSizes(ctx context.Context, roots []string) (map[string]int64, error) {
	sizes := make(map[string]int64)
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if os.IsNotExist(err) {
				return nil // Git may have just renamed a task-owned temporary file.
			}
			if err != nil {
				return err
			}
			if len(sizes) >= 200000 {
				return fmt.Errorf("portable capacity scan exceeds 200000 files")
			}
			if entry.Type().IsRegular() {
				info, err := entry.Info()
				if os.IsNotExist(err) {
					return nil
				}
				if err != nil {
					return err
				}
				sizes[path] = info.Size()
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return sizes, nil
}

func newPortableBudget(ctx context.Context, roots []string, reserve uint64, growth int64) (*portableBudget, error) {
	baseline, err := portableFileSizes(ctx, roots)
	if err != nil {
		return nil, err
	}
	budget := &portableBudget{roots: roots, baseline: baseline, observed: portableCapacity{ReserveBytes: reserve, GrowthLimit: growth}}
	if err := budget.check(ctx); err != nil {
		return budget, err
	}
	budget.observed.FreeBefore = budget.observed.FreeAfter
	if budget.observed.FreeBefore < reserve || budget.observed.FreeBefore-reserve < uint64(growth) {
		return budget, fmt.Errorf("insufficient capacity for free-space reserve plus growth budget")
	}
	return budget, nil
}

func (budget *portableBudget) check(ctx context.Context) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	sizes, err := portableFileSizes(ctx, budget.roots)
	if err != nil {
		return err
	}
	var growth int64
	for path, size := range sizes {
		if delta := size - budget.baseline[path]; delta > 0 {
			if delta > budget.observed.GrowthLimit-growth {
				budget.observed.PeakGrowth = max(budget.observed.PeakGrowth, growth+delta)
				return fmt.Errorf("portable temporary growth budget exceeded")
			}
			growth += delta
		}
	}
	budget.observed.PeakGrowth = max(budget.observed.PeakGrowth, growth)
	for _, root := range budget.roots {
		free, err := portableFreeBytes(root)
		if err != nil {
			return fmt.Errorf("measure portable free space: %w", err)
		}
		if budget.observed.FreeAfter == 0 || free < budget.observed.FreeAfter {
			budget.observed.FreeAfter = free
		}
		if free < budget.observed.ReserveBytes {
			return fmt.Errorf("portable free-space reserve crossed")
		}
	}
	return nil
}

func (budget *portableBudget) snapshot() portableCapacity {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return budget.observed
}

func (budget *portableBudget) monitor(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := budget.check(ctx); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	return ctx, func() { cancel(nil); <-done }
}
