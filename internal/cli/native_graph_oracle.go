//go:build native_cluster_overlay

package cli

import (
	"context"

	"github.com/openclaw/gitcrawl/internal/store"
)

// NativeOriginalGraphOracle runs the Go reference builder for synthetic native
// graph comparisons. It is excluded from ordinary builds.
func NativeOriginalGraphOracle(ctx context.Context, st *store.Store, repoID int64, vectors []store.ThreadVector) ([]store.DurableClusterInput, int, error) {
	return buildDurableClusterInputs(ctx, st, repoID, vectors, clusterBuildOptions{
		Threshold: .80, MinSize: 1, MaxClusterSize: 40, Fanout: 16,
		CrossKindThreshold: .93, RetireMissing: true,
	})
}
