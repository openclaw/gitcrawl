package remotedelivery

import (
	"context"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

// PrepareClusterSeed returns the same bounded exact-byte requests as private
// seeds, with a separate closed public profile and content-bound token. It does
// not create an outbox, acquire data, publish, or advance an ingestion cursor.
func PrepareClusterSeed(ctx context.Context, report ClusterReport, request PrivateSeedRequest) (PrivateSeed, error) {
	if err := ctx.Err(); err != nil {
		return PrivateSeed{}, err
	}
	if report.Profile != store.NativeClusterContextProfile || len(report.Tables) != len(clusterLayouts) || report.Rows < 1 || !digest(report.ScopeSHA256) || !digest(report.SHA256) {
		return PrivateSeed{}, ErrClusterProjection
	}
	if request.MaxBytes <= 0 {
		return PrivateSeed{}, ErrPrivateSeedLimit
	}
	m := remote.GitHubSeedManifest{Contract: remote.GitHubSeedContract, App: "gitcrawl", Archive: request.Archive, BindingID: request.BindingID, SeedID: request.SeedID, Profile: report.Profile, NativeSchema: 16, Repository: report.Repository, RepositoryID: report.RepositoryID, ScopeSHA256: report.ScopeSHA256, NativeSHA256: report.SHA256, NativeThrough: "gitcrawl-cluster-v1:" + report.ScopeSHA256 + ":" + report.SHA256}
	return prepareGitHubSeed(ctx, m, report.Tables, clusterLayouts, report.Rows, request.MaxBytes)
}
