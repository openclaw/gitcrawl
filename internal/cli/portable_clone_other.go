//go:build !darwin && !linux && !windows

package cli

import "context"

// Preserve the existing source-build contract on other operating systems;
// the published platforms use their exclusive directory rename primitives.
func clonePortableStore(ctx context.Context, remoteURL, dir string) error {
	return clonePortableStoreLegacy(ctx, remoteURL, dir)
}
