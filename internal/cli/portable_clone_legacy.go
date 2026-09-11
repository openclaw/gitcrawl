package cli

import "context"

// Retain the existing clone contract on platforms and filesystems without
// exclusive publication, rather than introducing a new installation minimum.
func clonePortableStoreLegacy(ctx context.Context, remoteURL, dir string) error {
	if err := runGit(ctx, "", "clone", "--depth", "1", "--", remoteURL, dir); err != nil {
		return err
	}
	if err := markPortableStoreCheckout(dir); err != nil {
		return err
	}
	return removePortableSQLiteSidecars(dir)
}
