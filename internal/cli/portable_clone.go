//go:build darwin || linux || windows

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Stage clones so a failed Git transfer cannot leave an unusable .git at the
// destination, where the next init would mistake it for an existing checkout.
func clonePortableStore(ctx context.Context, remoteURL, dir string) error {
	return clonePortableStoreWithRename(ctx, remoteURL, dir, renamePortableExclusive)
}

var errPortableExclusiveUnavailable = errors.New("exclusive rename unavailable")

func clonePortableStoreWithRename(ctx context.Context, remoteURL, dir string, rename func(string, string) error) (resultErr error) {
	parent := filepath.Dir(dir)
	existing := false
	if entries, err := os.ReadDir(dir); err == nil {
		if len(entries) != 0 {
			return fmt.Errorf("portable clone destination is no longer empty")
		}
		// Keep pre-created directories in place, including their permissions,
		// ACLs and ability to work beneath a non-writable parent.
		parent, existing = dir, true
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read portable clone destination: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".gitcrawl-clone-")
	if err != nil {
		return fmt.Errorf("create portable clone staging directory: %w", err)
	}
	preserve := false
	owned := true
	defer func() {
		if !owned {
			return
		}
		if preserve {
			resultErr = errors.Join(resultErr, fmt.Errorf("incomplete portable publication preserved at %s", staging))
			return
		}
		if err := os.RemoveAll(staging); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove portable clone staging directory: %w", err))
		}
	}()
	probeSource, probeTarget := filepath.Join(staging, "probe-source"), filepath.Join(staging, "probe-target")
	for _, path := range []string{probeSource, probeTarget} {
		if err := os.Mkdir(path, 0o700); err != nil {
			return err
		}
	}
	probeErr := rename(probeSource, probeTarget)
	if errors.Is(probeErr, os.ErrExist) {
		if err := os.Remove(probeTarget); err != nil {
			return err
		}
		// Linux may reject an existing destination before consulting the
		// filesystem. Also prove that an absent destination can be published.
		probeErr = rename(probeSource, probeTarget)
	} else if probeErr == nil {
		probeErr = errPortableExclusiveUnavailable
	}
	if errors.Is(probeErr, errPortableExclusiveUnavailable) {
		// Probe before transferring data. A pre-existing empty destination must
		// be empty again before Git's compatibility path can use it.
		if err := os.RemoveAll(staging); err != nil {
			return err
		}
		owned = false
		return clonePortableStoreLegacy(ctx, remoteURL, dir)
	} else if probeErr != nil {
		return fmt.Errorf("probe exclusive portable publication: %w", probeErr)
	}
	checkout := filepath.Join(staging, "checkout")
	if err := runGit(ctx, "", "clone", "--depth", "1", "--", remoteURL, checkout); err != nil {
		return err
	}
	if err := markPortableStoreCheckout(checkout); err != nil {
		return err
	}
	if err := removePortableSQLiteSidecars(checkout); err != nil {
		return err
	}
	if !existing {
		return rename(checkout, dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(staging) {
		return fmt.Errorf("portable clone destination changed during cloning")
	}
	entries, err = os.ReadDir(checkout)
	if err != nil {
		return err
	}
	var names []string
	for _, entry := range entries {
		if entry.Name() != ".git" {
			names = append(names, entry.Name())
		}
	}
	// Publish Git metadata last; failed transfers never become checkouts.
	names = append(names, ".git")
	var moved []string
	for _, name := range names {
		if err := rename(filepath.Join(checkout, name), filepath.Join(dir, name)); err != nil {
			resultErr = fmt.Errorf("publish portable clone: %w", err)
			preserve = len(moved) > 0
			for _, previous := range moved {
				if err := rename(filepath.Join(dir, previous), filepath.Join(checkout, previous)); err != nil {
					resultErr = errors.Join(resultErr, fmt.Errorf("roll back portable publication: %w", err))
				}
			}
			return resultErr
		}
		moved = append(moved, name)
	}
	return nil
}
