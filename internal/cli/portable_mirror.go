package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type portableMirror struct {
	path     string
	exists   bool
	info     os.FileInfo
	parent   os.FileInfo
	state    os.FileInfo
	stateSHA [32]byte
	digest   [32]byte
	preserve bool
	sidecars map[string]os.FileInfo
}

func inspectPortableMirror(ctx context.Context, path, source string) (portableMirror, error) {
	mirror := portableMirror{path: path, sidecars: make(map[string]os.FileInfo)}
	var err error
	mirror.parent, err = os.Stat(filepath.Dir(path))
	if err != nil {
		return mirror, err
	}
	statePath := portableStoreRefreshStatePath(path)
	if _, err := os.Lstat(statePath + ".lock"); !errors.Is(err, os.ErrNotExist) {
		return mirror, fmt.Errorf("legacy runtime refresh lock exists or cannot be inspected")
	}
	var state portableStoreRefreshState
	if info, err := os.Lstat(statePath); err == nil {
		if !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return mirror, fmt.Errorf("invalid runtime refresh metadata")
		}
		data, err := os.ReadFile(statePath)
		if err != nil {
			return mirror, err
		}
		if err := json.Unmarshal(data, &state); err != nil {
			return mirror, fmt.Errorf("invalid runtime refresh metadata")
		}
		mirror.state = info
		mirror.stateSHA, err = portableFileSHA256(ctx, statePath)
		if err != nil {
			return mirror, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return mirror, err
	}
	info, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return mirror, err
	}
	if err == nil {
		if !info.Mode().IsRegular() {
			return mirror, fmt.Errorf("runtime mirror is not a regular file")
		}
		mirror.exists, mirror.info = true, info
		mirror.digest, err = portableFileSHA256(ctx, path)
		if err != nil {
			return mirror, err
		}
		sourceSHA := state.MirrorHealthSourceSHA256
		if sourceSHA == "" {
			manifest, _, err := readPortableDBManifest(portableDBManifestPath(source))
			if err != nil {
				return mirror, err
			}
			sourceSHA = manifest.SHA256
		}
		mirror.preserve = !strings.EqualFold(fmt.Sprintf("%x", mirror.digest), sourceSHA)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if err == nil {
			mirror.sidecars[suffix] = info
			mirror.preserve = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return mirror, err
		}
	}
	return mirror, nil
}

func (mirror portableMirror) recheck(ctx context.Context) error {
	parent, err := os.Stat(filepath.Dir(mirror.path))
	if err != nil || !os.SameFile(parent, mirror.parent) {
		return fmt.Errorf("runtime mirror directory changed during refresh")
	}
	statePath := portableStoreRefreshStatePath(mirror.path)
	if _, err := os.Lstat(statePath + ".lock"); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("legacy runtime refresh lock appeared or cannot be inspected")
	}
	state, stateErr := os.Lstat(statePath)
	if mirror.state == nil {
		if !errors.Is(stateErr, os.ErrNotExist) {
			return fmt.Errorf("runtime refresh metadata appeared during refresh")
		}
	} else {
		if stateErr != nil || !os.SameFile(state, mirror.state) {
			return fmt.Errorf("runtime refresh metadata changed during refresh")
		}
		digest, err := portableFileSHA256(ctx, statePath)
		if err != nil || digest != mirror.stateSHA {
			return fmt.Errorf("runtime refresh metadata changed during refresh")
		}
	}
	info, err := os.Lstat(mirror.path)
	if !mirror.exists {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("runtime mirror appeared during refresh")
		}
	} else {
		if err != nil || !os.SameFile(info, mirror.info) || info.Size() != mirror.info.Size() || !info.ModTime().Equal(mirror.info.ModTime()) {
			return fmt.Errorf("runtime mirror changed during refresh")
		}
		digest, err := portableFileSHA256(ctx, mirror.path)
		if err != nil || digest != mirror.digest {
			return fmt.Errorf("runtime mirror changed during refresh")
		}
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		info, err := os.Lstat(mirror.path + suffix)
		old, existed := mirror.sidecars[suffix]
		if existed {
			if err != nil || !os.SameFile(info, old) || !info.ModTime().Equal(old.ModTime()) || info.Size() != old.Size() {
				return fmt.Errorf("runtime SQLite sidecar changed during refresh")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("runtime SQLite sidecar appeared during refresh")
		}
	}
	return nil
}
