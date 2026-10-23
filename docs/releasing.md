---
title: Releasing
permalink: /releasing/
---

# Releasing Gitcrawl

`.github/workflows/release-unified.yml` is the only official release path. It calls `openclaw/release-workflows@v1` from protected `main`, verifies the immutable fleet-signed annotated version tag, preserves Gitcrawl's six thin platform archives and `checksums.txt`, signs and notarizes both macOS binaries as `org.openclaw.gitcrawl`, verifies the complete asset inventory independently on arm64 and Intel macOS, and waits for `openclaw/homebrew-tap` to update successfully.

The public compatibility contract remains:

- `gitcrawl_VERSION_{darwin,linux}_{amd64,arm64}.tar.gz`
- `gitcrawl_VERSION_windows_{amd64,arm64}.zip`
- `checksums.txt`
- `CHANGELOG.md`, `LICENSE`, `README.md`, and the Gitcrawl executable inside every platform archive
- OpenClaw Foundation Team ID `FWJYW4S8P8` and code identifier `org.openclaw.gitcrawl`

The shared pipeline also publishes verifier control assets (`ASSET-INVENTORY.json`, `SIGNING-MANIFEST.json`, and `RELEASE-NOTES.md`).

Official builds run on macOS. CI uses the Apple Silicon `xcode-27` runner, with macOS 13.0 explicit in both CI and the Darwin GoReleaser environment. A GoReleaser post-build hook checks `LC_BUILD_VERSION` with `otool` for every Darwin slice before signing or publishing, rejecting deployment targets newer than the documented macOS 13.0 minimum. Local release verification repeats this check and runs Intel binaries with Rosetta, asserting or installing Rosetta before execution. Linux CI snapshots skip this platform-specific check.

## Release

Prepare a dated changelog section while retaining an empty `Unreleased` section, land it on protected `main`, and require green CI at that exact commit. First dispatch a dry run and require it to pass:

```sh
gh workflow run release-unified.yml --repo openclaw/gitcrawl --ref main -f version=X.Y.Z -f dry_run=true
```

After the dry run, sign an annotated `vX.Y.Z` tag at that verified commit using only the fleet release SSH key. Verify it against `.github/release-allowed-signers` before pushing; never move or replace an existing tag:

```sh
set -e
git -c gpg.format=ssh -c user.signingkey=/secure/path/to/fleet-release-key tag -s vX.Y.Z VERIFIED_COMMIT -m "Release vX.Y.Z"
git -c gpg.format=ssh -c gpg.ssh.allowedSignersFile=.github/release-allowed-signers verify-tag vX.Y.Z
git push origin vX.Y.Z
gh workflow run release-unified.yml --repo openclaw/gitcrawl --ref main -f version=X.Y.Z -f dry_run=false
```

The live workflow verifies the tag signature and freezes that exact tagged commit. Retries reuse the same tag and target commit. Artifact signing, notarization, and Homebrew delivery use only GitHub Actions and repository or organization secrets; local artifact publishing remains disabled.

The release is complete only when the GitHub Release contains the full asset set, both native macOS verification jobs pass, and the Homebrew handoff is green.

## Local diagnostics

Local publishing is disabled. `make release`, `make release-artifacts`, and `scripts/package-release.sh` refuse and print the official workflow command. `make snapshot` remains credential-free, and `make verify-release VERSION=vX.Y.Z` rechecks already downloaded Darwin artifacts in `dist/` against `checksums.txt`, the stable Foundation designated requirement, architecture, embedded version, and Apple's online notarization ticket.
