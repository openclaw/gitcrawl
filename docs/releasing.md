---
title: Releasing
permalink: /releasing/
---

# Releasing Gitcrawl

`.github/workflows/release-unified.yml` is the only official release path. It calls `openclaw/release-workflows@v1` from protected `main`, creates and owns the immutable annotated version tag, preserves Gitcrawl's six thin platform archives and `checksums.txt`, signs and notarizes both macOS binaries as `org.openclaw.gitcrawl`, verifies the complete asset inventory independently on arm64 and Intel macOS, and waits for `openclaw/homebrew-tap` to update successfully.

The public compatibility contract remains:

- `gitcrawl_VERSION_{darwin,linux}_{amd64,arm64}.tar.gz`
- `gitcrawl_VERSION_windows_{amd64,arm64}.zip`
- `checksums.txt`
- `CHANGELOG.md`, `LICENSE`, `README.md`, and the Gitcrawl executable inside every platform archive
- OpenClaw Foundation Team ID `FWJYW4S8P8` and code identifier `org.openclaw.gitcrawl`

The shared pipeline also publishes verifier control assets (`ASSET-INVENTORY.json`, `SIGNING-MANIFEST.json`, and `RELEASE-NOTES.md`).

Official builds run on macOS. CI uses the Apple Silicon `xcode-27` runner, with macOS 13.0 explicit in both CI and the Darwin GoReleaser environment. A GoReleaser post-build hook checks `LC_BUILD_VERSION` with `otool` for every Darwin slice before signing or publishing, rejecting deployment targets newer than the documented macOS 13.0 minimum. Local release verification repeats this check and runs Intel binaries with Rosetta, asserting or installing Rosetta before execution. Linux CI snapshots skip this platform-specific check.

## Release

Prepare a dated changelog section, land it on protected `main`, and dispatch the workflow:

```sh
gh workflow run release-unified.yml --repo openclaw/gitcrawl -f version=X.Y.Z
```

The first run freezes the exact protected `main` head with an immutable annotated tag. Retries reuse that tag and target commit. Tagging, signing, notarization, and Homebrew delivery use only GitHub Actions and repository or organization secrets; maintainers need no local release credentials.

The release is complete only when the GitHub Release contains the full asset set, both native macOS verification jobs pass, and the Homebrew handoff is green.

## Local diagnostics

Local publishing is disabled. `make release`, `make release-artifacts`, and `scripts/package-release.sh` refuse and print the official workflow command. `make snapshot` remains credential-free, and `make verify-release VERSION=vX.Y.Z` rechecks already downloaded Darwin artifacts in `dist/` against `checksums.txt`, the stable Foundation designated requirement, architecture, embedded version, and Apple's online notarization ticket.
