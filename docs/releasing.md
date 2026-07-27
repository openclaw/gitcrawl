---
title: Releasing
permalink: /releasing/
---

# Releasing Gitcrawl

`.github/workflows/release-unified.yml` is the only official release path. It calls `openclaw/release-workflows@v1` from protected `main`, requires an existing SSH-signed version tag, preserves Gitcrawl's six thin platform archives and `checksums.txt`, signs and notarizes both macOS binaries as `org.openclaw.gitcrawl`, verifies the complete asset inventory independently on arm64 and Intel macOS, and waits for `openclaw/homebrew-tap` to update successfully.

The public compatibility contract remains:

- `gitcrawl_VERSION_{darwin,linux}_{amd64,arm64}.tar.gz`
- `gitcrawl_VERSION_windows_{amd64,arm64}.zip`
- `checksums.txt`
- `CHANGELOG.md`, `LICENSE`, `README.md`, and the Gitcrawl executable inside every platform archive
- OpenClaw Foundation Team ID `FWJYW4S8P8` and code identifier `org.openclaw.gitcrawl`

The shared pipeline also publishes verifier control assets (`ASSET-INVENTORY.json`, `SIGNING-MANIFEST.json`, and `RELEASE-NOTES.md`).

## Release

Prepare a dated changelog section and land it on protected `main`. The `user.signingkey` SSH key must be listed for your principal in `.github/release-allowed-signers`. Create the annotated signed tag, verify it explicitly against the repository allowlist, push it, and dispatch the workflow:

```sh
git -c gpg.format=ssh tag -s vX.Y.Z -m "Release X.Y.Z"
git -c gpg.format=ssh -c gpg.ssh.allowedSignersFile=.github/release-allowed-signers tag -v vX.Y.Z
git push origin vX.Y.Z
gh workflow run release-unified.yml --repo openclaw/gitcrawl -f version=X.Y.Z
```

The release is complete only when the GitHub Release contains the full asset set, both native macOS verification jobs pass, and the Homebrew handoff is green.

## Local diagnostics

Local publishing is disabled. `make release`, `make release-artifacts`, and `scripts/package-release.sh` refuse and print the official workflow command. `make snapshot` remains credential-free, and `make verify-release VERSION=vX.Y.Z` rechecks already downloaded Darwin artifacts in `dist/` against `checksums.txt`, the stable Foundation designated requirement, architecture, embedded version, and Apple's online notarization ticket.
