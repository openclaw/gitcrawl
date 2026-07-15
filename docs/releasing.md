---
title: Releasing
permalink: /releasing/
---

# Releasing Gitcrawl

Official releases are assembled locally on an authorized maintainer Mac. GitHub Actions only validates credential-free snapshots and never publishes release artifacts.

1. Prepare and sign the release commit and tag on `main`, then ensure the checkout is clean and `HEAD` exactly matches that tag.
2. Configure the shared `release-mac-app` helper at runtime for the passwordless managed keychain. The identity must be `Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)`. Keep keychain and 1Password routing in the ignored `.mac-release.env` or another approved private environment, never in Git.
3. Build all release archives locally; Darwin binaries are signed as `org.openclaw.gitcrawl`, while Linux and Windows builds remain ordinary cross-compiles:

   ```bash
   make release-artifacts VERSION=vX.Y.Z
   scripts/verify-release.sh vX.Y.Z
   ```

4. Create a draft GitHub release from the signed tag. Attach the archives and `checksums.txt` from `dist/`, then manually run the `Release Assets` workflow for that tag. Its ephemeral token has `contents: write` only because GitHub otherwise hides drafts; the token is scoped to read-only asset downloads and is removed before verification. Publish only after both macOS verification jobs pass.
5. After publication, verify the release notes and assets, then dispatch the `openclaw/homebrew-tap` formula update for `gitcrawl` and verify the installed binary.

Local `go build`, `make build`, tests, and GoReleaser snapshots never require release credentials. `scripts/package-release.sh` fails closed unless it runs from the exact trusted signed tag with the Foundation identity supplied by `release-mac-app codesign-run`.
