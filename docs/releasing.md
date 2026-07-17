---
title: Releasing
permalink: /releasing/
---

# Releasing Gitcrawl

Official releases are assembled locally on an authorized maintainer Mac. GitHub Actions only validates credential-free snapshots and never publishes release artifacts.

1. Prepare and sign the release commit and tag on `main`, then ensure the checkout is clean and `HEAD` exactly matches that tag.
2. Configure the shared `release-mac-app` helper at runtime for the passwordless managed keychain. The identity must be `Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)`. Supply `NOTARYTOOL_KEYCHAIN_PROFILE` through the approved private runtime environment; never commit its value or add it to GitHub Actions. Keep keychain and 1Password routing in the ignored `.mac-release.env` or another approved private environment, never in Git.
3. Build all release archives locally; each thin Darwin binary is signed as `org.openclaw.gitcrawl` with the hardened runtime and a trusted timestamp, submitted to Apple in an ephemeral ZIP, and required to pass the notarized code requirement before packaging. Linux and Windows builds remain ordinary cross-compiles:

   ```bash
   make release-artifacts VERSION=vX.Y.Z
   scripts/verify-release.sh vX.Y.Z
   ```

4. Create a draft GitHub release from the signed tag. Attach the archives and `checksums.txt` from `dist/`, then manually run the `Release Assets` workflow for that tag. Its ephemeral token has `contents: write` only because GitHub otherwise hides drafts; the token is scoped to read-only asset downloads and is removed before verification. Publish only after both macOS verification jobs pass.
5. After publication, verify the release notes and assets, then dispatch the `openclaw/homebrew-tap` formula update for `gitcrawl` and verify the installed binary.

Local `go build`, `make build`, tests, and credential-free GoReleaser snapshots never require release credentials. Official snapshots and releases set `GITCRAWL_REQUIRE_CODESIGN=1`; the signing hook then requires `NOTARYTOOL_KEYCHAIN_PROFILE`, waits for an accepted notarization response, and replaces the GoReleaser output only after online notarization verification succeeds. `scripts/verify-release.sh` independently checks every extracted Darwin binary for the Foundation designated requirement and the notarized requirement. Raw executables cannot carry a stapled ticket, so verification requires network access to Apple.

`scripts/package-release.sh` fails closed unless it runs from the exact trusted signed tag with the Foundation identity supplied by `release-mac-app codesign-run` and a runtime notary profile.
