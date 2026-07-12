---
title: Installation
nav_order: 2
permalink: /installation/
---

# Installation
{: .no_toc }

1. TOC
{:toc}

## Requirements

- **Go 1.26+** if building from source
- **Git** for cloning the repository (and for portable stores)
- **A GitHub token** for any command that talks to GitHub (`sync`, `refresh`)
- **An OpenAI API key** for `summarize`, `embed`, and the embedding stage of `refresh`
- **`gh` CLI** optional, used by gitcrawl only as a token source via `gh auth token`

gitcrawl runs on macOS and Linux. Windows is not actively tested.

## Install from Homebrew

```bash
brew install openclaw/tap/gitcrawl
```

Homebrew installs the `gitcrawl` binary. GitHub CLI shim behavior moved to Octopool.

## Install from a GitHub release

Each GitHub release includes archives for `darwin_amd64`, `darwin_arm64`, `linux_amd64`, and `linux_arm64` built via [GoReleaser](https://github.com/openclaw/gitcrawl/blob/main/.goreleaser.yaml).
Official macOS archives are signed as `org.openclaw.gitcrawl` by `Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)`; local source and snapshot builds do not require signing credentials.

```bash
# Replace VERSION and PLATFORM with the values you want.
VERSION=v0.1.2
PLATFORM=darwin_arm64
mkdir -p "$HOME/bin"
curl -L "https://github.com/openclaw/gitcrawl/releases/download/${VERSION}/gitcrawl_${VERSION#v}_${PLATFORM}.tar.gz" \
  | tar -xz -C "$HOME/bin" gitcrawl

gitcrawl --version
```

Browse the [releases page](https://github.com/openclaw/gitcrawl/releases) for the latest tag and the full asset list. Use a directory that is already on your `PATH`; `~/bin` and `~/.local/bin` avoid needing elevated permissions.

## Check for updates

```bash
gitcrawl check-update
gitcrawl check-update --json
```

Interactive terminal runs perform a cached daily release check and print a
stderr notice when a newer OpenClaw release is available. Scripted, JSON, CI,
and non-TTY runs skip the passive notice. Set `GITCRAWL_NO_UPDATE_CHECK=1` or
`CRAWLKIT_NO_UPDATE_CHECK=1` to disable it.

## Install from source

```bash
git clone https://github.com/openclaw/gitcrawl.git
cd gitcrawl
go build \
  -ldflags "-X github.com/openclaw/gitcrawl/internal/cli.version=$(git describe --tags --always --dirty)" \
  -o bin/gitcrawl ./cmd/gitcrawl

./bin/gitcrawl --version
```

Symlink or copy `bin/gitcrawl` somewhere on your `PATH` (`~/bin`, `/usr/local/bin`, `~/.local/bin`).

## GitHub CLI shim migration

`gitcrawl gh` moved to Octopool:

```bash
octopool login
octopool gh api repos/openclaw/openclaw/pulls/123
```

See [the gh shim guide](/gh-shim/) for migration details.

## Verify the install

```bash
gitcrawl init           # creates config and data under platform default paths
gitcrawl doctor         # confirms config, database, and credential discovery
gitcrawl doctor --json  # same, machine-readable
```

`doctor` reports whether `GITHUB_TOKEN` and `OPENAI_API_KEY` are present, where they came from, the version, repository count, and the last sync timestamp. If anything is missing, the message tells you which env var or config field to set.

## Updating

- **Release archives:** download the new tarball and replace the binary.
- **Source builds:** `git pull && go build ...` — the version string comes from `git describe`.
- **Configuration is forward-compatible.** Existing `config.toml` and `gitcrawl.db` files are reused across versions; no migration step is needed for normal point releases. Existing `~/.config/gitcrawl/config.toml` installs continue to load from that path until the new platform config path exists.
