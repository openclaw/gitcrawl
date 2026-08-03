---
title: Docker
nav_order: 17
permalink: /docker/
---

# Docker

Build a local Gitcrawl image from the repository when a container is more convenient than a native binary.

## Build

```bash
docker build -t gitcrawl .
```

The multi-stage build compiles a static Linux binary with the Go version pinned by the Dockerfile, then copies it into an unprivileged Alpine runtime image.

## Run

Mount `/data` to keep configuration, SQLite data, caches, vectors, and logs between containers:

```bash
docker run --rm \
  -e GITHUB_TOKEN \
  -v "$PWD/.gitcrawl:/data" \
  gitcrawl sync owner/repo

docker run --rm \
  -v "$PWD/.gitcrawl:/data" \
  gitcrawl search issues "hot loop" -R owner/repo
```

Pass `OPENAI_API_KEY` with `-e` when the command generates summaries or embeddings. Gitcrawl runs as the non-root `gitcrawl` user with UID 10001, so the mounted directory must be writable by that user.

## Storage layout

The image maps the XDG directories into `/data`:

| Path | Contents |
| --- | --- |
| `/data/config` | `config.toml` |
| `/data/data` | SQLite database and vectors |
| `/data/cache` | Runtime caches |
| `/data/state` | Logs and state files |

The repository publishes native release archives and a Homebrew formula. The Dockerfile is a source-build path; this project does not document a prebuilt container registry image.
