---
title: Cloud archives
nav_order: 14
permalink: /cloud-archives/
---

# Cloud archives
{: .no_toc }

Use a separately deployed Worker service as an authenticated read and snapshot-publication boundary for a Gitcrawl archive.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Responsibilities

Gitcrawl stores the Worker endpoint and archive identifier in its config and calls the remote service. The service itself is deployed separately from Gitcrawl.

Cloud mode does not replace the local or Git-backed portable-store workflows. A cloud-mode configuration sends supported read commands, including `status` and direct `search`, to the remote archive without creating a local SQLite database.

## Configure an archive

```bash
gitcrawl init \
  --remote URL \
  --archive gitcrawl/openclaw__openclaw
```

Bearer-authenticated remote endpoints must use HTTPS. Plain HTTP is accepted only for loopback development endpoints.

## Authenticate

```bash
gitcrawl remote login --endpoint URL --json
gitcrawl whoami --json
gitcrawl remote archives --json
gitcrawl remote status --json
```

`remote login` starts the service's GitHub OAuth flow, verifies organization and team membership server-side, and stores the returned signed bearer token in the operating-system keyring.

For non-browser bootstrap, name an environment variable that contains a GitHub token:

```bash
gitcrawl remote login \
  --endpoint URL \
  --github-token-env GITHUB_TOKEN \
  --json
```

The Worker verifies the GitHub token against the same organization and team policy. Gitcrawl stores only the remote session token.

## Read from cloud mode

After cloud initialization and login, supported read commands use the configured archive:

```bash
gitcrawl status --json
gitcrawl search openclaw/openclaw --query "manifest cache" --json
```

Commands that require a writable local database fail in cloud mode instead of silently creating one.

## Publish a local archive

Publication starts from a local SQLite archive:

```bash
gitcrawl cloud publish \
  --remote URL \
  --archive gitcrawl/openclaw__openclaw \
  --json
```

Before any upload or ingest, Gitcrawl checks the configured credential through the advertised `/v1/whoami` route and requires both publisher and reader roles. This also applies to stage-only publication.

The publisher freezes one local SQLite image and uses its SHA-256 digest as the snapshot identity. Repositories, threads, revisions, fingerprints, summaries, durable clusters, and pull request detail/file rows are exported from that same image. Gitcrawl negotiates the remote snapshot contract before changing R2, uploads a digest-scoped gzip bundle, and sends D1 data in row- and encoded-byte-bounded batches.

Gzip is lossless transport, not portable-store compaction. The cloud SQLite image retains complete canonical issue and pull request bodies, comments, revisions, review comments, and pull request patch text.

## Staging and cutover

The remote must advertise `gitcrawl.snapshot.staging.v1`. Pass `--stage-only` to upload and validate an immutable snapshot without changing the archive served to unpinned readers:

```bash
gitcrawl cloud publish \
  --remote URL \
  --archive gitcrawl/openclaw__openclaw \
  --stage-only \
  --json
```

A later publish verifies the candidate through the publisher-only status projection. It skips repeated ingest only when the digest, source sync, schema, resolved publication profile, generation timestamp, and coverage match.

Cutover requires reader-authenticated `GET /sqlite`. Gitcrawl validates the cutover acknowledgement, polls the scoped reader projection until its digest, profile, generation, and dataset coverage match, rechecks the publisher metadata, downloads the bound SQLite image, and verifies its hash before reporting success. Without `--stage-only`, a successful publish moves unpinned reads to the complete snapshot.

Incomplete local enrichment fails before remote mutation. `--allow-incomplete` is the explicit override. `--observation-order` publishes durable fetch ordering only after the remote operator fence is enabled.

## Privacy and retention

Digest-scoped bundles can contain private issue and pull request text. Bundle metadata declares both message-body and source-code sensitivity because patch text is retained.

Publication excludes raw API payloads, blob-backed payloads, local run diagnostics, source-code indexes, and machine-local paths. These are not part of the shared archive contract.

Gitcrawl intentionally has no remote deletion command. Operators should enable publication only when the remote deployment has bounded lifecycle rules for failed, superseded, and uncut staged bundles. `--stage-only` does not move retention responsibility back to the client.

## Command summary

| Command | Purpose |
| --- | --- |
| `gitcrawl init --remote URL --archive id` | Configure cloud mode |
| `gitcrawl remote login` | Authenticate and store a remote session token |
| `gitcrawl whoami` | Print the current remote identity |
| `gitcrawl remote archives` | List archives visible to that identity |
| `gitcrawl remote status` | Inspect the configured archive |
| `gitcrawl cloud publish` | Publish a local snapshot to the Worker remote |
