package cli

import (
	"fmt"
)

func (a *App) printUsage() {
	fmt.Fprint(a.Stdout, usageText)
}

func (a *App) printCommandUsage(command string) error {
	if text, ok := commandUsageTexts[command]; ok {
		fmt.Fprint(a.Stdout, text)
		return nil
	}
	switch command {
	case "cluster-explain":
		fmt.Fprint(a.Stdout, commandUsageTexts["cluster-detail"])
		return nil
	case "portable":
		fmt.Fprint(a.Stdout, portableUsageText)
		return nil
	case "tui":
		fmt.Fprint(a.Stdout, tuiUsageText)
		return nil
	default:
		return usageErr(fmt.Errorf("unknown help topic %q", command))
	}
}

const usageText = `gitcrawl mirrors GitHub issues and pull requests into local SQLite for maintainer triage.

Usage:
  gitcrawl [global flags] <command> [command flags]
  gitcrawl help <command>

Global flags:
  --github-token-command <path>  absolute GitHub credential executable (Unix only)
  --config <path>       config path
  --format <mode>      output format: text|json|log
  --json               write JSON output
  --version            print version

Core commands:
  metadata             print crawlkit control metadata
  check-update         check for a newer gitcrawl release
  status               print fast read-only archive status
  remote status        print remote archive status
  remote archives      list remote archives visible to the current identity
  remote login         authenticate with GitHub org access for a remote archive
  whoami               print remote archive identity
  cloud publish        publish the local archive to a Worker remote
  init                 create config, optionally from a portable store
  doctor               check config, token, and database readiness
  sync                 sync GitHub issue and pull request metadata
  sync-failures        list failed sync hydration attempts
  coverage             report local archive PR-detail completeness
  fill-pr-details      hydrate locally missing pull request detail rows
  refresh              run sync, enrichment, embedding, and clustering pipeline
  summarize            generate key summaries for current thread revisions
  embed                generate OpenAI embeddings for local thread documents
  threads              list local issue and pull request rows
  capture              export a stable code-free conversation snapshot
  code index           index tracked text files from a local Git checkout
  cluster              build durable clusters from local thread vectors
  close-thread         locally hide one issue or pull request row
  reopen-thread        clear a local hide for one issue or pull request row
  close-cluster        locally hide one durable cluster
  reopen-cluster       clear a local hide for one durable cluster
  exclude-cluster-member
                       locally remove one row from a durable cluster
  include-cluster-member
                       restore one row to a durable cluster
  set-cluster-canonical
                       set the canonical row for a durable cluster
  clusters             list latest run cluster summaries, with durable fallback
  clusters-report      write a Markdown report for the top clusters
  durable-clusters     list durable cluster groups
  cluster-detail       dump one latest run cluster, with durable fallback
  cluster-explain      alias for cluster-detail
  neighbors            list vector-nearest local issue and pull request rows
  search               search local thread and source documents; also supports search issues|prs gh syntax
  gh                   moved to Octopool; prints migration note
  portable prune       prune volatile payloads from a portable store
  portable export      create an immutable derived portable generation
  portable refresh     safely refresh a configured portable subscriber
  tui [owner/repo]     browse clusters in the terminal UI; repo is inferred when omitted

No API server is provided. There is intentionally no serve command.
`

var commandUsageTexts = map[string]string{
	"metadata": `gitcrawl metadata prints crawlkit control metadata.

Usage:
  gitcrawl metadata [--json]
`,
	"status": `gitcrawl status prints fast read-only archive status.

Usage:
  gitcrawl status [--json]
`,
	"remote": `gitcrawl remote queries a configured Cloudflare-backed archive.

Usage:
  gitcrawl remote status [--json]
  gitcrawl remote archives [--json]
  gitcrawl remote login --endpoint URL [--json]
  gitcrawl remote login --endpoint URL --github-token-env GITHUB_TOKEN [--json]
  gitcrawl remote whoami [--json]
`,
	"cloud": `gitcrawl cloud manages Worker-backed remote archives.

Usage:
  gitcrawl cloud publish --remote URL --archive id [--allow-incomplete | --admission-policy=archive-v1] [--observation-order] [--stage-only] [--json]
`,
	"whoami": `gitcrawl whoami prints the configured remote archive identity.

Usage:
  gitcrawl whoami [--json]
`,
	"check-update": `gitcrawl check-update checks GitHub Releases for a newer gitcrawl build.

Usage:
  gitcrawl check-update [--json] [--force]
`,
	"init": `gitcrawl init creates a local, portable, or cloud archive config.

Usage:
  gitcrawl init [--db path | --runtime-dir path | --portable-store URL | --remote URL --archive id] [--json]
`,
	"configure": `gitcrawl configure updates model fields in the config.

Usage:
  gitcrawl configure [--summary-model name] [--embed-model name] [--embed-base-url url] [--embedding-basis title_original] [--json]
`,
	"doctor": `gitcrawl doctor checks config, token, and database readiness.

Usage:
  gitcrawl doctor [--json] [--locks]
`,
	"sync": `gitcrawl sync mirrors GitHub issue and pull request metadata.

Usage:
  gitcrawl sync owner/repo [--state open|closed|all] [--numbers refs] [--with pr-metadata|pr-details] [--include-pr-details] [--json]

pr-metadata fetches only the pull request object; pr-details also hydrates files,
commits, checks, workflows, and review threads. Comments are selected separately.
`,
	"sync-failures": `gitcrawl sync-failures lists failed sync hydration attempts.

Usage:
  gitcrawl sync-failures owner/repo [--include-resolved] [--limit N] [--json]
`,
	"coverage": `gitcrawl coverage reports local archive completeness by repository.

Usage:
  gitcrawl coverage [owner/repo | --repos owner/a,owner/b] [--min-missing-pr-details N] [--json]
`,
	"fill-pr-details": `gitcrawl fill-pr-details hydrates locally missing pull request detail rows.

Usage:
  gitcrawl fill-pr-details owner/repo [--limit N] [--order newest-first|oldest-first|open-first] [--batch-size N] [--reserve-rate-limit N] [--include-comments] [--json-progress] [--json]

Before each GitHub request issued by this command, Gitcrawl refreshes GitHub's
/rate_limit view of the shared token and stops if that observed quota would
cross the reserve. The default floor is 1500 remaining requests, providing
headroom for concurrent consumers. This is best-effort: another process can
spend quota between the probe and request. Pass --reserve-rate-limit N to
choose a different floor.
`,
	"refresh": `gitcrawl refresh runs sync, enrichment, embedding, and clustering.

Usage:
  gitcrawl refresh owner/repo [--state open|closed|all] [--with pr-metadata|pr-details] [--include-pr-details] [--no-sync] [--no-embed] [--no-cluster] [--strict-vectors] [--json]
`,
	"summarize": `gitcrawl summarize generates key summaries for current thread revisions.

Usage:
  gitcrawl summarize owner/repo [--number ref] [--limit N] [--force] [--include-closed] [--json]
`,
	"embed": `gitcrawl embed generates OpenAI embeddings for local thread documents.

Usage:
  gitcrawl embed owner/repo [--number ref] [--limit N] [--force] [--include-closed] [--json]
`,
	"threads": `gitcrawl threads lists local issue and pull request rows.

Usage:
  gitcrawl threads owner/repo [--include-closed] [--numbers refs] [--limit N] [--json]
`,
	"capture": `gitcrawl capture exports a stable code-free conversation snapshot.

Managed --github-token-command credentials are not supported by offline capture.

Usage:
  gitcrawl capture owner/repo [--schema gitcrawl.capture.v1] [--since RFC3339] [--output path] [--json]
`,
	"search": `gitcrawl search queries local thread documents, or accepts gh-shaped issue and PR search.

Usage:
  gitcrawl search owner/repo --query text [--scope threads|code|all] [--mode keyword|semantic|hybrid] [--limit N] [--json]
  gitcrawl search issues|prs <query> -R owner/repo [--state open|closed|all] [--json fields] [--limit N] [--sync-if-stale duration]
`,
	"code": `gitcrawl code indexes source from a local Git checkout.

Usage:
  gitcrawl code index owner/repo [--path DIR] [--max-file-bytes N] [--max-total-bytes N] [--max-files N] [--json]
`,
	"cluster": `gitcrawl cluster builds durable clusters from local thread vectors.

Usage:
  gitcrawl cluster owner/repo [--threshold N] [--min-size N] [--max-cluster-size N] [--k N] [--cross-kind-threshold N] [--limit N] [--model name] [--basis semantic|references|hybrid] [--include-closed] [--strict-vectors] [--json]
`,
	"clusters": `gitcrawl clusters lists latest display clusters with durable fallback.

Usage:
  gitcrawl clusters owner/repo [--sort size|recent|oldest] [--min-size N] [--limit N] [--hide-closed] [--json]
`,
	"clusters-report": `gitcrawl clusters-report writes a Markdown report for top display clusters.

Usage:
  gitcrawl clusters-report owner/repo [--sort size|recent|oldest] [--min-size N] [--limit N] [--member-limit N] [--body-chars N] [--hide-closed] [--json]
`,
	"durable-clusters": `gitcrawl durable-clusters lists governed durable cluster groups.

Usage:
  gitcrawl durable-clusters owner/repo [--include-closed] [--sort size|recent|oldest] [--min-size N] [--limit N] [--json]
`,
	"cluster-detail": `gitcrawl cluster-detail dumps one cluster and its member rows.

Usage:
  gitcrawl cluster-detail owner/repo --id N [--source auto|run|durable] [--member-limit N] [--body-chars N] [--hide-closed] [--json]
`,
	"neighbors": `gitcrawl neighbors lists vector-nearest local issue and pull request rows.

Usage:
  gitcrawl neighbors owner/repo --number ref [--limit N] [--include-closed] [--json]
`,
	"runs": `gitcrawl runs lists local pipeline run history.

Usage:
  gitcrawl runs owner/repo [--kind sync|summary|embedding|cluster] [--limit N] [--json]
`,
	"close-thread": `gitcrawl close-thread locally hides one issue or pull request row.

Usage:
  gitcrawl close-thread owner/repo --number ref [--reason text] [--json]
`,
	"reopen-thread": `gitcrawl reopen-thread clears a local thread hide.

Usage:
  gitcrawl reopen-thread owner/repo --number ref [--json]
`,
	"close-cluster": `gitcrawl close-cluster locally hides one durable cluster.

Usage:
  gitcrawl close-cluster owner/repo --id N [--reason text] [--json]
`,
	"reopen-cluster": `gitcrawl reopen-cluster clears a local cluster hide.

Usage:
  gitcrawl reopen-cluster owner/repo --id N [--json]
`,
	"exclude-cluster-member": `gitcrawl exclude-cluster-member locally removes one row from a durable cluster.

Usage:
  gitcrawl exclude-cluster-member owner/repo --id N --number ref [--reason text] [--json]
`,
	"include-cluster-member": `gitcrawl include-cluster-member restores one row to a durable cluster.

Usage:
  gitcrawl include-cluster-member owner/repo --id N --number ref [--json]
`,
	"set-cluster-canonical": `gitcrawl set-cluster-canonical sets the canonical row for a durable cluster.

Usage:
  gitcrawl set-cluster-canonical owner/repo --id N --number ref [--reason text] [--json]
`,
	"gh": `gitcrawl gh moved to octopool.

Usage:
  octopool login
  octopool gh <gh command>
`,
}

const tuiUsageText = `gitcrawl tui opens the local terminal cluster browser.

Usage:
  gitcrawl tui [owner/repo] [--limit N] [--min-size N] [--sort recent|oldest|size] [--layout focus|columns|right-stack] [--hide-closed]

If owner/repo is omitted, gitcrawl uses the most recently updated repository in the local database.
The TUI starts with ghcrawl-style cluster display defaults: --min-size 5, --sort size, --layout columns, and closed historical clusters visible. Pass --min-size 1 for singleton clusters or --hide-closed to focus open-only.
Mouse is supported: click rows, wheel panes, right-click for actions, and use the menu for copy/sort/filter/jump/member triage controls.
Press a to open the same action menu from the keyboard.
Press # to jump directly to an issue or PR number.
Press p to switch between repositories already present in the local store.
Press n to load neighbors for the selected issue or PR.
Enter from the members pane also loads neighbors before opening detail.
The TUI quietly refreshes from the local store every 15 seconds and leaves the current status alone when nothing changed.
`

const portableUsageText = `gitcrawl portable manages local portable-store snapshots.

Usage:
  gitcrawl portable refresh --expected-remote URL [--store-dir PATH] [--portable-db PATH] [--branch main] [--git PATH] [--timeout 2m] [--min-free-bytes N] [--max-growth-bytes N] [--json]
  gitcrawl portable prune [--body-chars N] [--no-vacuum] [--include-sync-failures] [--no-publish] [--json]
  gitcrawl portable export --profile current-state-v1 --output-dir PATH [--repository owner/repo] [--database-name NAME] [--public-path PATH] [--body-chars N] [--max-bytes N] [--compression gzip] [--max-archive-bytes N] [--json]

Subcommands:
  refresh             validate and fast-forward a clean configured subscriber
  prune               prune volatile payloads from the configured portable store
  export              create a validated portable artifact in a new directory

The sync failure ledger is excluded by default. --include-sync-failures keeps
the ledger but replaces every error message with a redaction marker. A present
or pending ledger forces a secure database rewrite even with --no-vacuum.
For a portable checkout, prune publishes the database and manifest back into
the checkout by default. --no-publish leaves them only in the runtime mirror.
Export never changes the active database or publishes the artifact. It creates
a complete database and manifest generation at a previously nonexistent path.
With --compression gzip, it commits the gzip archive and manifest without the
uncompressed database; --max-archive-bytes applies to that published archive.
--repository semantically restricts the disposable snapshot to one owner/repo.
Refresh never regenerates config or invokes repair. It requires a matching
origin, clean checkout and manifest-backed artifact. --git requires an absolute
path (or set GITCRAWL_PORTABLE_GIT). Timeout defaults to 2m; free-space reserve
and measured growth budget each default to 2147483648 bytes. Refusals exit
nonzero; JSON distinguishes updated, no-op, refused and partial results.
`
