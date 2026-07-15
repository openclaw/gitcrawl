#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

import { css, faviconSvg, js, themeInitJs, themeToggleHtml } from "./docs-site-assets.mjs";

const root = process.cwd();
const docsDir = path.join(root, "docs");
const outDir = path.join(root, "dist", "docs-site");
const repoBase = "https://github.com/openclaw/gitcrawl";
const repoEditBase = `${repoBase}/edit/main/docs`;
const cname = readCname();
const siteBase = cname ? `https://${cname}` : "";

const sections = [
  ["Start", ["index.md", "installation.md", "quickstart.md", "concepts.md"]],
  ["Configure", ["configuration.md", "sync.md", "refresh-and-embed.md"]],
  ["Use", ["search.md", "clustering.md", "governance.md", "tui.md", "gh-shim.md"]],
  ["Operate", ["portable-stores.md", "maintainer-archive.md", "automation.md", "releasing.md"]],
  ["Reference", ["commands.md", "reference.md"]],
];

fs.rmSync(outDir, { recursive: true, force: true });
fs.mkdirSync(outDir, { recursive: true });

const pages = allMarkdown(docsDir).map((file) => {
  const rel = path.relative(docsDir, file).replaceAll(path.sep, "/");
  const raw = fs.readFileSync(file, "utf8");
  const { frontmatter, body } = parseFrontmatter(raw);
  const cleaned = cleanKramdown(body);
  const title = frontmatter.title || firstHeading(cleaned) || titleize(path.basename(rel, ".md"));
  return { file, rel, title, outRel: outPath(rel, frontmatter), markdown: cleaned, frontmatter };
});

const pageMap = new Map(pages.map((page) => [page.rel, page]));
const permalinkMap = new Map();
for (const page of pages) {
  if (page.frontmatter.permalink) {
    permalinkMap.set(normalizePermalink(page.frontmatter.permalink), page);
  }
}

const nav = sections
  .map(([name, rels]) => ({
    name,
    pages: rels.map((rel) => pageMap.get(rel)).filter(Boolean),
  }))
  .filter((section) => section.pages.length);

const sectionByRel = new Map();
for (const section of nav) for (const page of section.pages) sectionByRel.set(page.rel, section.name);
const orderedPages = nav.flatMap((s) => s.pages);

for (const page of pages) {
  const html = markdownToHtml(page.markdown, page.rel);
  const toc = tocFromHtml(html);
  const idx = orderedPages.findIndex((p) => p.rel === page.rel);
  const prev = idx > 0 ? orderedPages[idx - 1] : null;
  const next = idx >= 0 && idx < orderedPages.length - 1 ? orderedPages[idx + 1] : null;
  const sectionName = sectionByRel.get(page.rel) || "Docs";
  const pageOut = path.join(outDir, page.outRel);
  fs.mkdirSync(path.dirname(pageOut), { recursive: true });
  fs.writeFileSync(pageOut, layout({ page, html, toc, prev, next, sectionName }), "utf8");
}

fs.writeFileSync(path.join(outDir, "favicon.svg"), faviconSvg(), "utf8");
copyStaticAsset("social-card.svg");
copyStaticAsset("social-card.png");
fs.writeFileSync(path.join(outDir, ".nojekyll"), "", "utf8");
if (cname) fs.writeFileSync(path.join(outDir, "CNAME"), cname, "utf8");
validateLinks(outDir);
fs.writeFileSync(path.join(outDir, "llms.txt"), llmsTxt(), "utf8");
console.log(`built docs site: ${path.relative(root, outDir)}`);

function llmsTxt() {
  const origin = docsOrigin();
  const source = docsSourceUrl();
  const name = typeof productName !== "undefined" ? productName : path.basename(root);
  const description = typeof productDescription !== "undefined" ? productDescription : `${name} documentation index.`;
  const install = docsInstallHint();
  const docPages = docsLlmsPages().map((page) => `- ${page.title}: ${pageUrl(origin, page.outRel)}`);
  const lines = [
    `# ${name}`,
    "",
    description,
    "",
    "Canonical documentation:",
    ...docPages,
  ];
  if (install) {
    lines.push("", "Install:", `- ${install}`);
  }
  if (source) {
    lines.push("", `Source: ${source}`);
  }
  lines.push("", "Guidance for agents:", "- Prefer the canonical documentation URLs above over README excerpts or package metadata.", "- Fetch only the pages needed for the current task; this is an index, not a full-site corpus.");
  return `${lines.join("\n")}\n`;
}

function docsLlmsPages() {
  const seen = new Set();
  const ordered = typeof orderedPages !== "undefined" ? orderedPages : [];
  return [...ordered, ...pages].filter((page) => page.outRel && !seen.has(page.outRel) && seen.add(page.outRel));
}

function docsOrigin() {
  const value =
    (typeof siteBase !== "undefined" && siteBase) ||
    (typeof siteUrl !== "undefined" && siteUrl) ||
    (typeof customDomain !== "undefined" && customDomain ? `https://${customDomain}` : "");
  return value.replace(/\/$/, "");
}

function docsSourceUrl() {
  if (typeof repoBase !== "undefined") return repoBase;
  if (typeof repoUrl !== "undefined") return repoUrl;
  if (typeof repoEditBase !== "undefined") return repoEditBase.replace(/\/edit\/main\/docs\/?$/, "");
  return "";
}

function docsInstallHint() {
  if (typeof installCommand !== "undefined") return installCommand;
  if (typeof installLine !== "undefined") return installLine;
  if (typeof installCmd !== "undefined") return installCmd;
  if (typeof installSnippet !== "undefined") return installSnippet;
  if (typeof brewInstall !== "undefined") return brewInstall;
  return "";
}

function pageUrl(origin, outRel) {
  const normalized = outRel === "index.html" ? "" : outRel.replace(/(?:^|\/)index\.html$/, (match) => match === "index.html" ? "" : "/");
  if (!origin) return normalized || "index.html";
  return normalized ? `${origin}/${normalized}` : `${origin}/`;
}

function readCname() {
  for (const candidate of [path.join(docsDir, "CNAME"), path.join(root, "CNAME")]) {
    if (fs.existsSync(candidate)) return fs.readFileSync(candidate, "utf8").trim();
  }
  return "";
}

function copyStaticAsset(name) {
  const source = path.join(docsDir, name);
  if (fs.existsSync(source)) fs.copyFileSync(source, path.join(outDir, name));
}

function parseFrontmatter(raw) {
  const match = raw.match(/^---\n([\s\S]*?)\n---\n?/);
  if (!match) return { frontmatter: {}, body: raw };
  const fm = {};
  for (const line of match[1].split("\n")) {
    const m = line.match(/^([A-Za-z0-9_-]+):\s*(.*?)\s*$/);
    if (!m) continue;
    let value = m[2];
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    fm[m[1]] = value;
  }
  return { frontmatter: fm, body: raw.slice(match[0].length) };
}

function cleanKramdown(body) {
  const lines = body.replace(/\r\n/g, "\n").split("\n");
  const out = [];
  let kramdownDivDepth = 0;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (/^\s*\{:\s*[^}]*\}\s*$/.test(line)) continue;
    if (/^\s*1\.\s+TOC\s*$/.test(line) && /^\s*\{:toc\}\s*$/.test(lines[i + 1] || "")) {
      i += 1;
      continue;
    }
    if (/^\s*<div\b[^>]*\bmarkdown\s*=\s*"1"[^>]*>\s*$/.test(line)) {
      kramdownDivDepth++;
      continue;
    }
    if (kramdownDivDepth > 0 && /^\s*<\/div>\s*$/.test(line)) {
      kramdownDivDepth--;
      continue;
    }
    out.push(line.replace(/\s*\{:\s*[^}]*\}\s*$/, ""));
  }
  return out.join("\n");
}

function normalizePermalink(value) {
  let v = value.trim();
  if (!v) return "/";
  if (!v.startsWith("/")) v = `/${v}`;
  if (v.length > 1 && v.endsWith("/")) v = v.slice(0, -1);
  return v;
}

function allMarkdown(dir) {
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .flatMap((entry) => {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) return allMarkdown(full);
      return entry.name.endsWith(".md") ? [full] : [];
    })
    .sort();
}

function outPath(rel, frontmatter = {}) {
  if (frontmatter.permalink) {
    const permalink = normalizePermalink(frontmatter.permalink);
    if (permalink === "/") return "index.html";
    return `${permalink.slice(1)}/index.html`;
  }
  if (rel === "index.md") return "index.html";
  if (rel === "README.md") return "index.html";
  if (rel.endsWith("/README.md")) return rel.replace(/README\.md$/, "index.html");
  return rel.replace(/\.md$/, ".html");
}

function firstHeading(markdown) {
  return markdown.match(/^#\s+(.+)$/m)?.[1]?.trim();
}

function titleize(input) {
  return input.replaceAll("-", " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function markdownToHtml(markdown, currentRel) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const html = [];
  let paragraph = [];
  let list = null;
  let fence = null;
  let blockquote = [];

  const flushParagraph = () => {
    if (!paragraph.length) return;
    const text = paragraph.join(" ");
    const className = currentRel === "index.md" && /^\[Quickstart\]\([^)]*\)\s+\[View on GitHub\]\(/.test(text) ? ' class="home-actions"' : "";
    html.push(`<p${className}>${inline(text, currentRel)}</p>`);
    paragraph = [];
  };
  const closeList = () => {
    if (!list) return;
    html.push(`</${list}>`);
    list = null;
  };
  const flushBlockquote = () => {
    if (!blockquote.length) return;
    const inner = markdownToHtml(blockquote.join("\n"), currentRel);
    html.push(`<blockquote>${inner}</blockquote>`);
    blockquote = [];
  };
  const splitRow = (line) => {
    let trimmed = line.trim();
    if (trimmed.startsWith("|")) trimmed = trimmed.slice(1);
    if (trimmed.endsWith("|") && !trimmed.endsWith("\\|")) trimmed = trimmed.slice(0, -1);
    const cells = [];
    let current = "";
    for (let idx = 0; idx < trimmed.length; idx++) {
      const char = trimmed[idx];
      if (char === "\\" && trimmed[idx + 1] === "|") {
        current += "\\|";
        idx += 1;
        continue;
      }
      if (char === "|") {
        cells.push(current.trim().replace(/\\\|/g, "|"));
        current = "";
        continue;
      }
      current += char;
    }
    cells.push(current.trim().replace(/\\\|/g, "|"));
    return cells;
  };
  const isDivider = (line) => /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)+\|?\s*$/.test(line);

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const fenceMatch = line.match(/^```(\w+)?\s*$/);
    if (fenceMatch) {
      flushParagraph();
      closeList();
      flushBlockquote();
      if (fence) {
        html.push(`<pre><code class="language-${escapeAttr(fence.lang)}">${highlightCode(fence.lines.join("\n"), fence.lang)}</code></pre>`);
        fence = null;
      } else {
        fence = { lang: fenceMatch[1] || "text", lines: [] };
      }
      continue;
    }
    if (fence) {
      fence.lines.push(line);
      continue;
    }
    if (/^>\s?/.test(line)) {
      flushParagraph();
      closeList();
      blockquote.push(line.replace(/^>\s?/, ""));
      continue;
    }
    flushBlockquote();
    if (!line.trim()) {
      flushParagraph();
      closeList();
      continue;
    }
    if (/^\s*---+\s*$/.test(line)) {
      flushParagraph();
      closeList();
      html.push("<hr>");
      continue;
    }
    const heading = line.match(/^(#{1,4})\s+(.+)$/);
    if (heading) {
      flushParagraph();
      closeList();
      const level = heading[1].length;
      const text = heading[2].trim();
      const id = slug(text);
      const inner = inline(text, currentRel);
      if (level === 1) {
        html.push(`<h1 id="${id}">${inner}</h1>`);
      } else {
        html.push(`<h${level} id="${id}"><a class="anchor" href="#${id}" aria-label="Anchor link">#</a>${inner}</h${level}>`);
      }
      continue;
    }
    if (line.trimStart().startsWith("|") && line.includes("|", line.indexOf("|") + 1) && isDivider(lines[i + 1] || "")) {
      flushParagraph();
      closeList();
      const header = splitRow(line);
      const aligns = splitRow(lines[i + 1]).map((cell) => {
        const left = cell.startsWith(":");
        const right = cell.endsWith(":");
        return right && left ? "center" : right ? "right" : left ? "left" : "";
      });
      i += 1;
      const rows = [];
      while (i + 1 < lines.length && lines[i + 1].trimStart().startsWith("|")) {
        i += 1;
        rows.push(splitRow(lines[i]));
      }
      const th = header.map((c, idx) => `<th${aligns[idx] ? ` style="text-align:${aligns[idx]}"` : ""}>${inline(c, currentRel)}</th>`).join("");
      const tb = rows.map((r) => `<tr>${r.map((c, idx) => `<td${aligns[idx] ? ` style="text-align:${aligns[idx]}"` : ""}>${inline(c, currentRel)}</td>`).join("")}</tr>`).join("");
      html.push(`<table><thead><tr>${th}</tr></thead><tbody>${tb}</tbody></table>`);
      continue;
    }
    const bullet = line.match(/^\s*-\s+(.+)$/);
    const numbered = line.match(/^\s*\d+\.\s+(.+)$/);
    if (bullet || numbered) {
      flushParagraph();
      const tag = bullet ? "ul" : "ol";
      if (list && list !== tag) closeList();
      if (!list) {
        list = tag;
        html.push(`<${tag}>`);
      }
      html.push(`<li>${inline((bullet || numbered)[1], currentRel)}</li>`);
      continue;
    }
    paragraph.push(line.trim());
  }
  flushParagraph();
  closeList();
  flushBlockquote();
  return html.join("\n");
}

function highlightCode(code, lang) {
  const normalized = String(lang || "text").toLowerCase();
  if (["bash", "sh", "shell", "zsh"].includes(normalized)) return highlightBash(code);
  if (normalized === "json") return highlightJSON(code);
  if (normalized === "toml") return highlightConfig(code, "toml");
  if (["yaml", "yml"].includes(normalized)) return highlightConfig(code, "yaml");
  if (normalized === "cron") return highlightCron(code);
  return escapeHtml(code);
}

function highlightBash(code) {
  return code.split("\n").map((line) => {
    if (/^\s*#/.test(line)) return span("comment", line);
    return highlightSegments(line, /("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`[^`]*`|\$\{?[A-Za-z_][A-Za-z0-9_]*\}?|--?[A-Za-z0-9][A-Za-z0-9_-]*|\b(?:brew|case|cd|curl|do|done|else|esac|export|fi|for|gh|git|gitcrawl|go|if|in|jq|ln|local|mkdir|set|then|while)\b|#.*)/g, (token) => {
      if (token.startsWith("#")) return span("comment", token);
      if (/^["'`]/.test(token)) return span("string", token);
      if (token.startsWith("$")) return span("variable", token);
      if (token.startsWith("-")) return span("option", token);
      return span("keyword", token);
    });
  }).join("\n");
}

function highlightJSON(code) {
  return highlightSegments(code, /("(?:\\.|[^"\\])*"\s*:)|("(?:\\.|[^"\\])*")|\b(?:true|false|null)\b|-?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/g, (token) => {
    if (token.endsWith(":")) return `${span("key", token.slice(0, -1))}:`;
    if (token.startsWith('"')) return span("string", token);
    if (/^(?:true|false|null)$/.test(token)) return span("literal", token);
    return span("number", token);
  });
}

function highlightConfig(code, lang) {
  return code.split("\n").map((line) => {
    if (/^\s*#/.test(line)) return span("comment", line);
    const commentMatch = line.match(/(^|[^"'])#.*/);
    const commentStart = commentMatch ? commentMatch.index + commentMatch[1].length : -1;
    const body = commentStart >= 0 ? line.slice(0, commentStart) : line;
    const comment = commentStart >= 0 ? line.slice(commentStart) : "";
    const highlighted = lang === "toml"
      ? highlightSegments(body, /(^\s*[A-Za-z0-9_.-]+(?=\s*=))|("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')|\b(?:true|false)\b|-?\b\d+(?:\.\d+)?\b/g, configToken)
      : highlightSegments(body, /(^\s*[A-Za-z0-9_.-]+(?=\s*:))|("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')|\b(?:true|false|null)\b|-?\b\d+(?:\.\d+)?\b/g, configToken);
    return highlighted + (comment ? span("comment", comment) : "");
  }).join("\n");
}

function configToken(token) {
  if (/^\s*[A-Za-z0-9_.-]+$/.test(token)) {
    const leading = token.match(/^\s*/)[0];
    return `${escapeHtml(leading)}${span("key", token.slice(leading.length))}`;
  }
  if (/^["']/.test(token)) return span("string", token);
  if (/^(?:true|false|null)$/.test(token)) return span("literal", token);
  return span("number", token);
}

function highlightCron(code) {
  return code.split("\n").map((line) => {
    if (/^\s*#/.test(line)) return span("comment", line);
    return highlightSegments(line, /(\*|(?:\d+)(?:[-/,]\d+)*)|("[^"]*"|'[^']*')|#.*|\b[A-Z_][A-Z0-9_]*=/g, (token) => {
      if (token.startsWith("#")) return span("comment", token);
      if (/^["']/.test(token)) return span("string", token);
      if (token.endsWith("=")) return span("key", token.slice(0, -1)) + "=";
      return span("number", token);
    });
  }).join("\n");
}

function highlightSegments(text, pattern, classify) {
  let out = "";
  let last = 0;
  for (const match of text.matchAll(pattern)) {
    out += escapeHtml(text.slice(last, match.index));
    out += classify(match[0]);
    last = match.index + match[0].length;
  }
  return out + escapeHtml(text.slice(last));
}

function span(kind, value) {
  return `<span class="hl-${kind}">${escapeHtml(value)}</span>`;
}

function inline(text, currentRel) {
  const stash = [];
  let out = text.replace(/`([^`]+)`/g, (_, code) => {
    stash.push(`<code>${escapeHtml(code)}</code>`);
    return `\u0000${stash.length - 1}\u0000`;
  });
  out = escapeHtml(out)
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/(^|[^*])\*([^*\s][^*]*?)\*(?!\*)/g, "$1<em>$2</em>")
    .replace(/(^|[^_])_([^_\s][^_]*?)_(?!_)/g, "$1<em>$2</em>")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, href) => `<a href="${escapeAttr(rewriteHref(href, currentRel))}">${label}</a>`)
    .replace(/&lt;(https?:\/\/[^\s<>]+)&gt;/g, '<a href="$1">$1</a>');
  return out.replace(/\u0000(\d+)\u0000/g, (_, i) => stash[Number(i)]);
}

function rewriteHref(href, currentRel) {
  if (/^(https?:|mailto:|#)/.test(href)) return href;
  const [raw, hash = ""] = href.split("#");
  if (!raw) return hash ? `#${hash}` : "";
  if (raw.startsWith("/")) {
    const target = permalinkMap.get(normalizePermalink(raw));
    if (target) {
      const currentOut = pageMap.get(currentRel)?.outRel || outPath(currentRel);
      const out = hrefToOutRel(target.outRel, currentOut);
      return hash ? `${out}#${hash}` : out;
    }
    return href;
  }
  if (!raw.endsWith(".md")) return href;
  const from = path.posix.dirname(currentRel);
  const target = path.posix.normalize(path.posix.join(from, raw));
  let rewritten = pageMap.get(target)?.outRel || outPath(target);
  const currentOut = pageMap.get(currentRel)?.outRel || outPath(currentRel);
  rewritten = hrefToOutRel(rewritten, currentOut);
  return `${rewritten}${hash ? `#${hash}` : ""}`;
}

function tocFromHtml(html) {
  const items = [];
  const re = /<h([23]) id="([^"]+)">([\s\S]*?)<\/h[23]>/g;
  let m;
  while ((m = re.exec(html))) {
    const text = htmlTextContent(m[3]).replace(/^#/, "").trim();
    items.push({ level: Number(m[1]), id: m[2], text });
  }
  if (items.length < 2) return "";
  return `<nav class="toc" aria-label="On this page"><h2>On this page</h2>${items
    .map((i) => `<a class="toc-l${i.level}" href="#${i.id}">${escapeHtml(i.text)}</a>`)
    .join("")}</nav>`;
}

function layout({ page, html, toc, prev, next, sectionName }) {
  const depth = page.outRel.split("/").length - 1;
  const rootPrefix = depth ? "../".repeat(depth) : "";
  const editUrl = `${repoEditBase}/${page.rel}`;
  const isHome = page.rel === "index.md" || page.rel === "README.md";
  const prevNext = !isHome && (prev || next) ? pageNavHtml(prev, next, page.outRel) : "";
  const heroBlock = isHome ? "" : standardHero(page, sectionName, editUrl);
  const articleClass = isHome ? "doc doc-home" : "doc";
  const tocBlock = isHome ? "" : toc;
  const titleSuffix = isHome ? "gitcrawl" : `${escapeHtml(page.title)} — gitcrawl`;
  const canonicalUrl = pageCanonicalUrl(page);
  const socialImage = siteBase ? `${siteBase}/social-card.png` : `${rootPrefix}social-card.png`;
  const socialMeta = [
    ["link", "rel", "canonical", "href", canonicalUrl],
    ["meta", "property", "og:type", "content", "website"],
    ["meta", "property", "og:site_name", "content", "gitcrawl"],
    ["meta", "property", "og:title", "content", titleSuffix],
    ["meta", "property", "og:description", "content", "Local-first GitHub issue and pull request crawler for maintainer triage."],
    ["meta", "property", "og:url", "content", canonicalUrl],
    ["meta", "property", "og:image", "content", socialImage],
    ["meta", "property", "og:image:width", "content", "1200"],
    ["meta", "property", "og:image:height", "content", "630"],
    ["meta", "name", "twitter:card", "content", "summary_large_image"],
    ["meta", "name", "twitter:title", "content", titleSuffix],
    ["meta", "name", "twitter:description", "content", "Local-first GitHub issue and pull request crawler for maintainer triage."],
    ["meta", "name", "twitter:image", "content", socialImage],
  ].map(tagHtml).join("\n  ");
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${titleSuffix}</title>
  <meta name="description" content="Local-first GitHub issue and pull request crawler for maintainer triage.">
  ${socialMeta}
  <link rel="icon" href="${rootPrefix}favicon.svg" type="image/svg+xml">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>${css()}</style>
  <script>${themeInitJs()}</script>
</head>
<body${isHome ? ' class="home"' : ""}>
  <button class="nav-toggle" type="button" aria-label="Toggle navigation" aria-expanded="false">
    <span aria-hidden="true"></span><span aria-hidden="true"></span><span aria-hidden="true"></span>
  </button>
  <div class="shell">
    <aside class="sidebar">
      <div class="sidebar-head">
        <a class="brand" href="${hrefToOutRel("index.html", page.outRel)}" aria-label="gitcrawl docs home">
          <img src="${rootPrefix}favicon.svg" alt="">
          <span class="brand-text">
            <strong class="brand-name">gitcrawl<span class="brand-tag">main</span></strong>
            <small>Local-first GitHub triage</small>
          </span>
        </a>
        ${themeToggleHtml()}
      </div>
      <label class="search"><span>Search</span><input id="doc-search" type="search" placeholder="sync, cluster, gh shim"></label>
      <nav>${navHtml(page)}</nav>
    </aside>
    <main>
      ${heroBlock}
      <div class="doc-grid${isHome ? " doc-grid-home" : ""}">
        <article class="${articleClass}">${html}${prevNext}</article>
        ${tocBlock}
      </div>
    </main>
  </div>
  <script>${js()}</script>
</body>
</html>`;
}

function pageCanonicalUrl(page) {
  if (!siteBase) return page.outRel;
  if (page.outRel === "index.html") return `${siteBase}/`;
  const rel = page.outRel.endsWith("/index.html") ? page.outRel.slice(0, -"index.html".length) : page.outRel;
  return `${siteBase}/${rel}`;
}

function tagHtml([tag, k1, v1, k2, v2]) {
  return tag === "link" ? `<link ${k1}="${v1}" ${k2}="${escapeAttr(v2)}">` : `<meta ${k1}="${v1}" ${k2}="${escapeAttr(v2)}">`;
}

function standardHero(page, sectionName, editUrl) {
  return `<header class="hero">
        <div class="hero-text">
          <p class="eyebrow">${escapeHtml(sectionName)}</p>
          <h1>${escapeHtml(page.title)}</h1>
        </div>
        <div class="hero-meta">
          <a class="repo" href="${repoBase}" rel="noopener">GitHub</a>
          <a class="edit" href="${escapeAttr(editUrl)}" rel="noopener">Edit page</a>
        </div>
      </header>`;
}

function pageNavHtml(prev, next, currentOutRel) {
  const cell = (page, dir) => {
    if (!page) return "";
    return `<a class="page-nav-${dir}" href="${hrefToOutRel(page.outRel, currentOutRel)}"><small>${dir === "prev" ? "Previous" : "Next"}</small><span>${escapeHtml(page.title)}</span></a>`;
  };
  return `<nav class="page-nav" aria-label="Pager">${cell(prev, "prev")}${cell(next, "next")}</nav>`;
}

function navHtml(currentPage) {
  return nav
    .map((section) => `<section><h2>${section.name}</h2>${section.pages.map((page) => {
      const href = hrefToOutRel(page.outRel, currentPage.outRel);
      const active = page.rel === currentPage.rel ? " active" : "";
      return `<a class="nav-link${active}" href="${href}">${escapeHtml(page.title)}</a>`;
    }).join("")}</section>`)
    .join("");
}

function hrefToOutRel(targetOutRel, currentOutRel) {
  const currentDir = path.posix.dirname(currentOutRel);
  if (targetOutRel.endsWith("/index.html")) {
    const targetDir = targetOutRel.slice(0, -"index.html".length);
    const rel = path.posix.relative(currentDir, targetDir || ".") || ".";
    return rel.endsWith("/") ? rel : `${rel}/`;
  }
  if (targetOutRel === "index.html") {
    const rel = path.posix.relative(currentDir, ".") || ".";
    return rel.endsWith("/") ? rel : `${rel}/`;
  }
  return path.posix.relative(currentDir, targetOutRel) || path.posix.basename(targetOutRel);
}

function slug(text) {
  return text.toLowerCase().replace(/`/g, "").replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

function escapeAttr(value) {
  return escapeHtml(value);
}

function htmlTextContent(fragment) {
  let out = "";
  let inTag = false;
  for (const char of fragment) {
    if (char === "<") {
      inTag = true;
      continue;
    }
    if (inTag) {
      if (char === ">") inTag = false;
      continue;
    }
    out += char;
  }
  return decodeHtmlText(out);
}

function decodeHtmlText(value) {
  return String(value).replace(/&(amp|lt|gt|quot|#39);/g, (_, entity) => ({
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    "#39": "'",
  })[entity]);
}

function validateLinks(outputDir) {
  const failures = [];
  for (const file of allHtml(outputDir)) {
    const html = fs.readFileSync(file, "utf8");
    for (const match of html.matchAll(/href="([^"]+)"/g)) {
      const href = match[1];
      if (/^(#|https?:|mailto:|tel:|javascript:)/.test(href)) continue;
      const [rawPath, anchor = ""] = href.split("#");
      const targetPath = rawPath
        ? path.resolve(path.dirname(file), rawPath)
        : file;
      const target = fs.existsSync(targetPath) && fs.statSync(targetPath).isDirectory()
        ? path.join(targetPath, "index.html")
        : targetPath;
      if (!fs.existsSync(target)) {
        failures.push(`${path.relative(outputDir, file)}: ${href} -> missing ${path.relative(outputDir, target)}`);
        continue;
      }
      if (anchor) {
        const targetHtml = fs.readFileSync(target, "utf8");
        if (!targetHtml.includes(`id="${anchor}"`) && !targetHtml.includes(`name="${anchor}"`)) {
          failures.push(`${path.relative(outputDir, file)}: ${href} -> missing anchor`);
        }
      }
    }
  }
  if (failures.length) {
    throw new Error(`broken docs links:\n${failures.join("\n")}`);
  }
}

function allHtml(dir) {
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .flatMap((entry) => {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) return allHtml(full);
      return entry.name.endsWith(".html") ? [full] : [];
    })
    .sort();
}
