# AGENTS.md

## Autoreview priority

Use `--max-priority P3` with the shared autoreview helper to preserve this
repository's existing P0–P3 review gate. Follow the
[shared skill setup](.agents/skills/autoreview/SKILL.md) before running it.

## Development workspace

- Keep one canonical development checkout per machine. Use branches in that
  existing checkout by default; a dirty checkout is not a reason to create another
  clone or worktree. Inspect and preserve unrelated edits before switching branches.
- Create an additional worktree only when the user explicitly requests it. Record
  its owner, purpose, path, branch and end-of-task removal responsibility. Account
  for every exception at closeout; do not leave abandoned checkouts in Projects.
- Store deployed binaries, private receipts, build/review evidence and rollback
  custody in native application storage outside Projects. Existing frozen evidence
  with live path bindings stays in place until its owner authorizes relocation.
- Before retiring a secondary checkout, inspect registered worktrees, the common
  Git directory, live process/task and service references, dirty/untracked/ignored
  files, unique commits and holds. A folder name or old timestamp is not proof of
  staleness. Preserve and verify useful contents in private custody first.
- Use supported `git worktree remove` for a verified stale checkout. Do not blindly
  force removal, delete branches, prune unknown registrations, or remove a common
  Git directory that supports another checkout. Keep branch/ref history.
- Native archives, WAL/SHM, cursors, credentials, schedules, portable stores and
  producer-owned source caches are runtime state, not stale development worktrees.
  Workspace cleanup does not authorize changes to them or release capture/cleanup
  holds. Do not restart collectors or invoke providers to validate documentation.
- Finish workspace work with `git worktree list --porcelain`; remove task-owned
  exceptions after verified custody, or report the exact owner/dependency/hold
  that prevents retirement. Never claim moving data has freed disk space.
