-- name: CountRepositories :one
select count(*) from repositories;

-- name: CountThreads :one
select count(*) from threads;

-- name: CountOpenThreads :one
select count(*) from threads where state = 'open' and closed_at_local is null;

-- name: CountClusters :one
select count(*) from cluster_groups;

-- name: MaxSuccessfulSyncFinishedAt :one
select cast(coalesce(max(finished_at), '') as text) as last_sync from sync_runs where status in ('success', 'completed');

-- name: PortableExportedAt :one
select value from portable_metadata where key = 'exported_at';

-- name: RepoSyncStateLastSync :one
select cast(coalesce(
  max(last_open_close_reconciled_at),
  max(last_overlapping_open_scan_completed_at),
  max(last_non_overlapping_scan_completed_at),
  max(last_full_open_scan_started_at),
  max(updated_at),
  ''
) as text) as last_sync from repo_sync_state;

-- name: UpsertRepository :one
insert into repositories(owner, name, full_name, github_repo_id, raw_json, updated_at)
values(sqlc.arg(owner), sqlc.arg(name), sqlc.arg(full_name), sqlc.narg(github_repo_id), sqlc.arg(raw_json), sqlc.arg(updated_at))
on conflict(full_name) do update set
  owner=excluded.owner,
  name=excluded.name,
  github_repo_id=excluded.github_repo_id,
  raw_json=excluded.raw_json,
  updated_at=excluded.updated_at
returning id;

-- name: RepositoryByFullName :one
select id, owner, name, full_name, github_repo_id, coalesce(raw_json, '') as raw_json, updated_at
from repositories
where full_name = sqlc.arg(full_name);

-- name: ListRepositories :many
select id, owner, name, full_name, github_repo_id, coalesce(raw_json, '') as raw_json, updated_at
from repositories
order by coalesce(updated_at, '') desc, id desc;

-- name: UpsertThread :one
insert into threads(
  repo_id, github_id, number, kind, state, title, body, author_login, author_type, author_association, html_url,
  labels_json, assignees_json, raw_json, content_hash, is_draft,
  created_at_gh, updated_at_gh, closed_at_gh, merged_at_gh,
  first_pulled_at, last_pulled_at, updated_at
)
values(
  sqlc.arg(repo_id), sqlc.arg(github_id), sqlc.arg(number), sqlc.arg(kind), sqlc.arg(state), sqlc.arg(title),
  sqlc.narg(body), sqlc.narg(author_login), sqlc.narg(author_type), sqlc.narg(author_association), sqlc.arg(html_url),
  sqlc.arg(labels_json), sqlc.arg(assignees_json), sqlc.arg(raw_json), sqlc.arg(content_hash), sqlc.arg(is_draft),
  sqlc.narg(created_at_gh), sqlc.narg(updated_at_gh), sqlc.narg(closed_at_gh), sqlc.narg(merged_at_gh),
  sqlc.narg(first_pulled_at), sqlc.narg(last_pulled_at), sqlc.arg(updated_at)
)
on conflict(repo_id, kind, number) do update set
  github_id=excluded.github_id,
  state=excluded.state,
  title=excluded.title,
  body=excluded.body,
  author_login=excluded.author_login,
  author_type=excluded.author_type,
  author_association=excluded.author_association,
  html_url=excluded.html_url,
  labels_json=excluded.labels_json,
  assignees_json=excluded.assignees_json,
  raw_json=excluded.raw_json,
  content_hash=excluded.content_hash,
  is_draft=excluded.is_draft,
  created_at_gh=excluded.created_at_gh,
  updated_at_gh=excluded.updated_at_gh,
  closed_at_gh=excluded.closed_at_gh,
  merged_at_gh=excluded.merged_at_gh,
  last_pulled_at=excluded.last_pulled_at,
  updated_at=excluded.updated_at
returning id;

-- name: UpsertThreadPreservingDraft :one
insert into threads(
  repo_id, github_id, number, kind, state, title, body, author_login, author_type, author_association, html_url,
  labels_json, assignees_json, raw_json, content_hash, is_draft,
  created_at_gh, updated_at_gh, closed_at_gh, merged_at_gh,
  first_pulled_at, last_pulled_at, updated_at
)
values(
  sqlc.arg(repo_id), sqlc.arg(github_id), sqlc.arg(number), sqlc.arg(kind), sqlc.arg(state), sqlc.arg(title),
  sqlc.narg(body), sqlc.narg(author_login), sqlc.narg(author_type), sqlc.narg(author_association), sqlc.arg(html_url),
  sqlc.arg(labels_json), sqlc.arg(assignees_json), sqlc.arg(raw_json), sqlc.arg(content_hash), sqlc.arg(is_draft),
  sqlc.narg(created_at_gh), sqlc.narg(updated_at_gh), sqlc.narg(closed_at_gh), sqlc.narg(merged_at_gh),
  sqlc.narg(first_pulled_at), sqlc.narg(last_pulled_at), sqlc.arg(updated_at)
)
on conflict(repo_id, kind, number) do update set
  github_id=excluded.github_id,
  state=excluded.state,
  title=excluded.title,
  body=excluded.body,
  author_login=excluded.author_login,
  author_type=excluded.author_type,
  author_association=excluded.author_association,
  html_url=excluded.html_url,
  labels_json=excluded.labels_json,
  assignees_json=excluded.assignees_json,
  raw_json=excluded.raw_json,
  content_hash=excluded.content_hash,
  is_draft=threads.is_draft,
  created_at_gh=excluded.created_at_gh,
  updated_at_gh=excluded.updated_at_gh,
  closed_at_gh=excluded.closed_at_gh,
  merged_at_gh=excluded.merged_at_gh,
  last_pulled_at=excluded.last_pulled_at,
  updated_at=excluded.updated_at
returning id;

-- name: MarkOpenThreadClosedFromGitHub :execrows
update threads
set github_id = sqlc.arg(github_id),
  state = sqlc.arg(state),
  title = sqlc.arg(title),
  body = sqlc.narg(body),
  author_login = sqlc.narg(author_login),
  author_type = sqlc.narg(author_type),
  author_association = sqlc.narg(author_association),
  html_url = sqlc.arg(html_url),
  labels_json = sqlc.arg(labels_json),
  assignees_json = sqlc.arg(assignees_json),
  raw_json = sqlc.arg(raw_json),
  content_hash = sqlc.arg(content_hash),
  created_at_gh = sqlc.narg(created_at_gh),
  updated_at_gh = sqlc.narg(updated_at_gh),
  closed_at_gh = sqlc.narg(closed_at_gh),
  merged_at_gh = sqlc.narg(merged_at_gh),
  last_pulled_at = sqlc.narg(last_pulled_at),
  updated_at = sqlc.arg(updated_at)
where repo_id = sqlc.arg(repo_id)
  and kind = sqlc.arg(kind)
  and number = sqlc.arg(number)
  and state = 'open'
  and closed_at_local is null;

-- name: ListThreadsCurrentSchema :many
select id, repo_id, github_id, number, kind, state, title, body, author_login, author_type, author_association, html_url,
  labels_json, assignees_json, coalesce(raw_json, '') as raw_json, content_hash, is_draft, created_at_gh, updated_at_gh,
  closed_at_gh, merged_at_gh, first_pulled_at, last_pulled_at, updated_at, closed_at_local, close_reason_local
from threads
where repo_id = sqlc.arg(repo_id)
  and (sqlc.arg(include_closed) != 0 or closed_at_local is null)
order by number
limit case when sqlc.arg(row_limit) <= 0 then -1 else sqlc.arg(row_limit) end;

-- name: CloseThreadLocally :execrows
update threads
set closed_at_local = sqlc.arg(closed_at), close_reason_local = sqlc.arg(reason), updated_at = sqlc.arg(closed_at)
where repo_id = sqlc.arg(repo_id) and number = sqlc.arg(number);

-- name: ReopenThreadLocally :execrows
update threads
set closed_at_local = null, close_reason_local = null, updated_at = sqlc.arg(updated_at)
where repo_id = sqlc.arg(repo_id) and number = sqlc.arg(number);

-- name: UpsertComment :one
insert into comments(thread_id, github_id, comment_type, author_login, author_type, body, is_bot, raw_json, created_at_gh, updated_at_gh)
values(sqlc.arg(thread_id), sqlc.arg(github_id), sqlc.arg(comment_type), sqlc.narg(author_login), sqlc.narg(author_type), sqlc.arg(body), sqlc.arg(is_bot), sqlc.arg(raw_json), sqlc.narg(created_at_gh), sqlc.narg(updated_at_gh))
on conflict(thread_id, comment_type, github_id) do update set
  author_login=excluded.author_login,
  author_type=excluded.author_type,
  body=excluded.body,
  is_bot=excluded.is_bot,
  raw_json=excluded.raw_json,
  created_at_gh=excluded.created_at_gh,
  updated_at_gh=excluded.updated_at_gh
returning id;

-- name: ListComments :many
select id, thread_id, github_id, comment_type, author_login, author_type, body, is_bot, raw_json, created_at_gh, updated_at_gh
from comments
where thread_id = sqlc.arg(thread_id)
order by created_at_gh, id;

-- name: UpsertDocument :one
insert into documents(thread_id, title, body, raw_text, dedupe_text, updated_at)
values(sqlc.arg(thread_id), sqlc.arg(title), sqlc.narg(body), sqlc.arg(raw_text), sqlc.arg(dedupe_text), sqlc.arg(updated_at))
on conflict(thread_id) do update set
  title=excluded.title,
  body=excluded.body,
  raw_text=excluded.raw_text,
  dedupe_text=excluded.dedupe_text,
  updated_at=excluded.updated_at
where documents.title is not excluded.title
   or documents.body is not excluded.body
   or documents.raw_text is not excluded.raw_text
   or documents.dedupe_text is not excluded.dedupe_text
returning id;

-- name: ListEmbeddingTasks :many
select t.id, t.number, t.kind, t.title, coalesce(d.body, t.body, '') as body, coalesce(d.raw_text, t.body, '') as raw_text,
  coalesce(d.dedupe_text, t.title || ' ' || coalesce(t.body, '')) as dedupe_text,
  cast(coalesce((
    select tks.key_text
    from thread_key_summaries tks
    join thread_revisions tr on tr.id = tks.thread_revision_id
    where tr.thread_id = t.id
      and tr.id = (
        select latest.id
        from thread_revisions latest
        where latest.thread_id = t.id
        order by julianday(coalesce(nullif(latest.source_updated_at, ''), latest.created_at)) desc,
          latest.id desc
        limit 1
      )
      and julianday(coalesce(nullif(tr.source_updated_at, ''), tr.created_at)) >=
        julianday(coalesce(nullif(t.updated_at_gh, ''), t.updated_at))
      and tks.summary_kind = 'llm_key_summary'
    order by tks.created_at desc, tks.id desc
    limit 1
  ), '') as text) as key_summary,
  coalesce(tv.content_hash, '') as existing_hash
from threads t
left join documents d on d.thread_id = t.id
left join thread_vectors tv on tv.thread_id = t.id and tv.basis = sqlc.arg(basis) and tv.model = sqlc.arg(model)
where t.repo_id = sqlc.arg(repo_id)
  and (sqlc.arg(include_closed) != 0 or (t.state = 'open' and t.closed_at_local is null))
  and (sqlc.narg(number) is null or t.number = sqlc.narg(number))
  and (
    sqlc.arg(basis) != 'llm_key_summary'
    or exists (
      select 1
      from thread_key_summaries eligible_summary
      join thread_revisions eligible_revision on eligible_revision.id = eligible_summary.thread_revision_id
      where eligible_revision.thread_id = t.id
        and eligible_revision.id = (
          select latest.id
          from thread_revisions latest
          where latest.thread_id = t.id
          order by julianday(coalesce(nullif(latest.source_updated_at, ''), latest.created_at)) desc,
            latest.id desc
          limit 1
        )
        and julianday(coalesce(nullif(eligible_revision.source_updated_at, ''), eligible_revision.created_at)) >=
          julianday(coalesce(nullif(t.updated_at_gh, ''), t.updated_at))
        and eligible_summary.summary_kind = 'llm_key_summary'
    )
  )
order by coalesce(t.updated_at_gh, t.updated_at) desc, t.number desc
limit case when sqlc.arg(row_limit) <= 0 then -1 else sqlc.arg(row_limit) end;

-- name: RecordSyncRun :one
insert into sync_runs(repo_id, scope, status, started_at, finished_at, stats_json, error_text)
values(sqlc.arg(repo_id), sqlc.arg(scope), sqlc.arg(status), sqlc.arg(started_at), sqlc.narg(finished_at), sqlc.narg(stats_json), sqlc.narg(error_text))
returning id;

-- name: RecordSummaryRun :one
insert into summary_runs(repo_id, scope, status, started_at, finished_at, stats_json, error_text)
values(sqlc.arg(repo_id), sqlc.arg(scope), sqlc.arg(status), sqlc.arg(started_at), sqlc.narg(finished_at), sqlc.narg(stats_json), sqlc.narg(error_text))
returning id;

-- name: RecordEmbeddingRun :one
insert into embedding_runs(repo_id, scope, status, started_at, finished_at, stats_json, error_text)
values(sqlc.arg(repo_id), sqlc.arg(scope), sqlc.arg(status), sqlc.arg(started_at), sqlc.narg(finished_at), sqlc.narg(stats_json), sqlc.narg(error_text))
returning id;

-- name: RecordClusterRun :one
insert into cluster_runs(repo_id, scope, status, started_at, finished_at, stats_json, error_text)
values(sqlc.arg(repo_id), sqlc.arg(scope), sqlc.arg(status), sqlc.arg(started_at), sqlc.narg(finished_at), sqlc.narg(stats_json), sqlc.narg(error_text))
returning id;

-- name: ListSyncRuns :many
select id, repo_id, scope, status, started_at, finished_at, stats_json, error_text
from sync_runs
where repo_id = sqlc.arg(repo_id)
order by id desc
limit sqlc.arg(row_limit);

-- name: ListSummaryRuns :many
select id, repo_id, scope, status, started_at, finished_at, stats_json, error_text
from summary_runs
where repo_id = sqlc.arg(repo_id)
order by id desc
limit sqlc.arg(row_limit);

-- name: ListEmbeddingRuns :many
select id, repo_id, scope, status, started_at, finished_at, stats_json, error_text
from embedding_runs
where repo_id = sqlc.arg(repo_id)
order by id desc
limit sqlc.arg(row_limit);

-- name: ListClusterRuns :many
select id, repo_id, scope, status, started_at, finished_at, stats_json, error_text
from cluster_runs
where repo_id = sqlc.arg(repo_id)
order by id desc
limit sqlc.arg(row_limit);

-- name: LastSuccessfulSyncAt :one
select cast(coalesce(max(finished_at), '') as text) as last_sync
from sync_runs
where repo_id = sqlc.arg(repo_id) and status in ('success', 'completed');

-- name: LastSuccessfulListSyncAt :one
select cast(coalesce(max(finished_at), '') as text) as last_sync
from sync_runs
where repo_id = sqlc.arg(repo_id)
  and status in ('success', 'completed')
  and (
    (sqlc.arg(state) = 'open' and scope in ('open', 'all')) or
    (sqlc.arg(state) = 'closed' and scope in ('closed', 'all')) or
    (sqlc.arg(state) = 'all' and scope = 'all')
  );

-- name: UpsertPullRequestDetail :exec
insert into pull_request_details(thread_id, repo_id, number, base_sha, head_sha, head_ref, head_repo_full_name, mergeable_state, additions, deletions, changed_files, raw_json, fetched_at, updated_at)
values(sqlc.arg(thread_id), sqlc.arg(repo_id), sqlc.arg(number), sqlc.narg(base_sha), sqlc.narg(head_sha), sqlc.narg(head_ref), sqlc.narg(head_repo_full_name), sqlc.narg(mergeable_state), sqlc.arg(additions), sqlc.arg(deletions), sqlc.arg(changed_files), sqlc.arg(raw_json), sqlc.arg(fetched_at), sqlc.arg(updated_at))
on conflict(thread_id) do update set
  repo_id=excluded.repo_id,
  number=excluded.number,
  base_sha=excluded.base_sha,
  head_sha=excluded.head_sha,
  head_ref=excluded.head_ref,
  head_repo_full_name=excluded.head_repo_full_name,
  mergeable_state=excluded.mergeable_state,
  additions=excluded.additions,
  deletions=excluded.deletions,
  changed_files=excluded.changed_files,
  raw_json=excluded.raw_json,
  fetched_at=excluded.fetched_at,
  updated_at=excluded.updated_at;

-- name: DeletePullRequestFiles :exec
delete from pull_request_files where thread_id = sqlc.arg(thread_id);

-- name: InsertPullRequestFile :exec
insert into pull_request_files(thread_id, position, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at)
values(sqlc.arg(thread_id), sqlc.arg(position), sqlc.arg(path), sqlc.narg(status), sqlc.arg(additions), sqlc.arg(deletions), sqlc.arg(changes), sqlc.narg(previous_path), sqlc.narg(patch), sqlc.arg(raw_json), sqlc.arg(fetched_at));

-- name: DeletePullRequestCommits :exec
delete from pull_request_commits where thread_id = sqlc.arg(thread_id);

-- name: InsertPullRequestCommit :exec
insert into pull_request_commits(thread_id, sha, message, author_login, author_name, committed_at, html_url, raw_json, fetched_at)
values(sqlc.arg(thread_id), sqlc.arg(sha), sqlc.narg(message), sqlc.narg(author_login), sqlc.narg(author_name), sqlc.narg(committed_at), sqlc.narg(html_url), sqlc.arg(raw_json), sqlc.arg(fetched_at));

-- name: DeletePullRequestChecks :exec
delete from pull_request_checks where thread_id = sqlc.arg(thread_id);

-- name: InsertPullRequestCheck :exec
insert into pull_request_checks(thread_id, name, status, conclusion, details_url, workflow_name, started_at, completed_at, raw_json, fetched_at)
values(sqlc.arg(thread_id), sqlc.arg(name), sqlc.narg(status), sqlc.narg(conclusion), sqlc.narg(details_url), sqlc.narg(workflow_name), sqlc.narg(started_at), sqlc.narg(completed_at), sqlc.arg(raw_json), sqlc.arg(fetched_at));

-- name: UpsertWorkflowRun :exec
insert into github_workflow_runs(repo_id, run_id, run_number, head_branch, head_sha, status, conclusion, workflow_name, event, html_url, created_at_gh, updated_at_gh, raw_json, fetched_at)
values(sqlc.arg(repo_id), sqlc.arg(run_id), sqlc.arg(run_number), sqlc.narg(head_branch), sqlc.narg(head_sha), sqlc.narg(status), sqlc.narg(conclusion), sqlc.narg(workflow_name), sqlc.narg(event), sqlc.narg(html_url), sqlc.narg(created_at_gh), sqlc.narg(updated_at_gh), sqlc.arg(raw_json), sqlc.arg(fetched_at))
on conflict(repo_id, run_id) do update set
  run_number=excluded.run_number,
  head_branch=excluded.head_branch,
  head_sha=excluded.head_sha,
  status=excluded.status,
  conclusion=excluded.conclusion,
  workflow_name=excluded.workflow_name,
  event=excluded.event,
  html_url=excluded.html_url,
  created_at_gh=excluded.created_at_gh,
  updated_at_gh=excluded.updated_at_gh,
  raw_json=excluded.raw_json,
  fetched_at=excluded.fetched_at;

-- name: PullRequestDetail :one
select thread_id, repo_id, number, base_sha, head_sha, head_ref, head_repo_full_name, mergeable_state, additions, deletions, changed_files, raw_json, fetched_at, updated_at
from pull_request_details
where repo_id = sqlc.arg(repo_id) and number = sqlc.arg(number);

-- name: PullRequestFiles :many
select thread_id, position, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at
from pull_request_files
where thread_id = sqlc.arg(thread_id)
order by path, position;

-- name: PullRequestCommits :many
select thread_id, sha, message, author_login, author_name, committed_at, html_url, raw_json, fetched_at
from pull_request_commits
where thread_id = sqlc.arg(thread_id)
order by rowid;

-- name: PullRequestChecks :many
select id, thread_id, name, status, conclusion, details_url, workflow_name, started_at, completed_at, raw_json, fetched_at
from pull_request_checks
where thread_id = sqlc.arg(thread_id)
order by name;

-- name: ListWorkflowRuns :many
select repo_id, run_id, run_number, head_branch, head_sha, status, conclusion, workflow_name, event, html_url, created_at_gh, updated_at_gh, raw_json, fetched_at
from github_workflow_runs
where repo_id = sqlc.arg(repo_id)
  and (sqlc.narg(head_branch) is null or head_branch = sqlc.narg(head_branch))
  and (sqlc.narg(head_sha) is null or head_sha = sqlc.narg(head_sha))
order by updated_at_gh desc, run_id desc
limit sqlc.arg(row_limit);

-- name: DeletePullRequestReviewThreads :exec
delete from pull_request_review_threads where thread_id = sqlc.arg(thread_id);

-- name: UpsertPullRequestReviewThreadSync :exec
insert into pull_request_review_thread_syncs(thread_id, fetched_at)
values(sqlc.arg(thread_id), sqlc.arg(fetched_at))
on conflict(thread_id) do update set fetched_at=excluded.fetched_at;

-- name: UpsertPullRequestReviewThread :exec
insert into pull_request_review_threads(
  thread_id, review_thread_id, path, line, start_line, is_resolved, is_outdated,
  viewer_can_resolve, viewer_can_unresolve, viewer_can_reply,
  first_author_login, first_author_type, first_comment_body, first_comment_url,
  first_comment_created_at, first_comment_updated_at, comments_json, raw_json, fetched_at
)
values(
  sqlc.arg(thread_id), sqlc.arg(review_thread_id), sqlc.narg(path), sqlc.arg(line), sqlc.arg(start_line), sqlc.arg(is_resolved), sqlc.arg(is_outdated),
  sqlc.arg(viewer_can_resolve), sqlc.arg(viewer_can_unresolve), sqlc.arg(viewer_can_reply),
  sqlc.narg(first_author_login), sqlc.narg(first_author_type), sqlc.narg(first_comment_body), sqlc.narg(first_comment_url),
  sqlc.narg(first_comment_created_at), sqlc.narg(first_comment_updated_at), sqlc.arg(comments_json), sqlc.arg(raw_json), sqlc.arg(fetched_at)
)
on conflict(thread_id, review_thread_id) do update set
  path=excluded.path,
  line=excluded.line,
  start_line=excluded.start_line,
  is_resolved=excluded.is_resolved,
  is_outdated=excluded.is_outdated,
  viewer_can_resolve=excluded.viewer_can_resolve,
  viewer_can_unresolve=excluded.viewer_can_unresolve,
  viewer_can_reply=excluded.viewer_can_reply,
  first_author_login=excluded.first_author_login,
  first_author_type=excluded.first_author_type,
  first_comment_body=excluded.first_comment_body,
  first_comment_url=excluded.first_comment_url,
  first_comment_created_at=excluded.first_comment_created_at,
  first_comment_updated_at=excluded.first_comment_updated_at,
  comments_json=excluded.comments_json,
  raw_json=excluded.raw_json,
  fetched_at=excluded.fetched_at;

-- name: PullRequestReviewThreads :many
select thread_id, review_thread_id, path, line, start_line, is_resolved, is_outdated,
  viewer_can_resolve, viewer_can_unresolve, viewer_can_reply,
  first_author_login, first_author_type, first_comment_body, first_comment_url,
  first_comment_created_at, first_comment_updated_at, comments_json, raw_json, fetched_at
from pull_request_review_threads
where thread_id = sqlc.arg(thread_id)
order by is_resolved, path, line, review_thread_id;

-- name: PullRequestReviewThreadsFetchedAt :one
select fetched_at from pull_request_review_thread_syncs where thread_id = sqlc.arg(thread_id);
