create table repositories (
  id integer primary key,
  owner text not null,
  name text not null,
  full_name text not null unique,
  github_repo_id text,
  raw_json text not null,
  updated_at text not null
);

create table threads (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  github_id text not null,
  number integer not null,
  kind text not null check (kind in ('issue', 'pull_request')),
  state text not null,
  title text not null,
  body text,
  author_login text,
  author_type text,
  author_association text,
  html_url text not null,
  labels_json text not null,
  assignees_json text not null,
  raw_json text not null,
  content_hash text not null,
  is_draft integer not null default 0,
  created_at_gh text,
  updated_at_gh text,
  closed_at_gh text,
  merged_at_gh text,
  closed_at_local text,
  close_reason_local text,
  first_pulled_at text,
  last_pulled_at text,
  observation_sequence integer not null default 0,
  updated_at text not null,
  unique(repo_id, kind, number)
);

create table comments (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  github_id text not null,
  comment_type text not null,
  author_login text,
  author_type text,
  body text not null,
  is_bot integer not null default 0,
  raw_json text not null,
  raw_json_blob_id integer references blobs(id) on delete set null,
  created_at_gh text,
  updated_at_gh text,
  unique(thread_id, comment_type, github_id)
);

create table blobs (
  id integer primary key,
  sha256 text not null unique,
  media_type text not null,
  compression text not null default 'none',
  size_bytes integer not null,
  storage_kind text not null,
  storage_path text,
  inline_text text,
  created_at text not null
);

create table thread_observation_sequence (
  id integer primary key check (id = 1),
  value integer not null,
  last_started_at text not null
);

create table thread_revisions (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  source_updated_at text,
  content_hash text not null,
  title_hash text not null,
  body_hash text not null,
  labels_hash text not null,
  raw_json_blob_id integer references blobs(id) on delete set null,
  observation_sequence integer not null default 0,
  created_at text not null
);

create table pull_request_details (
  thread_id integer primary key references threads(id) on delete cascade,
  repo_id integer not null references repositories(id) on delete cascade,
  number integer not null,
  base_sha text,
  head_sha text,
  head_ref text,
  head_repo_full_name text,
  mergeable_state text,
  additions integer not null default 0,
  deletions integer not null default 0,
  changed_files integer not null default 0,
  raw_json text not null,
  fetched_at text not null,
  updated_at text not null,
  unique(repo_id, number)
);

create table pull_request_files (
  thread_id integer not null references threads(id) on delete cascade,
  -- GitHub can return multiple file entries with the same filename for one PR,
  -- for example a removed file and an added file at the same path. Store the
  -- fetched file list as a replaceable snapshot instead of keying by path.
  -- See https://github.com/openclaw/gitcrawl/issues/77 for the duplicate-path
  -- bug and why position is snapshot-local rather than a durable file identity.
  position integer not null default 0,
  path text not null,
  status text,
  additions integer not null default 0,
  deletions integer not null default 0,
  changes integer not null default 0,
  previous_path text,
  patch text,
  raw_json text not null,
  fetched_at text not null,
  primary key(thread_id, position)
);

create table pull_request_commits (
  thread_id integer not null references threads(id) on delete cascade,
  sha text not null,
  message text,
  author_login text,
  author_name text,
  committed_at text,
  html_url text,
  raw_json text not null,
  fetched_at text not null,
  primary key(thread_id, sha)
);

create table pull_request_checks (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  name text not null,
  status text,
  conclusion text,
  details_url text,
  workflow_name text,
  started_at text,
  completed_at text,
  raw_json text not null,
  fetched_at text not null,
  unique(thread_id, name, details_url)
);

create table pull_request_review_threads (
  thread_id integer not null references threads(id) on delete cascade,
  review_thread_id text not null,
  path text,
  line integer not null default 0,
  start_line integer not null default 0,
  is_resolved integer not null default 0,
  is_outdated integer not null default 0,
  viewer_can_resolve integer not null default 0,
  viewer_can_unresolve integer not null default 0,
  viewer_can_reply integer not null default 0,
  first_author_login text,
  first_author_type text,
  first_comment_body text,
  first_comment_url text,
  first_comment_created_at text,
  first_comment_updated_at text,
  comments_json text not null,
  raw_json text not null,
  fetched_at text not null,
  primary key(thread_id, review_thread_id)
);

create table pull_request_review_thread_syncs (
  thread_id integer primary key references threads(id) on delete cascade,
  fetched_at text not null
);

create table github_workflow_runs (
  repo_id integer not null references repositories(id) on delete cascade,
  run_id text not null,
  run_number integer not null default 0,
  head_branch text,
  head_sha text,
  status text,
  conclusion text,
  workflow_name text,
  event text,
  html_url text,
  created_at_gh text,
  updated_at_gh text,
  raw_json text not null,
  fetched_at text not null,
  primary key(repo_id, run_id)
);

create table documents (
  id integer primary key,
  thread_id integer not null unique references threads(id) on delete cascade,
  title text not null,
  body text,
  raw_text text not null,
  dedupe_text text not null,
  updated_at text not null
);

create table code_snapshots (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  source_root text not null,
  git_sha text not null,
  worktree_dirty integer not null default 0,
  file_count integer not null,
  byte_count integer not null,
  indexed_at text not null
);

create table code_documents (
  id integer primary key,
  snapshot_id integer not null references code_snapshots(id) on delete cascade,
  repo_id integer not null references repositories(id) on delete cascade,
  path text not null,
  language text not null,
  content_hash text not null,
  text_content text not null,
  byte_size integer not null,
  updated_at text not null,
  unique(snapshot_id, path)
);

create table thread_vectors (
  thread_id integer not null references threads(id) on delete cascade,
  basis text not null,
  model text not null,
  dimensions integer not null,
  content_hash text not null,
  vector_json text not null,
  vector_backend text not null,
  created_at text not null,
  updated_at text not null,
  primary key(thread_id, basis, model)
);

create table thread_fingerprints (
  id integer primary key,
  thread_revision_id integer not null references thread_revisions(id) on delete cascade,
  algorithm_version text not null,
  fingerprint_hash text not null,
  fingerprint_slug text not null,
  title_tokens_json text not null,
  body_token_hash text not null,
  linked_refs_json text not null,
  file_set_hash text not null,
  module_buckets_json text not null,
  simhash64 text not null,
  feature_json text not null,
  created_at text not null,
  unique(thread_revision_id, algorithm_version)
);

create table thread_key_summaries (
  id integer primary key,
  thread_revision_id integer not null references thread_revisions(id) on delete cascade,
  summary_kind text not null,
  prompt_version text not null,
  provider text not null,
  model text not null,
  input_hash text not null,
  output_hash text not null,
  key_text text not null,
  created_at text not null,
  unique(thread_revision_id, summary_kind, prompt_version, provider, model)
);

create table sync_runs (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table summary_runs (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table embedding_runs (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table cluster_runs (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table repo_sync_state (
  repo_id integer primary key references repositories(id) on delete cascade,
  last_full_open_scan_started_at text,
  last_overlapping_open_scan_completed_at text,
  last_non_overlapping_scan_completed_at text,
  last_open_close_reconciled_at text,
  updated_at text not null
);

create table cluster_groups (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  stable_key text not null,
  stable_slug text not null,
  status text not null,
  cluster_type text,
  representative_thread_id integer references threads(id) on delete set null,
  title text,
  created_at text not null,
  updated_at text not null,
  closed_at text,
  unique(repo_id, stable_key),
  unique(repo_id, stable_slug)
);

create table portable_metadata (
  key text primary key,
  value text not null
);
