package store

const schemaSQL = `
pragma foreign_keys = on;
pragma journal_mode = wal;
pragma busy_timeout = 5000;

create table if not exists repositories (
  id integer primary key,
  owner text not null,
  name text not null,
  full_name text not null unique,
  github_repo_id text,
  raw_json text not null,
  updated_at text not null
);

create table if not exists threads (
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
  updated_at text not null,
  unique(repo_id, kind, number)
);

create table if not exists comments (
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

create table if not exists blobs (
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

create table if not exists thread_revisions (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  source_updated_at text,
  content_hash text not null,
  title_hash text not null,
  body_hash text not null,
  labels_hash text not null,
  raw_json_blob_id integer references blobs(id) on delete set null,
  created_at text not null,
  unique(thread_id, content_hash)
);

create table if not exists thread_code_snapshots (
  id integer primary key,
  thread_revision_id integer not null unique references thread_revisions(id) on delete cascade,
  base_sha text,
  head_sha text,
  files_changed integer not null default 0,
  additions integer not null default 0,
  deletions integer not null default 0,
  patch_digest text,
  raw_diff_blob_id integer references blobs(id) on delete set null,
  created_at text not null
);

create table if not exists thread_changed_files (
  snapshot_id integer not null references thread_code_snapshots(id) on delete cascade,
  path text not null,
  status text,
  additions integer not null default 0,
  deletions integer not null default 0,
  previous_path text,
  patch_blob_id integer references blobs(id) on delete set null,
  patch_hash text,
  primary key (snapshot_id, path)
);

create table if not exists thread_hunk_signatures (
  id integer primary key,
  snapshot_id integer not null references thread_code_snapshots(id) on delete cascade,
  path text not null,
  hunk_hash text not null,
  context_hash text not null,
  added_token_hash text not null,
  removed_token_hash text not null,
  created_at text not null,
  unique(snapshot_id, path, hunk_hash)
);

create table if not exists documents (
  id integer primary key,
  thread_id integer not null unique references threads(id) on delete cascade,
  title text not null,
  body text,
  raw_text text not null,
  dedupe_text text not null,
  updated_at text not null
);

create virtual table if not exists documents_fts using fts5(
  title,
  body,
  raw_text,
  dedupe_text,
  content='documents',
  content_rowid='id'
);

create trigger if not exists documents_ai after insert on documents begin
  insert into documents_fts(rowid, title, body, raw_text, dedupe_text)
  values (new.id, new.title, new.body, new.raw_text, new.dedupe_text);
end;

create trigger if not exists documents_ad after delete on documents begin
  insert into documents_fts(documents_fts, rowid, title, body, raw_text, dedupe_text)
  values ('delete', old.id, old.title, old.body, old.raw_text, old.dedupe_text);
end;

create trigger if not exists documents_au after update on documents begin
  insert into documents_fts(documents_fts, rowid, title, body, raw_text, dedupe_text)
  values ('delete', old.id, old.title, old.body, old.raw_text, old.dedupe_text);
  insert into documents_fts(rowid, title, body, raw_text, dedupe_text)
  values (new.id, new.title, new.body, new.raw_text, new.dedupe_text);
end;

create table if not exists document_summaries (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  summary_kind text not null,
  provider text not null default 'openai',
  model text not null,
  prompt_version text not null default 'v1',
  content_hash text not null,
  summary_text text not null,
  created_at text not null,
  updated_at text not null,
  unique(thread_id, summary_kind, model)
);

create table if not exists document_embeddings (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  source_kind text not null,
  model text not null,
  dimensions integer not null,
  content_hash text not null,
  embedding_json text not null,
  created_at text not null,
  updated_at text not null,
  unique(thread_id, source_kind, model)
);

create table if not exists thread_vectors (
  thread_id integer primary key references threads(id) on delete cascade,
  basis text not null,
  model text not null,
  dimensions integer not null,
  content_hash text not null,
  vector_json text not null,
  vector_backend text not null,
  created_at text not null,
  updated_at text not null
);

create table if not exists thread_fingerprints (
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

create table if not exists sync_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table if not exists summary_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table if not exists embedding_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table if not exists cluster_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);

create table if not exists repo_sync_state (
  repo_id integer primary key references repositories(id) on delete cascade,
  last_full_open_scan_started_at text,
  last_overlapping_open_scan_completed_at text,
  last_non_overlapping_scan_completed_at text,
  last_open_close_reconciled_at text,
  updated_at text not null
);

create table if not exists similarity_edges (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  cluster_run_id integer references cluster_runs(id) on delete cascade,
  left_thread_id integer not null references threads(id) on delete cascade,
  right_thread_id integer not null references threads(id) on delete cascade,
  method text not null,
  score real not null,
  explanation_json text not null,
  created_at text not null,
  unique(cluster_run_id, left_thread_id, right_thread_id)
);

create table if not exists clusters (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  cluster_run_id integer not null references cluster_runs(id) on delete cascade,
  representative_thread_id integer references threads(id) on delete set null,
  member_count integer not null,
  closed_at_local text,
  close_reason_local text,
  created_at text not null
);

create table if not exists cluster_members (
  cluster_id integer not null references clusters(id) on delete cascade,
  thread_id integer not null references threads(id) on delete cascade,
  score_to_representative real,
  created_at text not null,
  primary key (cluster_id, thread_id)
);

create table if not exists cluster_groups (
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

create table if not exists cluster_memberships (
  cluster_id integer not null references cluster_groups(id) on delete cascade,
  thread_id integer not null references threads(id) on delete cascade,
  role text not null,
  state text not null,
  score_to_representative real,
  first_seen_run_id integer,
  last_seen_run_id integer,
  added_by text not null,
  removed_by text,
  added_reason_json text not null,
  removed_reason_json text,
  created_at text not null,
  updated_at text not null,
  removed_at text,
  primary key (cluster_id, thread_id)
);

create table if not exists cluster_overrides (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  cluster_id integer not null references cluster_groups(id) on delete cascade,
  thread_id integer not null references threads(id) on delete cascade,
  action text not null,
  reason text,
  created_at text not null,
  expires_at text,
  unique(cluster_id, thread_id, action)
);

create table if not exists cluster_events (
  id integer primary key,
  cluster_id integer not null references cluster_groups(id) on delete cascade,
  run_id integer,
  event_type text not null,
  actor_kind text not null,
  payload_json text not null,
  created_at text not null
);

create table if not exists cluster_aliases (
  cluster_id integer not null references cluster_groups(id) on delete cascade,
  alias_slug text not null,
  reason text not null,
  created_at text not null,
  primary key (cluster_id, alias_slug)
);

create table if not exists cluster_closures (
  cluster_id integer primary key references cluster_groups(id) on delete cascade,
  reason text not null,
  actor_kind text not null,
  created_at text not null,
  updated_at text not null
);

create index if not exists idx_threads_repo_number on threads(repo_id, number);
create index if not exists idx_threads_repo_state_closed on threads(repo_id, state, closed_at_local);
create index if not exists idx_threads_repo_updated on threads(repo_id, updated_at);
create index if not exists idx_comments_thread_type on comments(thread_id, comment_type);
create index if not exists idx_thread_revisions_thread_created on thread_revisions(thread_id, created_at);
create index if not exists idx_thread_changed_files_path on thread_changed_files(path);
create index if not exists idx_thread_fingerprints_hash on thread_fingerprints(fingerprint_hash);
create index if not exists idx_thread_vectors_basis_model on thread_vectors(basis, model);
create index if not exists idx_sync_runs_repo_status_id on sync_runs(repo_id, status, id);
create index if not exists idx_cluster_runs_repo_status_id on cluster_runs(repo_id, status, id);
create index if not exists idx_similarity_edges_repo_score on similarity_edges(repo_id, score);
create index if not exists idx_cluster_groups_repo_status on cluster_groups(repo_id, status);
create index if not exists idx_cluster_memberships_thread_state on cluster_memberships(thread_id, state);
create index if not exists idx_cluster_events_cluster_created on cluster_events(cluster_id, created_at);
`
