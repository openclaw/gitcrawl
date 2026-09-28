-- Synthetic archive created by the unmodified v0.12.0 store (bd9143104d03).
-- Covers exact IDs, empty submitted review, inline review state/history and sync coverage.
BEGIN TRANSACTION;
CREATE TABLE blobs (
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
CREATE TABLE cluster_aliases (
  cluster_id integer not null references cluster_groups(id) on delete cascade,
  alias_slug text not null,
  reason text not null,
  created_at text not null,
  primary key (cluster_id, alias_slug)
);
CREATE TABLE cluster_closures (
  cluster_id integer primary key references cluster_groups(id) on delete cascade,
  reason text not null,
  actor_kind text not null,
  created_at text not null,
  updated_at text not null
);
CREATE TABLE cluster_events (
  id integer primary key,
  cluster_id integer not null references cluster_groups(id) on delete cascade,
  run_id integer,
  event_type text not null,
  actor_kind text not null,
  payload_json text not null,
  created_at text not null
);
CREATE TABLE cluster_groups (
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
CREATE TABLE cluster_members (
  cluster_id integer not null references clusters(id) on delete cascade,
  thread_id integer not null references threads(id) on delete cascade,
  score_to_representative real,
  created_at text not null,
  primary key (cluster_id, thread_id)
);
CREATE TABLE cluster_memberships (
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
CREATE TABLE cluster_overrides (
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
CREATE TABLE cluster_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);
CREATE TABLE clusters (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  cluster_run_id integer not null references cluster_runs(id) on delete cascade,
  representative_thread_id integer references threads(id) on delete set null,
  member_count integer not null,
  closed_at_local text,
  close_reason_local text,
  created_at text not null
);
CREATE TABLE code_documents (
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
PRAGMA writable_schema=ON;
INSERT INTO sqlite_master(type,name,tbl_name,rootpage,sql)VALUES('table','code_documents_fts','code_documents_fts',0,'CREATE VIRTUAL TABLE code_documents_fts using fts5(
  path,
  language,
  text_content,
  content=''code_documents'',
  content_rowid=''id''
)');
CREATE TABLE 'code_documents_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID;
INSERT INTO "code_documents_fts_config" VALUES('version',4);
CREATE TABLE 'code_documents_fts_data'(id INTEGER PRIMARY KEY, block BLOB);
INSERT INTO "code_documents_fts_data" VALUES(1,X'');
INSERT INTO "code_documents_fts_data" VALUES(10,X'00000000000000');
CREATE TABLE 'code_documents_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
CREATE TABLE 'code_documents_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
CREATE TABLE code_snapshots (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  source_root text not null,
  git_sha text not null,
  worktree_dirty integer not null default 0,
  file_count integer not null,
  byte_count integer not null,
  indexed_at text not null
);
CREATE TABLE comment_revisions (
  id integer primary key,
  comment_id integer not null references comments(id) on delete cascade,
  author_login text,
  author_type text,
  body text not null,
  is_bot integer not null default 0,
  raw_json text not null,
  created_at_gh text,
  updated_at_gh text,
  deleted_at text,
  deletion_reason text,
  recorded_at text not null
);
INSERT INTO "comment_revisions" VALUES(1,1,NULL,NULL,'',0,'{"state":"APPROVED","submitted_at":"2026-01-02T00:00:00Z"}','2026-01-01T00:00:00Z',NULL,NULL,NULL,'2026-09-28T09:20:56.381634Z');
CREATE TABLE comments (
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
  deleted_at text,
  deletion_reason text,
  check (
    (deleted_at is null and deletion_reason is null)
    or (deleted_at is not null and deletion_reason is not null and trim(deletion_reason) <> '')
  ),
  unique(thread_id, comment_type, github_id)
);
INSERT INTO "comments" VALUES(1,1,'9007199254740995','pull_review',NULL,NULL,'',0,'{"state":"APPROVED","submitted_at":"2026-01-02T00:00:00Z"}',NULL,'2026-01-01T00:00:00Z',NULL,NULL,NULL);
CREATE TABLE document_embeddings (
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
CREATE TABLE document_summaries (
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
CREATE TABLE documents (
  id integer primary key,
  thread_id integer not null unique references threads(id) on delete cascade,
  title text not null,
  body text,
  raw_text text not null,
  dedupe_text text not null,
  updated_at text not null
);
INSERT INTO sqlite_master(type,name,tbl_name,rootpage,sql)VALUES('table','documents_fts','documents_fts',0,'CREATE VIRTUAL TABLE documents_fts using fts5(
  title,
  body,
  raw_text,
  dedupe_text,
  content=''documents'',
  content_rowid=''id''
)');
CREATE TABLE 'documents_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID;
INSERT INTO "documents_fts_config" VALUES('version',4);
CREATE TABLE 'documents_fts_data'(id INTEGER PRIMARY KEY, block BLOB);
INSERT INTO "documents_fts_data" VALUES(1,X'');
INSERT INTO "documents_fts_data" VALUES(10,X'00000000000000');
CREATE TABLE 'documents_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
CREATE TABLE 'documents_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
CREATE TABLE embedding_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);
CREATE TABLE github_workflow_runs (
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
CREATE TABLE observation_schema_convergence (
  id integer primary key check (id = 1),
  checked_observation_sequence integer not null
    check (typeof(checked_observation_sequence) = 'integer')
);
INSERT INTO "observation_schema_convergence" VALUES(1,1);
CREATE TABLE pull_request_checks (
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
CREATE TABLE pull_request_commits (
  thread_id integer not null references threads(id) on delete cascade,
  sha text not null,
  message text,
  author_login text,
  author_name text,
  committed_at text,
  html_url text,
  raw_json text not null,
  fetched_at text not null,
  deleted_at text,
  deletion_reason text,
  check (
    (deleted_at is null and deletion_reason is null)
    or (deleted_at is not null and deletion_reason is not null and trim(deletion_reason) <> '')
  ),
  primary key(thread_id, sha)
);
CREATE TABLE pull_request_details (
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
CREATE TABLE pull_request_files (
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
CREATE TABLE pull_request_review_thread_revisions (
  id integer primary key,
  thread_id integer not null,
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
  deleted_at text,
  deletion_reason text,
  recorded_at text not null,
  foreign key(thread_id, review_thread_id)
    references pull_request_review_threads(thread_id, review_thread_id)
    on delete cascade
);
INSERT INTO "pull_request_review_thread_revisions" VALUES(1,1,'PRRT_fixture',NULL,0,0,1,0,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,'[{"id":"9007199254740996","body":"Inline body"}]','{"id":"PRRT_fixture","isResolved":true}','',NULL,NULL,'2026-09-28T09:20:56.381904Z');
CREATE TABLE pull_request_review_thread_syncs (
  thread_id integer primary key references threads(id) on delete cascade,
  fetched_at text not null
);
INSERT INTO "pull_request_review_thread_syncs" VALUES(1,'2026-01-01T00:00:00Z');
CREATE TABLE pull_request_review_threads (
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
  deleted_at text,
  deletion_reason text,
  check (
    (deleted_at is null and deletion_reason is null)
    or (deleted_at is not null and deletion_reason is not null and trim(deletion_reason) <> '')
  ),
  primary key(thread_id, review_thread_id)
);
INSERT INTO "pull_request_review_threads" VALUES(1,'PRRT_fixture',NULL,0,0,1,0,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,'[{"id":"9007199254740996","body":"Inline body"}]','{"id":"PRRT_fixture","isResolved":true}','',NULL,NULL);
CREATE TABLE repo_sync_state (
  repo_id integer primary key references repositories(id) on delete cascade,
  last_full_open_scan_started_at text,
  last_overlapping_open_scan_completed_at text,
  last_non_overlapping_scan_completed_at text,
  last_open_close_reconciled_at text,
  updated_at text not null
);
CREATE TABLE repositories (
  id integer primary key,
  owner text not null,
  name text not null,
  full_name text not null unique,
  github_repo_id text,
  raw_json text not null,
  updated_at text not null
);
INSERT INTO "repositories" VALUES(1,'fixture','repo','fixture/repo','9007199254740993','{"node_id":"R_fixture"}','2026-01-01T00:00:00Z');
CREATE TABLE similarity_edges (
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
CREATE TABLE summary_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);
CREATE TABLE sync_attempt_failures (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  thread_id integer references threads(id) on delete set null,
  number integer not null,
  operation text not null,
  error_class text not null,
  error_message text not null,
  first_seen_at text not null,
  last_seen_at text not null,
  retry_count integer not null default 0,
  resolved_at text,
  unique(repo_id, number, operation, error_class)
);
CREATE TABLE sync_runs (
  id integer primary key,
  repo_id integer references repositories(id) on delete cascade,
  scope text not null,
  status text not null,
  started_at text not null,
  finished_at text,
  stats_json text,
  error_text text
);
INSERT INTO "sync_runs" VALUES(1,1,'all','success','2026-01-01T00:00:00Z','2026-01-01T00:01:00Z','{"fixture":"retained coverage"}','');
CREATE TABLE thread_changed_files (
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
CREATE TABLE thread_child_observation_memberships (
  thread_id integer not null references threads(id) on delete cascade,
  family text not null,
  observation_sequence integer not null
    check (typeof(observation_sequence) = 'integer' and observation_sequence > 0),
  member_ids_json text not null,
  primary key(thread_id, family)
);
CREATE TABLE thread_child_observation_reservations (
  thread_id integer not null references threads(id) on delete cascade,
  family text not null check (family in (
    'comments',
    'pull_request_details',
    'pull_request_files',
    'pull_request_commits',
    'pull_request_checks',
    'pull_request_review_threads'
  )),
  source_updated_at text not null default '',
  observation_sequence integer not null
    check (typeof(observation_sequence) = 'integer' and observation_sequence > 0),
  primary key(thread_id, family)
);
INSERT INTO "thread_child_observation_reservations" VALUES(1,'comments','2026-01-01T00:00:00Z',1);
INSERT INTO "thread_child_observation_reservations" VALUES(1,'pull_request_details','2026-01-01T00:00:00Z',1);
INSERT INTO "thread_child_observation_reservations" VALUES(1,'pull_request_files','2026-01-01T00:00:00Z',1);
INSERT INTO "thread_child_observation_reservations" VALUES(1,'pull_request_commits','2026-01-01T00:00:00Z',1);
INSERT INTO "thread_child_observation_reservations" VALUES(1,'pull_request_checks','2026-01-01T00:00:00Z',1);
INSERT INTO "thread_child_observation_reservations" VALUES(1,'pull_request_review_threads','2026-01-01T00:00:00Z',1);
CREATE TABLE thread_code_snapshots (
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
CREATE TABLE thread_fingerprints (
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
CREATE TABLE thread_hunk_signatures (
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
CREATE TABLE thread_key_summaries (
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
CREATE TABLE thread_observation_sequence (
  id integer primary key check (id = 1),
  value integer not null check (typeof(value) = 'integer' and value >= 0),
  last_started_at text not null
);
INSERT INTO "thread_observation_sequence" VALUES(1,2,'2026-01-01T00:00:00Z');
CREATE TABLE thread_revisions (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  source_updated_at text,
  content_hash text not null,
  title_hash text not null,
  body_hash text not null,
  labels_hash text not null,
  raw_json_blob_id integer references blobs(id) on delete set null,
  observation_sequence integer not null default 0
    check (typeof(observation_sequence) = 'integer' and observation_sequence >= 0),
  created_at text not null
);
CREATE TABLE thread_vectors (
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
CREATE TABLE threads (
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
  observation_sequence integer not null default 0
    check (
      typeof(observation_sequence) = 'integer'
      and observation_sequence >= -9223372036854775807
    ),
  evidence_observation_sequence integer not null default 0
    check (typeof(evidence_observation_sequence) = 'integer' and evidence_observation_sequence >= 0),
  evidence_source_updated_at text not null default '',
  updated_at text not null,
  unique(repo_id, kind, number)
);
INSERT INTO "threads" VALUES(1,1,'9007199254740994',1,'pull_request','open','Migration fixture','Canonical body',NULL,NULL,NULL,'https://github.com/fixture/repo/pull/1','[]','[]','{"node_id":"PR_fixture"}','fixture',0,NULL,'2026-01-01T00:00:00Z',NULL,NULL,NULL,NULL,NULL,NULL,2,2,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
CREATE TABLE workflow_run_observation_reservations (
  repo_id integer not null references repositories(id) on delete cascade,
  head_sha text not null check (trim(head_sha) <> ''),
  source_updated_at text not null default '',
  observation_sequence integer not null
    check (typeof(observation_sequence) = 'integer' and observation_sequence > 0),
  primary key(repo_id, head_sha)
);
CREATE TRIGGER documents_ai after insert on documents begin
  insert into documents_fts(rowid, title, body, raw_text, dedupe_text)
  values (new.id, new.title, new.body, new.raw_text, new.dedupe_text);
end;
CREATE TRIGGER documents_ad after delete on documents begin
  insert into documents_fts(documents_fts, rowid, title, body, raw_text, dedupe_text)
  values ('delete', old.id, old.title, old.body, old.raw_text, old.dedupe_text);
end;
CREATE TRIGGER documents_au after update on documents begin
  insert into documents_fts(documents_fts, rowid, title, body, raw_text, dedupe_text)
  values ('delete', old.id, old.title, old.body, old.raw_text, old.dedupe_text);
  insert into documents_fts(rowid, title, body, raw_text, dedupe_text)
  values (new.id, new.title, new.body, new.raw_text, new.dedupe_text);
end;
CREATE TRIGGER code_documents_ai after insert on code_documents begin
  insert into code_documents_fts(rowid, path, language, text_content)
  values (new.id, new.path, new.language, new.text_content);
end;
CREATE TRIGGER code_documents_ad after delete on code_documents begin
  insert into code_documents_fts(code_documents_fts, rowid, path, language, text_content)
  values ('delete', old.id, old.path, old.language, old.text_content);
end;
CREATE TRIGGER code_documents_au after update on code_documents begin
  insert into code_documents_fts(code_documents_fts, rowid, path, language, text_content)
  values ('delete', old.id, old.path, old.language, old.text_content);
  insert into code_documents_fts(rowid, path, language, text_content)
  values (new.id, new.path, new.language, new.text_content);
end;
CREATE INDEX idx_threads_repo_number on threads(repo_id, number);
CREATE INDEX idx_threads_repo_state_closed on threads(repo_id, state, closed_at_local);
CREATE INDEX idx_threads_repo_updated on threads(repo_id, updated_at);
CREATE INDEX idx_comments_thread_type on comments(thread_id, comment_type);
CREATE INDEX idx_comment_revisions_comment on comment_revisions(comment_id, id);
CREATE INDEX idx_thread_revisions_thread_created on thread_revisions(thread_id, created_at);
CREATE INDEX idx_thread_changed_files_path on thread_changed_files(path);
CREATE INDEX idx_pull_request_details_repo_number on pull_request_details(repo_id, number);
CREATE INDEX idx_pull_request_files_path on pull_request_files(path);
CREATE INDEX idx_pull_request_files_thread_path on pull_request_files(thread_id, path);
CREATE INDEX idx_pull_request_checks_thread_status on pull_request_checks(thread_id, status, conclusion);
CREATE INDEX idx_pull_request_review_threads_thread_resolved on pull_request_review_threads(thread_id, is_resolved);
CREATE INDEX idx_pull_request_review_thread_revisions_thread on pull_request_review_thread_revisions(thread_id, review_thread_id, id);
CREATE INDEX idx_pull_request_review_thread_syncs_fetched on pull_request_review_thread_syncs(fetched_at);
CREATE INDEX idx_github_workflow_runs_repo_branch on github_workflow_runs(repo_id, head_branch, run_id);
CREATE INDEX idx_github_workflow_runs_repo_sha on github_workflow_runs(repo_id, head_sha, run_id);
CREATE INDEX idx_code_snapshots_repo_indexed on code_snapshots(repo_id, indexed_at, id);
CREATE INDEX idx_code_documents_repo_path on code_documents(repo_id, path);
CREATE INDEX idx_thread_fingerprints_hash on thread_fingerprints(fingerprint_hash);
CREATE INDEX idx_thread_vectors_basis_model on thread_vectors(basis, model);
CREATE INDEX idx_sync_runs_repo_status_id on sync_runs(repo_id, status, id);
CREATE INDEX idx_sync_attempt_failures_repo_unresolved on sync_attempt_failures(repo_id, resolved_at, last_seen_at);
CREATE INDEX idx_sync_attempt_failures_thread on sync_attempt_failures(thread_id, resolved_at);
CREATE INDEX idx_cluster_runs_repo_status_id on cluster_runs(repo_id, status, id);
CREATE INDEX idx_similarity_edges_repo_score on similarity_edges(repo_id, score);
CREATE INDEX idx_cluster_groups_repo_status on cluster_groups(repo_id, status);
CREATE INDEX idx_cluster_memberships_thread_state on cluster_memberships(thread_id, state);
CREATE INDEX idx_cluster_events_cluster_created on cluster_events(cluster_id, created_at);
CREATE INDEX idx_thread_revisions_thread_observation
		on thread_revisions(thread_id, observation_sequence desc)
	;
CREATE TRIGGER observation_convergence_threads_insert
after insert on threads
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_threads_update
after update on threads
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_threads_delete
after delete on threads
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_revisions_insert
after insert on thread_revisions
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_revisions_update
after update on thread_revisions
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_revisions_delete
after delete on thread_revisions
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_pr_details_insert
after insert on pull_request_details
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_pr_details_update
after update on pull_request_details
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_pr_details_delete
after delete on pull_request_details
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_children_insert
after insert on thread_child_observation_reservations
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_children_update
after update on thread_child_observation_reservations
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_children_delete
after delete on thread_child_observation_reservations
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_workflows_insert
after insert on workflow_run_observation_reservations
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_workflows_update
after update on workflow_run_observation_reservations
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_workflows_delete
after delete on workflow_run_observation_reservations
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_allocator_insert
after insert on thread_observation_sequence
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_allocator_update
after update on thread_observation_sequence
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
CREATE TRIGGER observation_convergence_allocator_delete
after delete on thread_observation_sequence
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end;
PRAGMA writable_schema=OFF;
COMMIT;
PRAGMA user_version=13;
