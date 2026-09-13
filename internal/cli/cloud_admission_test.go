package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/openclaw/gitcrawl/internal/config"
	portableexport "github.com/openclaw/gitcrawl/internal/portable"
	crawlstore "github.com/openclaw/gitcrawl/internal/store"
)

func TestGitcrawlCloudAdmissionPolicy(t *testing.T) {
	for _, options := range []gitcrawlCloudPublishOptions{
		{},
		{AllowIncomplete: true},
		{ObservationOrder: true},
		{AdmissionPolicy: gitcrawlArchiveAdmissionPolicy, ObservationOrder: true},
	} {
		if err := options.validate(); err != nil {
			t.Errorf("valid options %+v: %v", options, err)
		}
	}
	for _, options := range []gitcrawlCloudPublishOptions{
		{AdmissionPolicy: "unknown", ObservationOrder: true},
		{AdmissionPolicy: gitcrawlArchiveAdmissionPolicy},
		{AdmissionPolicy: gitcrawlArchiveAdmissionPolicy, ObservationOrder: true, AllowIncomplete: true},
	} {
		if err := options.validate(); err == nil {
			t.Errorf("invalid options accepted: %+v", options)
		}
	}
}

func TestCanonicalGitcrawlCloudWarnings(t *testing.T) {
	values := []string{
		"gitcrawl.archive.source.unknown",
		"gitcrawl.archive.enrichment.revisions.incomplete",
	}
	before := slices.Clone(values)
	got, err := canonicalGitcrawlCloudWarnings(values)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(values, before) || !slices.Equal(got, []string{before[1], before[0]}) {
		t.Fatalf("canonical=%v input=%v", got, values)
	}
	for _, empty := range [][]string{nil, {}} {
		got, err := canonicalGitcrawlCloudWarnings(empty)
		encoded, _ := json.Marshal(got)
		if err != nil || string(encoded) != "[]" {
			t.Fatalf("empty warnings = %s, %v", encoded, err)
		}
	}
	for name, invalid := range map[string][]string{
		"unknown":       {"arbitrary provider text"},
		"duplicate":     {values[0], values[0]},
		"count":         make([]string, gitcrawlCloudWarningsMaxElements+1),
		"bytes":         {strings.Repeat("x", gitcrawlCloudWarningsMaxBytes)},
		"json escaping": {strings.Repeat("\n", gitcrawlCloudWarningsMaxBytes/2)},
		"UTF-8 bytes":   {strings.Repeat("\u754c", gitcrawlCloudWarningsMaxBytes/3)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := canonicalGitcrawlCloudWarnings(invalid); err == nil {
				t.Fatal("invalid warnings accepted")
			}
		})
	}
}

func TestArchiveAdmissionFlagsRejectBeforeOpeningRuntime(t *testing.T) {
	for _, flags := range [][]string{
		{"--admission-policy=unknown"},
		{"--admission-policy=archive-v1"},
		{"--admission-policy=archive-v1", "--observation-order", "--allow-incomplete"},
	} {
		args := append([]string{"--config", filepath.Join(t.TempDir(), "missing.toml"), "cloud", "publish"}, flags...)
		if err := New().Run(context.Background(), args); err == nil || !strings.Contains(err.Error(), "admission") {
			t.Fatalf("flags %v did not reject policy first: %v", flags, err)
		}
	}
}

func seedArchiveAdmissionFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	st, err := crawlstore.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.DB().Exec(`
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1,'example','repo','example/repo','{"private":"private-admission-marker"}','2026-09-01T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body, html_url,
			labels_json, assignees_json, raw_json, content_hash, updated_at_gh, observation_sequence, updated_at)
		values(1,1,'9007199254740993',1,'pull_request','open','Archive','Full canonical body',
			'https://github.com/example/repo/pull/1','[]','[]','{}','body-hash','2026-09-01T00:00:00Z',1,'2026-09-01T00:00:00Z');
		insert into comments(id, thread_id, github_id, comment_type, body, raw_json)
		values(1,1,'9007199254740995','issue','Full canonical comment','{"private":"private-admission-marker"}');
		insert into comment_revisions(id, comment_id, body, raw_json, recorded_at)
		values(1,1,'Historical canonical body','{"private":"private-admission-marker"}','2026-09-01T00:00:00Z');
		insert into pull_request_files(thread_id, position, path, patch, raw_json, fetched_at)
		values(1,0,'example.go','@@ -1 +1 @@ original patch','{}','2026-09-01T00:00:00Z');
		insert into pull_request_commits(thread_id,sha,message,raw_json,fetched_at)
		values(1,'fixture-commit','Canonical commit subject','{"private":"private-admission-marker"}','2026-09-01T00:00:00Z');
		insert into pull_request_checks(thread_id,name,raw_json,fetched_at)
		values(1,'Canonical check','{"private":"private-admission-marker"}','2026-09-01T00:00:00Z');
		create table portable_metadata(key text primary key, value text not null);
		insert into portable_metadata(key,value) values('cloud_admission_v1','untrusted-old-marker'),
			('exported_at','2099-01-01T00:00:00Z'), ('source_path','private-admission-marker');
	`); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestArchiveAdmissionFreezesDeterministicEvidence(t *testing.T) {
	ctx := context.Background()
	sourcePath := seedArchiveAdmissionFixture(t)
	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sourceHash, err := cloudFileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	options := gitcrawlCloudPublishOptions{AdmissionPolicy: gitcrawlArchiveAdmissionPolicy, ObservationOrder: true}
	var firstHash string
	for pass := 0; pass < 2; pass++ {
		path, admission, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, "", options)
		if err != nil {
			t.Fatal(err)
		}
		defer cleanup()
		frozen, err := crawlstore.OpenReadOnlyImmutable(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		defer frozen.Close()
		snapshot, err := buildGitcrawlCloudSnapshot(ctx, frozen.DB(), path, options, admission)
		if err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			firstHash = snapshot.ID
		} else if snapshot.ID != firstHash {
			t.Fatal("identical input changed snapshot digest")
		}
		if len(snapshot.Datasets) != 9 || snapshot.SourceSyncAt != "" {
			t.Fatalf("snapshot source/datasets = %+v", snapshot)
		}
		if !slices.Contains(snapshot.Capabilities, gitcrawlArchiveAdmissionCapability) ||
			!slices.Contains(snapshot.Warnings, "gitcrawl.archive.source.unknown") ||
			!slices.Contains(snapshot.Warnings, "gitcrawl.archive.source.incomplete") {
			t.Fatalf("missing admission identity: %+v", snapshot)
		}
		if admission.Source.Repositories[0].Inventory.State != crawlstore.ArchiveObservationMissing {
			t.Fatal("export clock was treated as source evidence")
		}
		if admission.Enrichment.Revisions.Complete || admission.Enrichment.Summaries.Complete {
			t.Fatal("admission masked missing enrichment")
		}
		revision := snapshot.Datasets[2]
		if revision.Complete || revision.EligibleCount != 1 || revision.CoveredCount != 0 {
			t.Fatalf("revision coverage = %+v", revision)
		}
		encoded, _ := json.Marshal(admission)
		var evidence string
		if err := frozen.DB().QueryRow(`select value from portable_metadata where key = ?`, gitcrawlCloudAdmissionMetadataKey).Scan(&evidence); err != nil {
			t.Fatal(err)
		}
		if evidence != string(encoded) || strings.Contains(evidence, snapshot.ID) {
			t.Fatal("metadata is not deterministic assessment without self-reference")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("private-admission-marker")) || bytes.Contains(data, []byte("untrusted-old-marker")) {
			t.Fatal("snapshot leaked private or stale admission evidence")
		}
		for _, canonical := range []string{"Full canonical body", "Full canonical comment", "Historical canonical body", "@@ -1 +1 @@ original patch", "Canonical commit subject", "Canonical check"} {
			if !bytes.Contains(data, []byte(canonical)) {
				t.Fatalf("lost canonical content %q", canonical)
			}
		}
		manifest := gitcrawlCloudManifest("example/archive", snapshot)
		if !gitcrawlCloudWarningsMatch(manifest.Warnings, admission.Warnings) {
			t.Fatal("manifest lost warnings")
		}
		admission.Source.RemoteFreshness = crawlstore.ArchiveObservationComplete
		if _, err := buildGitcrawlCloudSnapshot(ctx, frozen.DB(), path, options, admission); err == nil {
			t.Fatal("mutated assessment was accepted")
		}
	}
	after, err := cloudFileSHA256(sourcePath)
	if err != nil || after != sourceHash {
		t.Fatalf("source changed: %s %v", after, err)
	}

	strictPath, admission, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, "", gitcrawlCloudPublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if admission != nil {
		t.Fatal("strict path inherited source admission")
	}
	strict, err := crawlstore.OpenReadOnlyImmutable(ctx, strictPath)
	if err != nil {
		t.Fatal(err)
	}
	defer strict.Close()
	var markers int
	if err := strict.DB().QueryRow(`select count(*) from portable_metadata where key = ?`, gitcrawlCloudAdmissionMetadataKey).Scan(&markers); err != nil || markers != 0 {
		t.Fatal("strict path retained stale policy marker")
	}
	if _, err := buildGitcrawlCloudSnapshot(ctx, strict.DB(), strictPath, gitcrawlCloudPublishOptions{}, nil); err == nil || !strings.Contains(err.Error(), "enrichment is incomplete") {
		t.Fatalf("strict default changed: %v", err)
	}
	if _, err := buildGitcrawlCloudSnapshot(ctx, strict.DB(), strictPath, gitcrawlCloudPublishOptions{AllowIncomplete: true}, nil); err != nil {
		t.Fatalf("allow-incomplete changed: %v", err)
	}
}

func TestArchiveAdmissionRejectsIntegrityFailures(t *testing.T) {
	for _, test := range []struct{ name, sql, want string }{
		{"missing dataset", `drop table thread_fingerprints`, "thread_fingerprints"},
		{"missing canonical history", `drop table comment_revisions`, "canonical comment_revisions"},
		{"missing repositories", `delete from repositories`, "no repositories"},
		{"orphan revision", `pragma foreign_keys=off; insert into thread_revisions(id,thread_id,content_hash,title_hash,body_hash,labels_hash,created_at) values(1,99,'h','h','h','h','2026-09-01T00:00:00Z')`, "referential closure"},
		{"cross repo details", `pragma foreign_keys=off; insert into pull_request_details(thread_id,repo_id,number,raw_json,fetched_at,updated_at) values(1,99,1,'{}','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')`, "referential closure"},
		{"truncated body", `alter table threads add column body_length integer not null default 999`, "truncated threads"},
		{"orphan review history without declared FK", `create table loose_history as select * from pull_request_review_thread_revisions;
			drop table pull_request_review_thread_revisions; alter table loose_history rename to pull_request_review_thread_revisions;
			insert into pull_request_review_thread_revisions(thread_id,review_thread_id,first_comment_body,comments_json,raw_json,fetched_at,recorded_at)
			values(1,'missing','history','[]','{}','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')`, "referential closure"},
		{"review history points at another PR", `create table loose_history as select * from pull_request_review_thread_revisions;
			drop table pull_request_review_thread_revisions; alter table loose_history rename to pull_request_review_thread_revisions;
			insert into threads(id,repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at)
			values(2,1,'2',2,'pull_request','open','Other PR','body','https://github.com/example/repo/pull/2','[]','[]','{}','h','2026-09-01T00:00:00Z');
			insert into pull_request_review_threads(thread_id,review_thread_id,comments_json,raw_json,fetched_at)
			values(2,'other-pr-review','[]','{}','2026-09-01T00:00:00Z');
			insert into pull_request_review_thread_revisions(thread_id,review_thread_id,first_comment_body,comments_json,raw_json,fetched_at,recorded_at)
			values(1,'other-pr-review','history','[]','{}','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')`, "referential closure"},
		{"orphan child observation without declared FK", `create table loose_reservations as select * from thread_child_observation_reservations;
			drop table thread_child_observation_reservations; alter table loose_reservations rename to thread_child_observation_reservations;
			insert into thread_child_observation_reservations(thread_id,family,source_updated_at,observation_sequence) values(99,'comments','',1)`, "referential closure"},
		{"orphan review sync without declared FK", `create table loose_syncs as select * from pull_request_review_thread_syncs;
			drop table pull_request_review_thread_syncs; alter table loose_syncs rename to pull_request_review_thread_syncs;
			insert into pull_request_review_thread_syncs(thread_id,fetched_at) values(99,'2026-09-01T00:00:00Z')`, "referential closure"},
		{"orphan legacy summary without declared FK", `create table loose_summaries as select * from document_summaries;
			drop table document_summaries; alter table loose_summaries rename to document_summaries;
			insert into document_summaries(thread_id,summary_text) values(99,'orphan')`, "referential closure"},
		{"orphan commit without declared FK", `create table loose_commits as select * from pull_request_commits;
			drop table pull_request_commits; alter table loose_commits rename to pull_request_commits;
			insert into pull_request_commits(thread_id, sha, raw_json, fetched_at) values(99,'orphan','{}','2026-09-01T00:00:00Z')`, "referential closure"},
		{"orphan check without declared FK", `create table loose_checks as select * from pull_request_checks;
			drop table pull_request_checks; alter table loose_checks rename to pull_request_checks;
			insert into pull_request_checks(thread_id, name, raw_json, fetched_at) values(99,'orphan','{}','2026-09-01T00:00:00Z')`, "referential closure"},
		{"orphan workflow without declared FK", `create table loose_workflows as select * from github_workflow_runs;
			drop table github_workflow_runs; alter table loose_workflows rename to github_workflow_runs;
			insert into github_workflow_runs(repo_id, run_id, raw_json, fetched_at) values(99,'orphan','{}','2026-09-01T00:00:00Z')`, "referential closure"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := seedArchiveAdmissionFixture(t)
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(test.sql); err != nil {
				t.Fatal(err)
			}
			_, _, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, "", gitcrawlCloudPublishOptions{AdmissionPolicy: gitcrawlArchiveAdmissionPolicy, ObservationOrder: true})
			defer cleanup()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestArchiveAdmissionEvidenceChangesDigest(t *testing.T) {
	ctx := context.Background()
	path := seedArchiveAdmissionFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	options := gitcrawlCloudPublishOptions{AdmissionPolicy: gitcrawlArchiveAdmissionPolicy, ObservationOrder: true}
	digest := func() string {
		t.Helper()
		frozen, _, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, "", options)
		if err != nil {
			t.Fatal(err)
		}
		defer cleanup()
		hash, err := cloudFileSHA256(frozen)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	if _, err := db.Exec(`insert into sync_runs(repo_id, scope, status, started_at, finished_at, stats_json) values(
		1,'all','success','2026-09-01T00:00:00Z','2026-09-01T00:01:00Z',
		'{"started_at":"2026-09-01T00:00:00Z","finished_at":"2026-09-01T00:01:00Z","metadata_only":false,"threads_synced":1}')`); err != nil {
		t.Fatal(err)
	}
	before := digest()
	// Stats are scrubbed from the artifact. Only derived admission evidence can
	// make this bounds change visible in the final sanitized bytes.
	if _, err := db.Exec(`update sync_runs set stats_json = json_set(stats_json, '$.limit', 1)`); err != nil {
		t.Fatal(err)
	}
	if after := digest(); before == after {
		t.Fatal("source assessment change did not change artifact digest")
	}
}

func TestArchiveAdmissionSourceSyncRequiresEveryRepositoryObservation(t *testing.T) {
	admission := gitcrawlCloudAdmission{Source: crawlstore.ArchiveSourceAssessment{
		Repositories: []crawlstore.ArchiveRepositoryObservations{
			{RepoID: 1, Inventory: crawlstore.ArchiveInventoryObservation{Successful: true, FinishedAt: "2026-09-01T00:00:00Z"}},
			{RepoID: 2, Inventory: crawlstore.ArchiveInventoryObservation{Successful: true, FinishedAt: "2026-09-02T00:00:00Z"}},
		},
	}}
	if got := admission.sourceSyncAt(); got != "2026-09-01T00:00:00Z" {
		t.Fatalf("source sync = %q, want oldest repository observation", got)
	}
	admission.Source.Repositories[0].Inventory.Successful = false
	if got := admission.sourceSyncAt(); got != "" {
		t.Fatalf("failed repository became a source sync: %q", got)
	}
	admission.Source.Repositories[0].Inventory.Successful = true
	admission.Source.Repositories[0].Inventory.FinishedAt = ""
	if got := admission.sourceSyncAt(); got != "" {
		t.Fatalf("missing observation inherited another repository clock: %q", got)
	}
}

func TestArchiveAdmissionRejectsLossyPortableExportBeforeHTTP(t *testing.T) {
	ctx := context.Background()
	sourcePath := seedArchiveAdmissionFixture(t)
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	longBody := strings.Repeat("canonical history and review body ", 16)
	if _, err := source.Exec(`update threads set body = 'short';
		update comments set body = 'short';
		update comment_revisions set body = ?;
		insert into pull_request_review_threads(thread_id, review_thread_id, first_comment_body, comments_json, raw_json, fetched_at)
		values(1, 'review-1', ?, '[]', '{}', '2026-09-01T00:00:00Z');
		insert into pull_request_review_thread_revisions(thread_id, review_thread_id, first_comment_body,
			comments_json, raw_json, fetched_at, recorded_at)
		values(1, 'review-1', ?, '[]', '{}', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')`,
		longBody, longBody, longBody); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	sourceHash, err := cloudFileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	exported, err := portableexport.Export(ctx, portableexport.ExportOptions{
		SourceDBPath: sourcePath, OutputDir: filepath.Join(t.TempDir(), "export"),
		DatabaseName: "portable.db", PublicPath: "db/portable.db",
		Profile: portableexport.CurrentStateV1, BodyChars: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	portable, err := crawlstore.OpenReadOnlyImmutable(ctx, exported.DatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer portable.Close()
	var truncatedCurrent, historyRows, reviewLength, reviewHistoryLength, patches int
	if err := portable.DB().QueryRow(`select
		(select count(*) from threads where body_length > length(coalesce(body, ''))) +
			(select count(*) from comments where body_length > length(coalesce(body, ''))),
		(select count(*) from comment_revisions),
		(select length(first_comment_body) from pull_request_review_threads limit 1),
		(select length(first_comment_body) from pull_request_review_thread_revisions limit 1),
		(select count(*) from pull_request_files where patch is not null and patch != '')`).Scan(
		&truncatedCurrent, &historyRows, &reviewLength, &reviewHistoryLength, &patches); err != nil {
		t.Fatal(err)
	}
	if truncatedCurrent != 0 || historyRows != 0 || reviewLength != 32 || reviewHistoryLength != 32 || patches != 0 {
		t.Fatalf("native portable loss fixture: current=%d history=%d review=%d review_history=%d patches=%d",
			truncatedCurrent, historyRows, reviewLength, reviewHistoryLength, patches)
	}

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()
	cfg := config.Default()
	cfg.DBPath = exported.DatabasePath
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_TEST_LOSSY_ARCHIVE_TOKEN", "fixture-token")
	err = New().Run(ctx, []string{
		"--config", configPath, "cloud", "publish", "--remote", server.URL,
		"--archive", "example/archive", "--token-env", "GITCRAWL_TEST_LOSSY_ARCHIVE_TOKEN",
		"--admission-policy=archive-v1", "--observation-order", "--stage-only", "--json",
	})
	if err == nil || !strings.Contains(err.Error(), "lossy portable profile") {
		t.Fatalf("lossy native export admission error = %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("lossy archive made %d HTTP requests", requests.Load())
	}
	after, err := cloudFileSHA256(sourcePath)
	if err != nil || after != sourceHash {
		t.Fatalf("original archive changed: %s %v", after, err)
	}
}

func TestArchiveAdmissionAcceptsFullRuntimeWithoutPatches(t *testing.T) {
	for _, statement := range []string{
		`update pull_request_files set patch = null`,
		`delete from pull_request_files`,
	} {
		t.Run(statement, func(t *testing.T) {
			ctx := context.Background()
			path := seedArchiveAdmissionFixture(t)
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			_, admission, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, "", gitcrawlCloudPublishOptions{
				AdmissionPolicy: gitcrawlArchiveAdmissionPolicy, ObservationOrder: true,
			})
			defer cleanup()
			if err != nil || admission == nil || !admission.Integrity.FullBodies {
				t.Fatalf("full runtime with legitimate absent patches rejected: admission=%+v error=%v", admission, err)
			}
		})
	}
}

func TestArchiveAdmissionLossyProfileDeclarations(t *testing.T) {
	for _, test := range []struct {
		key, value string
		lossy      bool
	}{
		{"profile", portableexport.CurrentStateV1, true},
		{"body_chars", "32", true},
		{"capabilities", "raw_json_stripped,body_excerpts", true},
		{"capabilities", "comment_excerpts,author_association", true},
		{"excluded", "raw_json,pull_request_file_patches,documents", true},
		{"excluded", "comment_revision_history", true},
		{"capabilities", "raw_json_stripped", false},
		{"excluded", "raw_json,documents", false},
		{gitcrawlCloudAdmissionMetadataKey, `{"policy":"archive-v1"}`, false},
	} {
		t.Run(test.key+"="+test.value, func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := requireLosslessGitcrawlCloudSource(ctx, db); err != nil {
				t.Fatalf("no portable metadata: %v", err)
			}
			if _, err := db.Exec(`create table portable_metadata(key text primary key, value text not null);
				insert into portable_metadata(key,value) values(?,?)`, test.key, test.value); err != nil {
				t.Fatal(err)
			}
			if err := requireLosslessGitcrawlCloudSource(ctx, db); (err != nil) != test.lossy {
				t.Fatalf("profile rejection = %v, want lossy=%v", err, test.lossy)
			}
		})
	}
}

func TestGitcrawlCloudWarningIdentity(t *testing.T) {
	warnings := []string{"gitcrawl.archive.source.unknown"}
	if !gitcrawlCloudWarningsMatch(nil, []string{}) ||
		!gitcrawlCloudWarningsMatch(warnings, slices.Clone(warnings)) {
		t.Fatal("equal canonical warnings did not match")
	}
	if gitcrawlCloudWarningsMatch(nil, warnings) ||
		gitcrawlCloudWarningsMatch(warnings, nil) ||
		gitcrawlCloudWarningsMatch([]string{"unknown"}, []string{"unknown"}) ||
		gitcrawlCloudWarningsMatch(append(slices.Clone(warnings), warnings[0]), warnings) {
		t.Fatal("invalid or changed warnings matched")
	}
}
