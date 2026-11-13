//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store"
)

func clusterBundleFixture(t *testing.T) (context.Context, string, SeedRegistration, ClusterBundle) {
	t.Helper()
	ctx, c, path := clusterReportSourceFixture(t)
	report, err := ProjectClusterContext(ctx, c, time.Date(2026, 1, 2, 0, 0, 0, 123, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	seed, err := PrepareClusterSeed(ctx, report, seedRequest())
	if err != nil {
		t.Fatal(err)
	}
	m := seed.Plan.Manifest()
	r := SeedRegistration{Origin: "https://archive.example", Archive: m.Archive, BindingID: m.BindingID, Repository: m.Repository, File: c.File}
	return ctx, path, r, ClusterBundle{Profile: report.Profile, Manifest: seed.Manifest, Chunks: seed.Chunks, Complete: seed.Complete}
}
func clusterLimits() ClusterStageLimits {
	return ClusterStageLimits{MaxRows: 1000, MaxEncodedBytes: 1 << 20}
}

func TestClusterOutboxExactCustodyAndReadOnlyStatus(t *testing.T) {
	ctx, source, r, bundle := clusterBundleFixture(t)
	bundle.Manifest = append(bundle.Manifest, '\n') // Preserve valid noncanonical bytes.
	plan, err := remote.DecodeGitHubSeedManifest(bundle.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	complete, err := remote.DecodeGitHubSeedComplete(bundle.Complete)
	if err != nil {
		t.Fatal(err)
	}
	complete.ManifestSHA256 = plan.ManifestSHA256()
	bundle.Complete, err = remote.EncodeGitHubSeedComplete(complete)
	if err != nil {
		t.Fatal(err)
	}
	envelope, _ := json.Marshal(bundle)
	decoded, err := ReadClusterBundle(ctx, bytes.NewReader(envelope), clusterLimits())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "private", "cluster.sqlite")
	state, err := StageClusterBundle(ctx, path, source, r, decoded, clusterLimits())
	if err != nil {
		t.Fatal(err)
	}
	if state.Rows != 6 || state.PartCount != 6 || state.Registration != r || state.GitHubRepositoryID != "9223372036854775808" {
		t.Fatal("staged metadata differs")
	}
	expected := append([][]byte{bundle.Manifest}, bundle.Chunks...)
	expected = append(expected, bundle.Complete)
	db, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.DB().QueryContext(ctx, `select body,payload_sha256 from parts order by part_index`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		var body []byte
		var hash string
		if err = rows.Scan(&body, &hash); err != nil {
			t.Fatal(err)
		}
		if n >= len(expected) || !bytes.Equal(body, expected[n]) || hash != privateSeedHash(expected[n]) {
			t.Fatal("saved bytes differ")
		}
		n++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if n != len(expected) {
		t.Fatal("missing part")
	}
	if _, err = db.DB().ExecContext(ctx, `pragma journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	got, err := ReadClusterOutboxState(ctx, path)
	if err != nil || got != state {
		t.Fatal("read-only state mismatch", err)
	}
	after, _ := os.ReadFile(path)
	info, _ := os.Stat(path)
	if !bytes.Equal(before, after) || info.Mode().Perm() != 0640 {
		t.Fatal("status changed file")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err = os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("status created sidecar")
		}
	}
	if _, err = OpenSeedOutbox(ctx, path); err == nil {
		t.Fatal("cluster mistaken for private baseline")
	}
	if _, err = StageClusterBundle(ctx, path, source, r, bundle, clusterLimits()); !errors.Is(err, os.ErrExist) {
		t.Fatal("existing custody overwritten", err)
	}
	after, _ = os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("refusal changed custody")
	}
}

func TestClusterOutboxRefusalsBeforeCreate(t *testing.T) {
	ctx, source, r, original := clusterBundleFixture(t)
	for _, name := range []string{"profile", "destination", "file", "chunk-hash", "missing-chunk", "complete", "inventory", "rows", "bytes", "canceled", "sidecar"} {
		t.Run(name, func(t *testing.T) {
			encoded, _ := json.Marshal(original)
			var bundle ClusterBundle
			json.Unmarshal(encoded, &bundle)
			registration, limits := r, clusterLimits()
			callCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			path := filepath.Join(t.TempDir(), "absent", "cluster.sqlite")
			switch name {
			case "profile":
				bundle.Profile = PrivateReportProfile
			case "destination":
				registration.Archive = "other"
			case "file":
				registration.File.Inode = "different"
			case "chunk-hash":
				bundle.Chunks[1] = append(bundle.Chunks[1], ' ')
			case "missing-chunk":
				bundle.Chunks = bundle.Chunks[:1]
			case "complete":
				bundle.Complete = []byte(`{}`)
			case "inventory":
				plan, _ := remote.DecodeGitHubSeedManifest(bundle.Manifest)
				m := plan.Manifest()
				m.NativeSHA256 = strings.Repeat("1", 64)
				m.NativeThrough = "gitcrawl-cluster-v1:" + m.ScopeSHA256 + ":" + m.NativeSHA256
				bundle.Manifest, _ = remote.EncodeGitHubSeedManifest(m)
			case "rows":
				limits.MaxRows = 5
			case "bytes":
				limits.MaxEncodedBytes = 1
			case "canceled":
				cancel()
			case "sidecar":
				path = filepath.Join(t.TempDir(), "cluster.sqlite")
				if err := os.Symlink("missing", path+"-wal"); err != nil {
					t.Fatal(err)
				}
			}
			got, err := StageClusterBundle(callCtx, path, source, registration, bundle, limits)
			if err == nil || !reflect.DeepEqual(got, ClusterState{}) {
				t.Fatal("invalid stage accepted", err)
			}
			if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refusal created file")
			}
			if name != "sidecar" {
				if _, err = os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refusal created parent")
				}
			}
		})
	}
	otherCtx, other := nativeFixture(t)
	if _, err := other.DB().ExecContext(otherCtx, `update repositories set github_repo_id='different'`); err != nil {
		t.Fatal(err)
	}
	r.File, _ = store.NativeReportFileIdentity(other.Path())
	if _, err := StageClusterBundle(ctx, filepath.Join(t.TempDir(), "cluster.sqlite"), other.Path(), r, original, clusterLimits()); err == nil {
		t.Fatal("provider identity mismatch admitted")
	}

	// A matching current repository in a different selected file is valid local
	// custody, not a claim that this inode produced the older exported bytes.
	if _, err := other.DB().ExecContext(otherCtx, `update repositories set github_repo_id='9223372036854775808'`); err != nil {
		t.Fatal(err)
	}
	staged, err := StageClusterBundle(ctx, filepath.Join(t.TempDir(), "current.sqlite"), other.Path(), r, original, clusterLimits())
	if err != nil || staged.Registration.File != r.File {
		t.Fatal("current file custody rejected", err)
	}
	if _, err := other.DB().ExecContext(otherCtx, `pragma user_version=15`); err != nil {
		t.Fatal(err)
	}
	if _, err := StageClusterBundle(ctx, filepath.Join(t.TempDir(), "old.sqlite"), other.Path(), r, original, clusterLimits()); err == nil {
		t.Fatal("old native schema admitted")
	}
	var version int
	if err := other.DB().QueryRowContext(otherCtx, `pragma user_version`).Scan(&version); err != nil || version != 15 {
		t.Fatal("stage migrated source", err)
	}
	if _, err := other.DB().ExecContext(otherCtx, `pragma user_version=16`); err != nil {
		t.Fatal(err)
	}
	capture, _ := projected(t, otherCtx, other)
	private, path := newSeedOutbox(t, otherCtx, capture)
	private.Close()
	if _, err := ReadClusterOutboxState(ctx, path); err == nil {
		t.Fatal("private outbox interpreted as clusters")
	}
}

func TestClusterBundleEnvelopeBounds(t *testing.T) {
	ctx, _, _, bundle := clusterBundleFixture(t)
	good, _ := json.Marshal(bundle)
	for name, body := range map[string][]byte{
		"duplicate": append([]byte(`{"profile":"wrong",`), good[1:]...), "unknown": append([]byte(`{"unexpected":true,`), good[1:]...),
		"missing": []byte(`{"profile":"x"}`), "trailing": append(append([]byte(nil), good...), []byte(`{}`)...),
		"invalid-base64": []byte(`{"profile":"x","manifest":"?","chunks":[],"complete":"?"}`), "outer-bound": bytes.Repeat([]byte{' '}, 150000),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ReadClusterBundle(ctx, bytes.NewReader(body), ClusterStageLimits{MaxRows: 100, MaxEncodedBytes: 1}); err == nil {
				t.Fatal("unsafe envelope admitted")
			}
		})
	}
}

func TestOutboxInitializationFailureRetainsOwnedFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "owned.sqlite")
	failure := errors.New("fixture initialization failed")
	o, err := createOutboxFile(ctx, path, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `create table not_committed(value text)`); err != nil {
			return err
		}
		return failure
	})
	if o != nil || !errors.Is(err, failure) {
		t.Fatal("wrong initialization result", err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("partial file not retained privately", err)
	}
	if _, err := ReadClusterOutboxState(ctx, path); err == nil {
		t.Fatal("partial initialization qualified")
	}
}

func TestClusterOutboxMalformedTypedRows(t *testing.T) {
	ctx, c, source := clusterReportSourceFixture(t)
	for _, name := range []string{"null-selection", "null-thread", "bool", "oversize", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			report, err := ProjectClusterContext(ctx, c, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "null-selection":
				report.Tables[0].Rows[0][3] = clusterNull()
			case "null-thread":
				report.Tables[3].Rows[0][1] = clusterNull()
			case "bool":
				report.Tables[0].Rows[0][12] = clusterInteger(2)
			case "oversize":
				report.Tables[3].Rows[0][4] = clusterText(strings.Repeat("x", clusterMaxScalarBytes+1))
			case "duplicate":
				report.Tables[3].Rows[1] = report.Tables[3].Rows[0]
			}
			seed, err := PrepareClusterSeed(ctx, report, seedRequest())
			if err != nil {
				t.Fatal(err)
			}
			m := seed.Plan.Manifest()
			r := SeedRegistration{Origin: "https://archive.example", Archive: m.Archive, BindingID: m.BindingID, Repository: m.Repository, File: c.File}
			path := filepath.Join(t.TempDir(), "absent", "outbox.sqlite")
			got, err := StageClusterBundle(ctx, path, source, r, ClusterBundle{Profile: report.Profile, Manifest: seed.Manifest, Chunks: seed.Chunks, Complete: seed.Complete}, clusterLimits())
			if err == nil || !reflect.DeepEqual(got, ClusterState{}) {
				t.Fatal("invalid typed row admitted")
			}
			if _, err = os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("typed refusal created directory")
			}
		})
	}
}

func TestClusterStatusRejectsExtraReceiptedPart(t *testing.T) {
	ctx, source, registration, bundle := clusterBundleFixture(t)
	path := filepath.Join(t.TempDir(), "cluster.sqlite")
	state, err := StageClusterBundle(ctx, path, source, registration, bundle, clusterLimits())
	if err != nil {
		t.Fatal(err)
	}
	db, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	// A receipt is forbidden in this stage-only format state, even on a part
	// beyond the declared prefix. Filtering it out must not qualify the prefix.
	extra := []byte(`{}`)
	_, err = db.DB().ExecContext(ctx, `insert into parts(seed_id,part_index,body,payload_sha256,receipt_json) values(?,?,?,?,?)`, state.SeedID, state.PartCount, extra, privateSeedHash(extra), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadClusterOutboxState(ctx, path)
	if err == nil || !reflect.DeepEqual(got, ClusterState{}) {
		t.Fatal("receipt-bearing extra part hidden by staged status")
	}
}
