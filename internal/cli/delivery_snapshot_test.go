//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/remotedelivery"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestDeliveryRetainedSnapshotWithoutJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "native.sqlite")
	s, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := s.UpsertRepository(ctx, store.Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := s.UpsertThread(ctx, store.Thread{RepoID: repo, GitHubID: "9223372036854775808", Number: 1, Kind: "issue", State: "open", Title: deliveryPrivate, Body: deliveryPrivate, RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.UpsertComment(ctx, store.Comment{ThreadID: thread, GitHubID: "comment", CommentType: "issue_comment", Body: deliveryPrivate, RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	sum := sha256.Sum256(before)
	hash := hex.EncodeToString(sum[:])
	run := func(args ...string) ([]byte, error) {
		a := New()
		var out bytes.Buffer
		a.Stdout = &out
		a.Stderr = &out
		a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("acquired credential"); return "", nil }
		err := a.Run(ctx, args)
		if bytes.Contains(out.Bytes(), []byte(deliveryPrivate)) {
			t.Fatal("content leaked")
		}
		return out.Bytes(), err
	}
	data, err := run("delivery", "measure-snapshot", "--db", path, "--repo", "fixture/repo", "--sha256", hash, "--timeout", "10s", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var measure store.NativeReportMeasurement
	if err = json.Unmarshal(data, &measure); err != nil || measure.Rows != 3 {
		t.Fatal("wrong measurement", string(data), err)
	}
	var manifests [][]byte
	for n := 0; n < 2; n++ {
		dir := filepath.Join(t.TempDir(), "export")
		args := []string{"delivery", "export-snapshot", "--db", path, "--repo", "fixture/repo", "--sha256", hash, "--directory", dir, "--archive", "fixture", "--binding", "fixture-binding", "--seed", "0123456789abcdef0123456789abcdef", "--max-rows", "100", "--max-native-bytes", "1048576", "--max-encoded-bytes", "1048576", "--timeout", "10s", "--json"}
		output, err := run(args...)
		if err != nil {
			t.Fatal(err)
		}
		var result remotedelivery.SnapshotExportResult
		if err = json.Unmarshal(output, &result); err != nil {
			t.Fatal(err)
		}
		manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, manifest)
		plan, err := remote.DecodeGitHubSeedManifest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		anchor, err := remote.ParseGitHubSnapshotAnchor(plan.Manifest().NativeThrough)
		if err != nil || anchor.FileSHA256 != hash || result.Rows != 3 || result.NativeBytes != measure.Bytes {
			t.Fatal("wrong snapshot anchor", err)
		}
		complete, err := os.ReadFile(filepath.Join(dir, "complete.json"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = plan.ValidateComplete(complete); err != nil {
			t.Fatal(err)
		}
		var foundTitle, foundBody, foundComment bool
		for i := 0; i < result.Chunks; i++ {
			body, err := os.ReadFile(filepath.Join(dir, chunkFilename(i)))
			if err != nil {
				t.Fatal(err)
			}
			chunk, err := plan.ValidateChunk(body)
			if err != nil {
				t.Fatal(err)
			}
			for _, change := range chunk.Changes {
				for _, value := range change.Row {
					if value.Value != nil && *value.Value == deliveryPrivate {
						switch change.Table {
						case "threads":
							if !foundTitle {
								foundTitle = true
							} else {
								foundBody = true
							}
						case "comments":
							foundComment = true
						}
					}
				}
			}
		}
		if !foundTitle || !foundBody || !foundComment {
			t.Fatal("full fields lost")
		}
		if _, err = run(args...); err == nil {
			t.Fatal("overwrote existing export")
		}

		if n == 0 {
			failedDir := filepath.Join(t.TempDir(), "partial")
			bad := append([]string(nil), args...)
			for i, arg := range bad {
				if arg == "--directory" {
					bad[i+1] = failedDir
				}
				if arg == "--max-encoded-bytes" {
					bad[i+1] = "1"
				}
			}
			if _, err = run(bad...); err == nil {
				t.Fatal("accepted encoded-byte overflow")
			}
			if _, err = os.Stat(filepath.Join(failedDir, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("partial export published manifest", err)
			}
		}
	}
	if !bytes.Equal(manifests[0], manifests[1]) {
		t.Fatal("non-deterministic manifest")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("changed source")
	}
}

func chunkFilename(i int) string { return fmt.Sprintf("chunk-%06d.json", i) }
