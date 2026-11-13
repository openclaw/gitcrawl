package remotedelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/remote"
)

var (
	ErrPrivateSeed      = errors.New("private seed requires a supported private report and request")
	ErrPrivateSeedLimit = errors.New("private seed exceeds its wire byte or chunk bounds")
)

type PrivateSeedRequest struct {
	Archive, BindingID, SeedID string
	// MaxBytes bounds all returned request bodies, not the existing report or RSS.
	MaxBytes int64
}

// PrivateSeed contains exact persistable requests, not a durable outbox or a
// qualified baseline. Callers must not mutate bytes during later persistence or
// delivery. Current exclusion policy and destination authority remain separate.
type PrivateSeed struct {
	Plan     *remote.GitHubSeedPlan
	Manifest []byte
	Chunks   [][]byte
	Complete []byte
}

// PreparePrivateSeed packs an unmodified ProjectPrivateReport result. It never
// reads native state, reinterprets exclusions, sends HTTP or acknowledges data.
// Any failure returns a zero result; caller cancellation is preserved.
func PreparePrivateSeed(ctx context.Context, report PrivateReport, request PrivateSeedRequest) (PrivateSeed, error) {
	if err := ctx.Err(); err != nil {
		return PrivateSeed{}, err
	}
	if request.MaxBytes <= 0 {
		return PrivateSeed{}, ErrPrivateSeedLimit
	}
	if report.Profile != PrivateReportProfile || len(report.Tables) != len(privateLayouts) {
		return PrivateSeed{}, ErrPrivateSeed
	}
	rows, err := strconv.ParseInt(report.Rows, 10, 64)
	if err != nil || rows < 1 || strconv.FormatInt(rows, 10) != report.Rows {
		return PrivateSeed{}, ErrPrivateSeed
	}
	c := report.Cursor
	m := remote.GitHubSeedManifest{Contract: remote.GitHubSeedContract, App: "gitcrawl", Archive: request.Archive,
		BindingID: request.BindingID, SeedID: request.SeedID, Profile: report.Profile, NativeSchema: 16,
		Repository: report.Repository, RepositoryID: report.RepositoryID, ScopeSHA256: report.ScopeSHA256, NativeSHA256: report.SHA256,
		NativeThrough: strings.Join([]string{"gitcrawl-native-v1", c.Epoch, c.Generation, c.SchemaVersion, c.Sequence, c.PrunedThrough, report.ScopeSHA256, report.SHA256}, ":")}
	return prepareGitHubSeed(ctx, m, report.Tables, privateLayouts, rows, request.MaxBytes)
}

// Both closed profiles share exact chunk packing and receipt contracts. Their
// callers supply fixed layouts and distinct content-token metadata.
func prepareGitHubSeed(ctx context.Context, m remote.GitHubSeedManifest, tables []PrivateTable, layouts []privateLayout, rows, maxBytes int64) (PrivateSeed, error) {
	for _, layout := range layouts {
		m.Tables = append(m.Tables, remote.SeedTable{Name: layout.name, Rows: "0", SHA256: privateSeedHash(nil), Chunks: []remote.SeedChunkInfo{}})
	}
	// Validate caller identity and native-token metadata before processing rows.
	if _, err := remote.EncodeGitHubSeedManifest(m); err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	// Index 1 has the shortest valid envelope (its predecessor is "0", not
	// "begin"). Measure each row there, then account for its actual header.
	// This probe is never retained or sent.
	probe := privateSeedChunk(m, 1)
	probeHeaderBytes := privateSeedHeaderBytes(probe)
	var out PrivateSeed
	var err error
	var used, seen int64
	retain := func(body []byte) error {
		if int64(len(body)) > maxBytes-used {
			return ErrPrivateSeedLimit
		}
		used += int64(len(body))
		return nil
	}
	for i, table := range tables {
		if err := ctx.Err(); err != nil {
			return PrivateSeed{}, err
		}
		layout := layouts[i]
		columns, keys := strings.Fields(layout.columns), strings.Fields(layout.keys)
		if table.Name != layout.name || !slices.Equal(table.Columns, columns) || !slices.Equal(table.KeyColumns, keys) {
			return PrivateSeed{}, ErrPrivateSeed
		}
		keyIndexes := make([]int, len(keys))
		for j, key := range keys {
			keyIndexes[j] = slices.Index(columns, key)
		}
		descriptor := &m.Tables[i]
		hash := sha256.New()
		chunk := privateSeedChunk(m, len(out.Chunks))
		headerBytes := privateSeedHeaderBytes(chunk)
		chunkBytes := headerBytes
		flush := func() error {
			if len(chunk.Changes) == 0 {
				return nil
			}
			body, err := remote.EncodeGitHubSeedChunk(chunk)
			if err != nil {
				return ErrPrivateSeed
			}
			if err := retain(body); err != nil {
				return err
			}
			digest := sha256.Sum256(body)
			_, _ = hash.Write(digest[:])
			descriptor.Chunks = append(descriptor.Chunks, remote.SeedChunkInfo{Rows: len(chunk.Changes), Bytes: len(body), SHA256: hex.EncodeToString(digest[:])})
			out.Chunks = append(out.Chunks, body)
			chunk = privateSeedChunk(m, len(out.Chunks))
			headerBytes = privateSeedHeaderBytes(chunk)
			chunkBytes = headerBytes
			return nil
		}
		for _, row := range table.Rows {
			if err := ctx.Err(); err != nil {
				return PrivateSeed{}, err
			}
			if len(row) != len(columns) || seen >= rows {
				return PrivateSeed{}, ErrPrivateSeed
			}
			seen++
			change := remote.Change{Table: table.Name, Operation: "upsert", Row: make([]remote.ChangeValue, len(row)), Key: make([]remote.ChangeValue, len(keys))}
			for j, value := range row {
				change.Row[j] = remote.ChangeValue{Kind: value.Kind, Value: value.Value}
			}
			for j, index := range keyIndexes {
				change.Key[j] = change.Row[index]
			}
			if chunk.Index >= remote.MaxSeedChunks {
				return PrivateSeed{}, ErrPrivateSeedLimit
			}
			// One-row encoding owns scalar validation and exact JSON escaping.
			// Only a completed chunk is encoded again, never each growing prefix.
			probe.Changes = []remote.Change{change}
			body, err := remote.EncodeGitHubSeedChunk(probe)
			if err != nil {
				return PrivateSeed{}, ErrPrivateSeed
			}
			rowBytes := len(body) - probeHeaderBytes
			separator := 0
			if len(chunk.Changes) > 0 {
				separator = 1
			}
			if len(chunk.Changes) == remote.MaxChanges || chunkBytes+separator+rowBytes > remote.MaxChangeBytes {
				if err := flush(); err != nil {
					return PrivateSeed{}, err
				}
				separator = 0
			}
			if chunk.Index >= remote.MaxSeedChunks || headerBytes+rowBytes > remote.MaxChangeBytes {
				return PrivateSeed{}, ErrPrivateSeedLimit
			}
			chunk.Changes = append(chunk.Changes, change)
			chunkBytes += separator + rowBytes
		}
		if err := flush(); err != nil {
			return PrivateSeed{}, err
		}
		descriptor.Rows = strconv.Itoa(len(table.Rows))
		descriptor.SHA256 = hex.EncodeToString(hash.Sum(nil))
	}
	if seen != rows {
		return PrivateSeed{}, ErrPrivateSeed
	}
	out.Manifest, err = remote.EncodeGitHubSeedManifest(m)
	if err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	if err := retain(out.Manifest); err != nil {
		return PrivateSeed{}, err
	}
	out.Plan, err = remote.DecodeGitHubSeedManifest(out.Manifest)
	if err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	out.Complete, err = remote.EncodeGitHubSeedComplete(remote.GitHubSeedComplete{Contract: m.Contract, App: m.App,
		Archive: m.Archive, BindingID: m.BindingID, SeedID: m.SeedID, ManifestSHA256: out.Plan.ManifestSHA256(),
		ExpectedCursor: privateSeedChunk(m, len(out.Chunks)).ExpectedCursor})
	if err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	if err := retain(out.Complete); err != nil {
		return PrivateSeed{}, err
	}
	if err := ctx.Err(); err != nil {
		return PrivateSeed{}, err
	}
	return out, nil
}

func privateSeedChunk(m remote.GitHubSeedManifest, index int) remote.GitHubSeedChunk {
	previous := "begin"
	if index > 0 {
		previous = strconv.Itoa(index - 1)
	}
	return remote.GitHubSeedChunk{Contract: m.Contract, App: m.App, Archive: m.Archive, BindingID: m.BindingID, SeedID: m.SeedID,
		Index: index, ExpectedCursor: "seed1:" + m.SeedID + ":" + previous, Changes: []remote.Change{}}
}

func privateSeedHeaderBytes(chunk remote.GitHubSeedChunk) int {
	// Internal size calculation only: empty chunks are never emitted. These
	// concrete fields cannot fail json.Marshal; Crawlkit uses the same encoder.
	body, _ := json.Marshal(chunk)
	return len(body)
}

func privateSeedHash(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}
