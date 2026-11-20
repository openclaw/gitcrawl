package remotedelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
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

// Closed profiles share exact chunk packing and receipt contracts. Their
// callers supply fixed layouts and distinct content-token metadata.
func prepareGitHubSeed(ctx context.Context, m remote.GitHubSeedManifest, tables []PrivateTable, layouts []privateLayout, rows, maxBytes int64) (PrivateSeed, error) {
	p := newSeedPacker(ctx, m, maxBytes, nil)
	// Validate caller metadata before processing any row.
	for _, layout := range layouts {
		p.manifest.Tables = append(p.manifest.Tables, remote.SeedTable{Name: layout.name, Rows: "0", SHA256: privateSeedHash(nil), Chunks: []remote.SeedChunkInfo{}})
	}
	if _, err := remote.EncodeGitHubSeedManifest(p.manifest); err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	p.manifest.Tables = nil
	var seen int64
	for i, table := range tables {
		if i >= len(layouts) {
			return PrivateSeed{}, ErrPrivateSeed
		}
		layout := layouts[i]
		if table.Name != layout.name || !slices.Equal(table.Columns, strings.Fields(layout.columns)) || !slices.Equal(table.KeyColumns, strings.Fields(layout.keys)) {
			return PrivateSeed{}, ErrPrivateSeed
		}
		p.begin(layout)
		for _, row := range table.Rows {
			if seen >= rows {
				return PrivateSeed{}, ErrPrivateSeed
			}
			if err := p.row(row); err != nil {
				return PrivateSeed{}, err
			}
			seen++
		}
		if err := p.end(); err != nil {
			return PrivateSeed{}, err
		}
	}
	if seen != rows || len(tables) != len(layouts) {
		return PrivateSeed{}, ErrPrivateSeed
	}
	return p.finish()
}

// seedPacker owns the common deterministic wire boundaries. A sink consumes
// completed chunks immediately; nil retains them for existing outbox callers.
type seedPacker struct {
	ctx                                       context.Context
	manifest                                  remote.GitHubSeedManifest
	maxBytes, used                            int64
	out                                       PrivateSeed
	sink                                      func(int, []byte) error
	index                                     int
	layout                                    privateLayout
	columns                                   []string
	keyIndexes                                []int
	descriptor                                remote.SeedTable
	hash                                      hash.Hash
	chunk, probe                              remote.GitHubSeedChunk
	headerBytes, chunkBytes, probeHeaderBytes int
	rows                                      int64
}

func newSeedPacker(ctx context.Context, m remote.GitHubSeedManifest, maxBytes int64, sink func(int, []byte) error) *seedPacker {
	p := &seedPacker{ctx: ctx, manifest: m, maxBytes: maxBytes, sink: sink}
	p.probe = privateSeedChunk(m, 1)
	p.probeHeaderBytes = privateSeedHeaderBytes(p.probe)
	return p
}
func (p *seedPacker) retain(body []byte) error {
	if int64(len(body)) > p.maxBytes-p.used {
		return ErrPrivateSeedLimit
	}
	p.used += int64(len(body))
	return nil
}
func (p *seedPacker) begin(layout privateLayout) {
	p.layout = layout
	p.columns = strings.Fields(layout.columns)
	keys := strings.Fields(layout.keys)
	p.keyIndexes = make([]int, len(keys))
	for i, key := range keys {
		p.keyIndexes[i] = slices.Index(p.columns, key)
	}
	p.descriptor = remote.SeedTable{Name: layout.name, Rows: "0", Chunks: []remote.SeedChunkInfo{}}
	p.hash = sha256.New()
	p.rows = 0
	p.reset()
}
func (p *seedPacker) reset() {
	p.chunk = privateSeedChunk(p.manifest, p.index)
	p.headerBytes = privateSeedHeaderBytes(p.chunk)
	p.chunkBytes = p.headerBytes
}
func (p *seedPacker) flush() error {
	if len(p.chunk.Changes) == 0 {
		return nil
	}
	body, err := remote.EncodeGitHubSeedChunk(p.chunk)
	if err != nil {
		return ErrPrivateSeed
	}
	if err := p.retain(body); err != nil {
		return err
	}
	digest := sha256.Sum256(body)
	p.hash.Write(digest[:])
	p.descriptor.Chunks = append(p.descriptor.Chunks, remote.SeedChunkInfo{Rows: len(p.chunk.Changes), Bytes: len(body), SHA256: hex.EncodeToString(digest[:])})
	if p.sink == nil {
		p.out.Chunks = append(p.out.Chunks, body)
	} else if err := p.sink(p.index, body); err != nil {
		return err
	}
	p.index++
	p.reset()
	return nil
}
func (p *seedPacker) row(row []PrivateValue) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	if len(row) != len(p.columns) {
		return ErrPrivateSeed
	}
	change := remote.Change{Table: p.layout.name, Operation: "upsert", Row: make([]remote.ChangeValue, len(row)), Key: make([]remote.ChangeValue, len(p.keyIndexes))}
	for i, value := range row {
		change.Row[i] = remote.ChangeValue{Kind: value.Kind, Value: value.Value}
	}
	for i, index := range p.keyIndexes {
		change.Key[i] = change.Row[index]
	}
	if p.index >= remote.MaxSeedChunks {
		return ErrPrivateSeedLimit
	}
	p.probe.Changes = []remote.Change{change}
	body, err := remote.EncodeGitHubSeedChunk(p.probe)
	if err != nil {
		return ErrPrivateSeed
	}
	rowBytes := len(body) - p.probeHeaderBytes
	separator := 0
	if len(p.chunk.Changes) > 0 {
		separator = 1
	}
	if len(p.chunk.Changes) == remote.MaxChanges || p.chunkBytes+separator+rowBytes > remote.MaxChangeBytes {
		if err := p.flush(); err != nil {
			return err
		}
		separator = 0
	}
	if p.index >= remote.MaxSeedChunks || p.headerBytes+rowBytes > remote.MaxChangeBytes {
		return ErrPrivateSeedLimit
	}
	p.chunk.Changes = append(p.chunk.Changes, change)
	p.chunkBytes += separator + rowBytes
	p.rows++
	return nil
}
func (p *seedPacker) end() error {
	if err := p.flush(); err != nil {
		return err
	}
	p.descriptor.Rows = strconv.FormatInt(p.rows, 10)
	p.descriptor.SHA256 = hex.EncodeToString(p.hash.Sum(nil))
	p.manifest.Tables = append(p.manifest.Tables, p.descriptor)
	return nil
}
func (p *seedPacker) finish() (PrivateSeed, error) {
	var err error
	p.out.Manifest, err = remote.EncodeGitHubSeedManifest(p.manifest)
	if err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	if err = p.retain(p.out.Manifest); err != nil {
		return PrivateSeed{}, err
	}
	p.out.Plan, err = remote.DecodeGitHubSeedManifest(p.out.Manifest)
	if err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	p.out.Complete, err = remote.EncodeGitHubSeedComplete(remote.GitHubSeedComplete{Contract: p.manifest.Contract, App: p.manifest.App, Archive: p.manifest.Archive, BindingID: p.manifest.BindingID, SeedID: p.manifest.SeedID, ManifestSHA256: p.out.Plan.ManifestSHA256(), ExpectedCursor: privateSeedChunk(p.manifest, p.index).ExpectedCursor})
	if err != nil {
		return PrivateSeed{}, ErrPrivateSeed
	}
	if err = p.retain(p.out.Complete); err != nil {
		return PrivateSeed{}, err
	}
	if err = p.ctx.Err(); err != nil {
		return PrivateSeed{}, err
	}
	return p.out, nil
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
