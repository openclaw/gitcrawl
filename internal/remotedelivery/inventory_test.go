package remotedelivery

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/remote"
)

func inventoryScalar(kind, value string) remote.ChangeValue {
	return remote.ChangeValue{Kind: kind, Value: &value}
}

// These independently framed vectors are identical in Gitcrawl and crawl-remote.
// The key and row bind the profile/layout IDs and all seven declared layouts,
// including every column's order/type/nullability and each table's key order.
func TestInventoryGoldenRepository(t *testing.T) {
	codec, err := privateInventoryCodec()
	if err != nil {
		t.Fatal(err)
	}
	key := []remote.ChangeValue{inventoryScalar("integer", "9007199254740993")}
	row := []remote.ChangeValue{
		key[0], inventoryScalar("text", "fixture"), inventoryScalar("text", "repo"),
		inventoryScalar("text", "fixture/repo"), inventoryScalar("text", "9223372036854775808"),
		inventoryScalar("text", `{ "body": "snow☃\u0000", "n": 9007199254740993 }`),
		{Kind: "null"},
	}
	encoded, err := codec.EncodeKey("repositories", key)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != "979bc8c2c763dd91a9565e27d2f913b7da41dc981ffd9c2cfa3583b15c39b259" {
		t.Fatalf("repository key/layout digest: %s", got)
	}
	digest, err := codec.RowDigest("repositories", key, row)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", digest); got != "1f03a5777c70b3f736bfe74f4b10dcf5e1e234ab4977f813ec53f81cb2e88c29" {
		t.Fatalf("repository row/layout digest: %s", got)
	}
	reordered := slices.Clone(row)
	reordered[1], reordered[2] = reordered[2], reordered[1]
	if got, err := codec.RowDigest("repositories", key, reordered); err != nil || got == digest {
		t.Fatal("text column order lost", err)
	}
	if _, err := codec.RowDigest("repositories", key, row[:len(row)-1]); !errors.Is(err, remote.ErrInventory) {
		t.Fatal("missing column accepted", err)
	}
}

func TestInventoryWireTypesAndNullability(t *testing.T) {
	codec, err := privateInventoryCodec()
	if err != nil {
		t.Fatal(err)
	}
	for _, layout := range privateLayouts {
		name, columnNames, keyNames, integerNames := layout.name, layout.columns, layout.keys, layout.integers
		t.Run(name, func(t *testing.T) {
			columns, keys, integers := strings.Fields(columnNames), strings.Fields(keyNames), strings.Fields(integerNames)
			row := make([]remote.ChangeValue, len(columns))
			for i, column := range columns {
				row[i] = inventoryScalar("text", column)
				if slices.Contains(integers, column) {
					row[i] = inventoryScalar("integer", "42")
				}
			}
			key := make([]remote.ChangeValue, len(keys))
			for i, column := range keys {
				key[i] = row[slices.Index(columns, column)]
			}
			// All typed non-key values, including raw_json_blob_id, are hashable.
			// Projection/receiver eligibility remains a separate, stricter owner.
			if _, err := codec.RowDigest(name, key, row); err != nil {
				t.Fatal("declared scalar layout rejected", err)
			}
			for i, column := range columns {
				wrongType := slices.Clone(row)
				wrongType[i] = inventoryScalar("integer", "42")
				if row[i].Kind == "integer" {
					wrongType[i] = inventoryScalar("text", "42")
				}
				wrongKey := slices.Clone(key)
				keyIndex := slices.Index(keys, column)
				if keyIndex >= 0 {
					wrongKey[keyIndex] = wrongType[i]
				}
				if _, err := codec.RowDigest(name, wrongKey, wrongType); !errors.Is(err, remote.ErrInventory) {
					t.Fatalf("%s accepted wrong scalar type: %v", column, err)
				}
				nullRow, nullKey := slices.Clone(row), slices.Clone(key)
				nullRow[i] = remote.ChangeValue{Kind: "null"}
				if keyIndex >= 0 {
					nullKey[keyIndex] = nullRow[i]
				}
				_, err := codec.RowDigest(name, nullKey, nullRow)
				if keyIndex >= 0 {
					if !errors.Is(err, remote.ErrInventory) {
						t.Fatalf("%s accepted NULL key: %v", column, err)
					}
				} else if err != nil {
					t.Fatalf("%s rejected nullable wire value: %v", column, err)
				}
			}
		})
	}
}
