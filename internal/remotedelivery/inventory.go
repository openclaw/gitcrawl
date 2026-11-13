package remotedelivery

import (
	"slices"
	"strings"

	"github.com/openclaw/crawlkit/remote"
)

const privateInventoryLayoutID = "gitcrawl-native-report-16-v1"

// privateInventoryCodec binds the wire scalar layout, not native SQL nullability
// or source eligibility. Capture and projection retain their stricter checks.
func privateInventoryCodec() (*remote.InventoryCodec, error) {
	tables := make([]remote.InventoryTable, 0, len(privateLayouts))
	for _, layout := range privateLayouts {
		table := remote.InventoryTable{Name: layout.name, KeyColumns: strings.Fields(layout.keys)}
		integers := strings.Fields(layout.integers)
		for _, name := range strings.Fields(layout.columns) {
			kind := "text"
			if slices.Contains(integers, name) {
				kind = "integer"
			}
			table.Columns = append(table.Columns, remote.InventoryColumn{
				Name: name, Kind: kind, Nullable: !slices.Contains(table.KeyColumns, name),
			})
		}
		tables = append(tables, table)
	}
	return remote.NewInventoryCodec(PrivateReportProfile, privateInventoryLayoutID, tables)
}
