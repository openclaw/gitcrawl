package store

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"modernc.org/sqlite"
)

const timestampOrderLayout = "2006-01-02T15:04:05.000000000Z"

func init() {
	sqlite.MustRegisterDeterministicScalarFunction(
		"gitcrawl_timestamp_key",
		1,
		func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("gitcrawl_timestamp_key expects one argument")
			}
			switch value := args[0].(type) {
			case nil:
				return timestampOrderKey(""), nil
			case string:
				return timestampOrderKey(value), nil
			case []byte:
				return timestampOrderKey(string(value)), nil
			default:
				return nil, fmt.Errorf("gitcrawl_timestamp_key expects text, got %T", value)
			}
		},
	)
}

func timestampOrderKey(value string) string {
	value = strings.TrimSpace(value)
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return "0:" + value
	}
	return "1:" + parsed.UTC().Format(timestampOrderLayout)
}
