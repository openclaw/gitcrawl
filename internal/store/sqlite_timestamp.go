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
				return nil, nil
			case string:
				key, ok := timestampOrderKey(value)
				if !ok {
					return nil, nil
				}
				return key, nil
			case []byte:
				key, ok := timestampOrderKey(string(value))
				if !ok {
					return nil, nil
				}
				return key, nil
			default:
				return nil, fmt.Errorf("gitcrawl_timestamp_key expects text, got %T", value)
			}
		},
	)
}

func timestampOrderKey(value string) (string, bool) {
	value = strings.TrimSpace(value)
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return "", false
	}
	return parsed.UTC().Format(timestampOrderLayout), true
}
