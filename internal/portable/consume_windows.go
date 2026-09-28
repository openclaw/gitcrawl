package portable

import (
	"context"
	"fmt"
)

func consumeSQLite(_ context.Context, _, _ string) error {
	return fmt.Errorf("consume-source is not supported on Windows")
}
