//go:build darwin || linux

package remotedelivery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

type versionRefusalTransport struct{ calls int }

func (r *versionRefusalTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls++
	return nil, errors.New("unexpected transport call")
}

func TestSeedOutboxAlreadyOpenRefusesChangedSchema(t *testing.T) {
	ctx, native := nativeFixture(t)
	capture, _ := projected(t, ctx, native)
	for name, mutation := range map[string]string{
		"future version":      "pragma user_version=2",
		"foreign application": "pragma application_id=0",
	} {
		t.Run(name, func(t *testing.T) {
			o, path := newSeedOutbox(t, ctx, capture)
			first := nextSeedPart(t, ctx, o)
			if err := o.RecordReceipt(ctx, first.Ref, seedReceipt(first)); err != nil {
				t.Fatal(err)
			}
			before := outboxState(t, ctx, o)
			pending := nextSeedPart(t, ctx, o)
			// Include every saved body/hash and both acknowledged/pending receipts.
			parts := func() string {
				t.Helper()
				var saved string
				if err := o.base.DB().QueryRowContext(ctx, `select group_concat(part_index || ':' || hex(body) || ':' || payload_sha256 || ':' || hex(receipt_json), '|') from (select * from parts order by part_index)`).Scan(&saved); err != nil {
					t.Fatal(err)
				}
				return saved
			}
			saved := parts()
			if _, err := o.base.DB().ExecContext(ctx, mutation); err != nil {
				t.Fatal(err)
			}
			if got, err := o.State(ctx); !errors.Is(err, ErrSeedOutbox) || got != (SeedState{}) {
				t.Error("already-open State accepted changed schema", err)
			}
			if got, err := o.NextPart(ctx); !errors.Is(err, ErrSeedOutbox) || got != nil {
				t.Error("already-open NextPart exposed changed schema", err)
			}
			if err := o.RecordReceipt(ctx, pending.Ref, seedReceipt(pending)); !errors.Is(err, ErrSeedOutbox) {
				t.Error("already-open RecordReceipt accepted changed schema", err)
			}
			tokens := 0
			token := deliveryToken(func(context.Context) (string, error) { tokens++; return "fixture", nil })
			transport := &versionRefusalTransport{}
			limits := deliveryLimits()
			limits.MaxParts = 1
			result, err := o.DeliverSeed(ctx, native.Path(), token, &http.Client{Transport: transport}, limits)
			if !errors.Is(err, ErrSeedOutbox) || result != (DeliveryResult{}) || tokens != 0 || transport.calls != 0 {
				t.Errorf("already-open DeliverSeed failed schema boundary: error=%v tokens=%d transport=%d", err, tokens, transport.calls)
			}
			if got, err := ReadSeedOutboxState(ctx, path); !errors.Is(err, ErrSeedOutbox) || got != (SeedState{}) {
				t.Error("read-only state accepted changed schema", err)
			}
			if reopened, err := OpenSeedOutbox(ctx, path); !errors.Is(err, ErrSeedOutbox) || reopened != nil {
				if reopened != nil {
					reopened.Close()
				}
				t.Error("open accepted changed schema", err)
			}
			if parts() != saved {
				t.Error("schema refusal changed staged bytes or receipts")
			}
			// Restoring only the fixture header proves refusal rolled back cleanly
			// and left the original baseline and pending request usable.
			if _, err := o.base.DB().ExecContext(ctx, fmt.Sprintf("pragma application_id=%d; pragma user_version=%d", seedOutboxApplicationID, seedOutboxVersion)); err != nil {
				t.Fatal(err)
			}
			if got := outboxState(t, ctx, o); got != before {
				t.Error("schema refusal changed baseline or receipt count")
			}
			if got := nextSeedPart(t, ctx, o); !reflect.DeepEqual(got, pending) {
				t.Error("schema refusal changed pending request")
			}
		})
	}
}
