package acquireapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
)

// Window runs keep their original completed inventory pin and number bounds in
// their ID. The native parent checkpoint still belongs to conversation progress.
const lifecycleWindowPrefix = "gitcrawl-lifecycle-window-"
const lifecycleWindowSize = 100

type lifecycleWindow struct {
	sequence    int64
	pinTag      string
	after, last int
}

func (w lifecycleWindow) prefix() string {
	return fmt.Sprintf("%s%x-%x-%x-%s-", lifecycleWindowPrefix, w.sequence, w.after, w.last, w.pinTag)
}

func parseLifecycleWindow(id string) (*lifecycleWindow, error) {
	tail, ok := strings.CutPrefix(id, lifecycleWindowPrefix)
	if !ok {
		return nil, nil
	}
	parts := strings.Split(tail, "-")
	if len(parts) == 5 {
		seq, e1 := strconv.ParseInt(parts[0], 16, 64)
		after, e2 := strconv.ParseInt(parts[1], 16, 0)
		last, e3 := strconv.ParseInt(parts[2], 16, 0)
		pin, e4 := hex.DecodeString(parts[3])
		hash, e5 := hex.DecodeString(parts[4])
		window := lifecycleWindow{sequence: seq, after: int(after), last: int(last), pinTag: parts[3]}
		if e1 == nil && e2 == nil && e3 == nil && e4 == nil && e5 == nil && len(pin) == 8 && len(hash) == 16 && seq > 0 && after >= 0 && last > after && window.prefix()+parts[4] == id {
			return &window, nil
		}
	}
	return nil, errors.New("invalid lifecycle window run ID")
}

type cycleLifecycle struct {
	parents []LifecycleParent
	since   time.Time
	prefix  string
}

func selectCycleLifecycle(ctx context.Context, api CycleAPI, state State, o CycleOptions, since time.Time) (cycleLifecycle, error) {
	selected := cycleLifecycle{since: since, prefix: "gitcrawl-lifecycle-"}
	if state.CompletedThrough == nil {
		return selected, errors.New("lifecycle requires an established acquisition horizon")
	}
	previous := state.ActiveRun
	if previous == nil {
		previous = state.CompletedRun
	}
	window, err := parseLifecycleWindow(previous.ID)
	if err != nil {
		return selected, err
	}
	if window == nil {
		if len(o.LifecycleNumbers) > 0 {
			selected.parents, err = api.LifecycleParents(ctx, o.Conversations.Repository, o.LifecycleNumbers)
			return selected, err
		}
		if state.ActiveRun != nil {
			return selected, errors.New("resume the active fixed lifecycle run with its original lifecycle-numbers-file")
		}
		window = &lifecycleWindow{sequence: state.Sequence}
	} else {
		var scope LifecycleScope
		if json.Unmarshal(previous.Scope, &scope) != nil || scope.Schema != "github-lifecycle-scope/v1" {
			return selected, errors.New("invalid lifecycle window scope")
		}
		selected.since = scope.Since
		if state.ActiveRun == nil {
			if _, err := parentProgressCheckpoint(state, o.Conversations.Repository); err != nil {
				return selected, err
			}
			window.after = window.last
		}
	}
	if selected.since.IsZero() || !selected.since.Before(*state.CompletedThrough) {
		return selected, errors.New("invalid lifecycle window bounds")
	}
	selected.parents, window.pinTag, err = api.LifecycleWindowParents(ctx, o.Conversations.Repository, selected.since, *state.CompletedThrough, window.sequence, window.after, window.pinTag)
	if err != nil {
		return selected, err
	}
	if len(selected.parents) == 0 {
		if state.ActiveRun != nil {
			return selected, errors.New("active lifecycle window no longer matches its selection")
		}
		return selected, nil
	}
	last := selected.parents[len(selected.parents)-1].Number
	if state.ActiveRun != nil && window.last != last {
		return selected, errors.New("active lifecycle window boundary changed")
	}
	window.last = last
	selected.prefix = window.prefix()
	return selected, nil
}

// LifecycleWindowParents reads only parent numbers first, then retrieves the
// original native observations for that bounded cohort at the same publication.
func (c *Client) LifecycleWindowParents(ctx context.Context, repository string, since, until time.Time, sequence int64, after int, expectedTag string) ([]LifecycleParent, string, error) {
	source, err := c.lifecycleRepository(ctx, repository)
	if err != nil {
		return nil, "", err
	}
	prefix, current, ok := strings.Cut(strings.TrimPrefix(source.pin, "pg-gitcrawl-current-v1:"), ":")
	published, err := positiveID(current)
	if !strings.HasPrefix(source.pin, "pg-gitcrawl-current-v1:") || !ok || prefix == "" || err != nil || sequence < 1 || sequence > published {
		return nil, "", errors.New("invalid completed lifecycle inventory pin")
	}
	source.pin = "pg-gitcrawl-current-v1:" + prefix + ":" + strconv.FormatInt(sequence, 10)
	digest := sha256.Sum256([]byte(source.pin))
	tag := hex.EncodeToString(digest[:8])
	if expectedTag != "" && tag != expectedTag {
		return nil, "", errors.New("lifecycle inventory binding changed")
	}
	args := map[string]any{"since": since.UTC().Format(time.RFC3339Nano), "until": until.UTC().Format(time.RFC3339Nano)}
	if after > 0 {
		args["after_number"] = strconv.Itoa(after)
	}
	page, err := c.lifecycleReader.Query(ctx, "gitcrawl", c.source, remote.QueryRequest{Name: "gitcrawl.lifecycle.candidates", Args: args, SnapshotID: source.pin, Limit: lifecycleWindowSize})
	if err != nil {
		return nil, "", err
	}
	if page.Stats.SnapshotID != source.pin || page.Stats.SchemaVersion != 1 || page.Stats.ObservationOrder != "completed-publication/thread-number" || !slices.Equal(page.Columns, []string{"number"}) || len(page.Rows) > lifecycleWindowSize || page.Cursor != "" || page.Stats.NextCursor != "" {
		return nil, "", errors.New("invalid lifecycle candidate page")
	}
	numbers := make([]int, 0, len(page.Rows))
	for _, row := range page.Rows {
		if len(row) != 1 {
			return nil, "", errors.New("invalid lifecycle candidate row")
		}
		value, _ := row[0].(string)
		number, err := strconv.Atoi(value)
		if err != nil || number <= after || strconv.Itoa(number) != value {
			return nil, "", errors.New("invalid lifecycle candidate order")
		}
		numbers = append(numbers, number)
		after = number
	}
	if len(numbers) == 0 {
		return nil, tag, nil
	}
	parents, err := c.lifecycleParents(ctx, source, numbers)
	return parents, tag, err
}
