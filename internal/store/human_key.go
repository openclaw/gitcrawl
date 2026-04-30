package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

var humanKeyWords = []string{
	"able", "acid", "acre", "actor", "acute", "admin", "aisle", "album",
	"alert", "alias", "amber", "angle", "apple", "apron", "array", "asset",
	"atlas", "audio", "badge", "basic", "batch", "beach", "beacon", "bench",
	"binary", "block", "bonus", "border", "branch", "bridge", "brief", "buffer",
	"build", "bundle", "cable", "cache", "canal", "canvas", "carbon", "cargo",
	"cedar", "center", "chance", "change", "charge", "chart", "cipher", "circle",
	"civic", "clear", "client", "cloud", "cobalt", "column", "comet", "common",
	"copper", "corner", "course", "credit", "crisp", "cycle", "daily", "data",
	"delta", "detail", "device", "domain", "draft", "drift", "driver", "early",
	"earth", "echo", "edge", "ember", "engine", "entry", "error", "event",
	"fabric", "factor", "field", "filter", "final", "focus", "forge", "format",
	"frame", "fresh", "future", "garden", "gentle", "glide", "golden", "graph",
	"grid", "group", "harbor", "header", "helix", "hidden", "hollow", "honest",
	"icon", "index", "input", "island", "kernel", "key", "keystone", "label",
	"lantern", "laser", "latest", "lattice", "layer", "ledger", "level", "light",
	"limit", "linear", "local", "logic", "major", "maple", "margin", "matrix",
	"meadow", "medium", "memory", "merge", "method", "mirror", "mobile", "module",
	"motion", "native", "needle", "noble", "normal", "notion", "nova", "number",
	"object", "ocean", "offset", "olive", "online", "option", "orbit", "origin",
	"output", "packet", "panel", "parcel", "patch", "pattern", "phase", "pillar",
	"pixel", "plain", "planet", "plume", "point", "portal", "prime", "profile",
	"prompt", "proper", "public", "pulse", "query", "quartz", "quiet", "radar",
	"range", "rapid", "record", "region", "relay", "render", "reply", "report",
	"result", "ripple", "river", "route", "sample", "schema", "screen", "script",
	"search", "second", "section", "secure", "select", "shadow", "signal", "silver",
	"simple", "single", "sketch", "socket", "solar", "source", "space", "span",
	"spiral", "spring", "stable", "static", "status", "steady", "stone", "stream",
	"strict", "studio", "subtle", "summit", "switch", "system", "table", "target",
	"thread", "timber", "token", "trace", "transit", "union", "update", "usage",
	"valid", "vector", "velvet", "vertex", "vessel", "view", "violet", "virtual",
	"vista", "visual", "volume", "wave", "window", "yellow", "zenith", "zero",
}

func clusterHumanName(repoID, representativeThreadID, clusterID int64) string {
	key := fmt.Sprintf("repo:%d:cluster:%d", repoID, clusterID)
	if representativeThreadID != 0 {
		key = fmt.Sprintf("repo:%d:cluster-representative:%d", repoID, representativeThreadID)
	}
	return HumanKeyForValue(key).Slug
}

type HumanKey struct {
	Hash     string
	Slug     string
	Checksum string
}

func StableHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func HumanKeyForValue(value string) HumanKey {
	return HumanKeyFromHash(StableHash(value))
}

func HumanKeyFromHash(hash string) HumanKey {
	normalized := strings.ToLower(hash)
	index := func(offset int) int {
		value, err := strconv.ParseUint(normalized[offset:offset+2], 16, 8)
		if err != nil {
			return 0
		}
		return int(value) % len(humanKeyWords)
	}
	checksumValue, err := strconv.ParseInt(normalized[6:12], 16, 64)
	checksum := "0000"
	if err == nil {
		checksum = strconv.FormatInt(checksumValue, 36)
		if len(checksum) < 4 {
			checksum = strings.Repeat("0", 4-len(checksum)) + checksum
		}
		if len(checksum) > 4 {
			checksum = checksum[len(checksum)-4:]
		}
	}
	return HumanKey{
		Hash:     normalized,
		Slug:     fmt.Sprintf("%s-%s-%s", humanKeyWords[index(0)], humanKeyWords[index(2)], humanKeyWords[index(4)]),
		Checksum: checksum,
	}
}

func HumanKeyStableSlug(key HumanKey) string {
	return key.Slug + "-" + key.Checksum
}
