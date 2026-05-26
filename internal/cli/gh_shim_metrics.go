package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ghXCacheCounters struct {
	LocalHits              int64                            `json:"local_hits"`
	FallbackHits           int64                            `json:"fallback_hits"`
	StaleHits              int64                            `json:"stale_hits"`
	LowBudgetStaleHits     int64                            `json:"low_budget_stale_hits,omitempty"`
	WebHits                int64                            `json:"web_hits,omitempty"`
	LiveBypasses           int64                            `json:"live_bypasses,omitempty"`
	BackendMisses          int64                            `json:"backend_misses"`
	PassThroughWrites      int64                            `json:"pass_through_writes"`
	BackendMissesByCommand map[string]int64                 `json:"backend_misses_by_command,omitempty"`
	BackendMissesByRoute   map[string]int64                 `json:"backend_misses_by_route,omitempty"`
	BackendMissesByKey     map[string]int64                 `json:"backend_misses_by_key,omitempty"`
	Hourly                 map[string]ghXCacheCounterBucket `json:"hourly,omitempty"`
}

type ghXCacheCounterBucket struct {
	StartedAt              time.Time        `json:"started_at"`
	LocalHits              int64            `json:"local_hits,omitempty"`
	FallbackHits           int64            `json:"fallback_hits,omitempty"`
	StaleHits              int64            `json:"stale_hits,omitempty"`
	LowBudgetStaleHits     int64            `json:"low_budget_stale_hits,omitempty"`
	WebHits                int64            `json:"web_hits,omitempty"`
	LiveBypasses           int64            `json:"live_bypasses,omitempty"`
	BackendMisses          int64            `json:"backend_misses,omitempty"`
	PassThroughWrites      int64            `json:"pass_through_writes,omitempty"`
	BackendMissesByCommand map[string]int64 `json:"backend_misses_by_command,omitempty"`
	BackendMissesByRoute   map[string]int64 `json:"backend_misses_by_route,omitempty"`
	BackendMissesByKey     map[string]int64 `json:"backend_misses_by_key,omitempty"`
}

func (a *App) ghXCacheCounters() (ghXCacheCounters, error) {
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return ghXCacheCounters{}, err
	}
	return readGHXCacheCounters(filepath.Join(dir, ghXCacheStatsFile)), nil
}

func (a *App) resetGHXCacheCounters() error {
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return err
	}
	return writeAtomicFile(filepath.Join(dir, ghXCacheStatsFile), []byte("{}"), 0o600)
}

func (a *App) incrementGHXCacheCounter(name string) error {
	return a.incrementGHXCacheCounterWithArgs(name, nil)
}

func (a *App) incrementGHXCacheBackendMiss(args []string) error {
	return a.incrementGHXCacheCounterWithArgs("backend_misses", args)
}

func (a *App) incrementGHXCacheCounterWithArgs(name string, args []string) error {
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return err
	}
	path := filepath.Join(dir, ghXCacheStatsFile)
	lockPath := path + ".lock"
	lock, locked := tryGHCommandCacheLock(lockPath)
	if !locked {
		return nil
	}
	defer func() {
		_ = lock.Close()
		_ = os.Remove(lockPath)
	}()
	stats := readGHXCacheCounters(path)
	if !incrementGHXCacheCounters(&stats, name, args) {
		return nil
	}
	bucketKey, bucketStart := ghXCacheCurrentBucket(time.Now())
	if stats.Hourly == nil {
		stats.Hourly = map[string]ghXCacheCounterBucket{}
	}
	bucket := stats.Hourly[bucketKey]
	if bucket.StartedAt.IsZero() {
		bucket.StartedAt = bucketStart
	}
	_ = incrementGHXCacheCounterBucket(&bucket, name, args)
	stats.Hourly[bucketKey] = bucket
	pruneGHXCacheBuckets(stats.Hourly, time.Now().Add(-7*24*time.Hour))
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	return writeAtomicFile(path, data, 0o600)
}

func incrementGHXCacheCounters(stats *ghXCacheCounters, name string, args []string) bool {
	switch name {
	case "local_hits":
		stats.LocalHits++
	case "fallback_hits":
		stats.FallbackHits++
	case "stale_hits":
		stats.StaleHits++
	case "low_budget_stale_hits":
		stats.LowBudgetStaleHits++
	case "web_hits":
		stats.WebHits++
	case "live_bypasses":
		stats.LiveBypasses++
	case "backend_misses":
		stats.BackendMisses++
		incrementGHXCacheMissMaps(&stats.BackendMissesByCommand, &stats.BackendMissesByRoute, &stats.BackendMissesByKey, args)
	case "pass_through_writes":
		stats.PassThroughWrites++
	default:
		return false
	}
	return true
}

func incrementGHXCacheCounterBucket(bucket *ghXCacheCounterBucket, name string, args []string) bool {
	switch name {
	case "local_hits":
		bucket.LocalHits++
	case "fallback_hits":
		bucket.FallbackHits++
	case "stale_hits":
		bucket.StaleHits++
	case "low_budget_stale_hits":
		bucket.LowBudgetStaleHits++
	case "web_hits":
		bucket.WebHits++
	case "live_bypasses":
		bucket.LiveBypasses++
	case "backend_misses":
		bucket.BackendMisses++
		incrementGHXCacheMissMaps(&bucket.BackendMissesByCommand, &bucket.BackendMissesByRoute, &bucket.BackendMissesByKey, args)
	case "pass_through_writes":
		bucket.PassThroughWrites++
	default:
		return false
	}
	return true
}

func incrementGHXCacheMissMaps(byCommand, byRoute, byKey *map[string]int64, args []string) {
	if len(args) == 0 {
		return
	}
	if *byCommand == nil {
		*byCommand = map[string]int64{}
	}
	(*byCommand)[ghCommandName(args)]++
	if route := ghCommandRoute(args); route != "" {
		if *byRoute == nil {
			*byRoute = map[string]int64{}
		}
		(*byRoute)[route]++
	}
	if key := ghCommandMissKey(args); key != "" {
		if *byKey == nil {
			*byKey = map[string]int64{}
		}
		(*byKey)[key]++
	}
}

func ghCommandMissKey(args []string) string {
	if len(args) == 0 {
		return ""
	}
	canonical := canonicalGHCommandArgs(args)
	if len(canonical) == 0 {
		return ghCommandName(args)
	}
	key := strings.Join(canonical, " ")
	if len(key) > 180 {
		key = key[:177] + "..."
	}
	return key
}

func ghCommandRoute(args []string) string {
	if len(args) == 0 {
		return ""
	}
	if args[0] == "api" {
		return normalizeGHAPIRoute(args[1:])
	}
	if len(args) >= 2 {
		return ghCommandName(args)
	}
	return args[0]
}

func readGHXCacheCounters(path string) ghXCacheCounters {
	data, err := os.ReadFile(path)
	if err != nil {
		return ghXCacheCounters{}
	}
	var stats ghXCacheCounters
	if err := json.Unmarshal(data, &stats); err != nil {
		return ghXCacheCounters{}
	}
	return stats
}

func (c ghXCacheCounters) since(since time.Duration, now time.Time) ghXCacheCounters {
	if since <= 0 {
		return c
	}
	cutoff := now.Add(-since)
	var out ghXCacheCounters
	for _, bucket := range c.Hourly {
		if bucket.StartedAt.IsZero() || bucket.StartedAt.Before(cutoff) {
			continue
		}
		out.LocalHits += bucket.LocalHits
		out.FallbackHits += bucket.FallbackHits
		out.StaleHits += bucket.StaleHits
		out.LowBudgetStaleHits += bucket.LowBudgetStaleHits
		out.WebHits += bucket.WebHits
		out.LiveBypasses += bucket.LiveBypasses
		out.BackendMisses += bucket.BackendMisses
		out.PassThroughWrites += bucket.PassThroughWrites
		mergeCounterMap(&out.BackendMissesByCommand, bucket.BackendMissesByCommand)
		mergeCounterMap(&out.BackendMissesByRoute, bucket.BackendMissesByRoute)
		mergeCounterMap(&out.BackendMissesByKey, bucket.BackendMissesByKey)
	}
	return out
}

func mergeCounterMap(dst *map[string]int64, src map[string]int64) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = map[string]int64{}
	}
	for key, value := range src {
		(*dst)[key] += value
	}
}

func ghXCacheCurrentBucket(now time.Time) (string, time.Time) {
	start := now.UTC().Truncate(time.Hour)
	return start.Format("2006-01-02T15:00:00Z"), start
}

func pruneGHXCacheBuckets(buckets map[string]ghXCacheCounterBucket, cutoff time.Time) {
	for key, bucket := range buckets {
		if !bucket.StartedAt.IsZero() && bucket.StartedAt.Before(cutoff) {
			delete(buckets, key)
		}
	}
}

func writeAtomicFile(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Chmod(perm); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func staleGHCommandCacheLock(info os.FileInfo) bool {
	return time.Since(info.ModTime()) > 2*time.Minute
}
