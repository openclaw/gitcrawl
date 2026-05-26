package cli

import (
	"os"
	"strings"
)

type ghShimControls struct {
	Live        bool
	Cached      bool
	WebFallback bool
}

func parseGHShimControls(args []string) ([]string, ghShimControls) {
	controls := ghShimControls{
		Live:        envTruthy("GITCRAWL_GH_LIVE"),
		WebFallback: envTruthy("GITCRAWL_GH_WEB"),
	}
	out := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg {
		case "--live":
			controls.Live = true
		case "--cached":
			controls.Cached = true
		case "--web-fallback":
			controls.WebFallback = true
		default:
			out = append(out, arg)
		}
	}
	if controls.Cached {
		controls.Live = false
		controls.WebFallback = false
	}
	return out, controls
}

func envTruthy(name string) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false
	}
	switch strings.ToLower(value) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
