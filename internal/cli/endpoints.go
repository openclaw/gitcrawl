package cli

import (
	"os"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
)

func openAIBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("GITCRAWL_OPENAI_BASE_URL")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("OPENAI_BASE_URL"))
}

func embedBaseURL(cfg config.Config) string {
	if value := strings.TrimSpace(cfg.OpenAI.EmbedBaseURL); value != "" {
		return value
	}
	return openAIBaseURL()
}

func githubBaseURL() string {
	if value := strings.TrimSpace(os.Getenv("GITCRAWL_GITHUB_BASE_URL")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("GITHUB_BASE_URL"))
}
